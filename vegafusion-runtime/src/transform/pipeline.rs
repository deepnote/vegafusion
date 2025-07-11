use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use itertools::Itertools;
use std::collections::HashMap;

use vegafusion_core::data::util::{DataFrameUtils, SessionContextUtils};
use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_expr::expr;
use vegafusion_common::column::flat_col;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::tasks::{Variable, VariableNamespace};
use vegafusion_core::proto::gen::transforms::TransformPipeline;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::TransformDependencies;

#[async_trait]
pub trait TransformPipelineUtils {
    async fn eval_sql(
        &self,
        dataframe: DataFrame,
        config: &CompilationConfig,
        ctx: &SessionContext,
    ) -> Result<(VegaFusionTable, Vec<TaskValue>)>;
    
    async fn eval_to_df(
        &self,
        dataframe: DataFrame,
        config: &CompilationConfig,
        ctx: &SessionContext,
    ) -> Result<(DataFrame, Vec<TaskValue>)>;
}

#[async_trait]
impl TransformPipelineUtils for TransformPipeline {
    async fn eval_sql(
        &self,
        dataframe: DataFrame,
        config: &CompilationConfig,
        ctx: &SessionContext,
    ) -> Result<(VegaFusionTable, Vec<TaskValue>)> {
        let (result_df, signals) = build_dataframe(self, dataframe, config, ctx).await?;
        let table = result_df.collect_to_table().await?;
        Ok((table, signals))
    }
    
    async fn eval_to_df(
        &self,
        dataframe: DataFrame,
        config: &CompilationConfig,
        ctx: &SessionContext,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let (result_df, signals) = build_dataframe(self, dataframe, config, ctx).await?;
        Ok((result_df, signals))
    }
}

/// Common logic for building the final DataFrame after applying all transforms
async fn build_dataframe(
    pipeline: &TransformPipeline,
    sql_df: DataFrame,
    config: &CompilationConfig,
    ctx: &SessionContext,
) -> Result<(DataFrame, Vec<TaskValue>)> {
    let mut result_sql_df = sql_df;
    let mut result_outputs: HashMap<Variable, TaskValue> = Default::default();
    let mut config = config.clone();

    if result_sql_df
        .schema()
        .inner()
        .column_with_name(ORDER_COL)
        .is_none()
    {
        return Err(VegaFusionError::internal(format!(
            "DataFrame input to eval_sql does not have the expected {ORDER_COL} ordering column"
        )));
    }

    for tx in pipeline.transforms.iter() {
        // Append transform and update result df
        let tx_result = tx.eval(result_sql_df.clone(), &config).await?;

        result_sql_df = tx_result.0;

        if result_sql_df
            .schema()
            .inner()
            .column_with_name(ORDER_COL)
            .is_none()
        {
            return Err(VegaFusionError::internal(
                format!("DataFrame output of transform does not have the expected {ORDER_COL} ordering column: {tx:?}")
            ));
        }

        // Collect output variables
        for (var, val) in tx.output_vars().iter().zip(tx_result.1) {
            result_outputs.insert(var.clone(), val.clone());

            // Also add output signals to config scope so they are available to the following
            // transforms
            match var.ns() {
                VariableNamespace::Signal => {
                    config
                        .signal_scope
                        .insert(var.name.clone(), val.as_scalar()?.clone());
                }
                VariableNamespace::Data => {
                    match val {
                        TaskValue::DataFrame(df) => {
                            config
                                .data_scope
                                .insert(var.name.clone(), df.clone());
                        },
                        TaskValue::Table(table) => {
                            let df = ctx.vegafusion_table(table.clone()).await?;
                            config
                                .data_scope
                                .insert(var.name.clone(), df);
                        },
                        _ => unimplemented!()
                    }
                    
                }
                VariableNamespace::Scale => {
                    unimplemented!()
                }
            }
        }
    }

    // Sort by ordering column at the end
    result_sql_df = result_sql_df.sort(vec![expr::Sort {
        expr: flat_col(ORDER_COL),
        asc: true,
        nulls_first: false,
    }])?;

    // Sort result signal value by signal name
    let (_, signals_values): (Vec<_>, Vec<_>) = result_outputs
        .into_iter()
        .sorted_by_key(|(k, _v)| k.clone())
        .unzip();

    Ok((result_sql_df, signals_values))
}
