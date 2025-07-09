use crate::data::tasks::build_compilation_config;
use crate::expression::compiler::compile;
use crate::expression::compiler::utils::ExprHelpers;
use crate::task_graph::task::TaskCall;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use vegafusion_core::data::dataset::VegaFusionDataset;

use crate::task_graph::timezone::RuntimeTzConfig;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::tasks::SignalTask;
use vegafusion_core::task_graph::task::TaskDependencies;
use vegafusion_core::task_graph::task_value::{TaskValue, TaskPlan};
use datafusion_common::ScalarValue;

async fn eval_signal_to_scalar_value(
    signal_task: &SignalTask,
    values: &[TaskValue],
    tz_config: &Option<RuntimeTzConfig>,
) -> Result<ScalarValue> {
    let config = build_compilation_config(&signal_task.input_vars(), values, tz_config);
    let expression = signal_task.expr.as_ref().unwrap();
    let expr = compile(expression, &config, None)?;
    expr.eval_to_scalar()
}

#[async_trait]
impl TaskCall for SignalTask {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        _ctx: Arc<SessionContext>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        let value = eval_signal_to_scalar_value(self, values, tz_config).await?;
        let task_value = TaskValue::Scalar(value);
        Ok((task_value, Default::default()))
    }
    
    async fn plan(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        _ctx: Arc<SessionContext>,
    ) -> Result<(TaskPlan, Vec<TaskValue>)> {
        let value = eval_signal_to_scalar_value(self, values, tz_config).await?;
        let task_plan = TaskPlan::Scalar(value);
        Ok((task_plan, Default::default()))
    }
}
