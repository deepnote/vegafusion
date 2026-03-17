use std::sync::Arc;

use async_trait::async_trait;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use tokio::sync::Mutex;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::runtime::{PlanResolver, ResolutionResult};
use vegafusion_runtime::sql::logical_plan_to_spark_sql;

pub struct SparkSqlPlanResolver {
    python_executor: Py<PyAny>,
    mutex: Arc<Mutex<()>>,
}

impl SparkSqlPlanResolver {
    pub fn new(python_executor: Py<PyAny>) -> Self {
        Self {
            python_executor,
            mutex: Arc::new(Mutex::new(())),
        }
    }
}

/// Select the appropriate plan resolver(s) based on the vendor string.
pub fn select_resolvers_for_vendor(
    vendor: Option<String>,
    executor: Option<Py<PyAny>>,
) -> PyResult<Vec<Arc<dyn PlanResolver>>> {
    match vendor.as_deref() {
        Some("sparksql") => {
            let py_exec = executor.ok_or_else(|| {
                PyValueError::new_err(
                    "'executor' is required for vendor='sparksql' and must be callable or have execute_plan method",
                )
            })?;

            Python::attach(|py| -> PyResult<()> {
                let obj_ref = py_exec.bind(py);
                if obj_ref.is_callable() || obj_ref.hasattr("execute_plan")? {
                    Ok(())
                } else {
                    Err(PyValueError::new_err(
                        "Executor must be callable or have an execute_plan method",
                    ))
                }
            })?;

            Ok(vec![Arc::new(SparkSqlPlanResolver::new(py_exec))])
        }
        Some("datafusion") | Some("") | None => {
            if executor.is_some() {
                return Err(PyValueError::new_err(
                    "Custom executors are not supported for the default DataFusion runtime. Remove executor parameter or use different vendor.",
                ));
            }
            Ok(vec![])
        }
        Some(other) => Err(PyValueError::new_err(format!(
            "Unsupported vendor: '{}'. Supported vendors: 'datafusion', 'sparksql'",
            other
        ))),
    }
}

#[async_trait]
impl PlanResolver for SparkSqlPlanResolver {
    fn name(&self) -> &str {
        "SparkSqlPlanResolver"
    }

    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        let _lock = self.mutex.lock().await;

        let spark_sql = logical_plan_to_spark_sql(&plan)?;

        let python_executor = &self.python_executor;
        let result = tokio::task::spawn_blocking({
            let python_executor = Python::attach(|py| python_executor.clone_ref(py));
            let spark_sql = spark_sql.clone();

            move || {
                Python::attach(|py| -> PyResult<VegaFusionTable> {
                    let sql_py = PyString::new(py, &spark_sql);

                    let table_result = if python_executor.bind(py).is_callable() {
                        python_executor.call1(py, (sql_py,))
                    } else if python_executor.bind(py).hasattr("execute_plan")? {
                        let execute_plan_method =
                            python_executor.bind(py).getattr("execute_plan")?;
                        execute_plan_method
                            .call1((sql_py,))
                            .map(|result| result.into())
                    } else {
                        return Err(PyValueError::new_err(
                            "Executor must be callable or have an execute_plan method",
                        ));
                    }?;

                    VegaFusionTable::from_pyarrow(py, &table_result.bind(py))
                })
            }
        })
        .await;

        match result {
            Ok(Ok(table)) => Ok(ResolutionResult::Table(table)),
            Ok(Err(py_err)) => Err(VegaFusionError::internal(format!(
                "Python executor error: {}",
                py_err
            ))),
            Err(join_err) => Err(VegaFusionError::internal(format!(
                "Failed to execute Python executor: {}",
                join_err
            ))),
        }
    }
}
