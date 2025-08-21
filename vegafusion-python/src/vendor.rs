use std::sync::Arc;

use crate::executor::python_object_to_executor;
use async_trait::async_trait;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use tokio::sync::Mutex;
use vegafusion_common::{data::table::VegaFusionTable, datafusion_expr::LogicalPlan};
use vegafusion_core::runtime::PlanExecutor;
use vegafusion_runtime::sql::logical_plan_to_spark_sql;

pub struct SparkSqlPlanExecutor {
    python_executor: PyObject,
    mutex: Arc<Mutex<()>>,
}

impl SparkSqlPlanExecutor {
    pub fn new(python_executor: PyObject) -> Self {
        Self {
            python_executor,
            mutex: Arc::new(Mutex::new(())),
        }
    }
}

pub fn select_executor_for_vendor(
    vendor: Option<String>,
    executor: Option<PyObject>,
) -> PyResult<Option<Arc<dyn PlanExecutor>>> {
    match vendor.as_deref() {
        Some("sparksql") => {
            let py_exec = executor.ok_or_else(|| {
                PyValueError::new_err(
                    "'executor' is required for vendor='sparksql' and must be callable or have execute_plan",
                )
            })?;

            Python::with_gil(|py| -> PyResult<()> {
                let obj_ref = py_exec.bind(py);
                if obj_ref.is_callable() || obj_ref.hasattr("execute_plan")? {
                    Ok(())
                } else {
                    Err(PyValueError::new_err(
                        "Executor must be callable or have an execute_plan method",
                    ))
                }
            })?;

            Ok(Some(Arc::new(SparkSqlPlanExecutor::new(py_exec))))
        }
        Some("datafusion") | Some("") | None => {
            // Fall back to default DataFusion if no executor; otherwise wrap Python executor
            python_object_to_executor(executor)
        }
        Some(other) => Err(PyValueError::new_err(format!(
            "Unsupported vendor: '{}'. Supported vendors: 'datafusion', 'sparksql'",
            other
        ))),
    }
}

#[async_trait]
impl PlanExecutor for SparkSqlPlanExecutor {
    async fn execute_plan(
        &self,
        plan: LogicalPlan,
    ) -> vegafusion_common::error::Result<VegaFusionTable> {
        // Acquire mutex lock to ensure only one request runs at a time
        let _lock = self.mutex.lock().await;

        // Convert logical plan to SparkSQL
        let spark_sql = logical_plan_to_spark_sql(&plan)?;

        let python_executor = &self.python_executor;
        let result = tokio::task::spawn_blocking({
            let python_executor = Python::with_gil(|py| python_executor.clone_ref(py));
            let spark_sql = spark_sql.clone();

            move || {
                Python::with_gil(|py| -> PyResult<VegaFusionTable> {
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
            Ok(Ok(table)) => Ok(table),
            Ok(Err(py_err)) => Err(vegafusion_common::error::VegaFusionError::internal(
                format!("Python executor error: {}", py_err),
            )),
            Err(join_err) => Err(vegafusion_common::error::VegaFusionError::internal(
                format!("Failed to execute Python executor: {}", join_err),
            )),
        }
    }
}
