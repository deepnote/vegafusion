use std::sync::Arc;

use async_trait::async_trait;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use vegafusion_common::{data::table::VegaFusionTable, datafusion_expr::LogicalPlan};
use vegafusion_core::runtime::PlanExecutor;

pub struct PythonPlanExecutor {
    python_executor: PyObject,
}

impl PythonPlanExecutor {
    fn new(python_executor: PyObject) -> Self {
        Self { python_executor }
    }
}

#[async_trait]
impl PlanExecutor for PythonPlanExecutor {
    async fn execute_plan(
        &self,
        plan: LogicalPlan,
    ) -> vegafusion_common::error::Result<VegaFusionTable> {
        let plan_str = plan.display_pg_json().to_string();

        let python_executor = &self.python_executor;
        let result = tokio::task::spawn_blocking({
            let python_executor = Python::with_gil(|py| python_executor.clone_ref(py));
            let plan_str = plan_str.clone();

            move || {
                Python::with_gil(|py| -> PyResult<VegaFusionTable> {
                    let plan_py = PyString::new(py, &plan_str);

                    let table_result = if python_executor.bind(py).is_callable() {
                        python_executor.call1(py, (plan_py,))
                    } else if python_executor.bind(py).hasattr("execute_plan")? {
                        let execute_plan_method =
                            python_executor.bind(py).getattr("execute_plan")?;
                        execute_plan_method
                            .call1((plan_py,))
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

/// Helper function to convert a Python object to a PlanExecutor
/// Accepts either:
/// - A callable that takes a logical plan string and returns an Arrow table
/// - An object with execute_plan method that has the same signature
pub fn python_object_to_executor(
    python_obj: Option<PyObject>,
) -> PyResult<Option<Arc<dyn PlanExecutor>>> {
    match python_obj {
        Some(obj) => {
            Python::with_gil(|py| -> PyResult<Option<Arc<dyn PlanExecutor>>> {
                let obj_ref = obj.bind(py);

                // Validate that the object is either callable or has execute_plan method
                if obj_ref.is_callable() || obj_ref.hasattr("execute_plan")? {
                    Ok(Some(Arc::new(PythonPlanExecutor::new(obj))))
                } else {
                    Err(PyValueError::new_err(
                        "Executor must be callable or have an execute_plan method",
                    ))
                }
            })
        }
        None => Ok(None),
    }
}
