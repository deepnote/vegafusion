mod chart_state;
mod executor;
mod utils;
mod vendor;

use lazy_static::lazy_static;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use std::str::FromStr;
use std::sync::{Arc, Once};
use tokio::runtime::Runtime;
use tonic::transport::{Channel, Uri};

use vegafusion_core::error::{ToExternalError, VegaFusionError};
use vegafusion_core::proto::gen::pretransform::pre_transform_extract_warning::WarningType as ExtractWarningType;
use vegafusion_core::proto::gen::pretransform::pre_transform_logical_plan_warning::WarningType as LogicalPlanWarningType;
use vegafusion_core::proto::gen::pretransform::pre_transform_values_warning::WarningType as ValueWarningType;
use vegafusion_core::proto::gen::pretransform::{
    PreTransformExtractOpts, PreTransformLogicalPlanOpts, PreTransformSpecOpts,
    PreTransformValuesOpts, PreTransformVariable,
};
use vegafusion_core::proto::gen::tasks::{TzConfig, Variable};
use vegafusion_runtime::task_graph::GrpcVegaFusionRuntime;

use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

use env_logger::{Builder, Target};
use serde_json::json;
use vegafusion_core::planning::plan::{PlannerConfig, PreTransformSpecWarningSpec, SpecPlan};
use vegafusion_core::planning::projection_pushdown::get_column_usage as rs_get_column_usage;
use vegafusion_core::planning::watch::WatchPlan;

use vegafusion_core::task_graph::graph::ScopedVariable;
use vegafusion_core::task_graph::task_value::{MaterializedTaskValue, TaskValue};
use vegafusion_runtime::tokio_runtime::TOKIO_THREAD_STACK_SIZE;

use vegafusion_core::runtime::{PlanExecutor, VegaFusionRuntimeTrait};
use vegafusion_runtime::task_graph::cache::VegaFusionCache;

use vegafusion_common::data::scalar::ScalarValueHelpers;

use crate::chart_state::PyChartState;
use crate::utils::{parse_json_spec, process_inline_datasets};
use crate::vendor::select_executor_for_vendor;

static INIT: Once = Once::new();

pub fn initialize_logging() {
    INIT.call_once(|| {
        // Init logger to write to stdout
        let mut builder = Builder::from_default_env();
        builder.target(Target::Stdout);
        builder.init();
    });
}

lazy_static! {
    static ref SYSTEM_INSTANCE: sysinfo::System = sysinfo::System::new_all();
}

#[pyclass]
struct PyVegaFusionRuntime {
    runtime: Arc<dyn VegaFusionRuntimeTrait>,
    tokio_runtime: Arc<Runtime>,
}

impl PyVegaFusionRuntime {
    fn build_with_executor(
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
        rust_executor: Option<Arc<dyn PlanExecutor>>,
    ) -> PyResult<Self> {
        initialize_logging();

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads) = worker_threads {
            builder.worker_threads(worker_threads.max(1) as usize);
        }

        let tokio_runtime_connection = builder
            .enable_all()
            .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
            .build()
            .external("Failed to create Tokio thread pool")?;

        Ok(Self {
            runtime: Arc::new(VegaFusionRuntime::new(
                Some(VegaFusionCache::new(max_capacity, memory_limit)),
                rust_executor,
            )),
            tokio_runtime: Arc::new(tokio_runtime_connection),
        })
    }
}

#[pymethods]
impl PyVegaFusionRuntime {
    #[staticmethod]
    #[pyo3(signature = (max_capacity=None, memory_limit=None, worker_threads=None, executor=None))]
    pub fn new_embedded(
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
        executor: Option<PyObject>,
    ) -> PyResult<Self> {
        let rust_executor = select_executor_for_vendor(None, executor)?;
        Self::build_with_executor(max_capacity, memory_limit, worker_threads, rust_executor)
    }

    #[staticmethod]
    #[pyo3(signature = (max_capacity=None, memory_limit=None, worker_threads=None, vendor=None, executor=None))]
    pub fn new_embedded_vendor(
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
        vendor: Option<String>,
        executor: Option<PyObject>,
    ) -> PyResult<Self> {
        let rust_executor = select_executor_for_vendor(vendor, executor)?;
        Self::build_with_executor(max_capacity, memory_limit, worker_threads, rust_executor)
    }

    #[staticmethod]
    pub fn new_grpc(url: &str) -> PyResult<Self> {
        let tokio_runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        );
        let runtime = tokio_runtime.block_on(async move {
            let innter_url = url;
            let uri =
                Uri::from_str(innter_url).map_err(|e| VegaFusionError::internal(e.to_string()))?;

            GrpcVegaFusionRuntime::try_new(Channel::builder(uri).connect().await.map_err(|e| {
                let msg = format!("Error connecting to gRPC server at {}: {}", innter_url, e);
                VegaFusionError::internal(msg)
            })?)
            .await
        })?;

        Ok(Self {
            runtime: Arc::new(runtime),
            tokio_runtime: tokio_runtime.clone(),
        })
    }

    #[pyo3(signature = (spec, local_tz, default_input_tz=None, row_limit=None, inline_datasets=None))]
    pub fn new_chart_state(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        inline_datasets: Option<&Bound<PyDict>>,
    ) -> PyResult<PyChartState> {
        let spec = parse_json_spec(spec)?;
        let tz_config = TzConfig {
            local_tz: local_tz.to_string(),
            default_input_tz: default_input_tz.clone(),
        };

        let inline_datasets = process_inline_datasets(inline_datasets)?;

        py.allow_threads(|| {
            PyChartState::try_new(
                self.runtime.clone(),
                self.tokio_runtime.clone(),
                spec,
                inline_datasets,
                tz_config,
                row_limit,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (spec, local_tz, default_input_tz=None, row_limit=None, preserve_interactivity=None, inline_datasets=None, keep_signals=None, keep_datasets=None))]
    pub fn pre_transform_spec(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        preserve_interactivity: Option<bool>,
        inline_datasets: Option<&Bound<PyDict>>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(PyObject, PyObject)> {
        let inline_datasets = process_inline_datasets(inline_datasets)?;

        let spec = parse_json_spec(spec)?;
        let preserve_interactivity = preserve_interactivity.unwrap_or(false);

        // Build keep_variables
        let mut keep_variables: Vec<ScopedVariable> = Vec::new();
        for (name, scope) in keep_signals.unwrap_or_default() {
            keep_variables.push((Variable::new_signal(&name), scope))
        }
        for (name, scope) in keep_datasets.unwrap_or_default() {
            keep_variables.push((Variable::new_data(&name), scope))
        }

        let (spec, warnings) = py.allow_threads(|| {
            self.tokio_runtime.block_on(
                self.runtime.pre_transform_spec(
                    &spec,
                    &inline_datasets,
                    &PreTransformSpecOpts {
                        local_tz,
                        default_input_tz,
                        row_limit,
                        preserve_interactivity,
                        keep_variables: keep_variables
                            .into_iter()
                            .map(|v| PreTransformVariable {
                                variable: Some(v.0),
                                scope: v.1,
                            })
                            .collect(),
                    },
                ),
            )
        })?;

        let warnings: Vec<_> = warnings
            .iter()
            .map(PreTransformSpecWarningSpec::from)
            .collect();

        Python::with_gil(|py| -> PyResult<(PyObject, PyObject)> {
            let py_spec = pythonize::pythonize(py, &spec)?;
            let py_warnings = pythonize::pythonize(py, &warnings)?;
            Ok((py_spec.into(), py_warnings.into()))
        })
    }

    #[pyo3(signature = (spec, variables, local_tz, default_input_tz=None, row_limit=None, inline_datasets=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn pre_transform_datasets(
        &self,
        py: Python,
        spec: PyObject,
        variables: Vec<(String, Vec<u32>)>,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        inline_datasets: Option<&Bound<PyDict>>,
    ) -> PyResult<(PyObject, PyObject)> {
        let inline_datasets = process_inline_datasets(inline_datasets)?;
        let spec = parse_json_spec(spec)?;

        // Build variables
        let variables: Vec<ScopedVariable> = variables
            .iter()
            .map(|input_var| {
                let var = Variable::new_data(&input_var.0);
                let scope = input_var.1.clone();
                (var, scope)
            })
            .collect();

        let (values, warnings) = py.allow_threads(|| {
            self.tokio_runtime
                .block_on(self.runtime.pre_transform_values(
                    &spec,
                    &variables,
                    &inline_datasets,
                    &PreTransformValuesOpts {
                        local_tz,
                        default_input_tz,
                        row_limit,
                    },
                ))
        })?;

        let warnings: Vec<_> = warnings
            .iter()
            .map(|warning| match warning.warning_type.as_ref().unwrap() {
                ValueWarningType::Planner(planner_warning) => PreTransformSpecWarningSpec {
                    typ: "Planner".to_string(),
                    message: planner_warning.message.clone(),
                },
                ValueWarningType::RowLimit(_) => PreTransformSpecWarningSpec {
                    typ: "RowLimitExceeded".to_string(),
                    message: "Some datasets in resulting Vega specification have been truncated to the provided row limit".to_string()
                }
            })
            .collect();

        Python::with_gil(|py| -> PyResult<(PyObject, PyObject)> {
            let py_response_list = PyList::empty(py);
            for value in values {
                let pytable: PyObject = if let MaterializedTaskValue::Table(table) = value {
                    table.to_pyo3_arrow()?.to_pyarrow(py)?.into()
                } else {
                    return Err(PyErr::from(VegaFusionError::internal(
                        "Unexpected value type",
                    )));
                };
                py_response_list.append(pytable)?;
            }

            let py_warnings = pythonize::pythonize(py, &warnings)?;
            Ok((py_response_list.into(), py_warnings.into()))
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (spec, local_tz, default_input_tz=None, preserve_interactivity=None, extract_threshold=None, extracted_format=None, inline_datasets=None, keep_signals=None, keep_datasets=None))]
    pub fn pre_transform_extract(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        preserve_interactivity: Option<bool>,
        extract_threshold: Option<usize>,
        extracted_format: Option<String>,
        inline_datasets: Option<&Bound<PyDict>>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(PyObject, Vec<PyObject>, PyObject)> {
        let inline_datasets = process_inline_datasets(inline_datasets)?;
        let spec = parse_json_spec(spec)?;
        let preserve_interactivity = preserve_interactivity.unwrap_or(true);
        let extract_threshold = extract_threshold.unwrap_or(20);
        let extracted_format = extracted_format.unwrap_or_else(|| "pyarrow".to_string());

        // Build keep_variables
        let mut keep_variables: Vec<PreTransformVariable> = Vec::new();
        for (name, scope) in keep_signals.unwrap_or_default() {
            keep_variables.push(PreTransformVariable {
                variable: Some(Variable::new_signal(&name)),
                scope,
            });
        }
        for (name, scope) in keep_datasets.unwrap_or_default() {
            keep_variables.push(PreTransformVariable {
                variable: Some(Variable::new_data(&name)),
                scope,
            });
        }

        let (tx_spec, datasets, warnings) = py.allow_threads(|| {
            self.tokio_runtime
                .block_on(self.runtime.pre_transform_extract(
                    &spec,
                    &inline_datasets,
                    &PreTransformExtractOpts {
                        local_tz,
                        default_input_tz,
                        preserve_interactivity,
                        extract_threshold: extract_threshold as i32,
                        keep_variables,
                    },
                ))
        })?;

        let warnings: Vec<_> = warnings
            .iter()
            .map(|warning| match warning.warning_type.as_ref().unwrap() {
                ExtractWarningType::Planner(planner_warning) => PreTransformSpecWarningSpec {
                    typ: "Planner".to_string(),
                    message: planner_warning.message.clone(),
                },
            })
            .collect();

        Python::with_gil(|py| {
            let tx_spec = pythonize::pythonize(py, &tx_spec)?;

            let datasets = datasets
                .into_iter()
                .map(|tbl| {
                    let name: PyObject = tbl.name.into_pyobject(py)?.into();
                    let scope: PyObject = tbl.scope.into_pyobject(py)?.into();
                    let table = match extracted_format.as_str() {
                        "arro3" => {
                            let pytable = tbl.table.to_pyo3_arrow()?;
                            pytable.to_pyarrow(py)?.into()
                        }
                        "pyarrow" => tbl.table.to_pyo3_arrow()?.to_pyarrow(py)?.into(),
                        "arrow-ipc" => {
                            PyBytes::new(py, tbl.table.to_ipc_bytes()?.as_slice()).into()
                        }
                        "arrow-ipc-base64" => tbl.table.to_ipc_base64()?.into_pyobject(py)?.into(),
                        _ => {
                            return Err(PyValueError::new_err(format!(
                                "Invalid extracted_format: {}",
                                extracted_format
                            )))
                        }
                    };

                    let dataset: PyObject = PyTuple::new(py, &[name, scope, table])?.into();
                    Ok(dataset)
                })
                .collect::<PyResult<Vec<_>>>()?;

            let warnings = pythonize::pythonize(py, &warnings)?;

            Ok((tx_spec.into(), datasets, warnings.into()))
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (spec, local_tz, default_input_tz=None, preserve_interactivity=None, inline_datasets=None, keep_signals=None, keep_datasets=None))]
    pub fn pre_transform_logical_plan(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        preserve_interactivity: Option<bool>,
        inline_datasets: Option<&Bound<PyDict>>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(PyObject, PyObject, PyObject)> {
        let spec = parse_json_spec(spec)?;
        let preserve_interactivity = preserve_interactivity.unwrap_or(true);

        let inline_datasets = process_inline_datasets(inline_datasets)?;

        let mut keep_variables: Vec<PreTransformVariable> = Vec::new();
        for (name, scope) in keep_signals.unwrap_or_default() {
            keep_variables.push(PreTransformVariable {
                variable: Some(Variable::new_signal(&name)),
                scope,
            });
        }
        for (name, scope) in keep_datasets.unwrap_or_default() {
            keep_variables.push(PreTransformVariable {
                variable: Some(Variable::new_data(&name)),
                scope,
            });
        }

        let (client_spec, export_updates, warnings) = py.allow_threads(|| {
            self.tokio_runtime
                .block_on(self.runtime.pre_transform_logical_plan(
                    &spec,
                    inline_datasets,
                    &PreTransformLogicalPlanOpts {
                        local_tz,
                        default_input_tz,
                        preserve_interactivity,
                        keep_variables,
                    },
                ))
        })?;

        Python::with_gil(|py| -> PyResult<(PyObject, PyObject, PyObject)> {
            let py_spec = pythonize::pythonize(py, &client_spec)?;

            let py_export_list = PyList::empty(py);
            for export_update in export_updates {
                let py_export_dict = PyDict::new(py);
                py_export_dict.set_item(
                    "namespace",
                    pythonize::pythonize(py, &export_update.namespace)?,
                )?;
                py_export_dict.set_item("name", export_update.name)?;

                match export_update.value {
                    TaskValue::Plan(plan) => {
                        // TODO: we probably want more flexible serialization format than pg_json, but protobuf
                        // fails with our memtable (possibly fixed in DataFusion 49)
                        let lp_str = plan.display_pg_json().to_string();
                        py_export_dict.set_item("logical_plan", PyString::new(py, &lp_str))?;
                    }
                    TaskValue::Table(table) => {
                        // Convert table to PyArrow
                        let pytable = table.to_pyo3_arrow()?.to_pyarrow(py)?;
                        py_export_dict.set_item("data", pytable)?;
                    }
                    TaskValue::Scalar(scalar) => {
                        // Convert scalar to JSON value
                        let json_value = scalar.to_json()?;
                        let py_json_value = pythonize::pythonize(py, &json_value)?;
                        py_export_dict.set_item("data", py_json_value)?;
                    }
                }

                py_export_list.append(py_export_dict)?;
            }

            let warnings: Vec<_> = warnings
                .iter()
                .map(|warning| match warning.warning_type.as_ref().unwrap() {
                    LogicalPlanWarningType::Planner(planner_warning) => {
                        PreTransformSpecWarningSpec {
                            typ: "Planner".to_string(),
                            message: planner_warning.message.clone(),
                        }
                    }
                })
                .collect();

            let py_warnings = pythonize::pythonize(py, &warnings)?;

            Ok((py_spec.into(), py_export_list.into(), py_warnings.into()))
        })
    }

    pub fn clear_cache(&self) -> PyResult<()> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            self.tokio_runtime.block_on(runtime.clear_cache());
            Ok(())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support clear_cache",
            ))
        }
    }

    pub fn size(&self) -> PyResult<usize> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            Ok(runtime.cache.size())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support size",
            ))
        }
    }

    pub fn total_memory(&self) -> PyResult<usize> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            Ok(runtime.cache.total_memory())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support total_memory",
            ))
        }
    }

    pub fn protected_memory(&self) -> PyResult<usize> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            Ok(runtime.cache.protected_memory())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support protected_memory",
            ))
        }
    }

    pub fn probationary_memory(&self) -> PyResult<usize> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            Ok(runtime.cache.probationary_memory())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support probationary_memory",
            ))
        }
    }
}

#[pyfunction]
#[pyo3(signature = ())]
pub fn get_virtual_memory() -> u64 {
    SYSTEM_INSTANCE.total_memory()
}

#[pyfunction]
#[pyo3(signature = ())]
pub fn get_cpu_count() -> u64 {
    SYSTEM_INSTANCE.cpus().len() as u64
}

#[pyfunction]
#[pyo3(signature = (spec))]
pub fn get_column_usage(py: Python, spec: PyObject) -> PyResult<PyObject> {
    let spec = parse_json_spec(spec)?;
    let usage = rs_get_column_usage(&spec)?;
    Ok(pythonize::pythonize(py, &usage)?.into())
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (spec, preserve_interactivity=None, keep_signals=None, keep_datasets=None))]
pub fn build_pre_transform_spec_plan(
    spec: PyObject,
    preserve_interactivity: Option<bool>,
    keep_signals: Option<Vec<(String, Vec<u32>)>>,
    keep_datasets: Option<Vec<(String, Vec<u32>)>>,
) -> PyResult<PyObject> {
    let spec = parse_json_spec(spec)?;
    let preserve_interactivity = preserve_interactivity.unwrap_or(false);

    // Build keep_variables
    let mut keep_variables: Vec<ScopedVariable> = Vec::new();
    for (name, scope) in keep_signals.unwrap_or_default() {
        keep_variables.push((Variable::new_signal(&name), scope))
    }
    for (name, scope) in keep_datasets.unwrap_or_default() {
        keep_variables.push((Variable::new_data(&name), scope))
    }

    let plan = SpecPlan::try_new(
        &spec,
        &PlannerConfig::pre_transformed_spec_config(preserve_interactivity, keep_variables),
    )?;
    let watch_plan = WatchPlan::from(plan.comm_plan);

    let json_plan = json!({
        "server_spec": plan.server_spec,
        "client_spec": plan.client_spec,
        "comm_plan": watch_plan,
        "warnings": plan.warnings,
    });

    Python::with_gil(|py| -> PyResult<PyObject> {
        let py_plan = pythonize::pythonize(py, &json_plan)?;
        Ok(py_plan.into())
    })
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _vegafusion(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyVegaFusionRuntime>()?;
    m.add_class::<PyChartState>()?;
    m.add_function(wrap_pyfunction!(get_column_usage, m)?)?;
    m.add_function(wrap_pyfunction!(build_pre_transform_spec_plan, m)?)?;
    m.add_function(wrap_pyfunction!(get_virtual_memory, m)?)?;
    m.add_function(wrap_pyfunction!(get_cpu_count, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
