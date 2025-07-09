use crate::task_graph::timezone::RuntimeTzConfig;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::tasks::task::TaskKind;
use vegafusion_core::proto::gen::tasks::Task;
use vegafusion_core::task_graph::task_value::{TaskValue, TaskPlan};

#[async_trait]
pub trait TaskCall {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        ctx: Arc<SessionContext>,
    ) -> Result<(TaskValue, Vec<TaskValue>)>;
    
    async fn plan(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        ctx: Arc<SessionContext>,
    ) -> Result<(TaskPlan, Vec<TaskValue>)>;
}

#[async_trait]
impl TaskCall for Task {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        ctx: Arc<SessionContext>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        match self.task_kind() {
            TaskKind::Value(value) => Ok((value.try_into()?, Default::default())),
            TaskKind::DataUrl(task) => task.eval(values, tz_config, inline_datasets, ctx).await,
            TaskKind::DataValues(task) => task.eval(values, tz_config, inline_datasets, ctx).await,
            TaskKind::DataSource(task) => task.eval(values, tz_config, inline_datasets, ctx).await,
            TaskKind::Signal(task) => task.eval(values, tz_config, inline_datasets, ctx).await,
        }
    }
    
    async fn plan(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        ctx: Arc<SessionContext>,
    ) -> Result<(TaskPlan, Vec<TaskValue>)> {
        match self.task_kind() {
            TaskKind::Value(value) => {
                let task_value: TaskValue = value.try_into()?;
                let task_plan = match task_value {
                    TaskValue::Scalar(scalar) => TaskPlan::Scalar(scalar),
                    TaskValue::Table(_) => {
                        return Err(vegafusion_core::error::VegaFusionError::internal(
                            "Cannot convert Table TaskValue to TaskPlan"
                        ));
                    }
                };
                Ok((task_plan, Default::default()))
            }
            TaskKind::DataUrl(task) => task.plan(values, tz_config, inline_datasets, ctx).await,
            TaskKind::DataValues(task) => task.plan(values, tz_config, inline_datasets, ctx).await,
            TaskKind::DataSource(task) => task.plan(values, tz_config, inline_datasets, ctx).await,
            TaskKind::Signal(task) => task.plan(values, tz_config, inline_datasets, ctx).await,
        }
    }
}
