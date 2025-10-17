use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::proto::gen::pretransform::PreTransformSpecOpts;
use vegafusion_core::runtime::{PlanExecutor, VegaFusionRuntimeTrait};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::plan_executor::DataFusionPlanExecutor;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;
use vegafusion_runtime::datafusion::context::make_datafusion_context;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion_expr::LogicalPlanBuilder;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
use vegafusion_common::arrow::array::{
    StringArray, Int64Array, Float64Array, RecordBatch,
};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::{LogicalPlan as DFLogicalPlan, Expr, TableSource};
use datafusion::catalog::TableProvider;
use std::any::Any;
use std::borrow::Cow;

#[derive(Debug, Clone)]
struct SchemaOnlyTableSource {
    schema: Arc<Schema>,
}

impl SchemaOnlyTableSource {
    fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

impl TableSource for SchemaOnlyTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<datafusion_expr::TableProviderFilterPushDown>> {
        Ok(vec![])
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, DFLogicalPlan>> {
        None
    }
}

#[derive(Clone)]
struct TrackingPlanExecutor {
    call_count: Arc<AtomicUsize>,
    plans_received: Arc<Mutex<Vec<LogicalPlan>>>,
    movies_table: Arc<dyn TableProvider>,
    fallback_executor: Arc<DataFusionPlanExecutor>,
}

impl TrackingPlanExecutor {
    fn new() -> Self {
        let ctx = Arc::new(make_datafusion_context());
        
        let movies_table = create_movies_table();
        let schema = movies_table.schema.clone();
        let batches = movies_table.batches.clone();
        let mem_table = Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()) as Arc<dyn TableProvider>;
        
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            plans_received: Arc::new(Mutex::new(Vec::new())),
            movies_table: mem_table,
            fallback_executor: Arc::new(DataFusionPlanExecutor::new(ctx)),
        }
    }

    fn get_call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }

    fn get_plans_received(&self) -> Vec<LogicalPlan> {
        self.plans_received.lock().unwrap().clone()
    }
}

struct TableRewriter {
    movies_table: Arc<dyn TableProvider>,
}

impl TreeNodeRewriter for TableRewriter {
    type Node = DFLogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let DFLogicalPlan::TableScan(scan) = &node {
            if scan.table_name.table() == "movies" {
                let new_scan = DFLogicalPlan::TableScan(datafusion_expr::TableScan {
                    table_name: scan.table_name.clone(),
                    source: provider_as_source(self.movies_table.clone()),
                    projection: scan.projection.clone(),
                    projected_schema: scan.projected_schema.clone(),
                    filters: scan.filters.clone(),
                    fetch: scan.fetch,
                });
                return Ok(Transformed::yes(new_scan));
            }
        }
        Ok(Transformed::no(node))
    }
}

#[async_trait]
impl PlanExecutor for TrackingPlanExecutor {
    async fn execute_plan(&self, plan: LogicalPlan) -> Result<VegaFusionTable> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        
        self.plans_received.lock().unwrap().push(plan.clone());
        
        let mut rewriter = TableRewriter { movies_table: self.movies_table.clone() };
        let rewritten_plan = plan.rewrite(&mut rewriter).unwrap().data;
        
        self.fallback_executor.execute_plan(rewritten_plan).await
    }
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_spec() {
    let tracking_executor = TrackingPlanExecutor::new();
    let executor_clone = tracking_executor.clone();
    
    let runtime = VegaFusionRuntime::new(None, Some(Arc::new(tracking_executor)));
    
    let spec = get_simple_spec();
    let inline_datasets = get_inline_datasets();
    
    let (_transformed_spec, warnings) = runtime
        .pre_transform_spec(
            &spec,
            &inline_datasets,
            &PreTransformSpecOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
                keep_variables: vec![],
            },
        )
        .await
        .unwrap();
    
    assert!(warnings.is_empty());
    
    println!("Transformed spec:\n{}", serde_json::to_string_pretty(&_transformed_spec).unwrap());

    let call_count = executor_clone.get_call_count();
    println!("Custom executor was called {} times", call_count);
    assert!(call_count > 0, "Custom executor should have been called at least once");
    
    let plans = executor_clone.get_plans_received();
    println!("Received {} plans", plans.len());
    assert!(!plans.is_empty(), "Should have received at least one logical plan");
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_extract() {
    let tracking_executor = TrackingPlanExecutor::new();
    let executor_clone = tracking_executor.clone();
    
    let runtime = VegaFusionRuntime::new(None, Some(Arc::new(tracking_executor)));
    
    let spec = get_simple_spec();
    let inline_datasets = get_inline_datasets();
    
    let (_transformed_spec, _datasets, warnings) = runtime
        .pre_transform_extract(
            &spec,
            &inline_datasets,
            &vegafusion_core::proto::gen::pretransform::PreTransformExtractOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                extract_threshold: 100,
                keep_variables: vec![],
            },
        )
        .await
        .unwrap();
    
    assert!(warnings.is_empty());
    
    let call_count = executor_clone.get_call_count();
    println!("Custom executor was called {} times", call_count);
    assert!(call_count > 0, "Custom executor should have been called at least once");
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_values() {
    let tracking_executor = TrackingPlanExecutor::new();
    let executor_clone = tracking_executor.clone();
    
    let runtime = VegaFusionRuntime::new(None, Some(Arc::new(tracking_executor)));
    
    let spec = get_simple_spec();
    let inline_datasets = get_inline_datasets();
    
    let variables = vec![(
        vegafusion_core::proto::gen::tasks::Variable {
            namespace: vegafusion_core::proto::gen::tasks::VariableNamespace::Data as i32,
            name: "source_0".to_string(),
        },
        vec![],
    )];
    
    let (values, warnings) = runtime
        .pre_transform_values(
            &spec,
            &variables,
            &inline_datasets,
            &vegafusion_core::proto::gen::pretransform::PreTransformValuesOpts {
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
            },
        )
        .await
        .unwrap();
    
    assert!(warnings.is_empty());
    assert_eq!(values.len(), 1);
    
    let call_count = executor_clone.get_call_count();
    println!("Custom executor was called {} times", call_count);
    assert!(call_count > 0, "Custom executor should have been called at least once");
}

fn get_simple_spec() -> ChartSpec {
    let spec_str = r#"{
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "width": 400,
        "height": 200,
        "padding": 5,
        "data": [
            {
                "name": "source_0",
                "url": "table://movies",
                "transform": [
                    {
                        "type": "aggregate",
                        "groupby": ["genre"],
                        "ops": ["sum", "average", "count"],
                        "fields": ["worldwide_gross", "imdb_rating", null],
                        "as": ["total_gross", "avg_rating", "movie_count"]
                    }
                ]
            }
        ],
        "scales": [
            {
                "name": "xscale",
                "type": "band",
                "domain": {"data": "source_0", "field": "genre"},
                "range": "width",
                "padding": 0.05
            },
            {
                "name": "yscale",
                "domain": {"data": "source_0", "field": "total_gross"},
                "nice": true,
                "range": "height"
            }
        ],
        "marks": [
            {
                "type": "rect",
                "from": {"data": "source_0"},
                "encode": {
                    "enter": {
                        "x": {"scale": "xscale", "field": "genre"},
                        "width": {"scale": "xscale", "band": 1},
                        "y": {"scale": "yscale", "field": "total_gross"},
                        "y2": {"scale": "yscale", "value": 0}
                    }
                }
            }
        ]
    }"#;
    
    serde_json::from_str(spec_str).unwrap()
}

fn get_movies_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("title", DataType::Utf8, false),
        Field::new("genre", DataType::Utf8, false),
        Field::new("director", DataType::Utf8, true),
        Field::new("release_year", DataType::Int64, false),
        Field::new("worldwide_gross", DataType::Int64, false),
        Field::new("production_budget", DataType::Int64, true),
        Field::new("imdb_rating", DataType::Float64, true),
        Field::new("rotten_tomatoes", DataType::Int64, true),
    ]))
}

#[allow(dead_code)]
fn create_movies_table() -> VegaFusionTable {
    let schema = get_movies_schema();
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "The Shawshank Redemption",
                "The Dark Knight",
                "Inception",
                "Pulp Fiction",
                "Forrest Gump",
                "The Matrix",
                "Goodfellas",
                "The Silence of the Lambs",
                "Interstellar",
                "The Prestige",
            ])),
            Arc::new(StringArray::from(vec![
                "Drama",
                "Action",
                "Action",
                "Crime",
                "Drama",
                "Action",
                "Crime",
                "Thriller",
                "Sci-Fi",
                "Thriller",
            ])),
            Arc::new(StringArray::from(vec![
                Some("Frank Darabont"),
                Some("Christopher Nolan"),
                Some("Christopher Nolan"),
                Some("Quentin Tarantino"),
                Some("Robert Zemeckis"),
                Some("The Wachowskis"),
                Some("Martin Scorsese"),
                Some("Jonathan Demme"),
                Some("Christopher Nolan"),
                Some("Christopher Nolan"),
            ])),
            Arc::new(Int64Array::from(vec![
                1994, 2008, 2010, 1994, 1994, 1999, 1990, 1991, 2014, 2006,
            ])),
            Arc::new(Int64Array::from(vec![
                28341469,
                1004558444,
                836848102,
                213928762,
                678226465,
                463517383,
                46836394,
                272742922,
                677471339,
                109676311,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(25000000),
                Some(185000000),
                Some(160000000),
                Some(8000000),
                Some(55000000),
                Some(63000000),
                Some(25000000),
                Some(19000000),
                Some(165000000),
                Some(40000000),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(9.3),
                Some(9.0),
                Some(8.8),
                Some(8.9),
                Some(8.8),
                Some(8.7),
                Some(8.7),
                Some(8.6),
                Some(8.6),
                Some(8.5),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(91),
                Some(94),
                Some(87),
                Some(92),
                Some(71),
                Some(88),
                Some(96),
                Some(96),
                Some(72),
                Some(76),
            ])),
        ],
    )
    .unwrap();
    
    VegaFusionTable::from(batch)
}

fn create_movies_logical_plan() -> LogicalPlan {
    let schema = get_movies_schema();
    
    let table_source = Arc::new(SchemaOnlyTableSource::new(schema));
    
    LogicalPlanBuilder::scan("movies", table_source, None)
        .unwrap()
        .build()
        .unwrap()
}

fn get_inline_datasets() -> std::collections::HashMap<String, VegaFusionDataset> {
    let logical_plan = create_movies_logical_plan();
    let dataset = VegaFusionDataset::from_plan(logical_plan);
    
    let mut datasets = std::collections::HashMap::new();
    datasets.insert("movies".to_string(), dataset);
    datasets
}

