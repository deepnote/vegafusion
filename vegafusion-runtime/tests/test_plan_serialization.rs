// Exploration test for logical plan serialization via datafusion-proto.
//
// Problem: EmptyTable (our lazy schema-only provider) isn't handled by
// datafusion-proto's default codec — only ListingTable, ViewTable, and
// CteWorkTable are. We also can't use a custom LogicalExtensionCodec because
// datafusion-python hardcodes DefaultLogicalExtensionCodec in from_proto/to_proto.
//
// Solution: before serialization, rewrite EmptyTable scans into ViewTable scans.
// ViewTable wraps an inner logical plan, so we encode the schema as:
//   ViewTable(Projection(CAST(NULL AS <type>) AS <col>, ...) -> EmptyRelation)
//
// After deserialization (Rust or Python side), the inverse rewrite restores
// EmptyTable scans. See test_plan_deserialization.py for the Python counterpart.
//
// Caveat: nullability is lost — CAST(NULL AS T) makes all fields nullable.
// This is acceptable since these plans are schema-only placeholders.

use datafusion::catalog::view::ViewTable;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::{cast, lit, Expr, LogicalPlanBuilder};
use datafusion_expr::LogicalPlan;
use datafusion_proto::bytes::logical_plan_to_bytes;
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};

fn get_movies_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("title", DataType::Utf8, false),
        Field::new("genre", DataType::Utf8, false),
        Field::new("imdb_rating", DataType::Float64, true),
        Field::new("worldwide_gross", DataType::Int64, false),
    ]))
}

fn create_empty_table_logical_plan() -> LogicalPlan {
    let schema = get_movies_schema();
    let provider = Arc::new(EmptyTable::new(schema));
    let table_source = provider_as_source(provider);

    LogicalPlanBuilder::scan("movies", table_source, None)
        .unwrap()
        .build()
        .unwrap()
}

// --- Pre-serialization rewrite: EmptyTable -> ViewTable ---

struct EmptyTableToViewRewriter;

impl TreeNodeRewriter for EmptyTableToViewRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let LogicalPlan::TableScan(scan) = &node {
            let provider = datafusion::datasource::source_as_provider(&scan.source).ok();
            let is_empty_table = provider
                .as_ref()
                .map(|p| p.as_any().downcast_ref::<EmptyTable>().is_some())
                .unwrap_or(false);

            if is_empty_table {
                let schema = scan.source.schema();

                // Encode each field as CAST(NULL AS <type>) AS <name>.
                // This is the only way to carry column names + types through
                // a plan that serializes with the default codec.
                let exprs: Vec<Expr> = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        cast(lit(datafusion_common::ScalarValue::Null), f.data_type().clone())
                            .alias(f.name())
                    })
                    .collect();

                let inner_plan = LogicalPlanBuilder::empty(false)
                    .project(exprs)?
                    .build()?;

                let view = Arc::new(ViewTable::new(inner_plan, None));
                let new_plan = LogicalPlanBuilder::scan(
                    scan.table_name.clone(),
                    provider_as_source(view),
                    scan.projection.clone(),
                )?
                .build()?;

                return Ok(Transformed::yes(new_plan));
            }
        }
        Ok(Transformed::no(node))
    }
}

// --- Post-deserialization rewrite: ViewTable pattern -> EmptyTable ---
//
// After proto round-trip, ViewTable scans become SubqueryAlias nodes
// (LogicalPlanBuilder::scan inlines the view's inner plan):
//
//   SubqueryAlias: <table_name>
//     Projection: CAST(NULL AS <type>) AS <col>, ...
//       EmptyRelation

struct ViewToEmptyTableRewriter;

impl ViewToEmptyTableRewriter {
    fn is_null_projection_over_empty(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Projection(proj) = plan {
            if let LogicalPlan::EmptyRelation(_) = proj.input.as_ref() {
                return proj.expr.iter().all(|e| {
                    matches!(e, Expr::Alias(alias) if matches!(alias.expr.as_ref(), Expr::Cast(_)))
                });
            }
        }
        false
    }
}

impl TreeNodeRewriter for ViewToEmptyTableRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> datafusion_common::Result<Transformed<Self::Node>> {
        // Post-deserialization: match the inlined SubqueryAlias pattern
        if let LogicalPlan::SubqueryAlias(alias) = &node {
            if Self::is_null_projection_over_empty(alias.input.as_ref()) {
                let schema = node.schema().inner().clone();
                let empty = Arc::new(EmptyTable::new(schema));
                let new_plan = LogicalPlanBuilder::scan(
                    alias.alias.clone(),
                    provider_as_source(empty),
                    None,
                )?
                .build()?;

                return Ok(Transformed::yes(new_plan));
            }
        }

        // Pre-deserialization: match TableScan still holding a ViewTable provider
        if let LogicalPlan::TableScan(scan) = &node {
            let provider = datafusion::datasource::source_as_provider(&scan.source).ok();
            let is_view_table = provider
                .as_ref()
                .map(|p| p.as_any().downcast_ref::<ViewTable>().is_some())
                .unwrap_or(false);

            if is_view_table {
                let schema = scan.source.schema();
                let empty = Arc::new(EmptyTable::new(schema));
                let new_plan = LogicalPlanBuilder::scan(
                    scan.table_name.clone(),
                    provider_as_source(empty),
                    scan.projection.clone(),
                )?
                .build()?;

                return Ok(Transformed::yes(new_plan));
            }
        }
        Ok(Transformed::no(node))
    }
}

#[test]
fn test_logical_plan_rewrite_serialize_roundtrip() {
    let original_plan = create_empty_table_logical_plan();
    let original_schema = original_plan.schema().clone();
    println!("Original plan (EmptyTable):\n{original_plan}\n");

    // Rewrite for serialization
    let serializable_plan = original_plan
        .rewrite(&mut EmptyTableToViewRewriter)
        .expect("rewrite to ViewTable should succeed")
        .data;
    println!("Rewritten plan (ViewTable):\n{serializable_plan}\n");

    // Serialize and write to disk (consumed by test_plan_deserialization.py)
    let bytes = logical_plan_to_bytes(&serializable_plan).expect("serialization should succeed");
    println!("Serialized to {} bytes\n", bytes.len());

    let out_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("plan_bytes.bin");
    std::fs::write(&out_path, &bytes).expect("failed to write plan bytes");

    // Deserialize and restore
    let ctx = datafusion::prelude::SessionContext::new();
    let deserialized =
        datafusion_proto::bytes::logical_plan_from_bytes(&bytes, &ctx.task_ctx())
            .expect("deserialization should succeed");
    println!("Deserialized plan (ViewTable):\n{deserialized}\n");

    let restored_plan = deserialized
        .rewrite(&mut ViewToEmptyTableRewriter)
        .expect("rewrite to EmptyTable should succeed")
        .data;
    println!("Restored plan (EmptyTable):\n{restored_plan}\n");

    // Verify field names and types survive the roundtrip
    let original_fields = original_schema.inner();
    let restored_fields = restored_plan.schema().inner();
    assert_eq!(original_fields.fields().len(), restored_fields.fields().len());
    for (orig, restored) in original_fields.fields().iter().zip(restored_fields.fields()) {
        assert_eq!(orig.name(), restored.name());
        assert_eq!(orig.data_type(), restored.data_type());
    }

    // Verify the restored plan uses EmptyTable
    if let LogicalPlan::TableScan(scan) = &restored_plan {
        let provider = datafusion::datasource::source_as_provider(&scan.source).unwrap();
        assert!(
            provider.as_any().downcast_ref::<EmptyTable>().is_some(),
            "Restored plan should use EmptyTable"
        );
    } else {
        panic!("Restored plan should be a TableScan");
    }

    // Verify the restored plan unparses to clean SQL
    let sql = datafusion_sql::unparser::plan_to_sql(&restored_plan)
        .expect("unparse to SQL should succeed");
    println!("SQL:\n{sql}");
    // => SELECT * FROM movies
}
