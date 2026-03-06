// Exploration test for logical plan serialization via datafusion-proto.
//
// Problem: EmptyTable (our lazy schema-only provider) isn't handled by
// datafusion-proto's default codec — only ListingTable, ViewTable, and
// CteWorkTable are. We also can't use a custom LogicalExtensionCodec because
// datafusion-python hardcodes DefaultLogicalExtensionCodec in from_proto/to_proto.
//
// Solution: before serialization, rewrite EmptyTable scans into ListingTable scans
// pointing at a dummy path. ListingTable serializes natively — schema, paths, and
// format all go into the proto. After deserialization, rewrite back to EmptyTable.
//
// See test_plan_deserialization.py for the Python counterpart.

use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::provider_as_source;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use datafusion_proto::bytes::logical_plan_to_bytes;
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};


const PLACEHOLDER_URL: &str = "file:///vegafusion/placeholder.parquet";

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

// --- Pre-serialization rewrite: EmptyTable -> ListingTable ---

struct EmptyTableToListingRewriter;

impl TreeNodeRewriter for EmptyTableToListingRewriter {
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

                let table_url = ListingTableUrl::parse(PLACEHOLDER_URL)
                    .map_err(|e| datafusion_common::DataFusionError::Internal(e.to_string()))?;
                let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
                let config = ListingTableConfig::new(table_url)
                    .with_listing_options(listing_options)
                    .with_schema(schema);
                let listing_table = Arc::new(ListingTable::try_new(config)?);

                let new_plan = LogicalPlanBuilder::scan(
                    scan.table_name.clone(),
                    provider_as_source(listing_table),
                    scan.projection.clone(),
                )?
                .build()?;

                return Ok(Transformed::yes(new_plan));
            }
        }
        Ok(Transformed::no(node))
    }
}

// --- Post-deserialization rewrite: ListingTable -> EmptyTable ---
//
// After proto round-trip, the plan is still a plain TableScan with a
// ListingTable provider pointing at PLACEHOLDER_URL. We detect that
// and swap it back to EmptyTable.

struct ListingToEmptyTableRewriter;

impl TreeNodeRewriter for ListingToEmptyTableRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let LogicalPlan::TableScan(scan) = &node {
            let provider = datafusion::datasource::source_as_provider(&scan.source).ok();
            let is_placeholder_listing = provider.as_ref().map_or(false, |p| {
                p.as_any()
                    .downcast_ref::<ListingTable>()
                    .map_or(false, |lt| {
                        lt.table_paths()
                            .iter()
                            .any(|p| p.as_str() == PLACEHOLDER_URL)
                    })
            });

            if is_placeholder_listing {
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
fn test_listing_table_rewrite_serialize_roundtrip() {
    let original_plan = create_empty_table_logical_plan();
    let original_schema = original_plan.schema().clone();
    println!("Original plan (EmptyTable):\n{original_plan}\n");

    // Rewrite for serialization
    let serializable_plan = original_plan
        .rewrite(&mut EmptyTableToListingRewriter)
        .expect("rewrite to ListingTable should succeed")
        .data;
    println!("Rewritten plan (ListingTable):\n{serializable_plan}\n");

    // Serialize and write to disk (consumed by test_plan_deserialization.py)
    let bytes = logical_plan_to_bytes(&serializable_plan).expect("serialization should succeed");
    println!("Serialized to {} bytes\n", bytes.len());

    let out_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("plan_bytes.bin");
    std::fs::write(&out_path, &bytes).expect("failed to write plan bytes");

    // Deserialize
    let ctx = datafusion::prelude::SessionContext::new();
    let deserialized =
        datafusion_proto::bytes::logical_plan_from_bytes(&bytes, &ctx.task_ctx())
            .expect("deserialization should succeed");
    println!("Deserialized plan (ListingTable):\n{deserialized}\n");

    // Verify it stays as a TableScan (no inlining like ViewTable)
    assert!(matches!(&deserialized, LogicalPlan::TableScan(_)));

    // Restore EmptyTable
    let restored_plan = deserialized
        .rewrite(&mut ListingToEmptyTableRewriter)
        .expect("rewrite to EmptyTable should succeed")
        .data;
    println!("Restored plan (EmptyTable):\n{restored_plan}\n");

    // Verify full schema fidelity including nullability
    let original_fields = original_schema.inner();
    let restored_fields = restored_plan.schema().inner();
    assert_eq!(original_fields.fields().len(), restored_fields.fields().len());
    for (orig, restored) in original_fields.fields().iter().zip(restored_fields.fields()) {
        assert_eq!(orig.name(), restored.name());
        assert_eq!(orig.data_type(), restored.data_type());
        assert_eq!(
            orig.is_nullable(),
            restored.is_nullable(),
            "nullability mismatch for field '{}'",
            orig.name()
        );
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

    let sql = datafusion_sql::unparser::plan_to_sql(&restored_plan)
        .expect("unparse to SQL should succeed");
    println!("SQL:\n{sql}");
    // => SELECT * FROM movies
}

// Same roundtrip but with a more complex plan: filter + sort on top of
// the EmptyTable scan, built via DataFrame API. Verifies that the rewriters
// only touch the leaf TableScan and leave the rest of the plan intact.
#[test]
fn test_complex_plan_rewrite_serialize_roundtrip() {
    use datafusion::prelude::*;

    let ctx = SessionContext::new();

    // Build a plan via DataFrame API: scan -> filter -> sort
    let scan_plan = create_empty_table_logical_plan();
    let df = DataFrame::new(ctx.state(), scan_plan);
    let df = df
        .filter(col("imdb_rating").gt(lit(8.0)))
        .unwrap()
        .sort(vec![col("worldwide_gross").sort(true, false)])
        .unwrap();

    let original_plan = df.logical_plan().clone();
    println!("Original plan:\n{}\n", original_plan.display_indent());

    // Rewrite leaf EmptyTable -> ListingTable
    let serializable_plan = original_plan
        .clone()
        .rewrite(&mut EmptyTableToListingRewriter)
        .expect("rewrite should succeed")
        .data;
    println!("Rewritten plan:\n{}\n", serializable_plan.display_indent());

    // Serialize and write to disk (consumed by test_plan_deserialization.py)
    let bytes = logical_plan_to_bytes(&serializable_plan).expect("serialization should succeed");
    println!("Serialized to {} bytes\n", bytes.len());

    let out_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("plan_bytes_complex.bin");
    std::fs::write(&out_path, &bytes).expect("failed to write plan bytes");

    // Deserialize
    let deserialized =
        datafusion_proto::bytes::logical_plan_from_bytes(&bytes, &ctx.task_ctx())
            .expect("deserialization should succeed");
    println!("Deserialized plan:\n{}\n", deserialized.display_indent());

    // Restore EmptyTable at the leaf
    let restored_plan = deserialized
        .rewrite(&mut ListingToEmptyTableRewriter)
        .expect("rewrite should succeed")
        .data;
    println!("Restored plan:\n{}\n", restored_plan.display_indent());

    // Schema of the full plan should match
    assert_eq!(original_plan.schema(), restored_plan.schema());

    // The plan structure should be Sort -> Filter -> TableScan
    let LogicalPlan::Sort(sort) = &restored_plan else {
        panic!("Expected Sort at top, got: {}", restored_plan.display());
    };
    let LogicalPlan::Filter(_) = sort.input.as_ref() else {
        panic!("Expected Filter under Sort");
    };

    let sql = datafusion_sql::unparser::plan_to_sql(&restored_plan)
        .expect("unparse to SQL should succeed");
    println!("SQL:\n{sql}");
    // => SELECT ... FROM movies WHERE imdb_rating > 8.0 ORDER BY worldwide_gross ASC NULLS LAST
}
