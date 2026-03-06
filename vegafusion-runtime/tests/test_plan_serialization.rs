use datafusion::catalog::view::ViewTable;
use datafusion::datasource::provider_as_source;
use datafusion_expr::LogicalPlanBuilder;
use datafusion_proto::bytes::logical_plan_to_bytes;
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
use vegafusion_common::datafusion_expr::LogicalPlan;

fn get_movies_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("title", DataType::Utf8, false),
        Field::new("genre", DataType::Utf8, false),
        Field::new("imdb_rating", DataType::Float64, true),
        Field::new("worldwide_gross", DataType::Int64, false),
    ]))
}

/// Build a lazy logical plan that carries schema metadata but no data,
/// using ViewTable wrapping an EmptyRelation so it serializes with the
/// built-in datafusion-proto codec (no custom extension codec needed).
fn create_view_table_logical_plan() -> LogicalPlan {
    let schema = get_movies_schema();

    // Build a schema-only inner plan: project typed NULL casts over an empty relation
    // so the ViewTable carries our column names and types.
    let inner_plan = {
        use datafusion_expr::{cast, lit, Expr};
        let mut builder = LogicalPlanBuilder::empty(false);

        let exprs: Vec<Expr> = schema
            .fields()
            .iter()
            .map(|f| {
                cast(lit(datafusion_common::ScalarValue::Null), f.data_type().clone())
                    .alias(f.name())
            })
            .collect();

        builder = builder.project(exprs).unwrap();
        builder.build().unwrap()
    };

    let view = Arc::new(ViewTable::new(inner_plan, None));
    let table_source = provider_as_source(view);

    LogicalPlanBuilder::scan("movies", table_source, None)
        .unwrap()
        .build()
        .unwrap()
}

#[test]
fn test_logical_plan_serialization_with_view_table() {
    let plan = create_view_table_logical_plan();

    println!("Logical plan:\n{plan}");

    let bytes = logical_plan_to_bytes(&plan);
    match &bytes {
        Ok(b) => println!("Serialized plan to {} bytes", b.len()),
        Err(e) => println!("Failed to serialize plan: {e}"),
    }

    let bytes = bytes.expect("serialization should succeed");

    // Deserialize it back
    let ctx = datafusion::prelude::SessionContext::new();
    let deserialized = datafusion_proto::bytes::logical_plan_from_bytes(&bytes, &ctx.task_ctx());
    match &deserialized {
        Ok(p) => println!("Deserialized plan:\n{p}"),
        Err(e) => println!("Failed to deserialize plan: {e}"),
    }

    let deserialized = deserialized.expect("deserialization should succeed");

    // Verify schemas match
    assert_eq!(plan.schema(), deserialized.schema());

    // Convert to SQL using the unparser
    let sql = datafusion_sql::unparser::plan_to_sql(&plan);
    match &sql {
        Ok(statement) => println!("SQL:\n{statement}"),
        Err(e) => println!("Failed to convert plan to SQL: {e}"),
    }

    let sql = sql.expect("unparse to SQL should succeed");
    println!("SQL (pretty):\n{}", sql.to_string());
    // The resulting SQL looks like:
    // SELECT * FROM (SELECT CAST(NULL AS VARCHAR) AS title, CAST(NULL AS VARCHAR) AS genre,
    //   CAST(NULL AS DOUBLE) AS imdb_rating, CAST(NULL AS BIGINT) AS worldwide_gross) AS movies

    // Write bytes to a file so we can test deserialization from Python
    let out_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("plan_bytes.bin");
    std::fs::write(&out_path, &bytes).expect("failed to write plan bytes");
    println!("Wrote plan bytes to {}", out_path.display());
}
