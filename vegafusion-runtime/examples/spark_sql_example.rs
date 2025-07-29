use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_expr::{LogicalPlanBuilder};
use vegafusion_common::column::flat_col;
use std::sync::Arc;
use vegafusion_common::arrow::array::RecordBatch;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
use vegafusion_runtime::sql::{logical_plan_to_spark_sql};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("VegaFusion Spark SQL Generation Example");
    println!("==================================================");

    // Create a SessionContext
    let ctx = SessionContext::new();

    // Define a schema for a "orders" table
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("customer_age", DataType::Float32, false),
        Field::new("customer_email", DataType::Utf8, true),
    ]));

    // Create an empty RecordBatch with the schema
    let empty_batch = RecordBatch::new_empty(schema.clone());

    // Create a MemTable from the schema and empty data
    let mem_table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]])?;

    // Create a logical plan by scanning the table
    let base_plan = LogicalPlanBuilder::scan(
        "orders", 
        provider_as_source(Arc::new(mem_table)), 
        None
    )?
    .build()?;

    println!("Schema:");
    for field in schema.fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }
    println!();

    let df = DataFrame::new(ctx.state(), base_plan);

    // Filter out users with age values that are infinity, negative infinity, or NaN
    let selected_df = df
    .select(vec![
        flat_col("customer_name"),
        flat_col("customer_age"),
    ])?
    .select(vec![
        flat_col("customer_name"),
        flat_col("customer_age"),
    ])?
    .select(vec![
        flat_col("customer_name"),
        flat_col("customer_age"),
    ])?
    .select(vec![
        flat_col("customer_name"),
        flat_col("customer_age"),
    ])?;

    let plan = selected_df.logical_plan().clone();

    println!("Final DataFusion Logical Plan:");
    println!("{}", plan.display_indent());
    println!("======================");

    // Convert to Spark SQL
    match logical_plan_to_spark_sql(&plan) {
        Ok(spark_sql) => {
            println!("Generated Spark SQL:");
            println!("{}", spark_sql);
            println!();
            println!("✓ Successfully converted logical plan to Spark SQL!");
        }
        Err(e) => {
            println!("✗ Failed to convert to Spark SQL: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
} 