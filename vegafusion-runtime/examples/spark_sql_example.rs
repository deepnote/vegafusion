use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_expr::lit;
use datafusion_expr::{col, expr_fn::wildcard, LogicalPlanBuilder};
use datafusion_functions::expr_fn::to_char;
use std::sync::Arc;
use vegafusion_common::arrow::array::RecordBatch;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use vegafusion_runtime::datafusion::udfs::datetime::make_timestamptz::make_timestamptz;
use vegafusion_runtime::expression::compiler::utils::ExprHelpers;
use vegafusion_runtime::sql::logical_plan_to_spark_sql;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("VegaFusion Spark SQL Generation Example");
    println!("Demonstrating Column Names with Spaces and Reserved SQL Words");
    println!("=============================================================");

    // Create a SessionContext
    let ctx = SessionContext::new();

    // Define a schema for a "orders" table with challenging column names
    // Including spaces, reserved SQL words, and special characters
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer name", DataType::Utf8, false), // Space in name
        Field::new("select", DataType::Float32, false),     // Reserved SQL word
        Field::new("customer-email", DataType::Utf8, true), // Hyphen in name
        Field::new("from", DataType::Utf8, true),           // Reserved SQL word
        Field::new(
            "order date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ), // Space in name
        Field::new("where", DataType::Int32, true),         // Reserved SQL word
        Field::new("Total Amount", DataType::Float64, false), // Space and capital letters
    ]));

    // Create an empty RecordBatch with the schema
    let empty_batch = RecordBatch::new_empty(schema.clone());

    // Create a MemTable from the schema and empty data
    let mem_table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]])?;

    // Create a logical plan by scanning the table
    let base_plan =
        LogicalPlanBuilder::scan("orders", provider_as_source(Arc::new(mem_table)), None)?
            .build()?;

    println!("Schema:");
    for field in schema.fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }
    println!();

    let df = DataFrame::new(ctx.state(), base_plan);
    let df_schema = df.schema().clone();

    // Demonstrate selecting columns with spaces and reserved SQL words
    // These will be properly escaped in the generated Spark SQL
    let selected_df = df
        .select(vec![
            col("customer name").alias("customer_name_clean"), // Column with space
        ])?
        .select(vec![
            col("customer_name_clean").alias("customer name with spaces"),
            col("customer_name_clean").alias("customer name with spaces2"),
            col("customer_name_clean").alias("customer name with spaces3"),
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
