use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_expr::{col, lit, LogicalPlanBuilder};
use datafusion_functions_window::row_number::row_number;
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

    // Define a schema for a "users" table
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
    ]));

    // Create an empty RecordBatch with the schema
    let empty_batch = RecordBatch::new_empty(schema.clone());

    // Create a MemTable from the schema and empty data
    let mem_table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]])?;

    // Create a logical plan by scanning the table
    let base_plan = LogicalPlanBuilder::scan(
        "users", 
        provider_as_source(Arc::new(mem_table)), 
        None
    )?
    .build()?;

    println!("Schema:");
    for field in schema.fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }
    println!();

    println!("Base DataFusion Logical Plan:");
    println!("{}", base_plan.display_indent());
    println!();

    let df = DataFrame::new(ctx.state(), base_plan);

    let filtered_df = df.filter(col("age").gt(lit(21)))?;
    let ordered_df = filtered_df.window(vec![row_number().alias("order")])?.select(vec![datafusion_expr::expr_fn::wildcard()])?;

    let plan = ordered_df.logical_plan().clone();

    println!("Final DataFusion Logical Plan (from DataFrame operations):");
    println!("{}", plan.display_indent());
    println!();

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