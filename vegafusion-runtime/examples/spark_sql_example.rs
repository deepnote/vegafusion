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
    println!("==================================================");

    // Create a SessionContext
    let ctx = SessionContext::new();

    // Define a schema for a "orders" table
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("customer_age", DataType::Float32, false),
        Field::new("customer_email", DataType::Utf8, true),
        Field::new(
            "order_date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
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

    // Add a new column with timestamp cast to string
    let selected_df = df.select(vec![
        wildcard(),
        col("order_date")
            .try_cast_to(
                &DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some("America/Los_Angeles".to_string().into()),
                ),
                &df_schema,
            )?
            .alias("order_date_tz")
            .into(),
        to_char(col("order_date"), lit("%Y-%m-%d %H:%M:%S"))
            .alias("order_date_formatted")
            .into(),
        make_timestamptz(
            lit(2012),
            lit(1),
            lit(1),
            lit(0),
            lit(0),
            lit(0),
            lit(0),
            "America/Los_Angeles",
        )
        .alias("made_ts")
        .into(),
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
