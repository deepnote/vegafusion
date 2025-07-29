use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_expr::{col, lit, LogicalPlanBuilder};
use std::sync::Arc;
use vegafusion_common::arrow::array::RecordBatch;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
use vegafusion_runtime::data::util::DataFrameUtils;
use vegafusion_runtime::sql::logical_plan_to_spark_sql;

async fn create_test_dataframe(schema_fields: Vec<Field>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(schema_fields));

    let empty_batch = RecordBatch::new_empty(schema.clone());
    let mem_table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]])?;

    let base_plan = LogicalPlanBuilder::scan(
        "test_table", 
        provider_as_source(Arc::new(mem_table)), 
        None
    )?
    .build()?;

    Ok(DataFrame::new(ctx.state(), base_plan))
}

#[tokio::test]
async fn test_logical_plan_to_spark_sql_rewrites_row_number() -> Result<(), Box<dyn std::error::Error>> {
    let schema_fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ];
    
    let df = create_test_dataframe(schema_fields).await?;
    
    let indexed_df = df
        .filter(col("value").gt(lit(0.0)))?
        .with_index()?;

    let plan = indexed_df.logical_plan().clone();
    let spark_sql = logical_plan_to_spark_sql(&plan)?;

    let expected_sql = "SELECT row_number() OVER (ORDER BY monotonically_increasing_id()) AS _vf_order, test_table.id, test_table.name, test_table.value FROM test_table WHERE test_table.value > 0.0";
    
    assert_eq!(
        spark_sql.trim(),
        expected_sql,
        "Generated SQL should use ORDER BY monotonically_increasing_id() as window for row_number()"
    );

    Ok(())
}

