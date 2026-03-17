use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_expr::Expr;
use datafusion_expr::{col, lit, LogicalPlanBuilder};
use datafusion_functions::expr_fn::{to_char, to_timestamp_seconds};
use std::sync::Arc;
use vegafusion_common::arrow::array::RecordBatch;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use vegafusion_common::column::flat_col;
use vegafusion_runtime::data::util::DataFrameUtils;
use vegafusion_runtime::datafusion::udfs::datetime::make_timestamptz::make_timestamptz;
use vegafusion_runtime::expression::compiler::utils::ExprHelpers;
use vegafusion_runtime::sql::logical_plan_to_spark_sql;

async fn create_test_dataframe(
    schema_fields: Vec<Field>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(schema_fields));

    let empty_batch = RecordBatch::new_empty(schema.clone());
    let mem_table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]])?;

    let base_plan =
        LogicalPlanBuilder::scan("test_table", provider_as_source(Arc::new(mem_table)), None)?
            .build()?;

    Ok(DataFrame::new(ctx.state(), base_plan))
}

#[tokio::test]
async fn test_logical_plan_to_spark_sql_rewrites_row_number(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ];

    let df = create_test_dataframe(schema_fields).await?;

    let indexed_df = df.filter(col("value").gt(lit(0.0)))?.with_index()?;

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

#[tokio::test]
async fn test_logical_plan_to_spark_sql_rewrites_inf_and_nan(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
    ];

    let df = create_test_dataframe(schema_fields).await?;

    // Create a query that will generate NaN and infinity literals in the logical plan
    let filtered_df = df
        .filter(col("value").gt(lit(f64::NAN)))?
        .filter(col("value").lt(lit(f64::INFINITY)))?
        .filter(col("value").gt(lit(f64::NEG_INFINITY)))?;

    let plan = filtered_df.logical_plan().clone();
    let spark_sql = logical_plan_to_spark_sql(&plan)?;

    let expected_sql = "SELECT * FROM test_table WHERE test_table.value > float('-inf') AND test_table.value < float('inf') AND test_table.value > float('NaN')";

    assert_eq!(
        spark_sql, expected_sql,
        "Should wrap NaN and Infinity literals in float()"
    );
    Ok(())
}

#[tokio::test]
async fn test_logical_plan_to_spark_sql_rewrites_subquery_column_identifiers(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_fields = vec![
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("customer_age", DataType::Float32, false),
    ];

    let df = create_test_dataframe(schema_fields).await?;

    // Create nested projections that would generate compound column names
    let nested_df = df
        .select(vec![flat_col("customer_name"), flat_col("customer_age")])?
        .select(vec![flat_col("customer_name"), flat_col("customer_age")])?;

    let plan = nested_df.logical_plan().clone();
    let spark_sql = logical_plan_to_spark_sql(&plan)?;

    let expected_sql = "SELECT customer_name, customer_age FROM (SELECT test_table.customer_name, test_table.customer_age FROM test_table)";

    assert_eq!(
        spark_sql.trim(),
        expected_sql,
        "Generated SQL should rewrite subquery column identifiers correctly"
    );

    Ok(())
}

#[tokio::test]
async fn test_logical_plan_to_spark_sql_chrono_formatting() -> Result<(), Box<dyn std::error::Error>>
{
    let schema_fields = vec![Field::new(
        "timestamp_col",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    )];

    let df = create_test_dataframe(schema_fields).await?;

    // Test basic datetime format conversion: %Y-%m-%d %H:%M:%S -> yyyy-MM-dd HH:mm:ss
    let df_basic = df.clone().select(vec![to_char(
        col("timestamp_col"),
        lit("%Y-%m-%d %H:%M:%S"),
    )])?;

    let plan_basic = df_basic.logical_plan().clone();
    let spark_sql_basic = logical_plan_to_spark_sql(&plan_basic)?;

    assert!(
        spark_sql_basic.contains("yyyy-MM-dd HH:mm:ss"),
        "Basic datetime format should be converted to Spark format. Got: {}",
        spark_sql_basic
    );

    // Test fractional seconds: %.3f -> .SSS
    let df_frac = df
        .clone()
        .select(vec![to_char(col("timestamp_col"), lit("%.3f"))])?;

    let plan_frac = df_frac.logical_plan().clone();
    let spark_sql_frac = logical_plan_to_spark_sql(&plan_frac)?;

    assert!(
        spark_sql_frac.contains(".SSS"),
        "Fractional seconds format should be converted to Spark format. Got: {}",
        spark_sql_frac
    );

    // Test full ISO format: %Y-%m-%dT%H:%M:%S%.f%:z -> yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX
    let df_iso = df.clone().select(vec![to_char(
        col("timestamp_col"),
        lit("%Y-%m-%dT%H:%M:%S%.f%:z"),
    )])?;

    let plan_iso = df_iso.logical_plan().clone();
    let spark_sql_iso = logical_plan_to_spark_sql(&plan_iso)?;

    assert!(
        // Double single quote because it will be inside SQL string and has to be escaped
        spark_sql_iso.contains("yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSSSSSXXX"),
        "ISO datetime format should be converted to Spark format. Got: {}",
        spark_sql_iso
    );

    Ok(())
}

#[tokio::test]
async fn test_logical_plan_to_spark_sql_rewrites_timestamps(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_fields = vec![Field::new(
        "order_date",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];

    let df = create_test_dataframe(schema_fields).await?;
    let df_schema = df.schema().clone();

    // Create operations that will generate make_timestamptz function calls, TIMESTAMP WITH TIME ZONE casts, and to_timestamp_seconds calls
    let timestamp_df = df.select(vec![
        // This should create a cast to TIMESTAMP WITH TIME ZONE, which should be rewritten to TIMESTAMP
        col("order_date")
            .try_cast_to(
                &DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some("America/Los_Angeles".to_string().into()),
                ),
                &df_schema,
            )?
            .alias("order_date_tz"),
        // This should create a make_timestamptz call, which should be rewritten to make_timestamp with 7th arg dropped
        make_timestamptz(
            lit(2023),
            lit(12),
            lit(25),
            lit(10),
            lit(30),
            lit(45),
            lit(123), // This 7th argument (milliseconds) should be dropped
            "UTC",
        )
        .alias("made_timestamp")
        .into(),
        // This should create a to_timestamp_seconds call, which should be rewritten to to_timestamp
        to_timestamp_seconds(vec![lit("2023-12-25 10:30:45")]).alias("parsed_timestamp"),
    ])?;

    let plan = timestamp_df.logical_plan().clone();
    let spark_sql = logical_plan_to_spark_sql(&plan)?;

    // Check that make_timestamptz was rewritten to make_timestamp and to_timestamp_seconds was rewritten to to_timestamp
    assert_eq!(
        spark_sql,
        "SELECT TRY_CAST(test_table.order_date AS TIMESTAMP) AS order_date_tz, make_timestamp(2023, 12, 25, 10, 30, 45, 'UTC') AS made_timestamp, to_timestamp('2023-12-25 10:30:45') AS parsed_timestamp FROM test_table",
        "Generated SQL should rewrite timestamp functions for Spark compatibility"
    );

    Ok(())
}

#[tokio::test]
async fn test_logical_plan_to_spark_sql_rewrites_intervals(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_fields = vec![Field::new(
        "timestamp_col",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];

    let df = create_test_dataframe(schema_fields).await?;

    // Create a query that will generate interval expressions with abbreviated names
    let interval_df = df.select(vec![
        col("timestamp_col") + lit(datafusion_common::ScalarValue::IntervalYearMonth(Some(1))),
    ])?;

    let plan = interval_df.logical_plan().clone();
    let spark_sql = logical_plan_to_spark_sql(&plan)?;

    let expected_sql =
        "SELECT test_table.timestamp_col + INTERVAL '0 YEARS 1 MONTHS' FROM test_table";

    assert_eq!(
        spark_sql, expected_sql,
        "Generated SQL should expand interval abbreviations to full names"
    );

    Ok(())
}

#[tokio::test]
async fn test_logical_plan_to_spark_sql_quotes_column_identifiers(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_fields = vec![
        Field::new("customer name", DataType::Utf8, false), // space in name
        Field::new("customer-email", DataType::Utf8, false), // hyphen in name
        Field::new("select", DataType::Utf8, false),        // SQL reserved word
        Field::new("from", DataType::Utf8, false),          // SQL reserved word
        Field::new("Total Amount", DataType::Float64, false), // space and capital letters
        Field::new("123field", DataType::Int32, false),     // starts with digit
        Field::new("normal_field", DataType::Int32, false), // normal field (shouldn't be quoted)
    ];

    let df = create_test_dataframe(schema_fields).await?;

    // Create a nested query structure to test recursive alias quoting
    // This creates: SELECT ... FROM (SELECT ... AS "nested alias" FROM (SELECT ... FROM test_table))
    let inner_df = df.select(vec![
        flat_col("customer name").alias("inner customer name"), // nested alias with space
        flat_col("select").alias("inner select"),               // nested alias with reserved word
        flat_col("normal_field").alias("inner_normal"),         // normal nested alias
    ])?;

    let middle_df = inner_df.select(vec![
        flat_col("inner customer name").alias("middle customer name"), // another level of nesting
        flat_col("inner select").alias("middle from"),                 // reserved word alias
        flat_col("inner_normal"),                                      // pass through
    ])?;

    let result_df = middle_df.select(vec![
        flat_col("middle customer name").alias("final name"), // final alias with space
        flat_col("middle from").alias("final location"),      // final alias with space
        flat_col("inner_normal").alias("final_normal"),
    ])?;

    let plan = result_df.logical_plan().clone();
    let spark_sql = logical_plan_to_spark_sql(&plan)?;

    // The expected SQL should have quoted aliases at all levels of nesting
    let expected_sql = "SELECT `middle customer name` AS `final name`, `middle from` AS `final location`, inner_normal AS final_normal FROM (SELECT `inner customer name` AS `middle customer name`, `inner select` AS `middle from`, inner_normal FROM (SELECT test_table.`customer name` AS `inner customer name`, test_table.`select` AS `inner select`, test_table.normal_field AS inner_normal FROM test_table))";

    assert_eq!(
        spark_sql.trim(),
        expected_sql,
        "Generated SQL should properly quote column names and aliases at all nesting levels"
    );

    Ok(())
}

#[tokio::test]
async fn test_logical_plan_to_spark_sql_parenthesizes_nested_is_null(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_fields = vec![Field::new("IMDB Rating", DataType::Utf8, false)];

    let df = create_test_dataframe(schema_fields).await?;

    // Build nested IS NULL: (test_table.`IMDB Rating` IS NULL) IS NULL
    let nested_is_null = Expr::IsNull(Box::new(Expr::IsNull(Box::new(col("IMDB Rating")))));
    let expr = nested_is_null.alias("check_null");

    let df = df.select(vec![expr])?;
    let plan = df.logical_plan().clone();
    let spark_sql = logical_plan_to_spark_sql(&plan)?;

    let expected_sql =
        "SELECT (test_table.`IMDB Rating` IS NULL) IS NULL AS check_null FROM test_table";

    assert_eq!(
        spark_sql.trim(),
        expected_sql,
        "Generated SQL should parenthesize nested IS NULL for Spark compatibility"
    );

    Ok(())
}
