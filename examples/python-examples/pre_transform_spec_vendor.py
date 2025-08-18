import pandas as pd
import pyarrow as pa
from typing import Any

try:
    from pyspark.sql import SparkSession

    SPARK_AVAILABLE = True
except ImportError:
    print("PySpark not available. Please install pyspark to run this example.")
    print("pip install pyspark")
    SPARK_AVAILABLE = False

import vegafusion as vf


def create_spark_executor(spark_session: SparkSession):
    """Create a Spark executor function that executes SQL queries."""

    def spark_executor(sql_query: str) -> pa.Table:
        """Execute SQL query using Spark and return Arrow table."""
        print("🔥 Executing Spark SQL:")
        print(f"   {sql_query}")
        print("-" * 60)

        spark_df = spark_session.sql(sql_query)

        pandas_df = spark_df.toPandas()
        print("Got response from Spark, rows:", len(pandas_df))
        arrow_table = pa.Table.from_pandas(pandas_df)
        return arrow_table

    return spark_executor


def setup_spark_session() -> SparkSession:
    """Initialize a local Spark session with appropriate configuration."""
    print("🚀 Setting up Spark session...")

    spark = (
        SparkSession.builder.appName("vegafusion-spec-vendor-example")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .master("local[2]")
        .getOrCreate()
    )

    # Set timezone to UTC for consistency
    spark.sql("SET TIME ZONE 'UTC'")
    print("✅ Spark session created successfully")

    return spark


def load_and_register_movies_data(spark: SparkSession) -> pa.Schema:
    """Load movies data and register it as a Spark table."""
    print("📊 Loading movies data...")

    movies_url = "https://raw.githubusercontent.com/vega/vega-datasets/refs/heads/main/data/movies.json"
    movies_df = pd.read_json(movies_url)

    print(f"✅ Loaded {len(movies_df)} movies")
    print(f"   Columns: {list(movies_df.columns)}")

    for col in movies_df.columns:
        if movies_df[col].dtype == "object":
            non_null_values = movies_df[col].dropna()
            if len(non_null_values) > 0:
                try:
                    pd.to_numeric(non_null_values, errors="raise")
                    movies_df[col] = pd.to_numeric(movies_df[col], errors="coerce")
                except (ValueError, TypeError):
                    movies_df[col] = movies_df[col].astype("string")

    spark_df = spark.createDataFrame(movies_df)

    spark_df.createOrReplaceTempView("movies")
    print("✅ Movies data registered as 'movies' table in Spark")

    arrow_schema = spark_df.schema.json()
    pandas_df_clean = spark_df.toPandas()
    arrow_schema = pa.Schema.from_pandas(pandas_df_clean)
    return arrow_schema


def main():
    """Main example function."""
    if not SPARK_AVAILABLE:
        return

    print("=" * 80)
    print("VegaFusion pre_transform_spec_vendor Example with Apache Spark")
    print("=" * 80)

    spark = setup_spark_session()

    try:
        movies_schema = load_and_register_movies_data(spark)

        spark_executor = create_spark_executor(spark)

        spec = get_spec()

        print("\n🔧 Running pre_transform_spec_vendor...")
        print(f"   Using Arrow schema with {len(movies_schema)} fields:")
        for field in movies_schema:
            print(f"     - {field.name}: {field.type}")

        transformed_spec, warnings = vf.runtime.pre_transform_spec_vendor(
            spec=spec,
            output_format="sparksql",
            executor=spark_executor,
            local_tz="UTC",
            preserve_interactivity=False,
            inline_datasets={"movies": movies_schema},
        )

        print("\n✅ Transformation completed!")
        print(f"   Warnings: {len(warnings)}")

        if warnings:
            print("⚠️  Warnings:")
            for warning in warnings:
                print(f"   - {warning['type']}: {warning['message']}")

        print("\n📋 Transformed specification:")
        print(f"   Data sources: {len(transformed_spec.get('data', []))}")

        for data_source in transformed_spec.get("data", []):
            if "values" in data_source:
                values = data_source["values"]
                print(f"   - '{data_source['name']}': {len(values)} inline rows")
            elif "url" in data_source:
                print(
                    f"   - '{data_source['name']}': external URL ({data_source['url']})"
                )

        print("\n🎯 Example completed successfully!")

    finally:
        print("\n🧹 Stopping Spark session...")
        spark.stop()
        print("✅ Cleanup completed")


def get_spec() -> dict[str, Any]:
    """
    Return a Vega specification that creates a histogram of IMDB ratings.
    Based on https://vega.github.io/editor/#/examples/vega/histogram-null-values
    """
    return {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "description": "A histogram of film IMDB ratings from the movies dataset.",
        "width": 500,
        "height": 300,
        "padding": 5,
        "autosize": {"type": "fit", "resize": True},
        "signals": [
            {
                "name": "maxbins",
                "value": 15,
                "bind": {"input": "select", "options": [5, 10, 15, 20]},
            },
            {"name": "binCount", "update": "(bins.stop - bins.start) / bins.step"},
            {"name": "nullGap", "value": 10},
            {"name": "barStep", "update": "(width - nullGap) / (1 + binCount)"},
        ],
        "data": [
            {
                "name": "movies_table",
                "url": "vegafusion+dataset://movies",
                "transform": [
                    {"type": "extent", "field": "IMDB Rating", "signal": "extent"},
                    {
                        "type": "bin",
                        "signal": "bins",
                        "field": "IMDB Rating",
                        "extent": {"signal": "extent"},
                        "maxbins": {"signal": "maxbins"},
                    },
                ],
            },
            {
                "name": "rating_counts",
                "source": "movies_table",
                "transform": [
                    {"type": "filter", "expr": "datum['IMDB Rating'] != null"},
                    {"type": "aggregate", "groupby": ["bin0", "bin1"]},
                ],
            },
            {
                "name": "null_ratings",
                "source": "movies_table",
                "transform": [
                    {"type": "filter", "expr": "datum['IMDB Rating'] == null"},
                    {"type": "aggregate", "groupby": []},
                ],
            },
            {
                "name": "genre_summary",
                "source": "movies_table",
                "transform": [
                    {"type": "filter", "expr": "datum['Major Genre'] != null"},
                    {
                        "type": "aggregate",
                        "groupby": ["Major Genre"],
                        "fields": ["IMDB Rating", "Production Budget"],
                        "ops": ["mean", "mean"],
                        "as": ["avg_rating", "avg_budget"],
                    },
                    {
                        "type": "filter",
                        "expr": "datum.count > 5",
                    },
                ],
            },
        ],
        "scales": [
            {
                "name": "yscale",
                "type": "linear",
                "range": "height",
                "round": True,
                "nice": True,
                "domain": {
                    "fields": [
                        {"data": "rating_counts", "field": "count"},
                        {"data": "null_ratings", "field": "count"},
                    ]
                },
            },
            {
                "name": "xscale",
                "type": "linear",
                "range": [{"signal": "barStep + nullGap"}, {"signal": "width"}],
                "round": True,
                "domain": {"signal": "[bins.start, bins.stop]"},
                "bins": {"signal": "bins"},
            },
            {
                "name": "xscale-null",
                "type": "band",
                "range": [0, {"signal": "barStep"}],
                "round": True,
                "domain": [None],
            },
        ],
        "axes": [
            {
                "orient": "bottom",
                "scale": "xscale",
                "title": "IMDB Rating",
                "tickMinStep": 0.5,
            },
            {"orient": "bottom", "scale": "xscale-null", "title": "Null Values"},
            {
                "orient": "left",
                "scale": "yscale",
                "title": "Number of Movies",
                "tickCount": 5,
                "offset": 5,
            },
        ],
        "marks": [
            {
                "type": "rect",
                "from": {"data": "rating_counts"},
                "encode": {
                    "update": {
                        "x": {"scale": "xscale", "field": "bin0", "offset": 1},
                        "x2": {"scale": "xscale", "field": "bin1"},
                        "y": {"scale": "yscale", "field": "count"},
                        "y2": {"scale": "yscale", "value": 0},
                        "fill": {"value": "steelblue"},
                        "stroke": {"value": "white"},
                        "strokeWidth": {"value": 1},
                    },
                    "hover": {"fill": {"value": "firebrick"}},
                },
            },
            {
                "type": "rect",
                "from": {"data": "null_ratings"},
                "encode": {
                    "update": {
                        "x": {"scale": "xscale-null", "value": None, "offset": 1},
                        "x2": {"scale": "xscale-null", "band": 1},
                        "y": {"scale": "yscale", "field": "count"},
                        "y2": {"scale": "yscale", "value": 0},
                        "fill": {"value": "#aaa"},
                        "stroke": {"value": "white"},
                        "strokeWidth": {"value": 1},
                    },
                    "hover": {"fill": {"value": "firebrick"}},
                },
            },
        ],
    }


if __name__ == "__main__":
    main()
