from __future__ import annotations
from pathlib import Path
import json
import pytest

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import vl_convert as vlc
import vegafusion as vf
import pyarrow as pa
from typing import Optional

# TODO: cleanup this file

# Root directory that contains the Vega-Lite spec fixtures used by this test suite
SPEC_ROOT = (Path(__file__).parent / "specs").resolve()
SALES_DATA_PATH = SPEC_ROOT / "sales_data_1kk.parquet"
SALES_DATA_DF = pd.read_parquet(SALES_DATA_PATH)


def _discover_spec_files(limit: Optional[int] = None) -> list[Path]:
    specs_all = SPEC_ROOT.rglob("*.json")
    specs_filtered = [p for p in specs_all if not p.name.startswith("_")]
    specs_sorted = sorted(specs_filtered)
    return specs_sorted[:limit] if limit is not None else specs_sorted


@pytest.fixture(scope="session")
def spark():
    """Initialise a local SparkSession for the duration of the test session."""

    session: SparkSession = (
        SparkSession.builder.appName("vegafusion-e2e")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "8g")
        .master("local[2]")
        .getOrCreate()
    )

    # In the toolkit we fork Spark session and explicitly switch it to UTC
    # as it affects temporal fields handling
    session.sql("SET TIME ZONE 'UTC'")

    sales_data_df = session.read.parquet(str(SALES_DATA_PATH))

    # Convert datetime column from bigint (nanoseconds) to actual timestamp
    sales_data_df = sales_data_df.withColumn(
        "datetime", (col("datetime") / 1e9).cast("timestamp")
    )

    sales_data_df.createOrReplaceTempView("sales_data_1kk")

    yield session

    session.stop()


# Discover all spec fixtures once so that *pytest* can parametrise the test

# Filter out any non-Path values that might have slipped in (e.g. pytest.NOTSET)
_SPEC_FILES = [p for p in _discover_spec_files() if isinstance(p, Path)]


@pytest.mark.parametrize("spec_path", _SPEC_FILES, ids=[p.stem for p in _SPEC_FILES])
def test_spec_against_spark(spec_path: Path, spark: SparkSession):
    """End-to-end comparison between in-memory evaluation and Spark SQL.

    For every Vega-Lite spec we:

    1. Load the spec JSON and discover the associated datasets.
    2. Evaluate the spec with the *in-memory* Vegafusion runtime to obtain the
       expected result.
    3. Register the datasets as Spark SQL tables.
    4. Ask Vegafusion to generate the equivalent Spark SQL statements.
    5. Execute the SQL with Spark and collect the actual result.
    6. Compare *expected* vs *actual*.
    """

    print(f"Testing {spec_path.name}")

    vegalite_spec = json.loads(spec_path.read_text("utf8"))
    vega_spec = vlc.vegalite_to_vega(vegalite_spec)

    print("Aggregating data in memory")
    _, inmemory_datasets, _ = vf.runtime.pre_transform_extract(
        vega_spec,
        extract_threshold=0,
        extracted_format="pyarrow",
        local_tz="UTC",
        default_input_tz="UTC",
        preserve_interactivity=False,
        inline_datasets={"sales_data_1kk": SALES_DATA_DF},
    )

    print("Converting resulting Arrow tables to Pandas")
    inmemory_dataframes = {ds[0]: ds[2].to_pandas() for ds in inmemory_datasets}

    print("Generating SparkSQL for aggregation")
    _, spark_datasets_sql, _ = vf.runtime.pre_transform_logical_plan_vendor(
        vega_spec,
        output_format="sparksql",
        local_tz="UTC",
        default_input_tz="UTC",
        preserve_interactivity=False,
        inline_dataset_schemas={"sales_data_1kk": pa.Schema.from_pandas(SALES_DATA_DF)},
    )

    print("Executing received SparkSQL")
    print(spark_datasets_sql)
    spark_dataframes = {
        ds["name"]: spark.sql(ds["sparksql"]).toPandas()
        if "sparksql" in ds
        else ds["data"].to_pandas()
        for ds in spark_datasets_sql
        if ds["namespace"] == "data"
    }

    print("Inmemory datasets:", set(inmemory_dataframes.keys()))
    print("Spark datasets:", set(spark_dataframes.keys()))
    assert set(inmemory_dataframes.keys()) == set(
        spark_dataframes.keys()
    ), "Mismatch in returned dataframes"

    for name, expected_df in inmemory_dataframes.items():
        print("Comparing datasets", name)
        actual_df = spark_dataframes[name]
        expected_sorted = expected_df.sort_index(axis=1).reset_index(drop=True)
        actual_sorted = actual_df.sort_index(axis=1).reset_index(drop=True)
        pd.testing.assert_frame_equal(
            expected_sorted,
            actual_sorted,
            check_dtype=False,
        )
