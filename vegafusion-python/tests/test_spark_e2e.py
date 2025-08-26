from __future__ import annotations
from pathlib import Path
import json
import pytest

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, current_timezone
from pyspark.sql.pandas.types import to_arrow_schema
import vl_convert as vlc
import vegafusion as vf
import pyarrow as pa
from typing import Optional


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

    # TODO: this is required for properly handling temporal. We need to check if we can work around different
    # timezone or if we should require users to setup their Spark sessions to operate in UTC
    session.sql("SET TIME ZONE 'UTC'")

    sales_data_df = session.read.parquet(str(SALES_DATA_PATH))

    # Convert datetime column from bigint (nanoseconds) to actual timestamp
    sales_data_df = sales_data_df.withColumn(
        "datetime",
        to_utc_timestamp((col("datetime") / 1e9).cast("timestamp"), current_timezone()),
    )

    sales_data_df.createOrReplaceTempView("sales_data_1kk")

    yield session

    session.stop()


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

    print("Generating Pandas-based transformed spec")
    inmemory_spec, inmemory_datasets, _ = vf.runtime.pre_transform_extract(
        spec=vega_spec,
        preserve_interactivity=False,
        local_tz="UTC",
        default_input_tz="UTC",
        inline_datasets={"sales_data_1kk": SALES_DATA_DF},
        extract_threshold=0,
        extracted_format="pyarrow",
    )

    print("Generating Spark-backed results via custom executor")

    def spark_executor(sql_query: str) -> pa.Table:
        spark_df = spark.sql(sql_query)
        return pa.Table.from_pandas(spark_df.toPandas())

    vf_spark_runtime = vf.VegaFusionRuntime.new_vendor("sparksql", spark_executor)

    sales_schema = to_arrow_schema(spark.table("sales_data_1kk").schema)

    spark_spec, spark_datasets, _ = vf_spark_runtime.pre_transform_extract(
        spec=vega_spec,
        preserve_interactivity=False,
        local_tz="UTC",
        default_input_tz="UTC",
        inline_datasets={"sales_data_1kk": sales_schema},
        extract_threshold=0,
        extracted_format="pyarrow",
    )

    print("Comparing transformed specs (Pandas vs Spark)")

    assert spark_spec == inmemory_spec

    assert len(inmemory_datasets) == len(spark_datasets), (
        f"Dataset count mismatch: in-memory={len(inmemory_datasets)} "
        f"vs spark={len(spark_datasets)}"
    )

    for inmemory_ds, spark_ds in zip(inmemory_datasets, spark_datasets):
        inmemory_name, _, inmemory_data = inmemory_ds
        spark_name, _, spark_data = spark_ds

        assert inmemory_name == spark_name

        pd.testing.assert_frame_equal(
            inmemory_data.to_pandas(),
            spark_data.to_pandas(),
            check_dtype=False,
            check_like=True,
            atol=1e-6,
            rtol=1e-6,
        )
