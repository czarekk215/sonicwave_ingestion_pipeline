from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StructField, StructType, TimestampType

from sonicwave_ingestion.pipelines.silver_users_pipeline import run_silver_users
from sonicwave_ingestion.schemas.users import silver_users_schema
from sonicwave_ingestion.silver.loader import silver_scd2

SILVER_USERS_SCD2_SCHEMA = StructType(
    [
        *silver_users_schema.fields,
        StructField("customer_sk", LongType(), False),
        StructField("valid_from", TimestampType(), True),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
    ]
)


def _fixture_path(data_dir: Path, filename: str) -> Path:
    return data_dir / "silver_users_scd2" / filename


def _read_users(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.schema(silver_users_schema).json(str(path))


def _read_users_scd2(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.schema(SILVER_USERS_SCD2_SCHEMA).json(str(path))


def _ordered_rows(df: DataFrame) -> list:
    columns = SILVER_USERS_SCD2_SCHEMA.fieldNames()
    return df.select(*columns).orderBy("user_id", "customer_sk").collect()


@pytest.fixture
def pipeline_workdir(tmp_path: Path) -> Path:
    return tmp_path


def _write_bronze_users(spark: SparkSession, source_path: Path, target_path: Path) -> None:
    users = _read_users(spark, source_path)
    bronze_users = users.select(
        F.col("user_id").cast("string").alias("user_id"),
        "email",
        "country",
        "plan_tier",
        F.date_format("created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
        F.date_format("updated_at", "yyyy-MM-dd HH:mm:ss").alias("updated_at"),
        F.col("bronze_ingested_at").alias("ingested_at"),
        "source_file",
        "snapshot_date",
    )
    bronze_users.write.mode("overwrite").parquet(str(target_path))


def test_silver_scd2_initial_load(
    spark: SparkSession,
    data_dir: Path,
) -> None:
    loaded_users = _read_users(spark, _fixture_path(data_dir, "loaded_initial_users.json"))
    expected = _read_users_scd2(spark, _fixture_path(data_dir, "expected_initial_scd2.json"))
    current_empty = spark.createDataFrame([], SILVER_USERS_SCD2_SCHEMA)

    actual = silver_scd2(current_empty, loaded_users)

    assert _ordered_rows(actual) == _ordered_rows(expected)


def test_silver_scd2_closes_changed_record_and_adds_new_version(
    spark: SparkSession,
    data_dir: Path,
) -> None:
    current_users = _read_users_scd2(spark, _fixture_path(data_dir, "current_users_scd2.json"))
    loaded_users = _read_users(spark, _fixture_path(data_dir, "loaded_incremental_users.json"))
    expected = _read_users_scd2(spark, _fixture_path(data_dir, "expected_incremental_scd2.json"))

    actual = silver_scd2(current_users, loaded_users)

    assert _ordered_rows(actual) == _ordered_rows(expected)


def test_silver_scd2_rerun_same_snapshot_is_idempotent(
    spark: SparkSession,
    data_dir: Path,
) -> None:
    current_users = _read_users_scd2(
        spark,
        _fixture_path(data_dir, "expected_incremental_scd2.json"),
    )
    loaded_users = _read_users(spark, _fixture_path(data_dir, "loaded_incremental_users.json"))

    actual = silver_scd2(current_users, loaded_users)

    assert _ordered_rows(actual) == _ordered_rows(current_users)


def test_run_silver_users_replaces_dimension_after_materializing_result(
    spark: SparkSession,
    data_dir: Path,
    pipeline_workdir: Path,
) -> None:
    bronze_path = pipeline_workdir / "data" / "bronze" / "users"
    silver_path = pipeline_workdir / "data" / "silver" / "users"

    _write_bronze_users(
        spark,
        _fixture_path(data_dir, "loaded_initial_users.json"),
        bronze_path,
    )
    run_silver_users(spark, str(bronze_path), "2026-03-01", str(silver_path.parent))

    _write_bronze_users(
        spark,
        _fixture_path(data_dir, "loaded_incremental_users.json"),
        bronze_path,
    )
    run_silver_users(spark, str(bronze_path), "2026-03-02", str(silver_path.parent))

    actual = spark.read.parquet(str(silver_path))
    user_two_versions = actual.filter(F.col("user_id") == 2)
    current_user_two = user_two_versions.filter(F.col("is_current")).first()

    assert actual.count() == 4
    assert actual.filter(F.col("is_current")).count() == 3
    assert user_two_versions.count() == 2
    assert current_user_two is not None
    assert current_user_two["country"] == "CA"
