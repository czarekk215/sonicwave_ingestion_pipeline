from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, LongType, StructField, StructType, TimestampType

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
