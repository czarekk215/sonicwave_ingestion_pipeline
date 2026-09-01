from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from sonicwave_ingestion.pipelines.silver_plays_pipeline import run_silver_plays
from sonicwave_ingestion.schemas.plays import bronze_plays_schema, silver_plays_schema
from sonicwave_ingestion.validates import deduplicate_plays, validate_plays

SILVER_PLAYS_WITH_SK_SCHEMA = StructType(
    [*silver_plays_schema.fields, StructField("play_sk", LongType(), False)]
)
PLAYS_ERROR_SCHEMA = StructType(
    [
        *silver_plays_schema.fields,
        StructField("error_reason", StringType(), False),
        StructField("error_stage", StringType(), False),
    ]
)


@pytest.fixture
def pipeline_workdir(tmp_path: Path) -> Path:
    return tmp_path


def _fixture_path(data_dir: Path, filename: str) -> Path:
    return data_dir / "silver_plays_new_rows" / filename


def _validation_fixture_path(data_dir: Path, filename: str) -> Path:
    return data_dir / "silver_plays_validation" / filename


def _late_arriving_fixture_path(data_dir: Path, filename: str) -> Path:
    return data_dir / "silver_plays_late_arriving" / filename


def _write_bronze_parquet(
    spark: SparkSession,
    source_json_path: Path,
    target_path: Path,
) -> None:
    (
        spark.read.schema(bronze_plays_schema)
        .json(str(source_json_path))
        .write.mode("overwrite")
        .parquet(str(target_path))
    )


def _read_expected_silver(spark: SparkSession, expected_json_path: Path) -> DataFrame:
    return spark.read.schema(SILVER_PLAYS_WITH_SK_SCHEMA).json(str(expected_json_path))


def _read_expected_errors(spark: SparkSession, expected_json_path: Path) -> DataFrame:
    return spark.read.schema(PLAYS_ERROR_SCHEMA).json(str(expected_json_path))


def _ordered_rows(df: DataFrame, schema: StructType, *order_by: str) -> list:
    columns = schema.fieldNames()
    return df.select(*columns).orderBy(*order_by).collect()


def _nullable_schema(*nullable_columns: str) -> StructType:
    return StructType(
        [
            StructField(
                field.name,
                field.dataType,
                True if field.name in nullable_columns else field.nullable,
            )
            for field in silver_plays_schema.fields
        ]
    )


@pytest.mark.parametrize(
    ("user_id", "ms_played", "expected_reason"),
    [
        (None, 180000, "null_user_id"),
        (10, 0, "non_positive_ms_played"),
    ],
)
def test_validate_plays_flags_invalid_rows(
    spark: SparkSession,
    user_id: int | None,
    ms_played: int,
    expected_reason: str,
) -> None:
    plays_df = spark.createDataFrame(
        [
            {
                "play_id": 1,
                "user_id": user_id,
                "content_id": 20,
                "device_id": 1,
                "played_at": datetime(2026, 3, 1, 12, 0, 0),
                "event_date": date(2026, 3, 1),
                "created_at": datetime(2026, 3, 1, 12, 0, 0),
                "late_arriving_data": False,
                "ms_played": ms_played,
                "bronze_ingested_at": datetime(2026, 3, 1, 13, 0, 0),
                "source_file": "file:///bronze/2026-03-01/plays.json",
                "snapshot_date": date(2026, 3, 1),
            }
        ],
        schema=_nullable_schema("user_id"),
    )

    _, invalid_df = validate_plays(plays_df)
    invalid_row = invalid_df.select("error_reason").first()

    assert invalid_row is not None
    assert invalid_row["error_reason"] == expected_reason


def test_deduplicate_plays_keeps_most_recent_record(spark: SparkSession) -> None:
    plays_df = spark.createDataFrame(
        [
            {
                "play_id": 1,
                "user_id": 10,
                "content_id": 20,
                "device_id": 1,
                "played_at": datetime(2026, 3, 1, 12, 0, 0),
                "event_date": date(2026, 3, 1),
                "created_at": datetime(2026, 3, 1, 12, 0, 0),
                "late_arriving_data": False,
                "ms_played": 180000,
                "bronze_ingested_at": datetime(2026, 3, 1, 13, 0, 0),
                "source_file": "file:///bronze/2026-03-01/plays_a.json",
                "snapshot_date": date(2026, 3, 1),
            },
            {
                "play_id": 1,
                "user_id": 10,
                "content_id": 20,
                "device_id": 1,
                "played_at": datetime(2026, 3, 1, 12, 0, 0),
                "event_date": date(2026, 3, 1),
                "created_at": datetime(2026, 3, 1, 12, 5, 0),
                "late_arriving_data": False,
                "ms_played": 181000,
                "bronze_ingested_at": datetime(2026, 3, 1, 14, 0, 0),
                "source_file": "file:///bronze/2026-03-01/plays_b.json",
                "snapshot_date": date(2026, 3, 1),
            },
        ],
        schema=silver_plays_schema,
    )

    deduplicated_df, duplicates_df = deduplicate_plays(plays_df)
    kept_row = deduplicated_df.first()
    duplicate_row = duplicates_df.select("error_reason").first()

    assert deduplicated_df.count() == 1
    assert duplicates_df.count() == 1
    assert kept_row is not None
    assert duplicate_row is not None
    assert kept_row["ms_played"] == 181000
    assert duplicate_row["error_reason"] == "duplicate_play_id"


def test_run_silver_plays_uses_newest_snapshot_and_assigns_sk(
    spark: SparkSession,
    data_dir: Path,
    pipeline_workdir: Path,
) -> None:
    bronze_path = pipeline_workdir / "data" / "bronze" / "plays"
    silver_path = pipeline_workdir / "data" / "silver" / "plays"
    errors_path = pipeline_workdir / "data" / "silver" / "plays_errors"

    _write_bronze_parquet(
        spark,
        _fixture_path(data_dir, "bronze_one_snapshot.json"),
        bronze_path,
    )

    run_silver_plays(spark, str(bronze_path), "2026-03-01", str(silver_path.parent))

    first_actual = spark.read.parquet(str(silver_path))
    first_expected = _read_expected_silver(
        spark,
        _fixture_path(data_dir, "silver_after_first_snapshot.json"),
    )

    assert _ordered_rows(first_actual, SILVER_PLAYS_WITH_SK_SCHEMA, "play_sk") == _ordered_rows(
        first_expected,
        SILVER_PLAYS_WITH_SK_SCHEMA,
        "play_sk",
    )
    assert not errors_path.exists()

    _write_bronze_parquet(
        spark,
        _fixture_path(data_dir, "bronze_two_snapshots.json"),
        bronze_path,
    )

    run_silver_plays(spark, str(bronze_path), "2026-03-02", str(silver_path.parent))

    reloaded_actual = spark.read.parquet(str(silver_path))
    reloaded_expected = _read_expected_silver(
        spark,
        _fixture_path(data_dir, "silver_after_reload.json"),
    )

    assert _ordered_rows(reloaded_actual, SILVER_PLAYS_WITH_SK_SCHEMA, "play_sk") == _ordered_rows(
        reloaded_expected,
        SILVER_PLAYS_WITH_SK_SCHEMA,
        "play_sk",
    )
    assert not errors_path.exists()


def test_run_silver_plays_redrop_preserves_existing_partition_rows(
    spark: SparkSession,
    data_dir: Path,
    pipeline_workdir: Path,
) -> None:
    bronze_path = pipeline_workdir / "data" / "bronze" / "plays"
    silver_path = pipeline_workdir / "data" / "silver" / "plays"
    initial_source = _fixture_path(data_dir, "bronze_one_snapshot.json")

    _write_bronze_parquet(spark, initial_source, bronze_path)
    run_silver_plays(spark, str(bronze_path), "2026-03-01", str(silver_path.parent))

    redrop_ingested_at = datetime(2026, 3, 1, 11, 0, 0)
    initial_bronze = spark.read.parquet(str(bronze_path)).withColumn(
        "ingested_at",
        F.lit(redrop_ingested_at),
    )
    redropped_bronze = initial_bronze.unionByName(
        spark.createDataFrame(
            [
                {
                    "play_id": "102",
                    "user_id": "3",
                    "content_id": "12",
                    "device_id": "3",
                    "played_at": "2026-03-01 02:00:00",
                    "created_at": "2026-03-01 02:00:00",
                    "ms_played": "300000",
                    "ingested_at": redrop_ingested_at,
                    "source_file": "file:///bronze/2026-03-01/plays_redrop.json",
                    "snapshot_date": date(2026, 3, 1),
                }
            ],
            schema=bronze_plays_schema,
        )
    ).localCheckpoint(eager=True)
    redropped_bronze.write.mode("overwrite").parquet(str(bronze_path))

    run_silver_plays(spark, str(bronze_path), "2026-03-01", str(silver_path.parent))

    actual = spark.read.parquet(str(silver_path))

    assert actual.count() == 3
    assert actual.filter(F.col("snapshot_date") == date(2026, 3, 1)).count() == 3
    assert actual.filter(F.col("play_id") == 100).count() == 1
    assert actual.filter(F.col("play_id") == 101).count() == 1
    assert actual.filter(F.col("play_id") == 102).count() == 1


def test_run_silver_plays_routes_invalid_rows_to_validation_errors(
    spark: SparkSession,
    data_dir: Path,
    pipeline_workdir: Path,
) -> None:
    bronze_path = pipeline_workdir / "data" / "bronze" / "plays"
    silver_path = pipeline_workdir / "data" / "silver" / "plays"
    errors_path = pipeline_workdir / "data" / "silver" / "plays_errors"

    _write_bronze_parquet(
        spark,
        _validation_fixture_path(data_dir, "bronze_with_validation_errors.json"),
        bronze_path,
    )

    run_silver_plays(spark, str(bronze_path), "2026-03-05", str(silver_path.parent))

    actual_silver = spark.read.parquet(str(silver_path))
    expected_silver = _read_expected_silver(
        spark,
        _validation_fixture_path(data_dir, "silver_valid_after_validation.json"),
    )
    actual_errors = spark.read.parquet(str(errors_path))
    expected_errors = _read_expected_errors(
        spark,
        _validation_fixture_path(data_dir, "silver_validation_errors.json"),
    )

    assert _ordered_rows(actual_silver, SILVER_PLAYS_WITH_SK_SCHEMA, "play_sk") == _ordered_rows(
        expected_silver,
        SILVER_PLAYS_WITH_SK_SCHEMA,
        "play_sk",
    )
    assert _ordered_rows(
        actual_errors,
        PLAYS_ERROR_SCHEMA,
        "error_stage",
        "play_id",
    ) == _ordered_rows(
        expected_errors,
        PLAYS_ERROR_SCHEMA,
        "error_stage",
        "play_id",
    )


def test_run_silver_plays_sets_late_arriving_flag_for_delayed_records(
    spark: SparkSession,
    data_dir: Path,
    pipeline_workdir: Path,
) -> None:
    bronze_path = pipeline_workdir / "data" / "bronze" / "plays"
    silver_path = pipeline_workdir / "data" / "silver" / "plays"
    errors_path = pipeline_workdir / "data" / "silver" / "plays_errors"

    _write_bronze_parquet(
        spark,
        _late_arriving_fixture_path(data_dir, "bronze_with_late_arriving.json"),
        bronze_path,
    )

    run_silver_plays(spark, str(bronze_path), "2026-03-04", str(silver_path.parent))
    run_silver_plays(spark, str(bronze_path), "2026-03-03", str(silver_path.parent))

    actual_silver = spark.read.parquet(str(silver_path))
    expected_silver = _read_expected_silver(
        spark,
        _late_arriving_fixture_path(data_dir, "silver_with_late_arriving.json"),
    )

    assert _ordered_rows(actual_silver, SILVER_PLAYS_WITH_SK_SCHEMA, "play_sk") == _ordered_rows(
        expected_silver,
        SILVER_PLAYS_WITH_SK_SCHEMA,
        "play_sk",
    )
    assert [row["play_id"] for row in actual_silver.filter("late_arriving_data").collect()] == [201]
    assert actual_silver.filter(F.col("play_id") == 201).first()["event_date"] == date(2026, 3, 2)
    assert not errors_path.exists()
