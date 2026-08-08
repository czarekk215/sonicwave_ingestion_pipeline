from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from sonicwave_ingestion.bronze import bronze_load
from sonicwave_ingestion.pipelines import run_bronze_plays
from sonicwave_ingestion.schemas import bronze_plays_schema, bronze_users_schema


def _plays_row() -> dict[str, str | None]:
    return {
        "play_id": "1000",
        "user_id": "1",
        "content_id": "10",
        "device_id": "1",
        "played_at": "2026-03-01T12:00:00",
        "created_at": "2026-03-01T12:00:00",
        "ms_played": "180000",
    }


def _users_row() -> dict[str, str | None]:
    return {
        "user_id": "1",
        "email": "alice@sonicwave.io",
        "country": "PL",
        "plan_tier": "free",
        "created_at": "2026-01-10T08:00:00",
        "updated_at": None,
    }


@pytest.mark.parametrize(
    ("table_name", "schema", "row_factory"),
    [
        ("plays", bronze_plays_schema, _plays_row),
        ("users", bronze_users_schema, _users_row),
    ],
)
def test_bronze_load_routes_corrupt_json_rows_to_quarantine(
    spark: SparkSession,
    tmp_path: Path,
    table_name: str,
    schema: StructType,
    row_factory: Callable[[], dict[str, str | None]],
) -> None:
    snapshot_date = "2026-03-02"
    source_dir = tmp_path / table_name / snapshot_date
    source_dir.mkdir(parents=True)
    payload = "\n".join([json.dumps(row_factory()), '{"broken_json":'])
    (source_dir / f"{table_name}.json").write_text(payload, encoding="utf-8")

    valid_df, error_df = bronze_load(spark, str(tmp_path / table_name), schema, snapshot_date)

    valid_row = valid_df.first()
    error_row = error_df.first()

    assert valid_df.count() == 1
    assert error_df.count() == 1
    assert valid_row is not None
    assert error_row is not None
    assert valid_row["snapshot_date"] == date.fromisoformat(snapshot_date)
    assert error_row["snapshot_date"] == date.fromisoformat(snapshot_date)
    assert error_row["error_reason"] is not None


@pytest.fixture
def bronze_workdir(spark_workdir: Path) -> Path:
    data_path = spark_workdir / "data"
    if data_path.exists():
        shutil.rmtree(data_path)
    return spark_workdir


def _write_source_rows(target_dir: Path, filename: str, rows: list[dict[str, str | None]]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(json.dumps(row) for row in rows)
    (target_dir / filename).write_text(payload, encoding="utf-8")


def test_run_bronze_plays_rerun_overwrites_snapshot_partition(
    spark: SparkSession,
    tmp_path: Path,
    bronze_workdir: Path,
) -> None:
    snapshot_date = "2026-03-02"
    source_path = tmp_path / "plays"
    source_snapshot_dir = source_path / snapshot_date
    bronze_path = bronze_workdir / "data" / "bronze" / "plays"

    first_rows: list[dict[str, str | None]] = [
        _plays_row(),
        {
            "play_id": "1001",
            "user_id": "2",
            "content_id": "11",
            "device_id": "2",
            "played_at": "2026-03-01T13:00:00",
            "created_at": "2026-03-01T13:00:00",
            "ms_played": "200000",
        },
    ]
    second_rows: list[dict[str, str | None]] = [
        {
            "play_id": "2000",
            "user_id": "7",
            "content_id": "15",
            "device_id": "4",
            "played_at": "2026-03-02T08:30:00",
            "created_at": "2026-03-02T08:30:00",
            "ms_played": "210000",
        }
    ]

    _write_source_rows(source_snapshot_dir, "plays.json", first_rows)
    run_bronze_plays(spark, str(source_path), snapshot_date)

    first_loaded = spark.read.parquet(str(bronze_path)).filter("snapshot_date = DATE'2026-03-02'")
    assert {row["play_id"] for row in first_loaded.select("play_id").collect()} == {"1000", "1001"}

    _write_source_rows(source_snapshot_dir, "plays.json", second_rows)
    run_bronze_plays(spark, str(source_path), snapshot_date)

    reloaded = spark.read.parquet(str(bronze_path)).filter("snapshot_date = DATE'2026-03-02'")
    assert reloaded.count() == 1
    assert {row["play_id"] for row in reloaded.select("play_id").collect()} == {"2000"}
