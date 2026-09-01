from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sonicwave_ingestion.schemas.plays import silver_plays_schema
from sonicwave_ingestion.silver import (
    save_silver,
    silver_facts,
    silver_load_bronze,
    silver_load_current_silver,
)
from sonicwave_ingestion.validates import prepare_silver_plays


def run_silver_plays(
    spark: SparkSession,
    source_path: str,
    snapshot_date: str,
    silver_path: str,
) -> None:
    silver_plays_loaded, cast_errors = silver_load_bronze(
        spark,
        source_path,
        silver_plays_schema,
        snapshot_date,
        derived_columns={
            "event_date": F.to_date("played_at"),
            "late_arriving_data": F.to_date("created_at") > F.to_date("played_at"),
        },
    )
    deduplicated_plays, plays_errors = prepare_silver_plays(silver_plays_loaded, cast_errors)
    processed_snapshot_dates = {
        row["snapshot_date"]
        for row in silver_plays_loaded.select("snapshot_date").distinct().collect()
    }

    save_silver(
        plays_errors,
        "plays_errors",
        silver_path,
        clear_empty_snapshot_dates=processed_snapshot_dates,
    )

    current_silver_path = Path(silver_path) / "plays"
    if current_silver_path.exists():
        current_silver = silver_load_current_silver(spark, str(current_silver_path))
    else:
        current_silver = spark.createDataFrame([], deduplicated_plays.schema)

    silver_plays = silver_facts(current_silver, deduplicated_plays, "play_sk")

    save_silver(
        silver_plays,
        "plays",
        silver_path,
        clear_empty_snapshot_dates=processed_snapshot_dates,
    )
