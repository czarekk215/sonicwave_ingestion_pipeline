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


def run_silver_plays(spark: SparkSession, source_path: str) -> None:
    silver_plays_loaded, cast_errors = silver_load_bronze(
        spark,
        source_path,
        silver_plays_schema,
        derived_columns={
            "late_arriving_data": F.coalesce(
                (
                    F.col("bronze_ingested_at").cast("long")
                    - F.to_timestamp("snapshot_date").cast("long")
                )
                > F.lit(24 * 60 * 60),
                F.lit(False),
            )
        },
    )
    deduplicated_plays, plays_errors = prepare_silver_plays(silver_plays_loaded, cast_errors)

    if not plays_errors.isEmpty():
        save_silver(plays_errors, "plays_errors")

    current_silver_path = Path("data/silver/plays")
    if current_silver_path.exists():
        current_silver = silver_load_current_silver(spark, str(current_silver_path))
    else:
        current_silver = spark.createDataFrame([], deduplicated_plays.schema)

    silver_plays = silver_facts(current_silver, deduplicated_plays, "play_sk")

    if not silver_plays.isEmpty():
        save_silver(silver_plays, "plays")
