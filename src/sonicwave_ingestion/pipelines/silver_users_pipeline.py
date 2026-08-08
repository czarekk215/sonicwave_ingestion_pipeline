from pathlib import Path

from pyspark.sql import SparkSession

from sonicwave_ingestion.schemas.users import silver_users_schema
from sonicwave_ingestion.silver import (
    replace_silver,
    save_silver,
    silver_load_bronze,
    silver_load_current_silver,
    silver_scd2,
)
from sonicwave_ingestion.validates import prepare_silver_users


def run_silver_users(spark: SparkSession, source_path: str) -> None:
    silver_users_loaded, cast_errors = silver_load_bronze(spark, source_path, silver_users_schema)
    deduplicated_users, users_errors = prepare_silver_users(silver_users_loaded, cast_errors)

    if not users_errors.isEmpty():
        save_silver(users_errors, "users_errors")

    current_silver_path = Path("data/silver/users")
    if current_silver_path.exists():
        current_silver = silver_load_current_silver(spark, str(current_silver_path))
    else:
        current_silver = spark.createDataFrame([], deduplicated_users.schema)

    silver_users = silver_scd2(current_silver, deduplicated_users)

    replace_silver(silver_users, "users")
