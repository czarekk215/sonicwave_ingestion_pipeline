from pyspark.sql import SparkSession

from sonicwave_ingestion.bronze import bronze_load, bronze_save, bronze_save_errors
from sonicwave_ingestion.schemas import bronze_users_schema


def run_bronze_users(
    spark: SparkSession,
    source_path: str,
    snapshot_date: str,
) -> None:
    users_df, corrupt_rows = bronze_load(
        spark=spark,
        path=source_path,
        schema=bronze_users_schema,
        snapshot_date=snapshot_date,
    )

    bronze_save(data=users_df, source_name="users")

    if not corrupt_rows.isEmpty():
        bronze_save_errors(data=corrupt_rows, source_name="users")
