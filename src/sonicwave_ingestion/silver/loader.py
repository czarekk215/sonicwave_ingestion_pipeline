import shutil
from datetime import date
from functools import reduce
from operator import or_
from pathlib import Path
from uuid import uuid4

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def silver_load_bronze(
    spark: SparkSession,
    path: str,
    schema: StructType,
    snapshot_date: str,
    derived_columns: dict[str, Column] | None = None,
) -> tuple[DataFrame, DataFrame]:
    bronze = spark.read.parquet(path)

    df = bronze.filter(
        F.col("snapshot_date") == F.lit(snapshot_date).cast("date")
    ).withColumnRenamed("ingested_at", "bronze_ingested_at")

    if derived_columns:
        for column_name, expression in derived_columns.items():
            df = df.withColumn(column_name, expression)

    error_conditions = []
    typed_columns = []
    cast_error_reasons = []

    for field in schema.fields:
        raw_col = f"{field.name}_raw"

        df = df.withColumnRenamed(field.name, raw_col).withColumn(
            field.name, F.expr(f"try_cast({raw_col} as {field.dataType.simpleString()})")
        )

        error_conditions.append(F.col(raw_col).isNotNull() & F.col(field.name).isNull())
        typed_columns.append(field.name)
        cast_error_reasons.append(
            F.when(
                F.col(raw_col).isNotNull() & F.col(field.name).isNull(),
                F.lit(f"cast_error_{field.name}"),
            )
        )

    error_condition = reduce(or_, error_conditions)
    error_reason = F.concat_ws("; ", *cast_error_reasons)

    correct_df = df.filter(~error_condition).select(*typed_columns)

    error_df = (
        df.filter(error_condition)
        .withColumn("error_reason", error_reason)
        .select(*typed_columns, "error_reason")
    )

    return correct_df, error_df


def silver_load_current_silver(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def silver_facts(current_silver: DataFrame, loaded_silver: DataFrame, sk_name: str) -> DataFrame:
    window = Window.orderBy(F.monotonically_increasing_id())
    if current_silver.isEmpty():
        return loaded_silver.withColumn(sk_name, F.row_number().over(window).cast("long"))

    max_sk_row = current_silver.agg(F.max(sk_name).alias("max_sk")).first()
    max_sk = (max_sk_row["max_sk"] if max_sk_row is not None else 0) or 0

    existing_keys = current_silver.select("play_id")
    existing_partition_keys = current_silver.select("play_id", "snapshot_date", sk_name)

    existing_records = loaded_silver.join(
        existing_partition_keys,
        on=["play_id", "snapshot_date"],
        how="inner",
    )
    new_records = loaded_silver.join(existing_keys, on="play_id", how="left_anti").withColumn(
        sk_name,
        (F.row_number().over(window) + F.lit(max_sk)).cast("long"),
    )

    # Return the complete partition so dynamic overwrite cannot discard old rows.
    return existing_records.unionByName(new_records)


def save_silver(
    data: DataFrame,
    source_name: str,
    silver_path: str,
    clear_empty_snapshot_dates: set[date] | None = None,
) -> None:
    if clear_empty_snapshot_dates:
        written_snapshot_dates = {
            row["snapshot_date"] for row in data.select("snapshot_date").distinct().collect()
        }
        for snapshot_date in clear_empty_snapshot_dates - written_snapshot_dates:
            empty_partition_path = (
                Path(silver_path) / source_name / f"snapshot_date={snapshot_date}"
            )
            if empty_partition_path.exists():
                shutil.rmtree(empty_partition_path)

    if data.isEmpty():
        return

    (
        data.write.option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(f"{silver_path}/{source_name}")
    )


def replace_silver(data: DataFrame, source_name: str, silver_path: str) -> None:
    target_path = Path(silver_path) / source_name
    temporary_path = target_path.parent / f".{source_name}.tmp-{uuid4().hex}"
    backup_path = target_path.parent / f".{source_name}.backup-{uuid4().hex}"

    # Materialize the plan away from its input before touching the current dimension.
    data.write.mode("overwrite").partitionBy("snapshot_date").parquet(str(temporary_path))

    moved_current_dimension = False
    try:
        if target_path.exists():
            target_path.rename(backup_path)
            moved_current_dimension = True
        temporary_path.rename(target_path)
    except OSError:
        if moved_current_dimension and backup_path.exists() and not target_path.exists():
            backup_path.rename(target_path)
        raise
    else:
        if backup_path.exists():
            shutil.rmtree(backup_path)


def silver_scd2(current_silver: DataFrame, loaded_silver: DataFrame) -> DataFrame:
    window = Window.orderBy("user_id")
    tracked_columns = ["email", "country", "plan_tier"]
    dimension_columns = [
        "user_id",
        "email",
        "country",
        "plan_tier",
        "created_at",
        "updated_at",
        "bronze_ingested_at",
        "source_file",
        "snapshot_date",
    ]

    if current_silver.isEmpty():
        return (
            loaded_silver.withColumn("customer_sk", F.row_number().over(window).cast("long"))
            .withColumn("valid_from", F.coalesce(F.col("updated_at"), F.col("created_at")))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )

    current_active = current_silver.filter(F.col("is_current"))
    current_history = current_silver.filter(~F.col("is_current"))

    joined = loaded_silver.alias("loaded").join(
        current_active.alias("current"), on="user_id", how="left"
    )

    change_condition = reduce(
        or_,
        [
            F.coalesce(F.col(f"loaded.{column}").cast("string"), F.lit("__NULL__"))
            != F.coalesce(F.col(f"current.{column}").cast("string"), F.lit("__NULL__"))
            for column in tracked_columns
        ],
    )

    new_records = joined.filter(F.col("current.user_id").isNull()).select("loaded.*")
    changed_records = joined.filter(F.col("current.user_id").isNotNull() & change_condition).select(
        "loaded.*"
    )

    unchanged_current = current_active.join(
        changed_records.select("user_id"),
        on="user_id",
        how="left_anti",
    )

    closed_records = (
        current_active.alias("current")
        .join(
            changed_records.select(
                "user_id",
                F.coalesce(F.col("updated_at"), F.col("created_at")).alias("new_valid_from"),
            ).alias("changed"),
            on="user_id",
            how="inner",
        )
        .select(
            *[F.col(f"current.{column}").alias(column) for column in dimension_columns],
            F.col("current.customer_sk").alias("customer_sk"),
            F.col("current.valid_from").alias("valid_from"),
            F.col("changed.new_valid_from").alias("valid_to"),
            F.lit(False).alias("is_current"),
        )
    )

    new_versions_source = new_records.unionByName(changed_records)
    max_sk_row = current_silver.agg(F.max("customer_sk").alias("max_sk")).first()
    max_sk = (max_sk_row["max_sk"] if max_sk_row is not None else 0) or 0

    new_versions = (
        new_versions_source.withColumn(
            "customer_sk",
            (F.row_number().over(window) + F.lit(max_sk)).cast("long"),
        )
        .withColumn("valid_from", F.coalesce(F.col("updated_at"), F.col("created_at")))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    return (
        current_history.unionByName(unchanged_current)
        .unionByName(closed_records)
        .unionByName(new_versions)
    )
