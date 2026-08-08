from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

PROVENANCE_COLUMNS = {"ingested_at", "source_file", "snapshot_date"}
CORRUPT_RECORD_COLUMN = "_corrupt_record"


def bronze_load(
    spark: SparkSession, path: str, schema: StructType, snapshot_date: str
) -> tuple[DataFrame, DataFrame]:
    source_snapshot_path = f"{path}/{snapshot_date}"
    spark.catalog.clearCache()
    spark.catalog.refreshByPath(source_snapshot_path)

    source_schema = StructType(
        [field for field in schema.fields if field.name not in PROVENANCE_COLUMNS]
    )
    permissive_schema = StructType(
        [*source_schema.fields, StructField(CORRUPT_RECORD_COLUMN, StringType(), True)]
    )

    raw_df = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .schema(permissive_schema)
        .json(source_snapshot_path)
    )
    with_provenance = (
        raw_df.withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("snapshot_date", F.to_date(F.lit(snapshot_date)))
    )
    with_provenance = with_provenance.cache()
    with_provenance.count()

    valid_df = with_provenance.filter(F.col(CORRUPT_RECORD_COLUMN).isNull()).drop(
        CORRUPT_RECORD_COLUMN
    )
    error_df = (
        with_provenance.filter(F.col(CORRUPT_RECORD_COLUMN).isNotNull())
        .withColumn("error_reason", F.col(CORRUPT_RECORD_COLUMN))
        .drop(CORRUPT_RECORD_COLUMN)
    )

    return valid_df, error_df


def bronze_save(data: DataFrame, source_name: str) -> None:
    (
        data.write.option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(f"./data/bronze/{source_name}")
    )


def bronze_save_errors(data: DataFrame, source_name: str) -> None:
    (
        data.write.option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(f"./data/bronze/{source_name}_errors")
    )
