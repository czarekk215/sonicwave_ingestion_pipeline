from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

bronze_plays_schema = StructType(
    [
        StructField("play_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("content_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("played_at", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("ms_played", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("snapshot_date", DateType(), False),
    ]
)

silver_plays_schema = StructType(
    [
        StructField("play_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("content_id", IntegerType(), False),
        StructField("device_id", IntegerType(), False),
        StructField("played_at", TimestampType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("late_arriving_data", BooleanType(), False),
        StructField("ms_played", IntegerType(), False),
        StructField("bronze_ingested_at", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("snapshot_date", DateType(), False),
    ]
)
