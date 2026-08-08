from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

bronze_users_schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("plan_tier", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("snapshot_date", DateType(), False),
    ]
)

silver_users_schema = StructType(
    [
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), False),
        StructField("country", StringType(), False),
        StructField("plan_tier", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), True),
        StructField("bronze_ingested_at", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("snapshot_date", DateType(), False),
    ]
)
