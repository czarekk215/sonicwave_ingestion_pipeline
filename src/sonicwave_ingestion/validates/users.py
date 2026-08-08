from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def validate_users(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    error_reason = F.concat_ws(
        "; ",
        F.when(F.col("user_id").isNull(), F.lit("null_user_id")),
        F.when(F.col("email").isNull(), F.lit("null_email")),
        F.when(F.col("country").isNull(), F.lit("null_country")),
        F.when(F.col("plan_tier").isNull(), F.lit("null_plan_tier")),
        F.when(F.col("created_at").isNull(), F.lit("null_created_at")),
    )

    validated = df.withColumn("error_reason", error_reason)

    valid_df = validated.filter(F.col("error_reason") == "").drop("error_reason")
    invalid_df = validated.filter(F.col("error_reason") != "")

    return valid_df, invalid_df


def deduplicate_users(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    window = Window.partitionBy("user_id").orderBy(
        F.coalesce(F.col("updated_at"), F.col("created_at")).desc(),
        F.col("bronze_ingested_at").desc(),
        F.col("source_file").desc(),
    )

    ranked = df.withColumn("row_num", F.row_number().over(window))

    deduplicated_df = ranked.filter(F.col("row_num") == 1).drop("row_num")
    duplicates_df = (
        ranked.filter(F.col("row_num") > 1)
        .drop("row_num")
        .withColumn("error_reason", F.lit("duplicate_user_id"))
    )

    return deduplicated_df, duplicates_df


def prepare_silver_users(
    loaded_users: DataFrame,
    cast_errors: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    valid_users, validation_errors = validate_users(loaded_users)
    deduplicated_users, duplicate_errors = deduplicate_users(valid_users)

    staged_errors = [
        frame.withColumn("error_stage", F.lit(stage))
        for stage, frame in [
            ("cast", cast_errors),
            ("validation", validation_errors),
            ("deduplication", duplicate_errors),
        ]
        if not frame.isEmpty()
    ]

    if not staged_errors:
        empty_error_schema = StructType(
            [
                *loaded_users.schema.fields,
                StructField("error_reason", StringType(), True),
                StructField("error_stage", StringType(), True),
            ]
        )
        return (
            deduplicated_users,
            loaded_users.sparkSession.createDataFrame([], empty_error_schema),
        )

    combined_errors = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        staged_errors[1:],
        staged_errors[0],
    )

    return deduplicated_users, combined_errors
