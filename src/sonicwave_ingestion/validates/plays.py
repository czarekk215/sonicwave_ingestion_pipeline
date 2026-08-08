from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def validate_plays(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    error_reason = F.concat_ws(
        "; ",
        F.when(F.col("play_id").isNull(), F.lit("null_play_id")),
        F.when(F.col("user_id").isNull(), F.lit("null_user_id")),
        F.when(F.col("content_id").isNull(), F.lit("null_content_id")),
        F.when(F.col("device_id").isNull(), F.lit("null_device_id")),
        F.when(F.col("played_at").isNull(), F.lit("null_played_at")),
        F.when(F.col("created_at").isNull(), F.lit("null_created_at")),
        F.when(F.col("ms_played").isNull(), F.lit("null_ms_played")),
        F.when(F.col("ms_played") <= 0, F.lit("non_positive_ms_played")),
    )

    validated = df.withColumn("error_reason", error_reason)

    valid_df = validated.filter(F.col("error_reason") == "").drop("error_reason")
    invalid_df = validated.filter(F.col("error_reason") != "")

    return valid_df, invalid_df


def deduplicate_plays(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    window = Window.partitionBy("play_id").orderBy(
        F.col("bronze_ingested_at").desc(),
        F.col("created_at").desc(),
        F.col("source_file").desc(),
    )

    ranked = df.withColumn("row_num", F.row_number().over(window))

    deduplicated_df = ranked.filter(F.col("row_num") == 1).drop("row_num")
    duplicates_df = (
        ranked.filter(F.col("row_num") > 1)
        .drop("row_num")
        .withColumn("error_reason", F.lit("duplicate_play_id"))
    )

    return deduplicated_df, duplicates_df


def prepare_silver_plays(
    loaded_plays: DataFrame,
    cast_errors: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    valid_plays, validation_errors = validate_plays(loaded_plays)
    deduplicated_plays, duplicate_errors = deduplicate_plays(valid_plays)

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
                *loaded_plays.schema.fields,
                StructField("error_reason", StringType(), True),
                StructField("error_stage", StringType(), True),
            ]
        )
        return (
            deduplicated_plays,
            loaded_plays.sparkSession.createDataFrame([], empty_error_schema),
        )

    combined_errors = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        staged_errors[1:],
        staged_errors[0],
    )

    return deduplicated_plays, combined_errors
