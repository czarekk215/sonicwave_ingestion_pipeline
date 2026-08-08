from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_SILVER_PLAYS_PATH = "./data/silver/plays"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read and inspect the Silver plays parquet dataset."
    )
    parser.add_argument(
        "--path",
        default=DEFAULT_SILVER_PLAYS_PATH,
        help=f"Path to the parquet dataset. Default: {DEFAULT_SILVER_PLAYS_PATH}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of rows to display. Default: 20",
    )
    parser.add_argument(
        "--snapshot-date",
        help="Optional snapshot_date filter, for example 2026-03-03.",
    )
    parser.add_argument(
        "--order-by",
        default="played_at",
        help="Column used for sorting the output. Prefix with '-' for descending order.",
    )
    return parser


def _load_plays(spark: SparkSession, parquet_path: str, snapshot_date: str | None) -> DataFrame:
    dataset_path = Path(parquet_path)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Parquet path does not exist: {dataset_path}")

    df = spark.read.option("recursiveFileLookup", "true").parquet(str(dataset_path))
    if snapshot_date:
        df = df.filter(F.col("snapshot_date") == snapshot_date)

    return df


def _sort_dataframe(df: DataFrame, order_by: str) -> DataFrame:
    if not order_by:
        return df

    if order_by.startswith("-"):
        return df.orderBy(F.col(order_by[1:]).desc())

    return df.orderBy(F.col(order_by).asc())


def main() -> None:
    args = _build_parser().parse_args()

    spark = SparkSession.builder.master("local[*]").appName("view-silver-plays").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        plays_df = _load_plays(
            spark=spark,
            parquet_path=args.path,
            snapshot_date=args.snapshot_date,
        )
        plays_df = _sort_dataframe(plays_df, args.order_by)

        print(f"Reading parquet from: {Path(args.path).resolve()}")
        if args.snapshot_date:
            print(f"Applied snapshot_date filter: {args.snapshot_date}")

        print(f"Row count: {plays_df.count()}")
        plays_df.printSchema()
        plays_df.show(args.limit, truncate=False)
        plays_df.count()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
