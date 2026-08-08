from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import SparkSession

if __package__ in {None, ""}:
    # Allow direct execution from the repository root.
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from sonicwave_ingestion.pipelines import run_silver_users


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Silver users ingestion pipeline for local Bronze parquet."
    )
    parser.add_argument(
        "--source",
        dest="source_path",
        required=True,
        help="Path to the Bronze parquet dataset, e.g. ./data/bronze/users",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    spark = SparkSession.builder.appName("silver-users-pipeline").getOrCreate()
    try:
        run_silver_users(
            spark=spark,
            source_path=args.source_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
