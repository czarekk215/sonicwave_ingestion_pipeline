from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

if __package__ in {None, ""}:
    # Allow direct execution from the repository root.
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from sonicwave_ingestion.pipelines import run_bronze_plays


def _parse_snapshot_date(value: str) -> str:
    return date.fromisoformat(value).isoformat()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Bronze plays ingestion pipeline for a local snapshot."
    )
    parser.add_argument(
        "--source",
        dest="source_path",
        required=True,
        help="Base directory with snapshot folders, e.g. ./data/source/plays",
    )
    parser.add_argument(
        "--snapshot-date",
        required=True,
        type=_parse_snapshot_date,
        help="Snapshot date in ISO format (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--bronze-path",
        default="./data/bronze",
        help="Base directory for Bronze parquet output.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    spark = SparkSession.builder.appName("bronze-plays-pipeline").getOrCreate()
    try:
        run_bronze_plays(
            spark=spark,
            source_path=args.source_path,
            snapshot_date=args.snapshot_date,
            bronze_path=args.bronze_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
