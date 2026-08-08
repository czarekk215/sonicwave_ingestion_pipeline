from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
)

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.append(str(PROJECT_SRC))


@pytest.fixture(scope="session")
def spark_workdir(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    workdir = tmp_path_factory.mktemp("spark-workdir")
    previous_cwd = Path.cwd()
    os.chdir(workdir)
    yield workdir
    os.chdir(previous_cwd)


@pytest.fixture(scope="session")
def spark(spark_workdir: Path) -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("sonicwave-transforms-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def data_dir() -> Path:
    """Absolute path to tests/data/, regardless of where pytest is invoked."""
    return Path(__file__).parent / "data"


def read_json(spark: SparkSession, path: Path, schema: StructType) -> DataFrame:
    """Read a newline-delimited JSON file into a DataFrame with an explicit schema.

    A tiny helper so the file-based tests read the same way every time: known
    path, known schema, no inference.
    """
    return spark.read.schema(schema).json(str(path))
