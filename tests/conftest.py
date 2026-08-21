"""Shared pytest fixtures.

The `spark` fixture is a local BATCH-mode SparkSession — safe to use in normalize_*
function tests, which only do standard (non-streaming) DataFrame operations. It is NOT
safe for actual Structured Streaming queries: those need checkpointing, which hits a
Windows-native-IO limitation this project doesn't work around locally (see
streaming/cdc_job.py's module docstring and docs/DECISIONS.md) — streaming itself is only
validated inside the `spark` Docker container, not in this test suite.
"""

import os

import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    os.environ.setdefault("PYSPARK_PYTHON", os.sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", os.sys.executable)

    session = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("steam-market-pipeline-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
