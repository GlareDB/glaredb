import logging
import pathlib

import pytest

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

import pyarrow.parquet as pq

import tests


logger = logging.getLogger("fixtures.iceberg")


@pytest.fixture
def pyiceberg_table() -> pathlib.Path:
    warehouse_path = tests.PKG_DIRECTORY.joinpath("testdata", "iceberg").absolute()
    taxi_data_source_path = warehouse_path.joinpath(
        "source_data",
        "yellow_tripdata_2023-01.parquet",
    )

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
        },
    )

    df = pq.read_table(taxi_data_source_path)

    NAMESPACE = "pyiceberg"
    db_dir = warehouse_path.joinpath(f"{NAMESPACE}.db")

    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        logger.warning(f"Catalog '{NAMESPACE}' already exists")

    TABLE_NAME = "taxi_dataset"

    try:
        table = catalog.create_table(
            f"{NAMESPACE}.{TABLE_NAME}",
            schema=df.schema,
        )
        table.overwrite(df)
        num_rows = len(table.scan().to_arrow())

        logger.info(f"Table '{TABLE_NAME}' created with {num_rows} rows.")

    except TableAlreadyExistsError:
        logger.warning(f"Table '{TABLE_NAME}' already exists in the catalog.")
        logger.info(f"To re-create, remove the existing table at {db_dir}")

    return db_dir.joinpath(TABLE_NAME)
