import logging
import pathlib
import sys
from urllib.request import urlretrieve

import pytest

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

import pyarrow.parquet as pq

import tests


logger = logging.getLogger("fixtures.iceberg")


@pytest.fixture
def pyiceberg_table(
    tmp_path_factory: pytest.TempPathFactory,
) -> pathlib.Path:
    warehouse_path = tests.PKG_DIRECTORY.joinpath("testdata", "iceberg").absolute()

    taxi_data_source_path = warehouse_path.joinpath(
        "source_data",
        "yellow_tripdata_2023-01.parquet",
    )

    # Check if the taxi_data_source_path exists, else download the data.
    if not taxi_data_source_path.is_file():
        logger.info(f"Downloading taxi_dataset at '{taxi_data_source_path}'")

        taxi_data_source_path, _ = urlretrieve(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
            taxi_data_source_path,
        )
    else:
        logger.warning(f"Dataset at path '{taxi_data_source_path}' already exists")
        logger.info("To re-create, remove the file and run the fixture again.")

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
