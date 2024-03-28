"""
How to run this script:
======================

$ python3 -m venv venv
$ source venv/bin/activate
$ pip install "pyiceberg[pyarrow,sql-sqlite]"
$ pip install botocore
$ python ./testdata/generate_pyiceberg.py
"""

import os

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

import pyarrow.parquet as pq


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

WAREHOUSE_PATH = f"{SCRIPT_DIR}/iceberg"

TAXI_DATA_SRC = f"{SCRIPT_DIR}/iceberg/source_data/yellow_tripdata_2023-01.parquet"

catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
        "warehouse": f"file://{WAREHOUSE_PATH}",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
    },
)

df = pq.read_table(TAXI_DATA_SRC)

try:
    catalog.create_namespace("pyiceberg")
except NamespaceAlreadyExistsError:
    print("Warehouse already exists")

try:
    _table = catalog.create_table(
        "pyiceberg.taxi_dataset",
        schema=df.schema,
    )
except TableAlreadyExistsError:
    print("Table already exists in the catalog")

table = catalog.load_table("pyiceberg.taxi_dataset")
table.overwrite(df)

num_rows = len(table.scan().to_arrow())

print(f"Successfully created table with {num_rows} rows")
