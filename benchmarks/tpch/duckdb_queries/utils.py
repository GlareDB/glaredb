import timeit
from importlib.metadata import version
from os.path import join

import duckdb
import polars as pl
from duckdb import DuckDBPyRelation
from linetimer import CodeTimer, linetimer
from polars import testing as pl_test

from utils import (
    DATASET_BASE_DIR,
    FILE_TYPE,
    INCLUDE_IO,
    LOG_TIMINGS,
    SHOW_OUTPUT,
    append_row,
)


def _scan_ds(path: str):
    path = f"{path}.{FILE_TYPE}"
    if FILE_TYPE == "parquet":
        if INCLUDE_IO:
            duckdb.read_parquet(path)
            return f"'{path}'"
        else:
            name = path.replace("/", "_").replace(".", "_")
            duckdb.sql(
                f"create temp table if not exists {name} as select * from read_parquet('{path}');"
            )
            return name
    elif FILE_TYPE == "feather":
        raise ValueError("duckdb does not support feather for now")
    else:
        raise ValueError(f"file type: {FILE_TYPE} not expected")
    return path


def get_line_item_ds(base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(join(base_dir, "lineitem"))


def get_orders_ds(base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(join(base_dir, "orders"))


def get_customer_ds(base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(join(base_dir, "customer"))


def get_region_ds(base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(join(base_dir, "region"))


def get_nation_ds(base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(join(base_dir, "nation"))


def get_supplier_ds(base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(join(base_dir, "supplier"))


def get_part_ds(base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(join(base_dir, "part"))


def get_part_supp_ds(base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(join(base_dir, "partsupp"))


def run_query(q_num: int, context: DuckDBPyRelation):
    @linetimer(name=f"Overall execution of duckdb Query {q_num}", unit="ms")
    def query():
        with CodeTimer(name=f"Get result of duckdb Query {q_num}", unit="ms"):
            t0 = timeit.default_timer()
            # force duckdb to materialize
            result = context.pl()

            secs = timeit.default_timer() - t0

        if LOG_TIMINGS:
            append_row(
                solution="duckdb", version=version("duckdb"), q=f"q{q_num}", secs=secs
            )

        if SHOW_OUTPUT:
            print(result)

    query()
