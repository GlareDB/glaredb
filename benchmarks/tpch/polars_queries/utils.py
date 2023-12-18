import os
import timeit
from os.path import join
from linetimer import CodeTimer, linetimer

import polars as pl
from polars import testing as pl_test

from utils import (
    FILE_TYPE,
    DATASET_BASE_DIR,
    SHOW_OUTPUT,
    LOG_TIMINGS,
    append_row
)



def _scan_ds(path: str):
    path = f"{path}.{FILE_TYPE}"
    scan = pl.scan_parquet(path)
    return scan.collect().rechunk().lazy()

def get_line_item_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "lineitem"))


def get_orders_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "orders"))


def get_customer_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "customer"))


def get_region_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "region"))


def get_nation_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "nation"))


def get_supplier_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "supplier"))


def get_part_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "part"))


def get_part_supp_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "partsupp"))

def run_query(q_num: int, lp: pl.LazyFrame):
    with CodeTimer(name=f"Get result of polars Query {q_num}", unit="ms"):
        t0 = timeit.default_timer()
        result = lp.collect()

        secs = timeit.default_timer() - t0
        if LOG_TIMINGS:
            append_row(
                solution="polars", version=pl.__version__, q=f"q{q_num}", secs=secs
            )
        if SHOW_OUTPUT:
            print(result)
        # print(result)


    
