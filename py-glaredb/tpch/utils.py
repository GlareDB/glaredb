from os.path import join
import timeit

import os
from linetimer import CodeTimer, linetimer

FILE_TYPE = os.environ.get("FILE_TYPE", "parquet")
SHOW_OUTPUT = bool(os.environ.get("TPCH_SHOW_OUTPUT", False))
DATASET_BASE_DIR = "/Users/corygrinstead/Development/tpch/tables_scale_10"
LOG_TIMINGS = bool(os.environ.get("LOG_TIMINGS", False))


def _scan_ds(con, path: str, name):
    path = f"{path}.{FILE_TYPE}"

    q = f"create temp table {name} as select * from parquet_scan('{path}');"

    con.execute(q)
    return name
  
def get_line_item_ds(con, base_dir: str = DATASET_BASE_DIR, ) -> str:
    # p = join(base_dir, "lineitem")
    # return f"parquet_scan('{p}.parquet')"
    return _scan_ds(con, join(base_dir, "lineitem"), "lineitem")

def get_orders_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    # p = join(base_dir, "orders")
    # return f"parquet_scan('{p}.parquet')"
    return _scan_ds(con, join(base_dir, "orders"), "orders")


def get_customer_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    # p = join(base_dir, "customer")
    # return f"parquet_scan('{p}.parquet')"
    return _scan_ds(con, join(base_dir, "customer"), "customer")


def get_region_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    # p = join(base_dir, "region")
    # return f"parquet_scan('{p}.parquet')"
    return _scan_ds(con, join(base_dir, "region"), "region")


def get_nation_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    # p = join(base_dir, "nation")
    # return f"parquet_scan('{p}.parquet')"
    return _scan_ds(con, join(base_dir, "nation"), "nation")


def get_supplier_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    # p = join(base_dir, "supplier")
    # return f"parquet_scan('{p}.parquet')"
    return _scan_ds(con, join(base_dir, "supplier"), "supplier")


def get_part_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    # p = join(base_dir, "part")
    # return f"parquet_scan('{p}.parquet')"
    return _scan_ds(con, join(base_dir, "part"), "part")


def get_part_supp_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    # p = join(base_dir, "partsupp")
    # return f"parquet_scan('{p}.parquet')"
    return _scan_ds(con, join(base_dir, "partsupp"), "partsupp")

def run_query(q_num: int, con, query_str: str):
    with CodeTimer(name=f"Get result of glaredb Query {q_num}", unit="ms"):
        t0 = timeit.default_timer()
        result = con.execute(query_str)

        secs = timeit.default_timer() - t0
        if SHOW_OUTPUT:
            result.show()

