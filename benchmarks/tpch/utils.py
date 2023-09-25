from os.path import join
import timeit

import os
import re
import sys
from subprocess import run
from linetimer import CodeTimer, linetimer

FILE_TYPE = os.environ.get("FILE_TYPE", "parquet")
SHOW_OUTPUT = bool(os.environ.get("TPCH_SHOW_OUTPUT", False))
SCALE_FACTOR = int(os.environ.get("SCALE_FACTOR", "1"))
LOG_TIMINGS = bool(os.environ.get("LOG_TIMINGS", True))

CWD = os.path.dirname(os.path.realpath(__file__))
DATASET_BASE_DIR = os.path.join(CWD, f"tables_scale_{SCALE_FACTOR}")
TIMINGS_FILE = os.path.join(CWD, os.environ.get("TIMINGS_FILE", "timings.csv"))
INCLUDE_IO = bool(os.environ.get("INCLUDE_IO", False))


def _scan_ds(con, path: str, name):
    path = f"{path}.{FILE_TYPE}"

    q = f"create temp table {name} as select * from parquet_scan('{path}');"

    con.execute(q)
    return name


def get_line_item_ds(
    con,
    base_dir: str = DATASET_BASE_DIR,
) -> str:
    return _scan_ds(con, join(base_dir, "lineitem"), "lineitem")


def get_orders_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(con, join(base_dir, "orders"), "orders")


def get_customer_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(con, join(base_dir, "customer"), "customer")


def get_region_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(con, join(base_dir, "region"), "region")


def get_nation_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(con, join(base_dir, "nation"), "nation")


def get_supplier_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(con, join(base_dir, "supplier"), "supplier")


def get_part_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(con, join(base_dir, "part"), "part")


def get_part_supp_ds(con, base_dir: str = DATASET_BASE_DIR) -> str:
    return _scan_ds(con, join(base_dir, "partsupp"), "partsupp")


def run_query(q_num: int, con, query_str: str):
    with CodeTimer(name=f"Get result of glaredb Query {q_num}", unit="ms"):
        t0 = timeit.default_timer()
        result = con.execute(query_str)

        secs = timeit.default_timer() - t0
        if LOG_TIMINGS:
            append_row(solution="glaredb", version="0.5.0", q=f"q{q_num}", secs=secs)
        if SHOW_OUTPUT:
            result.show()


def append_row(solution: str, q: str, secs: float, version: str, success=True):
    q = q[1:]
    with open(TIMINGS_FILE, "a") as f:
        if f.tell() == 0:
            f.write("solution,version,query_no,duration[s],include_io,success\n")
        f.write(f"{solution},{version},{q},{secs},{INCLUDE_IO},{success}\n")


def execute_all(solution: str):
    package_name = f"{solution}_queries"
    pkg_dir = os.path.join(CWD, package_name)
    expr = re.compile(r"^q(\d+).py")
    num_queries = 0
    for file in os.listdir(pkg_dir):
        g = expr.search(file)
        if g is not None:
            num_queries = max(int(g.group(1)), num_queries)

    with CodeTimer(name=f"Overall execution of ALL {solution} queries", unit="s"):
        for i in range(1, num_queries + 1):
            run([sys.executable, "-m", f"{package_name}.q{i}"])
