# these benchmarks compare the performance between the read_ndjson()
# function (which uses datafusion/arrow's json tape parsing facility)
# and read_json(), with the serde_json+streaming JSON code which has
# more relaxed parsing with regards to newlines.
import pathlib
import logging

import pytest
import psycopg2

logger = logging.getLogger("json")
CASES = [
    "c16.r256",
    "c16.r512",
    "c32.r256",
    "c32.r512",
    "glob.n16",
    "glob.n64",
    "glob.n256",
    "glob.n512",
]


@pytest.mark.parametrize(
    "case_name,read_fn_name",
    [
        bench
        for pair in [
            [
                (item, "read_json"),
                (item, "read_ndjson"),
            ]
            for item in CASES
        ]
        for bench in pair
    ],
)
@pytest.mark.benchmark
def test_json_function(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
    generated_json_bench_data: dict[str, pathlib.Path],
    case_name: str,
    read_fn_name: str,
    benchmark: callable,
):
    path = generated_json_bench_data[case_name]

    logger.info(f"using test data at '{path}' for {case_name}")

    benchmark(run_query_operation, glaredb_connection, path, read_fn_name)


@pytest.mark.parametrize("case_name", CASES)
@pytest.mark.benchmark
def test_bson_function(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
    generated_bson_bench_data: dict[str, pathlib.Path],
    case_name: str,
    benchmark: callable,
):
    path = generated_bson_bench_data[case_name]

    logger.info(f"using test data at '{path}' for {case_name}")

    benchmark(run_query_operation, glaredb_connection, path, "read_bson")


def run_query_operation(
    glaredb_connection: psycopg2.extensions.connection,
    path: pathlib.Path,
    read_fn_name: str,
):
    with glaredb_connection.cursor() as curr:
        curr.execute(f"SELECT count(*) FROM {read_fn_name}('{path}');")
        res = curr.fetchone()
        assert len(res) >= 1
        assert res[0] >= 1
