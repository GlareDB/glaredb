# these benchmarks compare the performance between the read_ndjson()
# function (which uses datafusion/arrow's json tape parsing facility)
# and read_json(), with the serde_json+streaming JSON code which has
# more relaxed parsing with regards to newlines.
import pathlib
import os
import json
import random
import string
import logging

import pytest
import psycopg2

import tests.tools

VALUES_SET = string.ascii_uppercase + string.digits
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


@pytest.fixture(scope="session")
def generated_bench_data(
    tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, pathlib.Path]:
    out: dict[str, pathlib.Path] = {}

    with tests.tools.timed(logger, "generating-test-data"):
        tmpdir = tmp_path_factory.mktemp(basename="json-bench-singles", numbered=False)
        for col, row in [(16, 256), (16, 512), (32, 256), (32, 512)]:
            out[f"c{col}.r{row}"] = write_random_json_file(
                path=tmpdir.joinpath(f"c{col}.r{row}.json"), column_count=col, row_count=row
            )

        globdir = tmp_path_factory.mktemp(basename="json-bench-globs", numbered=False)
        for num in [16, 64, 256, 512]:
            tmpdir = globdir.joinpath(f"n{num}")
            test_path = tmpdir.joinpath("*.json")
            out[f"glob.n{num}"] = test_path
            os.mkdir(tmpdir)
            logger.info(f"added glob test at '{test_path}'")
            for i in range(num):
                write_random_json_file(
                    tmpdir.joinpath(f"benchdata.{i}.json"), column_count=16, row_count=512
                )

    logger.info(f"wrote {len(out)} test cases; {len(CASES)} registered")

    return out


def write_random_json_file(
    path: pathlib.Path,
    column_count: int,
    row_count: int,
) -> pathlib.Path:
    vals = [
        "".join(random.choices(VALUES_SET, k=4)),
        "".join(random.choices(VALUES_SET, k=8)),
        "".join(random.choices(VALUES_SET, k=16)),
        "".join(random.choices(VALUES_SET, k=32)),
        "".join(random.choices(VALUES_SET, k=64)),
    ]

    with open(path, "w") as f:
        for idx, rc in enumerate(range(row_count)):
            doc = {}
            for cc in range(column_count):
                if cc % 4 == 0:
                    doc[f"{cc}.{idx}"] = random.randint(0, (column_count + 1) * (row_count + 1))
                else:
                    doc[f"{cc}.{idx}"] = random.choice(vals)

            json.dump(doc, f)
            f.write("\n")

    return path


@pytest.mark.parametrize(
    "case_name,read_fn_name",
    [
        bench
        for pair in [[(item, "read_json"), (item, "read_ndjson")] for item in CASES]
        for bench in pair
    ],
)
@pytest.mark.benchmark
def test_json_function(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
    generated_bench_data: dict[str, pathlib.Path],
    case_name: str,
    read_fn_name: str,
    benchmark: callable,
):
    path = generated_bench_data[case_name]

    logger.info(f"using test data at '{path}' for {case_name}")

    benchmark(run_query_operation, glaredb_connection, path, read_fn_name)


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
