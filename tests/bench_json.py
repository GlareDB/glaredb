# these benchmarks compare the performance between the read_ndjson()
# function (which uses datafusion/arrow's json tape parsing facility)
# and read_json(), with the serde_json+streaming JSON code which has
# more relaxed parsing with regards to newlines.
import pathlib
import json
import random
import string
import logging

import pytest
import psycopg2

VALUES_SET = string.ascii_uppercase + string.digits
logger = logging.getLogger("json")


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


@pytest.fixture
def test_data_path(
    tmp_path_factory: pytest.TempPathFactory,
    column_count: int,
    row_count: int,
) -> pathlib.Path:
    try:
        tmpdir = tmp_path_factory.mktemp(basename="json-bench", numbered=False)
    except FileExistsError as e:
        tmpdir = pathlib.Path(e.filename)

    path = tmpdir.joinpath(f"case-c{column_count}.r{row_count}.json")
    if path.exists():
        logger.info(f"using cached file {path}")
        return path

    with open(path, "w") as f:
        for rc in range(row_count):
            doc = {}
            for cc in range(column_count):
                rval = "".join(
                    random.choices(
                        VALUES_SET,
                        k=random.choices(
                            range(cc + 1),
                        )[0],
                    )
                )
                val = [i for i in range(42)]
                val.extend(
                    [
                        True,
                        False,
                        None,
                        cc,
                        rc,
                        str(path.absolute()),
                        rval,
                    ],
                )
                rkey = "".join(random.choices(VALUES_SET, k=8))
                doc[f"{cc}.{rkey}"] = random.choices(val)

            json.dump(doc, f)
            f.write("\n")

    logger.info(f"wrote test file at {path}")
    return path


@pytest.mark.parametrize(
    "column_count,row_count,read_fn_name",
    [
        (10, 10, "read_ndjson"),
        (10, 10, "read_json"),
        (100, 10, "read_ndjson"),
        (100, 10, "read_json"),
        (10, 100, "read_ndjson"),
        (10, 100, "read_json"),
        (10, 250, "read_ndjson"),
        (10, 250, "read_json"),
    ],
)
@pytest.mark.benchmark(
    warmup=True,
    warmup_iterations=2,
    max_time=20,
    min_time=5,
    min_rounds=10,
)
def test_json_function(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
    column_count: int,
    row_count: int,
    read_fn_name: str,
    test_data_path: pathlib.Path,
    benchmark: callable,
):
    benchmark(run_query_operation, glaredb_connection, test_data_path, read_fn_name)
