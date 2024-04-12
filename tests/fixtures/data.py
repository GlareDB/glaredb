import json
import logging
import string
import random
import pathlib
import os

import pytest
import bson

import tests.tools

VALUES_SET = string.ascii_uppercase + string.digits
logger = logging.getLogger("fixture.data")


@pytest.fixture(scope="session")
def generated_json_bench_data(tmp_path_factory: pytest.TempPathFactory) -> dict[str, pathlib.Path]:
    def _dump(obj, file):
        json.dump(obj, file)
        file.write("\n")

    return generate_bench_data(tmp_path_factory, "json", _dump)


@pytest.fixture(scope="session")
def generated_bson_bench_data(tmp_path_factory: pytest.TempPathFactory) -> dict[str, pathlib.Path]:
    def _dump(obj, file):
        file.write(bson.encode(obj))

    return generate_bench_data(tmp_path_factory, "bson", _dump)


def generate_bench_data(
    tmp_path_factory: pytest.TempPathFactory,
    ext: str,
    dump: callable,
) -> dict[str, pathlib.Path]:
    out: dict[str, pathlib.Path] = {}
    modes = {"json": "w", "bson": "wb"}

    with tests.tools.timed(logger, "generating-test-data"):
        tmpdir = tmp_path_factory.mktemp(basename=f"{ext}-bench-singles", numbered=False)
        for col, row in [(16, 256), (16, 512), (32, 256), (32, 512)]:
            out[f"c{col}.r{row}"] = write_test_docs(
                path=tmpdir.joinpath(f"c{col}.r{row}.{ext}"),
                column_count=col,
                row_count=row,
                dump=dump,
                mode=modes[ext],
            )

        globdir = tmp_path_factory.mktemp(basename=f"{ext}-bench-globs", numbered=False)
        for num in [16, 64, 256, 512]:
            tmpdir = globdir.joinpath(f"n{num}")
            test_path = tmpdir.joinpath(f"*.{ext}")
            out[f"glob.n{num}"] = test_path
            os.mkdir(tmpdir)
            logger.info(f"added glob test at '{test_path}'")
            for i in range(num):
                write_test_docs(
                    tmpdir.joinpath(f"benchdata.{i}.{ext}"),
                    column_count=16,
                    row_count=512,
                    dump=dump,
                    mode=modes[ext],
                )

    logger.info(f"wrote {len(out)} {ext} test cases")

    return out


def write_test_docs(
    path: pathlib.Path,
    column_count: int,
    row_count: int,
    dump: callable,
    mode: str,
) -> pathlib.Path:
    vals = [
        "".join(random.choices(VALUES_SET, k=4)),
        "".join(random.choices(VALUES_SET, k=8)),
        "".join(random.choices(VALUES_SET, k=16)),
        "".join(random.choices(VALUES_SET, k=32)),
        "".join(random.choices(VALUES_SET, k=64)),
    ]

    with open(path, mode) as f:
        for idx, rc in enumerate(range(row_count)):
            doc = {}
            for cc in range(column_count):
                if cc % 4 == 0:
                    doc[f"{cc}.{idx}"] = random.randint(0, (column_count + 1) * (row_count + 1))
                else:
                    doc[f"{cc}.{idx}"] = random.choice(vals)
            dump(doc, f)

    return path
