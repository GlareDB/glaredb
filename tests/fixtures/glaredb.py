import pathlib
import time
import tempfile
import subprocess
import sys
import typing

import pytest
import psycopg2

import tests


@pytest.fixture
def release():
    return tests.PKG_DIRECTORY.joinpath("target", "release", "glaredb")


@pytest.fixture
def debug():
    return tests.PKG_DIRECTORY.joinpath("target", "debug", "glaredb")


@pytest.fixture
def glaredb_connection(
    debug: pathlib.Path, tmp_path: typing.Generator[pathlib.Path, None, None]
) -> psycopg2.extensions.connection:
    addr = ("127.0.0.1", "5432")
    with tempfile.TemporaryDirectory() as tmpdir:
        with subprocess.Popen(
            [
                debug.absolute(),
                "--verbose",
                "server",
                "--data-dir",
                tmp_path.joinpath("data"),
                "--spill-path",
                tmp_path.joinpath("spill"),
                "--bind",
                ":".join(addr),
            ],
            cwd=tmpdir,
            close_fds="posix" in sys.builtin_module_names,
        ) as p:
            time.sleep(0.5)
            assert not p.poll(), p.stdout.read().decode("utf-8")
            conn = psycopg2.connect(host=addr[0], port=addr[1], dbname="glaredb")
            conn.autocommit = True
            yield conn
            conn.close()
            p.terminate()
