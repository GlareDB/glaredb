import pathlib
import time
import subprocess
import sys

import pytest
import psycopg2

import tests


@pytest.fixture
def release_path() -> pathlib.Path:
    return tests.PKG_DIRECTORY.joinpath("target", "release", "glaredb")


@pytest.fixture
def debug_path() -> pathlib.Path:
    return tests.PKG_DIRECTORY.joinpath("target", "debug", "glaredb")


@pytest.fixture
def glaredb_connection(
    debug_path: pathlib.Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> psycopg2.extensions.connection:
    addr = ("127.0.0.1", "5432")

    with subprocess.Popen(
        [
            debug_path.absolute(),
            "--verbose",
            "server",
            "--data-dir",
            tmp_path_factory.mktemp("data").absolute(),
            "--spill-path",
            tmp_path_factory.mktemp("spill").absolute(),
            "--bind",
            ":".join(addr),
        ],
        cwd=tmp_path_factory.mktemp("cwd").absolute(),
        close_fds="posix" in sys.builtin_module_names,
        env={"RUST_BACKTRACE": "1"},
    ) as p:
        time.sleep(0.5)
        assert not p.poll(), p.stdout.read().decode("utf-8")
        conn = psycopg2.connect(host=addr[0], port=addr[1], dbname="glaredb")
        conn.autocommit = True
        yield conn
        conn.close()
        p.terminate()
