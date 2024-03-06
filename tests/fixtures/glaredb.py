import socket
import contextlib
import pathlib
import time
import subprocess
import sys
import logging

import pytest
import psycopg2

import tests

logger = logging.getLogger("fixtures")


@pytest.fixture
def glaredb_path() -> list[pathlib.Path]:
    return [
        tests.PKG_DIRECTORY.joinpath("target", "release", "glaredb"),
        tests.PKG_DIRECTORY.joinpath("target", "debug", "glaredb"),
    ]


@pytest.fixture
def binary_path(glaredb_path: list[pathlib.Path]) -> pathlib.Path:
    return glaredb_path[0] if glaredb_path[0].exists() else glaredb_path[1]


@pytest.fixture
def glaredb_connection(
    binary_path: list[pathlib.Path],
    tmp_path_factory: pytest.TempPathFactory,
) -> psycopg2.extensions.connection:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        addr = ("127.0.0.1", str(s.getsockname()[1]))

    logger.info(f"starting glaredb on port {addr[0]}")

    with subprocess.Popen(
        [
            binary_path.absolute(),
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
