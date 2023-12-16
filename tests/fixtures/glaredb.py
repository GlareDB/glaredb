import time
import tempfile
import subprocess
import sys
import os.path
import pathlib
import pytest
import tests


@pytest.fixture
def release():
    return tests.PKG_DIRECTORY.joinpath("target", "release", "glaredb")


@pytest.fixture
def debug():
    return tests.PKG_DIRECTORY.joinpath("target", "debug", "glaredb")


@pytest.fixture
def run_debug(debug: pathlib.Path) -> str:
    addr = ("127.0.0.1", "5432")
    with tempfile.TemporaryDirectory() as tmpdir:
        with subprocess.Popen(
            [
                debug.absolute(),
                "--verbose",
                "server",
                "--data-dir",
                os.path.join(tmpdir, "data"),
                "--spill-path",
                os.path.join(tmpdir, "spill"),
                "--bind",
                ":".join(addr),
            ],
            cwd=tmpdir,
            close_fds="posix" in sys.builtin_module_names,
        ) as p:
            time.sleep(0.5)
            assert not p.poll(), p.stdout.read().decode("utf-8")
            yield addr
            p.terminate()
