import sys
import pathlib
import subprocess

import pytest
import psycopg2

from fixtures.glaredb import glaredb_connection, release_path, debug_path


def test_release_exists(release_path: pathlib.Path):
    assert not release_path.exists()  # the release binary does not exist


def test_debug_exists(debug_path: pathlib.Path):
    assert debug_path.exists()  # the debug binary exists


def test_debug_executes(debug_path: pathlib.Path):
    # run the binary and see if it returns:
    assert subprocess.check_call([debug_path.absolute(), "-q", "SELECT 1;"]) == 0


def test_start(
    glaredb_connection: psycopg2.extensions.connection,
):
    with glaredb_connection.cursor() as cur:
        cur.execute("SELECT 1;")


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="linux version of the test")
def test_expected_linking_linux(debug_path: pathlib.Path):
    out = [
        ll
        for cell in [
            item
            for item in [
                line.split(" ")
                for line in str(subprocess.check_output(["ldd", debug_path.absolute()], text=True))
                .replace("\t", "")
                .split("\n")
            ]
        ]
        for ll in cell
        if not (
            ll == "=>"
            or ll.startswith("(0x00")
            or ll.startswith("/usr/lib")
            or ll.startswith("/lib64")
        )
    ]

    # this is hella gross, but this number will change any time we add
    # a new library, this assertion will fail.
    assert len(out) == 9, "unexpected library in:\n" + "\n".join(out)

    # TODO: currently we link (open) libssl, which means the first time it
    # changes uncomment the first assertion in the loop below and
    # remove this comment:

    for lib in out:
        # assert not ("ssl" in lib)
        assert not ("libc++" in lib)
        assert not ("libstdc++" in lib)
