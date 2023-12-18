import pathlib
import subprocess

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
