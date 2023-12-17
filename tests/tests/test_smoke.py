import pathlib
import subprocess

import psycopg2

from fixtures.glaredb import glaredb_connection, release, debug


def test_release_exists(release):
    assert not release.exists()  # the release binary does not exist


def test_debug_exists(debug):
    assert debug.exists()  # the debug binary exists


def test_debug_executes(debug: pathlib.Path):
    # run the binary and see if it returns:
    assert subprocess.check_call([debug.absolute(), "-q", "SELECT 1;"]) == 0


def test_start(glaredb_connection: psycopg2.extensions.connection):
    with glaredb_connection.cursor() as cur:
        cur.execute("SELECT 1;")
