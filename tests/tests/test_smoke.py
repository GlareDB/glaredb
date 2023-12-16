import pathlib
import subprocess
import psycopg2

from fixtures.glaredb import run_debug, release, debug


def test_release_exists(release):
    assert not release.exists()  # the release binary does not exist


def test_debug_exists(debug):
    assert debug.exists()  # the debug binary exists


def test_debug_executes(debug: pathlib.Path):
    # run the binary and see if it returns:
    assert subprocess.check_call([debug.absolute(), "-q", "SELECT 1;"]) == 0


def test_start(run_debug: tuple[str, str]):
    assert run_debug[0] == "127.0.0.1"
    assert run_debug[1] == "5432"

    conn = psycopg2.connect(host=run_debug[0], port=run_debug[1], database="glaredb")

    with conn.cursor() as cur:
        cur.execute("SELECT 1")

    conn.close()
