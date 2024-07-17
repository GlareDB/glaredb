import sys
import pathlib
import subprocess

import pytest
import psycopg2


def test_binary_exists(glaredb_path: list[pathlib.Path]):
    assert len(glaredb_path) == 2
    assert glaredb_path[0].exists() or glaredb_path[1].exists()


def test_binary_executes(binary_path: list[pathlib.Path]):
    # run the binary and see if it returns:
    assert subprocess.check_call([binary_path.absolute(), "-q", "SELECT 1;"]) == 0


def test_start(
    glaredb_connection: psycopg2.extensions.connection,
):
    with glaredb_connection.cursor() as cur:
        cur.execute("SELECT 1;")


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="linux version of the test")
def test_expected_linking_linux(binary_path: pathlib.Path):
    out = [
        ll
        for cell in [
            item
            for item in [
                line.split(" ")
                for line in str(subprocess.check_output(["ldd", binary_path.absolute()], text=True))
                .replace("\t", "")
                .split("\n")
            ]
        ]
        for ll in cell
        if not (
            ll == "=>"
            or ll == ""
            or ll.startswith("(0x00")
            or ll.startswith("/usr/lib")
            or ll.startswith("/lib")
        )
    ]
    expected_prefix = ["libc.so", "libm.so", "linux-vdso"]
    possible_libs = ["libbz", "libgcc"]
    pending_removal = ["libcrypto", "libssl"]
    expected = 0
    possible = 0
    pending = 0
    for lib in out:
        for prefix in expected_prefix:
            if lib.startswith(prefix):
                expected += 1

        for prefix in possible_libs:
            if lib.startswith(prefix):
                possible += 1

        for prefix in pending_removal:
            if lib.startswith(prefix):
                pending += 1

    assert expected == 3, f"missing expected library {expected_prefix} in:\n" + "\n".join(out)

    # this is hella gross, but this number will change any time we add
    # a new library, this assertion will fail.
    #
    # it's two numbers because this is different on different distros;
    # as long as we don't have two numbers next to eachother this is fine;
    # presently: (ubuntu2004, archlinux)
    assert len(out) == (expected + possible + pending), "unexpected library in:\n" + "\n".join(out)

    # TODO: currently we link (open) libssl, which means the first time it
    # changes uncomment the first assertion in the loop below and
    # remove this comment:

    for lib in out:
        # assert not ("ssl" in lib)
        assert not ("libc++" in lib)
        assert not ("libstdc++" in lib)
