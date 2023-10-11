import glaredb
import pytest


def test_with_context():
    with glaredb.connect() as con:
        con.execute("select 1")
