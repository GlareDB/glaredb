import glaredb
import pytest


def test_conn_open_simple_query():
    con = glaredb.connect()
    res = con.sql("SELECT 3")
    print(res)
