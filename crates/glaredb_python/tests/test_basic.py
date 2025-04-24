import glaredb
import pytest

# TODO: Assert outputs


def test_simple_query():
    con = glaredb.connect()
    res = con.sql("SELECT 3")
    print(res)


def test_invalid_query():
    con = glaredb.connect()
    with pytest.raises(Exception):
        con.sql("CHOOSE 3")


def test_query_with_remote_io():
    # Losing the tokio task handle is very easy to mess up, ensure we're not
    # dropping it...
    con = glaredb.connect()
    res = con.sql("SELECT * FROM 's3://glaredb-public/userdata0.parquet' LIMIT 5")
    print(res)


def test_multiple_queries_same_connection():
    con = glaredb.connect()
    res = con.sql("SELECT 'dog'")
    print(res)
    res = con.sql("SELECT 'cat'")
    print(res)


def test_insert_and_query_temp_table():
    con = glaredb.connect()
    con.sql("CREATE TEMP TABLE t1 (a INT, b TEXT)")
    con.sql("INSERT INTO t1 VALUES (3, 'three'), (4, 'four')")
    res = con.sql("SELECT * FROM t1")
    print(res)
