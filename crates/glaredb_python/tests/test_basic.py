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
