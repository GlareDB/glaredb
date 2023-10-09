import glaredb
import polars as pl
import pytest


def test_sql():
    con = glaredb.connect()
    df = pl.DataFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        }
    )

    out = con.sql("select * from df where fruits = 'banana'").to_polars()
    expected = pl.DataFrame(
        {
            "A": [1, 2, 5],
            "fruits": ["banana", "banana", "banana"],
            "B": [5, 4, 1],
            "cars": ["beetle", "audi", "beetle"],
        }
    )

    assert out.frame_equal(expected)
    con.close()


def test_sql_multiple_references():
    con = glaredb.connect()
    df = pl.DataFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        }
    )

    lp = con.sql("select * from df where fruits = 'banana'")
    out1 = lp.to_polars()
    out2 = lp.to_polars()
    expected = pl.DataFrame(
        {
            "A": [1, 2, 5],
            "fruits": ["banana", "banana", "banana"],
            "B": [5, 4, 1],
            "cars": ["beetle", "audi", "beetle"],
        }
    )

    assert out1.frame_equal(expected)
    assert out2.frame_equal(expected)
    con.close()


def test_can_query_outer_scope_var():
    df = pl.DataFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        }
    )

    def inner_func():
        con = glaredb.connect()
        df = pl.DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            }
        )
        out = con.sql("select * from df where fruits = 'banana'").to_polars()
        con.close()
        return out

    out = inner_func()
    expected = pl.DataFrame(
        {
            "A": [1, 2, 5],
            "fruits": ["banana", "banana", "banana"],
            "B": [5, 4, 1],
            "cars": ["beetle", "audi", "beetle"],
        }
    )

    assert out.frame_equal(expected)


def test_execute():
    con = glaredb.connect()
    df = pl.DataFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        }
    )

    out = con.execute("select * from df where fruits = 'banana'").to_polars()
    expected = pl.DataFrame(
        {
            "A": [1, 2, 5],
            "fruits": ["banana", "banana", "banana"],
            "B": [5, 4, 1],
            "cars": ["beetle", "audi", "beetle"],
        }
    )

    assert out.frame_equal(expected)
    con.close()
