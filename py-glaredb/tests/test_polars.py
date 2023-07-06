import glaredb
import polars as pl
import pytest

def test_can_query():
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

    assert(out.frame_equal(expected))

