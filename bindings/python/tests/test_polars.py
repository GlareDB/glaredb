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

    assert out.equals(expected)
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

    assert out1.equals(expected)
    assert out2.equals(expected)
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

    assert out.equals(expected)


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

    assert out.equals(expected)
    con.close()


def test_select_polars_lazy():
    con = glaredb.connect()
    lazy_df = pl.DataFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        }
    ).lazy()

    out = con.sql(
        "select * from lazy_df where fruits = 'banana' order by \"A\""
    ).to_polars()
    expected = pl.DataFrame(
        {
            "A": [1, 2, 5],
            "fruits": ["banana", "banana", "banana"],
            "B": [5, 4, 1],
            "cars": ["beetle", "audi", "beetle"],
        }
    )

    assert out.equals(expected)
    con.close()


def test_create_table_from_dataframe(
    tmp_path_factory: pytest.TempPathFactory,
):
    out_dir = tmp_path_factory.mktemp("test_create_table_from_dataframe")
    con = glaredb.connect(str(out_dir))

    df = pl.DataFrame(
        {
            "fruits": ["banana"],
        }
    )
    con.execute("CREATE TABLE test_table AS SELECT * FROM df;")
    out = con.execute("SELECT * FROM test_table;").to_polars()
    assert out.equals(df)

    con.close()


def test_create_temp_table_from_dataframe():
    con = glaredb.connect()

    df = pl.DataFrame(
        {
            "fruits": ["banana"],
        }
    )

    con.execute("CREATE TEMP TABLE test_temp_table AS SELECT * FROM df;")
    out = con.execute("SELECT * FROM test_temp_table;").to_polars()
    assert out.equals(df)

    con.close()
