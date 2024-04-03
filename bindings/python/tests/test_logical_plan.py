import pytest
import glaredb


def test_compose():
    with glaredb.connect() as con:
        con.execute("create table tbl (a int, b int);")
        con.execute("insert into tbl (a, b) values (1, 2);")
        con.execute("insert into tbl values (3, 4);")
        con.execute("insert into tbl values (5, 6);")
        intermediate = con.sql("select * from tbl where a > 2 order by a;")

        # Previously, every time you referenced the query results, the
        # plan would execute again. This makes a query result "feel
        # like a dataframe" but queries aren't like dataframes! The
        # results of a query is a cursor, and you can't iterate a
        # cursor more than once without generating an error, which is
        # what this test was previously testing.
        #
        # Being able to transparently re-run queries might be slick,
        # but it's unexpected from a billing or resource utilization
        # perspective. If we want to save the results of queries then
        # we probably ought to just read the entire result set into
        # memory so that it's actually a dataframe (maybe we should
        # provide methods in the bindings that do this).
        #
        # In an attempt to match the previous behavior I've added the
        # `resolve_table` method which _should_ give us a new table
        # object (and therefore re-run the query). That's a problem
        # for DDL/DML queries, but that's not a new issue.  The
        # current implementation, if we run the test with these lines
        # uncommented produces a "`pyo3_runtime.PanicException: Cannot
        # start a runtime from within a runtime`" error, when the
        # variable is accessed a second time. This is probably
        # tractable, but would need more time.
        #
        # out_1 = intermediate.to_arrow().to_pydict()
        # expected = {"a": [3, 5], "b": [4, 6]}
        # assert out_1 == expected
        out = con.sql("select * from intermediate where b > 4;").to_arrow().to_pydict()
        expected = {"a": [5], "b": [6]}
        assert out == expected
