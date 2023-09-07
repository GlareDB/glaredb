import pytest
import glaredb

def pydict_sort(pd):
    res = dict()
    for key in sorted(pd):
        res[key] = sorted(pd[key])
    return res

def test_compose():
    con = glaredb.connect()
    con.execute("create table tbl (a int, b int);")
    con.execute("insert into tbl values (1, 2);")
    con.execute("insert into tbl values (3, 4);")
    con.execute("insert into tbl values (5, 6);")
    intermediate = con.sql("select * from tbl where a > 2;")
    out_1 = pydict_sort(intermediate.to_arrow().to_pydict())
    expected = {"a": [3, 5], "b": [4, 6]}
    assert out_1 == expected
    out_2 = con.sql("select * from intermediate where b > 4;").to_arrow().to_pydict()
    expected = {"a": [5], "b": [6]}
    assert out_2 == expected
    con.close()
    