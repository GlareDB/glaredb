import pytest
import glaredb


@pytest.mark.skip(reason="leaky state #2867")
def test_compose():
    with glaredb.connect() as con:
        con.execute("create table tblcomp (a int, b int);")
        con.execute("insert into tblcomp (a, b) values (1, 2);")
        con.execute("insert into tblcomp (a, b) values (3, 4);")
        con.execute("insert into tblcomp (a, b) values (5, 6);")
        intermediate = con.sql("select * from tblcomp where a > 2 order by b;")

        out_1 = intermediate.to_arrow().to_pydict()
        
        expected = {"a": [3, 5], "b": [4, 6]}
        assert out_1 == expected
        out_2 = con.sql("select * from intermediate where b > 4;").to_arrow().to_pydict()
        expected = {"a": [5], "b": [6]}
        assert out_2 == expected
