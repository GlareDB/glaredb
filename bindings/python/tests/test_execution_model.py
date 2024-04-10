import glaredb
import pytest


def test_eager_ddl():
    con = glaredb.connect()
    
    with pytest.raises(Exception):
        con.sql("select count(*) from tblsqlhelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 0

    one = con.sql("create table tblsqlhelper (a int, b int);")

    assert con.sql("select count(*) from tblsqlhelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 0

    two = con.sql("insert into tblsqlhelper values (1, 2);")
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 1
    assert con.sql("select count(*) from tblsqlhelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 1

    one.execute()
    two.execute()
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 1


def test_execute_is_eager():
    con = glaredb.connect()

    with pytest.raises(Exception):
        con.sql("select count(*) from tblexechelper;")

    con.execute("create table tblexechelper (a int, b int);")

    assert con.sql("select count(*) from tblexechelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 0

