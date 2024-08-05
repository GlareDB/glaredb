import glaredb
import pytest


def test_eager_ddl():
    con = glaredb.connect()
    
    with pytest.raises(Exception):
        con.sql("select count(*) from tblsqlhelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 0

    one = con.sql("create table tblsqlhelper (a int, b int);")
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 0

    two = con.sql("insert into tblsqlhelper values (4, 2);")
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 1

    with pytest.raises(Exception, match="Duplicate name"):
        one.execute()

    assert con.sql("select count(*) from tblsqlhelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 1

    two = con.sql("insert into tblsqlhelper values (1, 2);")
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 2
    assert con.sql("select count(*) from tblsqlhelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 2

    two.execute()
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 3


def test_execute_is_eager():
    con = glaredb.connect()

    with pytest.raises(Exception):
        con.sql("select count(*) from tblexechelper;")

    con.execute("create table tblexechelper (a int, b int);")

    assert con.sql("select count(*) from tblexechelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 0

