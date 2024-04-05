import glaredb
import pytest


def test_eager_ddl():
    con = glaredb.connect()
    one = con.sql("create table tblsqlhelper (a int, b int);")
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 0

    two = con.sql("insert into tblsqlhelper values (4, 2);")
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 1

    with pytest.raises(Exception, match="Duplicate name"):
        one.execute()

    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 1

    two.execute()
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 2


@pytest.mark.skip(reason="the table should be empty but isn't, "
                   "test interaction with above; see #2867")
def test_execute_is_eager():
    con = glaredb.connect()
    con.execute("create table tblexechelper (a int, b int);")
    assert con.sql("select * from tblexechelper;").to_arrow().num_rows == 0
