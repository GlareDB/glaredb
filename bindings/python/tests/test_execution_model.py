import glaredb


def test_eager_ddl():
    con = glaredb.connect()
    one = con.sql("create table tblsqlhelper (a int, b int);")

    two = con.sql("insert into tblsqlhelper values (1, 2);")
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 1

    one.execute()
    two.execute()
    assert con.sql("select * from tblsqlhelper;").to_arrow().num_rows == 1


def test_execute_is_eager():
    con = glaredb.connect()
    con.execute("create table tblexechelper (a int, b int);")
    assert con.sql("select * from tblexechelper;").to_arrow().num_rows == 1
