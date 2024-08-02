import glaredb
import pytest


def test_sql_validates_plan():
    con = glaredb.connect()

    with pytest.raises(Exception):
        con.sql("select count(*) from tblsqlhelper;")

def test_sql_excludes_ddl():
    con = glaredb.connect()

    with pytest.raises(Exception):
        con.sql("select count(*) from tblsqlhelper;").to_arrow().to_pydict()["COUNT(*)"][0]

    with pytest.raises(Exception):
        con.sql("create table tblsqlhelper (a int, b int);")

def test_execute_is_eager():
    con = glaredb.connect()

    with pytest.raises(Exception):
        con.sql("select count(*) from tblexechelper;")

    con.execute("create table tblexechelper (a int, b int);")

    assert con.sql("select count(*) from tblexechelper;").to_arrow().to_pydict()["COUNT(*)"][0] == 0
