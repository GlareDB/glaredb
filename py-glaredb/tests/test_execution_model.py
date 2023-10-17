import glaredb
import pytest


def test_execute_is_eager():
    con = glaredb.connect()
    con.execute("create table tbl (a int, b int);")
    con.execute("insert into tbl values (1, 2);")
    assert con.sql("select * from tbl;").to_arrow().num_rows == 1


def test_eager_ddl():
    con = glaredb.connect()
    one = con.sql("create table tbl (a int, b int);")

    two = con.sql("insert into tbl values (1, 2);")
    print(con.sql("select * from tbl;").to_arrow().num_rows)

    con.execute("insert into tbl values (1, 2);")

    one.execute()
    two.execute()
    print(con.sql("select * from tbl;").to_arrow().num_rows)

    
