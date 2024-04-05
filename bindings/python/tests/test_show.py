import glaredb
import pytest


def test_show():
    con = glaredb.connect()
    # Just make sure we don't error right now. We could error if we reference
    # the incorrect local during the print.
    con.sql("select 1").show()
