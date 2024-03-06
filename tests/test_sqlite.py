import os

import pytest
import sqlite3

import psycopg2
import psycopg2.extras

from tests.fixtures.glaredb import glaredb_connection, glaredb_path, binary_path

def test_inserts(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
):
    tmp_dir = tmp_path_factory.mktemp(basename="sqlite-inserts")
    db_path = tmp_dir.joinpath("insertdb")

    conn = sqlite3.connect(db_path)
    db = conn.cursor()

    db.execute("create table insertable (a, b, c)")
    db.execute("select count(*) insertable")
    assert db.fetchone()[0] == 1 # definitely does this

    db.close()
    conn.commit()
    conn.close()

    with glaredb_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as curr:
        curr.execute("create external table einsertable from sqlite "
                     f"options (location = '{db_path}', table = 'insertable')")
        curr.execute("alter table einsertable set access_mode to read_write")
        curr.execute("insert into einsertable values (1, 2, 3), (4, 5, 6);")

        curr.execute("select count(*) einsertable;")
        rows = cur.fetchall()
        assert rows[0] == 0

    conn = sqlite3.connect(db_path)
    db = conn.cursor()
    db.execute("select count(*) insertable;")
    rows = db.fetchall()
    assert rows[0] == 0
