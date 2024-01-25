import pandas as pd
import psycopg2.extensions
import pytest

def test_create_table_from_pandas(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
):
  df = pd.DataFrame(
    {
        "fruits": ["banana"],
    }
  )

  with glaredb_connection.cursor() as curr:
    curr.execute("CREATE TABLE pandas_df_test AS SELECT * FROM df")
  
  with glaredb_connection.cursor() as curr:
    curr.execute("select * FROM pandas_df_test;")
    res = curr.fetchone()
    assert res[0] == "banana"
