import adbc_driver_flightsql.dbapi
import polars as pl

with adbc_driver_flightsql.dbapi.connect("grpc://0.0.0.0:6789") as conn:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * from './testdata/csv/userdata1.csv'"
    )
    res = cursor.fetch_arrow_table()
    print(pl.from_arrow(res))
    cursor.close()