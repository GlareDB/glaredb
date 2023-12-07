import adbc_driver_flightsql.dbapi
import polars as pl

with adbc_driver_flightsql.dbapi.connect("grpc://0.0.0.0:6789") as conn:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * from '/Users/corygrinstead/Development/glaredb/testdata/csv/userdata1.csv'"
    )
    res = cursor.fetch_arrow_table()
    print(pl.from_arrow(res))

    with adbc_driver_flightsql.dbapi.connect("grpc://0.0.0.0:6789") as conn2:
        cursor = conn2.cursor()
        cursor.execute(
            "SELECT * from '/Users/corygrinstead/Development/glaredb/testdata/csv/userdata1.csv'"
        )
        res = cursor.fetch_arrow_table()
        print(pl.from_arrow(res))
        cursor.close()

        with adbc_driver_flightsql.dbapi.connect("grpc://0.0.0.0:6789") as conn3:
            cursor = conn3.cursor()
            cursor.execute(
                "SELECT * from '/Users/corygrinstead/Development/glaredb/testdata/csv/userdata1.csv'"
            )
            res = cursor.fetch_arrow_table()
            print(pl.from_arrow(res))
            cursor.close()
    cursor.close()

