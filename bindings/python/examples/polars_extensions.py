import glaredb
import polars as pl
import polars_distance as pld

df = pl.DataFrame(
        {
            "arr": [[1, 2.0, 3.0, 4.0]],
            "arr2": [[10.0, 8.0, 5.0, 3.0]],
        },
            schema={
            "arr": pl.Array(inner=pl.Float64, width=4),
            "arr2": pl.Array(inner=pl.Float64, width=4),
        },
)
con = glaredb.connect()
con.register_polars_extension(pld)

df = con.sql("""
    SELECT 
        dist_arr.cosine(arr, arr2) as cosine,
    FROM df
""").to_polars()

print(df)
con.close()
