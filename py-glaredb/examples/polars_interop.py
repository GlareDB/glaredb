import glaredb
import polars as pl

con = glaredb.connect("/tmp/glare.db")
arrow_tbl = con.sql("""
  SELECT * 
  FROM parquet_scan('https://github.com/GlareDB/glaredb/raw/main/testdata/parquet/userdata1.parquet') 
  LIMIT 1
""").to_arrow()

print(pl.from_arrow(arrow_tbl).select(pl.col("first_name")))
