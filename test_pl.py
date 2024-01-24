import polars as pl

pl.DataFrame({"foo": [1, 2, 3]}).to_arrow()