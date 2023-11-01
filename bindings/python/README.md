# Python bindings for GlareDB

Check out the [GlareDB repo](https://github.com/GlareDB/glaredb) to learn more.

Use GlareDB directly in Python to query and analyzer a variety of data sources,
including Polars and Pandas data frames.

```python
import glaredb
import polars as pl

df = pl.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
    }
)

con = glaredb.connect()

df = con.sql("select * from df where fruits = 'banana'").to_polars();

print(df)

# shape: (3, 4)
# ┌─────┬────────┬─────┬────────┐
# │ A   ┆ fruits ┆ B   ┆ cars   │
# │ --- ┆ ---    ┆ --- ┆ ---    │
# │ i64 ┆ str    ┆ i64 ┆ str    │
# ╞═════╪════════╪═════╪════════╡
# │ 1   ┆ banana ┆ 5   ┆ beetle │
# │ 2   ┆ banana ┆ 4   ┆ audi   │
# │ 5   ┆ banana ┆ 1   ┆ beetle │
# └─────┴────────┴─────┴────────┘
```
