import glaredb
import polars as pl
import pandas as pd

con = glaredb.connect()
df = pl.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
    }
)
con.sql("select * from df where fruits = 'banana'").show(True, 2)

df = pd.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
    }
)

con = glaredb.connect()

con.sql("select * from df where fruits = 'banana'").show(True, 4);
