import glaredb
import pandas as pd

df = pd.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
    }
)

con = glaredb.connect("/tmp/testing")

df = con.sql("select * from df where fruits = 'banana'").to_pandas();

print(df)
