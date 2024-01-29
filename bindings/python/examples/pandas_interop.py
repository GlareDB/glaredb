import glaredb
import pandas as pd
con = glaredb.connect()

df = pd.DataFrame(
    {
        "fruits": ["banana"],
    }
)

con.execute("CREATE TABLE test_table AS SELECT * FROM df;")
out = con.execute("SELECT * FROM test_table;").to_pandas()
print(out)
print(df)
assert out.equals(df)
