import glaredb

con1 = glaredb.connect()
con1.sql("create table hello (a int)").to_polars()

t1 = con1.sql(
    "select * from glare_catalog.tables where table_name = 'hello'"
).to_polars()
print(t1)

con2 = glaredb.connect()
con2.sql("create table hello (a int)").to_polars()

t2 = con2.sql(
    "select * from glare_catalog.tables where table_name = 'hello'"
).to_polars()
print(t2)

try:
    con2.sql("create table hello (a int)").to_polars()
except Exception:
    print("Duplicate table failed as expected")
