import glaredb

con = glaredb.connect("/tmp/glare.db")
sql = """
  SELECT * 
  FROM parquet_scan('https://github.com/GlareDB/glaredb/raw/main/testdata/parquet/userdata1.parquet') 
  LIMIT 1
"""

print(con.sql(sql))