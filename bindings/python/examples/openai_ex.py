import glaredb
import os
from pandasai import PandasAI
from pandasai.llm.openai import OpenAI

con = glaredb.connect()

host = os.environ["PG_HOST"]
user = os.environ["PG_USER"]
password = os.environ["PG_PASS"]

con.execute(
    f"""
CREATE EXTERNAL DATABASE my_pg
	FROM postgres
	OPTIONS (
		host = '{host}',
		port = '5432',
		user = '{user}',
		password = '{password}',
		database = 'postgres',
	);
"""
)

df = con.sql("select * from my_pg.public.users").to_pandas()

llm = OpenAI(api_token=os.environ["OPEN_AI_KEY"])

pandas_ai = PandasAI(llm)
pandas_ai(df, prompt="Which users are from GlareDB?")

con.close()
