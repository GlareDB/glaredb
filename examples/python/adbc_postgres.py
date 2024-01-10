
# %%
import adbc_driver_postgresql.dbapi

uri = "postgresql://localhost:6543"

# %%
with adbc_driver_postgresql.dbapi.connect(uri) as conn:
  cursor = conn.cursor()
  cursor.execute("select 1")
  cursor.close()

# %%
