# An example showing using RPostgres to connnect to GlareDB, which then connects
# to an external snowflake database.
#
# Note that glaredb needs to be started outside the script (embedded R bindings coming soon).

library(DBI)

# Connect to glaredb instance running out of process. These values are sufficient
# if running glaredb locally via './glaredb server'.
# 
# Alternatively could use a connection string for a cloud hosted deployment on
# console.glaredb.com
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = "glaredb",
                      host = "localhost",
                      port = 6543,
                      user = "glaredb")

# Attach snowflake db using required credentials.
#
# All options fields shown here need to be set.
res <- dbSendQuery(con, "
CREATE EXTERNAL DATABASE my_snowflake
	FROM snowflake
	OPTIONS (
		account = '***',
		username = '***',
		password = '***',
		database = 'glaredb_test',
		warehouse = 'compute_wh',
		role = 'accountadmin',
	);
")
dbClearResult(res)

# Query snowflake tables by fully qualifying the table.
res <- dbSendQuery(con, "SELECT * FROM my_snowflake.public.bikeshare_stations LIMIT 20")
while(!dbHasCompleted(res)){
  chunk <- dbFetch(res, n = 5)
  # Do whatever processing needed on the results.
  print(chunk)
}

