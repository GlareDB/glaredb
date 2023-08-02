import { Client } from "pg";

async function main() {
  // localHostStr will work for locally running glaredb in 'server' mode. If
  // you want to connect to a remote Cloud-hosted instance, use the connection
  // string generated for you in the web client.
  //
  // For more information on connection strings for our Cloud offering, see:
  //
  //     <https://docs.glaredb.com/cloud/access/connection-details/>
  //
  // An example connection string might look like:
  //
  //     postgresql://user:pass@my_org.proxy.glaredb.com:6543/my_db
  const localHostStr = "postgresql://glaredb:glaredb@localhost:6543/glaredb";

  const client = new Client({
    connectionString: localHostStr,
  });

  try {
    await client.connect();
    const result = await client.query("SELECT 'Connected to GlareDB!'");

    if (result.rows.length > 0) {
      console.log(result.rows[0]);
    } else {
      console.log("No data found.");
    }
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.end();
  }
}

main();
