import { Client } from "pg";

async function main() {
  const localHostStr =
    "postgresql://user:pass@my_org.proxy.glaredb.com:6543/my_db"; // Replace with your connection string

  const client = new Client({
    connectionString: localHostStr,
  });

  try {
    await client.connect();
    const result = await client.query("SELECT 'Connected to GlareDB!'");

    if (result.rows.length > 0) {
      console.log(result.rows[0].column1); // Assuming the column name is 'column1'
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
