const glaredb = require('../glaredb.js');

async function main() {
  try {
    let con = await glaredb.connect();
    let res = await con.sql("select * from '../../../testdata/json/userdata1.json' limit 1");
    await res.show()
  } catch (err) {
    console.error(err);
  }
}

main().catch(console.error);