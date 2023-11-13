import glaredb from './glaredb.js'

let con = await glaredb.connect();
let res = await con.sql("select * from '../../testdata/json/userdata1.json' where id = -1");
await res.show()
