import glaredb from './index.js'
import * as pl from 'nodejs-polars'
import {tableFromIPC} from 'apache-arrow'

let con = await glaredb.connect();
let res = await con.sql("select * from '../testdata/json/userdata1.json' limit 1");
let res1 = await res.toArrow();

let df = pl.readIPC(res1);
let arrow_tbl = tableFromIPC(res1);

console.log("polars\n---\n ", df.toString());
console.log("arrow\n---\n", arrow_tbl.toString());
