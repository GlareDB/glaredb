# Node.js bindings for GlareDB

Check out the [GlareDB repo](https://github.com/GlareDB/glaredb) to learn more.

Use GlareDB directly in Node, Bun, or Deno to query and analyze a variety of data sources,
including Polars dataframes

```js
> import glaredb from '@glaredb/glaredb'
> const conn = await glaredb.connect()
> const res = await conn.sql(`SELECT 'hello from js' as hello`)
> await res.show()

┌───────────────┐
│ hello         │
│ ──            │
│ Utf8          │
╞═══════════════╡
│ hello from js │
└───────────────┘
```
