# TypeScript Example

👋 This example demonstrates connecting to an instance of GlareDB and running
an example query in TypeScript using Node.js. Since GlareDB is postgres-compatible, familiar tools are used:

- [Node.js](https://nodejs.org/)
- [TypeScript](https://www.typescriptlang.org/)
- [yarn](https://yarnpkg.com/)
- [pg library](https://node-postgres.com/)

## Running

The example requires:

- running `glaredb` locally in `server` mode:

  ```console
  # change to a directory you'd like glaredb installed into
  cd ~
  curl https://glaredb.com/install.sh | sh
  ./glaredb server
  ```

- having [Node.js installed](https://nodejs.org/) and running `yarn start`:

  ```console
  # from the repository root
  cd examples/typescript
  yarn install
  yarn start
  ```

Make sure to replace the placeholder connection string in `example.ts` with the appropriate configuration for your GlareDB instance.

For more information on installing `glaredb` locally, see: [Trying GlareDB locally].
For more information on our cloud offering, see: [Getting Started].

[Node.js installed]: https://nodejs.org/
[Trying GlareDB locally]: https://docs.glaredb.com/glaredb/local/
[Getting Started]: https://docs.glaredb.com/docs/about/getting-started.html#glaredb-cloud
