# Go example

👋 This example demonstrates connecting to an instance of GlareDB and running
an example query in Go. Since GlareDB is postgres-compatible, familiar tools
are used:

- [`database/sql` standard library]
- [`lib/pq` Postgres driver]

## Running

 The example requires:

- running `glaredb` locally in `server` mode:

  ```console
  # change to a directory you'd like glaredb installed into
  cd ~
  curl https://glaredb.com/install.sh | sh
  ./glaredb server
  ```

- having [Go installed] and running `main.go`

  ```console
  # from the repository root
  cd examples/Go
  go run main.go
  ```

For more information on installing `glaredb` locally, see: [Trying GlareDB locally].
For more information on our cloud offering, see: [Getting Started].

[`database/sql` standard library]: https://pkg.go.dev/database/sql
[`lib/pq` Postgres driver]: https://github.com/lib/pq
[Go installed]: https://go.dev/
[Trying GlareDB locally]: https://docs.glaredb.com/glaredb/local/
[Getting Started]: https://docs.glaredb.com/docs/about/getting-started.html#glaredb-cloud
