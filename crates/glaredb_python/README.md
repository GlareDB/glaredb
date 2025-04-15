# GlareDB Python bindings

## Development

Create virtual env.

```sh
$ python -m venv crates/rayexec_python/venv
```

Activate virtual env.

```sh
$ cd crates/rayexec_python
$ source ./venv/bin/activate
# OR
$ export VIRTUAL_ENV="<path-to-venv-dir>"
$ export PATH="$VIRTUAL_ENV/bin:$PATH"
```

Build and test it (assuming in rayexec_python).

```sh
$ maturin develop
$ python
Python 3.11.9 (main, Apr  2 2024, 08:25:04) [Clang 16.0.6 ] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import rayexec
>>> conn = rayexec.connect()
>>> conn.query("select 1")
┌──────────┐
│ ?column? │
│ Int64    │
├──────────┤
│        1 │
└──────────┘
```
