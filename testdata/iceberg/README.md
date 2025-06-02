# Iceberg testdata

Python script(s) for generating iceberg data.

## Setup

In a new shell within this directory...

```bash
python -m venv ./.venv
export VIRTUAL_ENV="$PWD/.venv"
export PATH="$VIRTUAL_ENV/bin:$PATH"
pip install "pyiceberg[pyarrow,sql-sqlite]"
```

## Running

Run the generate script:

```bash
python generate.py
```

This should write to the `wh` directory.

Note that data generation isn't expected to be idempotent as UUIDs and time
sensitive fields are used.

Data generation for each "test case" should be defined within an isolated
function prefixed with `gen_` and be called from main.

## `tables-v1` and `tables-v2`

These iceberg tables were written by `tables.py` using an iceberg jar. I can't
say I understand why the metadata generated for these tables are different than
the ones using pyiceberg.

Prefer using the pyiceberg-based script (unless it turns out to not actually
generate metadata that's commonly used).
