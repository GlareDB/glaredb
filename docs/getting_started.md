---
title: Getting Started
order: 1
---

# Getting Started

The next iteration of GlareDB is under heavy development, and while we've
started shipping pre-built binaries and Python bindings, expect things to evolve
quickly. Read more about [what's to come]

You can now get started with GlareDB by installing either the Python package or
the standalone CLI:

## CLI

To use the GlareDB CLI, download the latest release from the [Releases page].

Choose the appropriate binary for your platform, make it executable, and run it
from your terminal.

> Making the binary executable is required as GitHub release artifacts do not
> preserve that information. Run `chmod +x <path>` where `path` is the path to
> the binary you downloaded.
>
> An installation script will automate this process in the future.

## Python

Install the GlareDB Python package via `pip`:

```bash
pip install glaredb
```

Once installed, you can start using GlareDB directly from Python.

## GlareDB in the Browser

The embedded terminal on [glaredb.com] provides a fully functional instance of
GlareDB running entirely inside your browser. You can use this to query remote
files including hosted csv files, parquet files in s3, and much more.

[glaredb.com]: https://glaredb.com
[Releases Page]: https://github.com/GlareDB/glaredb/releases/latest
[what's to come]: https://glaredb.com/blog/whole-new-engine
