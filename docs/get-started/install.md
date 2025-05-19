---
title: Install
order: 1
---

# Install GlareDB

## CLI

Install the latest version of the CLI:

```shell
$ curl -fsSL https://glaredb.com/install.sh | sh
```

The binary will be installed to `~/.glaredb/bin/glaredb`.

### Configuring CLI Installation

The install script reads certain environment variables for determining where to
install the binary, and which version of the binary to install.

| Environment variable  | Description                                                                                     |
|-----------------------|-------------------------------------------------------------------------------------------------|
| `GLAREDB_INSTALL_DIR` | Directory to install the binary in. Defaults to `$HOME/.glaredb/bin`.                           |
| `GLAREDB_VERSION`     | Which version to install. The value should be a release tag, or "latest". Defaults to "latest". |

## Python

Install the latest version of the GlareDB Python package:

```shell
$ pip install glaredb
```

## Supported Platforms

GlareDB distributes binaries for the following platforms.

| OS/Architecture | Additional requirements |
|-----------------|-------------------------|
| macOS 14+ Arm64 |                         |
| Linux x86_64    | glibc 2.28+             |
| Linux Arm64     | glibc 2.28+             |

Additional platforms will be officially supported in the future. For currently
unsupported platforms, [building from source](../development/building.md) may
work.

