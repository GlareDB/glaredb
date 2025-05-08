---
title: Getting Started
order: 1
---

# Getting Started

GlareDB is a lightweight analytical database engine designed for
high-performance SQL analytics.

## Install

GlareDB can be installed either as a standalone CLI, or used as a library inside
of Python programs.

### CLI

Install the latest version of the GlareDB CLI:

```bash
curl -fsSL https://glaredb.com/install.sh | sh
```

The binary will be located at `~/.glaredb/bin/glaredb`.

### Python

Install the GlareDB Python package via `pip`:

```bash
pip install glaredb
```

Then simply import `glaredb` inside your Python script.

## Supported Platforms

GlareDB supports the following platforms:

| OS/Architecture | Additional Requirements |
|-----------------|-------------------------|
| macOS 14+ Arm64 |                         |
| Linux x86_64    | glibc 2.28+             |
| Linux Arm64     | glibc 2.28+             |

Additional platforms will be supported in the future.

