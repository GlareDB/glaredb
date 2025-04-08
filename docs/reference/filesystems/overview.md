---
title: Overview
order: 0
---

# File System Overview

File systems are a core abstraction in GlareDB that determine how the system
locates and reads data files. Whether you’re working with files stored locally,
hosted over HTTP(S), or stored in cloud object storage, file systems provide a
unified interface for accessing them.

This abstraction makes it easy to query files from different sources using the
same SQL functions.

## File System Selection

Which file system GlareDB chooses to use is based on the path provided to
file-reading functions.

Paths that can be parsed as a valid URL and have either "http" or "https" as its
scheme will use the **HTTP(S) File System**.
Otherwise, GlareDB will fall back to using the **Local File System**.

> When using the WebAssembly bindings, GlareDB does not have access to the local
> file system, and will instead fail with an error indicating it cannot find a
> suitable file system to use for the path.
