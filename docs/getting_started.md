---
title: Getting Started
order: 1
---

# Getting Started

GlareDB is a lightweight analytical database engine designed for
high-performance SQL analytics.

For installation instructions, head over to the [Install] page.

## Query data

Running the CLI without any arguments will drop you into an interactive shell:

```
$ ~/.glaredb/bin/glaredb
GlareDB Shell
v0.10.10
Enter .help for usage hints.
glaredb>
```

From here, you'll be able to start running SQL queries. For example, we can
query some sample data from S3:

```sql
SELECT * FROM 's3://glaredb-public/userdata0.parquet' LIMIT 10;
```

[Install]: ./install
