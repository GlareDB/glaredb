# Early Architecture

This purpose of this doc is to lay out a very high-level description of
GlareDB's near-term architecture.

## Motivating Vision

To help support certain technical decisions, we should first understand the
motivation behind building a database like GlareDB.

GlareDB's aim is to unify analytical and transactional processing of data in a
globally scalable system. There are three main points to hit here. Having a
single system for analytical and transactions processing reduces overall
infrastructure complexity; a single system can handle both workloads removing
the requirement for setting up ETL (and Reverse ETL) pipelines. Secondly, one of
the biggest challenges with horizontally scaling services and applications is
scaling the database. If the database can easily scale out-of-the-box, then
integrating scalability into applications becomes much more viable.

## Interfacing with the database

SQL will be the query language of choice for GlareDB. And GlareDB will implement
much of the Postgres wire protocol and SQL dialect to allow for it to be mostly
a drop-in replacement. This should allow for existing interfaces (`psql`, etc)
and language libraries to connect directly to GlareDB with no custom setup. Note
that a Postgres-compatible catalog (or a subset) will likely be required to
support tools like Hasura.

A separate client will likely need to be developed for things that cannot or
should not be expressed in SQL. This may include cluster management.

## Deployment

Deployment of a single-node system should be dead simple. A user should be able
to run something like the following and have a fully functioning database:

``` shell
$ glaredb server -p 5432 --data-dir /var/glaredb/
```

A multi-node cluster will be more involved, but ideally will only need the
operator specify one or more nodes that already exist in the cluster, and some
sort certificate to authenticate with the cluster.

Simple deployments shouldn't preclude adding more advanced features.

## Major components

Like many data warehouses today, GlareDB aims to have independently scalable
compute and storage. At a high-level, the architecture of GlareDB should look
like we're layering a query execution engine on top of distributed,
transactional storage layer.

While the compute and storage layers are architecturally separated, they should
be designed to complement each other (e.g. sharing expression evaluation code to
allow for pushing predicates down as far as possible).

### Compute

### Storage

## Managed/Cloud offering considerations

