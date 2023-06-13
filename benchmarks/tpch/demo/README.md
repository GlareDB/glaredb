# Demo Data

This directory contains scripts for creating and updating demo data that is deployed publically.

## Postgres

First we get the data from GCS. Make sure `GCP_SERVICE_ACCOUNT_JSON` is set.

```
./benchmarks/tpch/download_data.sh
```

Then we run GlareDB locally and convert the data to CSV with the following queries.
The destination path must be absolute so change to your local absolute path before running these queries.

```
CREATE EXTERNAL TABLE customer FROM local OPTIONS (location = './benchmarks/artifacts/tpch_1/customer/part-0.parquet');
CREATE EXTERNAL TABLE lineitem FROM local OPTIONS (location = './benchmarks/artifacts/tpch_1/lineitem/part-0.parquet');
CREATE EXTERNAL TABLE nation FROM local OPTIONS (location = './benchmarks/artifacts/tpch_1/nation/part-0.parquet');
CREATE EXTERNAL TABLE orders FROM local OPTIONS (location = './benchmarks/artifacts/tpch_1/orders/part-0.parquet');
CREATE EXTERNAL TABLE part FROM local OPTIONS (location = './benchmarks/artifacts/tpch_1/part/part-0.parquet');
CREATE EXTERNAL TABLE partsupp FROM local OPTIONS (location = './benchmarks/artifacts/tpch_1/partsupp/part-0.parquet');
CREATE EXTERNAL TABLE region FROM local OPTIONS (location = './benchmarks/artifacts/tpch_1/region/part-0.parquet');
CREATE EXTERNAL TABLE supplier FROM local OPTIONS (location = './benchmarks/artifacts/tpch_1/supplier/part-0.parquet');
COPY (SELECT * FROM public.customer) to '/Users/example-dev/Code/glaredb/demo/customer.csv';
COPY (SELECT * FROM public.lineitem) to '/Users/example-dev/Code/glaredb/demo/lineitem.csv';
COPY (SELECT * FROM public.nation) to '/Users/example-dev/Code/glaredb/demo/nation.csv';
COPY (SELECT * FROM public.orders) to '/Users/example-dev/Code/glaredb/demo/orders.csv';
COPY (SELECT * FROM public.part) to '/Users/example-dev/Code/glaredb/demo/part.csv';
COPY (SELECT * FROM public.partsupp) to '/Users/example-dev/Code/glaredb/demo/partsupp.csv';
COPY (SELECT * FROM public.region) to '/Users/example-dev/Code/glaredb/demo/region.csv';
COPY (SELECT * FROM public.supplier) to '/Users/example-dev/Code/glaredb/demo/supplier.csv';
```

Then we create / re-create the tables in the demo Postgres instance.

```
DROP TABLE IF EXISTS PART CASCADE;
CREATE TABLE PART (

	P_PARTKEY		SERIAL PRIMARY KEY,
	P_NAME			VARCHAR(55),
	P_MFGR			CHAR(25),
	P_BRAND			CHAR(10),
	P_TYPE			VARCHAR(25),
	P_SIZE			INTEGER,
	P_CONTAINER		CHAR(10),
	P_RETAILPRICE	DECIMAL,
	P_COMMENT		VARCHAR(23)
);

DROP TABLE IF EXISTS SUPPLIER CASCADE;
CREATE TABLE SUPPLIER (
	S_SUPPKEY		SERIAL PRIMARY KEY,
	S_NAME			CHAR(25),
	S_ADDRESS		VARCHAR(40),
	S_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
	S_PHONE			CHAR(15),
	S_ACCTBAL		DECIMAL,
	S_COMMENT		VARCHAR(101)
);

DROP TABLE IF EXISTS PARTSUPP CASCADE;
CREATE TABLE PARTSUPP (
	PS_PARTKEY		BIGINT NOT NULL, -- references P_PARTKEY
	PS_SUPPKEY		BIGINT NOT NULL, -- references S_SUPPKEY
	PS_AVAILQTY		INTEGER,
	PS_SUPPLYCOST	DECIMAL,
	PS_COMMENT		VARCHAR(199),
	PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
);

DROP TABLE IF EXISTS CUSTOMER CASCADE;
CREATE TABLE CUSTOMER (
	C_CUSTKEY		SERIAL PRIMARY KEY,
	C_NAME			VARCHAR(25),
	C_ADDRESS		VARCHAR(40),
	C_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
	C_PHONE			CHAR(15),
	C_ACCTBAL		DECIMAL,
	C_MKTSEGMENT	CHAR(10),
	C_COMMENT		VARCHAR(117)
);

DROP TABLE IF EXISTS ORDERS CASCADE;
CREATE TABLE ORDERS (
	O_ORDERKEY		SERIAL PRIMARY KEY,
	O_CUSTKEY		BIGINT NOT NULL, -- references C_CUSTKEY
	O_ORDERSTATUS	CHAR(1),
	O_TOTALPRICE	DECIMAL,
	O_ORDERDATE		DATE,
	O_ORDERPRIORITY	CHAR(15),
	O_CLERK			CHAR(15),
	O_SHIPPRIORITY	INTEGER,
	O_COMMENT		VARCHAR(79)
);

DROP TABLE IF EXISTS LINEITEM CASCADE;
CREATE TABLE LINEITEM (
	L_ORDERKEY		BIGINT NOT NULL, -- references O_ORDERKEY
	L_PARTKEY		BIGINT NOT NULL, -- references P_PARTKEY (compound fk to PARTSUPP)
	L_SUPPKEY		BIGINT NOT NULL, -- references S_SUPPKEY (compound fk to PARTSUPP)
	L_LINENUMBER	INTEGER,
	L_QUANTITY		DECIMAL,
	L_EXTENDEDPRICE	DECIMAL,
	L_DISCOUNT		DECIMAL,
	L_TAX			DECIMAL,
	L_RETURNFLAG	CHAR(1),
	L_LINESTATUS	CHAR(1),
	L_SHIPDATE		DATE,
	L_COMMITDATE	DATE,
	L_RECEIPTDATE	DATE,
	L_SHIPINSTRUCT	CHAR(25),
	L_SHIPMODE		CHAR(10),
	L_COMMENT		VARCHAR(44),
	PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
);

DROP TABLE IF EXISTS NATION CASCADE;
CREATE TABLE NATION (
	N_NATIONKEY		SERIAL PRIMARY KEY,
	N_NAME			CHAR(25),
	N_REGIONKEY		BIGINT NOT NULL,  -- references R_REGIONKEY
	N_COMMENT		VARCHAR(152)
);

DROP TABLE IF EXISTS REGION CASCADE;
CREATE TABLE REGION (
	R_REGIONKEY	SERIAL PRIMARY KEY,
	R_NAME		CHAR(25),
	R_COMMENT	VARCHAR(152)
);
```

And then we copy the data in, remembering to change the file path here again:

```
\copy CUSTOMER FROM '/Users/example-dev/Code/glaredb/demo/customer.csv' DELIMITER ',' CSV HEADER
\copy LINEITEM FROM '/Users/example-dev/Code/glaredb/demo/lineitem.csv' DELIMITER ',' CSV HEADER
\copy NATION FROM '/Users/example-dev/Code/glaredb/demo/nation.csv' DELIMITER ',' CSV HEADER
\copy ORDERS FROM '/Users/example-dev/Code/glaredb/demo/orders.csv' DELIMITER ',' CSV HEADER
\copy PART FROM '/Users/example-dev/Code/glaredb/demo/part.csv' DELIMITER ',' CSV HEADER
\copy PARTSUPP FROM '/Users/example-dev/Code/glaredb/demo/partsupp.csv' DELIMITER ',' CSV HEADER
\copy REGION FROM '/Users/example-dev/Code/glaredb/demo/region.csv' DELIMITER ',' CSV HEADER
\copy SUPPLIER FROM '/Users/example-dev/Code/glaredb/demo/supplier.csv' DELIMITER ',' CSV HEADER
```

The output should look like the following:

```
COPY 150000
COPY 6001215
COPY 25
COPY 1500000
COPY 200000
COPY 800000
COPY 5
COPY 10000
```

### Postgres Demo User

Here are the steps used to setup the demo user in Postgres: Ran as the `postgres` user and assumes the database only needs the `postgres` and `demo` user on the instance.

```
REVOKE CREATE ON SCHEMA public FROM public;
GRANT ALL ON SCHEMA public to postgres;
CREATE ROLE demo WITH LOGIN ENCRYPTED PASSWORD 'demo';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO demo;
```
