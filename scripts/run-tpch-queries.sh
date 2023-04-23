#!/usr/bin/env bash

: ${CONNECTION_STRING?"CONNECTION_STRING needs to be set"}
: ${GCP_SERVICE_ACCOUNT_JSON?"GCP_SERVICE_ACCOUNT_JSON needs to be set"}

sf="${SCALE_FACTOR:-1}"

psql "${CONNECTION_STRING}" -c "
CREATE EXTERNAL TABLE customer FROM gcs OPTIONS (service_account_key = '${GCP_SERVICE_ACCOUNT_JSON}', bucket = 'glaredb-benchmarks', location = 'data/tpch/sf${sf}/customer/part-0.parquet' );
CREATE EXTERNAL TABLE lineitem FROM gcs OPTIONS (service_account_key = '${GCP_SERVICE_ACCOUNT_JSON}', bucket = 'glaredb-benchmarks', location = 'data/tpch/sf${sf}/lineitem/part-0.parquet' );
CREATE EXTERNAL TABLE nation FROM gcs OPTIONS (service_account_key = '${GCP_SERVICE_ACCOUNT_JSON}', bucket = 'glaredb-benchmarks', location = 'data/tpch/sf${sf}/nation/part-0.parquet' );
CREATE EXTERNAL TABLE orders FROM gcs OPTIONS (service_account_key = '${GCP_SERVICE_ACCOUNT_JSON}', bucket = 'glaredb-benchmarks', location = 'data/tpch/sf${sf}/orders/part-0.parquet' );
CREATE EXTERNAL TABLE part FROM gcs OPTIONS (service_account_key = '${GCP_SERVICE_ACCOUNT_JSON}', bucket = 'glaredb-benchmarks', location = 'data/tpch/sf${sf}/part/part-0.parquet' );
CREATE EXTERNAL TABLE partsupp FROM gcs OPTIONS (service_account_key = '${GCP_SERVICE_ACCOUNT_JSON}', bucket = 'glaredb-benchmarks', location = 'data/tpch/sf${sf}/partsupp/part-0.parquet' );
CREATE EXTERNAL TABLE region FROM gcs OPTIONS (service_account_key = '${GCP_SERVICE_ACCOUNT_JSON}', bucket = 'glaredb-benchmarks', location = 'data/tpch/sf${sf}/region/part-0.parquet' );
CREATE EXTERNAL TABLE supplier FROM gcs OPTIONS (service_account_key = '${GCP_SERVICE_ACCOUNT_JSON}', bucket = 'glaredb-benchmarks', location = 'data/tpch/sf${sf}/supplier/part-0.parquet' );
"

