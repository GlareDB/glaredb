#!/usr/bin/env bash

set -e

: ${GCP_SERVICE_ACCOUNT_JSON?"GCP_SERVICE_ACCOUNT_JSON needs to be set"}

reporoot="$(git rev-parse --show-toplevel)"
sf="${SCALE_FACTOR:-1}"

rm -r "${reporoot}/benchmarks/artifacts/tpch_${sf}" || true
mkdir -p "${reporoot}/benchmarks/artifacts/tpch_${sf}/customer"
mkdir -p "${reporoot}/benchmarks/artifacts/tpch_${sf}/lineitem"
mkdir -p "${reporoot}/benchmarks/artifacts/tpch_${sf}/nation"
mkdir -p "${reporoot}/benchmarks/artifacts/tpch_${sf}/orders"
mkdir -p "${reporoot}/benchmarks/artifacts/tpch_${sf}/part"
mkdir -p "${reporoot}/benchmarks/artifacts/tpch_${sf}/partsupp"
mkdir -p "${reporoot}/benchmarks/artifacts/tpch_${sf}/region"
mkdir -p "${reporoot}/benchmarks/artifacts/tpch_${sf}/supplier"

gcloud storage cp "gs://glaredb-benchmarks/data/tpch/sf${sf}/customer/part-0.parquet" "${reporoot}/benchmarks/artifacts/tpch_${sf}/customer/."
gcloud storage cp "gs://glaredb-benchmarks/data/tpch/sf${sf}/lineitem/part-0.parquet" "${reporoot}/benchmarks/artifacts/tpch_${sf}/lineitem/."
gcloud storage cp "gs://glaredb-benchmarks/data/tpch/sf${sf}/nation/part-0.parquet" "${reporoot}/benchmarks/artifacts/tpch_${sf}/nation/."
gcloud storage cp "gs://glaredb-benchmarks/data/tpch/sf${sf}/orders/part-0.parquet" "${reporoot}/benchmarks/artifacts/tpch_${sf}/orders/."
gcloud storage cp "gs://glaredb-benchmarks/data/tpch/sf${sf}/part/part-0.parquet" "${reporoot}/benchmarks/artifacts/tpch_${sf}/part/."
gcloud storage cp "gs://glaredb-benchmarks/data/tpch/sf${sf}/partsupp/part-0.parquet" "${reporoot}/benchmarks/artifacts/tpch_${sf}/partsupp/."
gcloud storage cp "gs://glaredb-benchmarks/data/tpch/sf${sf}/region/part-0.parquet" "${reporoot}/benchmarks/artifacts/tpch_${sf}/region/."
gcloud storage cp "gs://glaredb-benchmarks/data/tpch/sf${sf}/supplier/part-0.parquet" "${reporoot}/benchmarks/artifacts/tpch_${sf}/supplier/."
