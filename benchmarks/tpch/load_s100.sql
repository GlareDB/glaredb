CREATE EXTERNAL TABLE customer FROM local OPTIONS (location = './gcs-artifacts/tpch_100/customer/part-0.parquet');
CREATE EXTERNAL TABLE lineitem FROM local OPTIONS (location = './gcs-artifacts/tpch_100/lineitem/part-0.parquet');
CREATE EXTERNAL TABLE nation FROM local OPTIONS (location = './gcs-artifacts/tpch_100/nation/part-0.parquet');
CREATE EXTERNAL TABLE orders FROM local OPTIONS (location = './gcs-artifacts/tpch_100/orders/part-0.parquet');
CREATE EXTERNAL TABLE part FROM local OPTIONS (location = './gcs-artifacts/tpch_100/part/part-0.parquet');
CREATE EXTERNAL TABLE partsupp FROM local OPTIONS (location = './gcs-artifacts/tpch_100/partsupp/part-0.parquet');
CREATE EXTERNAL TABLE region FROM local OPTIONS (location = './gcs-artifacts/tpch_100/region/part-0.parquet');
CREATE EXTERNAL TABLE supplier FROM local OPTIONS (location = './gcs-artifacts/tpch_100/supplier/part-0.parquet');
