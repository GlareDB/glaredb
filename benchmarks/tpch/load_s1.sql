CREATE EXTERNAL TABLE customer FROM local OPTIONS (location = './gcs-artifacts/tpch_1/customer/part-0.parquet');
CREATE EXTERNAL TABLE lineitem FROM local OPTIONS (location = './gcs-artifacts/tpch_1/lineitem/part-0.parquet');
CREATE EXTERNAL TABLE nation FROM local OPTIONS (location = './gcs-artifacts/tpch_1/nation/part-0.parquet');
CREATE EXTERNAL TABLE orders FROM local OPTIONS (location = './gcs-artifacts/tpch_1/orders/part-0.parquet');
CREATE EXTERNAL TABLE part FROM local OPTIONS (location = './gcs-artifacts/tpch_1/part/part-0.parquet');
CREATE EXTERNAL TABLE partsupp FROM local OPTIONS (location = './gcs-artifacts/tpch_1/partsupp/part-0.parquet');
CREATE EXTERNAL TABLE region FROM local OPTIONS (location = './gcs-artifacts/tpch_1/region/part-0.parquet');
CREATE EXTERNAL TABLE supplier FROM local OPTIONS (location = './gcs-artifacts/tpch_1/supplier/part-0.parquet');
