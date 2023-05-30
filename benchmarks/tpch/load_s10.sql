CREATE EXTERNAL TABLE customer FROM local OPTIONS (location = './benchmarks/artifacts/tpch_10/customer/part-0.parquet');
CREATE EXTERNAL TABLE lineitem FROM local OPTIONS (location = './benchmarks/artifacts/tpch_10/lineitem/part-0.parquet');
CREATE EXTERNAL TABLE nation FROM local OPTIONS (location = './benchmarks/artifacts/tpch_10/nation/part-0.parquet');
CREATE EXTERNAL TABLE orders FROM local OPTIONS (location = './benchmarks/artifacts/tpch_10/orders/part-0.parquet');
CREATE EXTERNAL TABLE part FROM local OPTIONS (location = './benchmarks/artifacts/tpch_10/part/part-0.parquet');
CREATE EXTERNAL TABLE partsupp FROM local OPTIONS (location = './benchmarks/artifacts/tpch_10/partsupp/part-0.parquet');
CREATE EXTERNAL TABLE region FROM local OPTIONS (location = './benchmarks/artifacts/tpch_10/region/part-0.parquet');
CREATE EXTERNAL TABLE supplier FROM local OPTIONS (location = './benchmarks/artifacts/tpch_10/supplier/part-0.parquet');
