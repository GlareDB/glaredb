CREATE TEMP VIEW nation AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/nation.parquet';
CREATE TEMP VIEW region AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/region.parquet';
CREATE TEMP VIEW customer AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/customer.parquet';
CREATE TEMP VIEW supplier AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/supplier.parquet';
CREATE TEMP VIEW lineitem AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/lineitem.parquet';
CREATE TEMP VIEW orders AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/orders.parquet';
CREATE TEMP VIEW partsupp AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/partsupp.parquet';
CREATE TEMP VIEW part AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/part.parquet';

SELECT
    l_orderkey
FROM
    lineitem
GROUP BY
    l_orderkey
HAVING
    sum(l_quantity) > 300
