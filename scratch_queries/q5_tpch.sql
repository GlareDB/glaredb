CREATE TEMP VIEW nation AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-1/nation.parquet';
CREATE TEMP VIEW region AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-1/region.parquet';
CREATE TEMP VIEW customer AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-1/customer.parquet';
CREATE TEMP VIEW supplier AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-1/supplier.parquet';
CREATE TEMP VIEW lineitem AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-1/lineitem.parquet';
CREATE TEMP VIEW orders AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-1/orders.parquet';
CREATE TEMP VIEW partsupp AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-1/partsupp.parquet';
CREATE TEMP VIEW part AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-1/part.parquet';

SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC;
