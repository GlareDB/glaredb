CREATE TEMP VIEW nation AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-5/nation.parquet';
CREATE TEMP VIEW region AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-5/region.parquet';
CREATE TEMP VIEW customer AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-5/customer.parquet';
CREATE TEMP VIEW supplier AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-5/supplier.parquet';
CREATE TEMP VIEW lineitem AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-5/lineitem.parquet';
CREATE TEMP VIEW orders AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-5/orders.parquet';
CREATE TEMP VIEW partsupp AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-5/partsupp.parquet';
CREATE TEMP VIEW part AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-5/part.parquet';

SELECT
    o_year,
    sum(
        CASE WHEN nation = 'BRAZIL' THEN
            volume
        ELSE
            0
        END) / sum(volume) AS mkt_share
FROM (
    SELECT
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM
       part,
       supplier,
       lineitem,
       orders,
       customer,
       nation n1,
       nation n2,
       region
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
        AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;
