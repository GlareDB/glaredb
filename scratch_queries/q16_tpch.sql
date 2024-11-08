CREATE VIEW nation AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/nation.parquet';
CREATE VIEW region AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/region.parquet';
CREATE VIEW customer AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/customer.parquet';
CREATE VIEW supplier AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/supplier.parquet';
CREATE VIEW lineitem AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/lineitem.parquet';
CREATE VIEW orders AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/orders.parquet';
CREATE VIEW partsupp AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/partsupp.parquet';
CREATE VIEW part AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/part.parquet';

explain SELECT
    p_brand,
    p_type,
    p_size,
    count(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp,
    part
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT
            s_suppkey
        FROM
            supplier
        WHERE
            s_comment LIKE '%Customer%Complaints%')
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;
