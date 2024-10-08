CREATE VIEW nation AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/nation.parquet';
CREATE VIEW region AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/region.parquet';
CREATE VIEW customer AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/customer.parquet';
CREATE VIEW supplier AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/supplier.parquet';
CREATE VIEW lineitem AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/lineitem.parquet';
CREATE VIEW orders AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/orders.parquet';
CREATE VIEW partsupp AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/partsupp.parquet';
CREATE VIEW part AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/part.parquet';

explain SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-09-02'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;

