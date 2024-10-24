CREATE TEMP VIEW nation AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/nation.parquet';
CREATE TEMP VIEW region AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/region.parquet';
CREATE TEMP VIEW customer AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/customer.parquet';
CREATE TEMP VIEW supplier AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/supplier.parquet';
CREATE TEMP VIEW lineitem AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/lineitem.parquet';
CREATE TEMP VIEW orders AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/orders.parquet';
CREATE TEMP VIEW partsupp AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/partsupp.parquet';
CREATE TEMP VIEW part AS SELECT * FROM './crates/rayexec_python/benchmarks/data/tpch-10/part.parquet';

SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE'
                AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY'
                AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;
