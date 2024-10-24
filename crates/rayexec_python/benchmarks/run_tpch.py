import duckdb
import datafusion
import rayexec
import pathlib
import pandas as pd
import time
import os

# TPC-H scale factor.
sf = 10


def generate_data():
    if os.path.isdir(f"./benchmarks/data/tpch-{sf}"):
        return
    con = duckdb.connect()
    con.sql("PRAGMA disable_progress_bar;SET preserve_insertion_order=false")
    con.sql(f"CALL dbgen(sf={sf})")
    pathlib.Path(f"./benchmarks/data/tpch-{sf}").mkdir(parents=True, exist_ok=True)
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        con.sql(
            f"COPY (SELECT * FROM {tbl}) TO './benchmarks/data/tpch-{sf}/{tbl}.parquet'"
        )
    con.close()


generate_data()

queries = {
    1: """
SELECT
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
    """,
    2: """
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
      part,
      supplier,
      partsupp,
      nation,
      region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE')
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;
    """,
    3: """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
    """,
    4: """
    SELECT
        o_orderpriority,
        count(*) AS order_count
    FROM
        orders
    WHERE
        o_orderdate >= CAST('1993-07-01' AS date)
        AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS (
            SELECT
                *
            FROM
                lineitem
            WHERE
                l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate)
    GROUP BY
        o_orderpriority
    ORDER BY
        o_orderpriority;
        """,
    5: """
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
        """,
    6: """
    SELECT
        sum(l_extendedprice * l_discount) AS revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
        AND l_discount BETWEEN 0.05
        AND 0.07
        AND l_quantity < 24;
        """,
    7: """
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
        """,
    8: """
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
        """,
}


def setup_rayexec(conn):
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        conn.query(
            f"CREATE TEMP VIEW {tbl} AS SELECT * FROM './benchmarks/data/tpch-{sf}/{tbl}.parquet'"
        )


def setup_duckdb(conn):
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        conn.query(
            f"CREATE TEMP VIEW {tbl} AS SELECT * FROM './benchmarks/data/tpch-{sf}/{tbl}.parquet'"
        )


def setup_datafusion(ctx):
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        ctx.register_parquet(tbl, f"./benchmarks/data/tpch-{sf}/{tbl}.parquet")


def execute_rayexec(conn, dump_profile=False):
    df = pd.DataFrame(columns=["dur", "query"])
    for query_id, query in sorted(queries.items()):
        print("Query " + str(query_id))
        start = time.time()
        try:
            collect_profile_data = dump_profile
            table = conn.query(query, collect_profile_data)
            table.show()
            stop = time.time()
            duration = stop - start
            if dump_profile:
                table.dump_profile()
        except Exception as err:
            print(err)
        print(duration)
        row = {"dur": duration, "query": query_id}
        df = pd.concat(
            [
                df if not df.empty else None,
                pd.DataFrame(row, index=[query_id]),
            ],
            axis=0,
            ignore_index=True,
        )
    return df


def execute_datafusion(ctx):
    df = pd.DataFrame(columns=["dur", "query"])
    for query_id, query in sorted(queries.items()):
        start = time.time()
        print("Query " + str(query_id))
        try:
            print(ctx.sql(query))
            stop = time.time()
            duration = stop - start
        except Exception as er:
            print(er)
            duration = 0
        print(duration)
        row = {"dur": duration, "query": query_id}
        df = pd.concat(
            [
                df if not df.empty else None,
                pd.DataFrame(row, index=[query_id]),
            ],
            axis=0,
            ignore_index=True,
        )
    return df


def execute_duckdb(conn):
    df = pd.DataFrame(columns=["dur", "query"])
    for query_id, query in sorted(queries.items()):
        start = time.time()
        print("Query " + str(query_id))
        try:
            print(conn.sql(query))
            stop = time.time()
            duration = stop - start
        except Exception as er:
            print(er)
            duration = 0
        print(duration)
        row = {"dur": duration, "query": query_id}
        df = pd.concat(
            [
                df if not df.empty else None,
                pd.DataFrame(row, index=[query_id]),
            ],
            axis=0,
            ignore_index=True,
        )
    return df


rayexec_conn = rayexec.connect()
setup_rayexec(rayexec_conn)

datafusion_ctx = datafusion.SessionContext()
setup_datafusion(datafusion_ctx)

duckdb_conn = duckdb.connect()
setup_duckdb(duckdb_conn)

rayexec_times = execute_rayexec(rayexec_conn, False)
rayexec_conn.close()

datafusion_times = execute_datafusion(datafusion_ctx)

duckdb_times = execute_duckdb(duckdb_conn)
duckdb_conn.close()


print("RAYEXEC")
print(rayexec_times)
print("DATAFUSION")
print(datafusion_times)
print("DUCKDB")
print(duckdb_times)
