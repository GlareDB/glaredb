import duckdb
from duckdb import DuckDBPyConnection

from duckdb_queries import utils

Q_NUM = 15


def q():
    line_item_ds = utils.get_line_item_ds()
    supplier_ds = utils.get_supplier_ds()

    ddl = f"""
    create or replace temporary view revenue (supplier_no, total_revenue) as
        select
            l_suppkey,
            sum(l_extendedprice * (1 - l_discount))
        from
            {line_item_ds}
        where
            l_shipdate >= date '1996-01-01'
            and l_shipdate < date '1996-01-01' + interval '3' month
        group by
            l_suppkey
    """

    query_str = f"""
    select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
    from
        {supplier_ds},
        revenue
    where
        s_suppkey = supplier_no
        and total_revenue = (
            select
                max(total_revenue)
            from
                revenue
        )
    order by
        s_suppkey
	"""

    _ = duckdb.execute(ddl)
    q_final = duckdb.sql(query_str)

    utils.run_query(Q_NUM, q_final)
    duckdb.execute("DROP VIEW IF EXISTS revenue")


if __name__ == "__main__":
    q()
