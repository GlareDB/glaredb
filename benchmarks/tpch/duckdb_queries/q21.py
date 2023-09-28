import duckdb

from duckdb_queries import utils

Q_NUM = 21


def q():
    line_item_ds = utils.get_line_item_ds()
    supplier_ds = utils.get_supplier_ds()
    nation_ds = utils.get_nation_ds()
    orders_ds = utils.get_orders_ds()

    query_str = f"""
    select
        s_name,
        count(*) as numwait
    from
        {supplier_ds},
        {line_item_ds} l1,
        {orders_ds},
        {nation_ds}
    where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
            select
                *
            from
                {line_item_ds} l2
            where
                l2.l_orderkey = l1.l_orderkey
                and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
            select
                *
            from
                {line_item_ds} l3
            where
                l3.l_orderkey = l1.l_orderkey
                and l3.l_suppkey <> l1.l_suppkey
                and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
    group by
        s_name
    order by
        numwait desc,
        s_name
    limit 100
	"""

    q_final = duckdb.sql(query_str)

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
