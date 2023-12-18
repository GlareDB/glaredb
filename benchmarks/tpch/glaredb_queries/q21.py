import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 21


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    line_item_ds = utils.get_line_item_ds(con)
    supplier_ds = utils.get_supplier_ds(con)
    nation_ds = utils.get_nation_ds(con)
    orders_ds = utils.get_orders_ds(con)

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

    utils.run_query(Q_NUM, con, query_str)


if __name__ == "__main__":
    q()
