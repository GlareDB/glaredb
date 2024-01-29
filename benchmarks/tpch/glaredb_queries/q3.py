import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 3


@linetimer(name="Overall execution of glaredb Query 3", unit="ms")
def q():
    con = glaredb.connect()

    customer_ds = utils.get_customer_ds(con)
    line_item_ds = utils.get_line_item_ds(con)
    orders_ds = utils.get_orders_ds(con)

    query_str = f"""
    select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    from
        {customer_ds},
        {orders_ds},
        {line_item_ds}
    where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < '1995-03-15'
        and l_shipdate > '1995-03-15'
    group by
        l_orderkey,
        o_orderdate,
        o_shippriority
    order by
        revenue desc,
        o_orderdate
    limit 10
    """

    utils.run_query(Q_NUM, con, query_str)


if __name__ == "__main__":
    q()
