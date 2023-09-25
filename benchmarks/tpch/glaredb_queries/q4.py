import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 4


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()

    line_item_ds = utils.get_line_item_ds(con)  # parquet_scan('lineitem.parquet')
    orders_ds = utils.get_orders_ds(con)  # parquet_scan('orders.parquet')

    query_str = f"""
    select
        o_orderpriority,
        count(*) as order_count
    from
        {orders_ds}
    where
        o_orderdate >= timestamp '1993-07-01'
        and o_orderdate < timestamp '1993-07-01' + interval '3' month
        and exists (
            select
                *
            from
                {line_item_ds}
            where
                l_orderkey = o_orderkey
                and l_commitdate < l_receiptdate
        )
    group by
        o_orderpriority
    order by
        o_orderpriority
    """

    utils.run_query(4, con, query_str)


if __name__ == "__main__":
    q()
