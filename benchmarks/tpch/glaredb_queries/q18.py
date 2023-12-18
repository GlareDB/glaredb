import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 18


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    line_item_ds = utils.get_line_item_ds(con)
    orders_ds = utils.get_orders_ds(con)
    customer_ds = utils.get_customer_ds(con)

    query_str = f"""
    select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate as o_orderdat,
        o_totalprice,
        sum(l_quantity) as col6
    from
        {customer_ds},
        {orders_ds},
        {line_item_ds}
    where
        o_orderkey in (
            select
                l_orderkey
            from
                {line_item_ds}
            group by
                l_orderkey having
                    sum(l_quantity) > 300
        )
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
    group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
    order by
        o_totalprice desc,
        o_orderdate
    limit 100
	"""
    utils.run_query(Q_NUM, con, query_str)


if __name__ == "__main__":
    q()
