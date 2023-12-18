import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 13


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    orders_ds = utils.get_orders_ds(con)
    customer_ds = utils.get_customer_ds(con)

    query_str = f"""
    select
        c_count, count(*) as custdist
    from (
        select
            c_custkey,
            count(o_orderkey)
        from
            {customer_ds} left outer join {orders_ds} on
            c_custkey = o_custkey
            and o_comment not like '%special%requests%'
        group by
            c_custkey
        ) as c_orders (c_custkey, c_count)
    group by
        c_count
    order by
        custdist desc,
        c_count desc
	"""

    utils.run_query(Q_NUM, con, query_str)


if __name__ == "__main__":
    q()
