import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 10


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    customer_ds = utils.get_customer_ds(con)
    orders_ds = utils.get_orders_ds(con)
    line_item_ds = utils.get_line_item_ds(con)
    nation_ds = utils.get_nation_ds(con)

    query_str = f"""
    select
        c_custkey,
        c_name,
        round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
        c_acctbal,
        n_name,
        trim(c_address) as c_address,
        c_phone,
        trim(c_comment) as c_comment
    from
        {customer_ds},
        {orders_ds},
        {line_item_ds},
        {nation_ds}
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
	"""

    utils.run_query(Q_NUM, con, query_str)


if __name__ == "__main__":
    q()
