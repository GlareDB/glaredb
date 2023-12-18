import duckdb

from duckdb_queries import utils

Q_NUM = 10


def q():
    customer_ds = utils.get_customer_ds()
    orders_ds = utils.get_orders_ds()
    line_item_ds = utils.get_line_item_ds()
    nation_ds = utils.get_nation_ds()

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

    q_final = duckdb.sql(query_str)

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
