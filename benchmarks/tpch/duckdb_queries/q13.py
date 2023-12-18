import duckdb

from duckdb_queries import utils

Q_NUM = 13


def q():
    orders_ds = utils.get_orders_ds()
    customer_ds = utils.get_customer_ds()

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
        )as c_orders (c_custkey, c_count)
    group by
        c_count
    order by
        custdist desc,
        c_count desc
	"""

    q_final = duckdb.sql(query_str)

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
