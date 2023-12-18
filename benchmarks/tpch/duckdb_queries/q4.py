import duckdb

from duckdb_queries import utils

Q_NUM = 4


def q():
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()

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

    q_final = duckdb.sql(query_str)

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
