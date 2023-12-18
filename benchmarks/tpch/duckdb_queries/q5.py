import duckdb

from duckdb_queries import utils

Q_NUM = 5


def q():
    region_ds = utils.get_region_ds()
    nation_ds = utils.get_nation_ds()
    customer_ds = utils.get_customer_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    supplier_ds = utils.get_supplier_ds()

    query_str = f"""
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        {customer_ds},
        {orders_ds},
        {line_item_ds},
        {supplier_ds},
        {nation_ds},
        {region_ds}
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= timestamp '1994-01-01'
        and o_orderdate < timestamp '1994-01-01' + interval '1' year
    group by
        n_name
    order by
        revenue desc
    """

    q_final = duckdb.sql(query_str)

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
