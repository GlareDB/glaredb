import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 5

@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()

    region_ds = utils.get_region_ds(con)
    nation_ds = utils.get_nation_ds(con)
    customer_ds = utils.get_customer_ds(con)
    line_item_ds = utils.get_line_item_ds(con)
    orders_ds = utils.get_orders_ds(con)
    supplier_ds = utils.get_supplier_ds(con)

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

    utils.run_query(Q_NUM, con, query_str)

@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    var_1 = "ASIA"
    var_2 = datetime(1994, 1, 1)
    var_3 = datetime(1995, 1, 1)

    region_ds = polars_utils.get_region_ds()
    nation_ds = polars_utils.get_nation_ds()
    customer_ds = polars_utils.get_customer_ds()
    line_item_ds = polars_utils.get_line_item_ds()
    orders_ds = polars_utils.get_orders_ds()
    supplier_ds = polars_utils.get_supplier_ds()

    q_final = (
        region_ds.join(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
        .join(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(
            supplier_ds,
            left_on=["l_suppkey", "n_nationkey"],
            right_on=["s_suppkey", "s_nationkey"],
        )
        .filter(pl.col("r_name") == var_1)
        .filter(pl.col("o_orderdate").is_between(var_2, var_3, closed="left"))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .groupby("n_name")
        .agg([pl.sum("revenue")])
        .sort(by="revenue", descending=True)
    )

    polars_utils.run_query(Q_NUM, q_final)

if __name__ == "__main__":
    q()
    q_polars()
    
