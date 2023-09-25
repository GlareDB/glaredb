import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 10
pl.Config().set_fmt_float("full")


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


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    customer_ds = polars_utils.get_customer_ds()
    orders_ds = polars_utils.get_orders_ds()
    line_item_ds = polars_utils.get_line_item_ds()
    nation_ds = polars_utils.get_nation_ds()

    var_1 = datetime(1993, 10, 1)
    var_2 = datetime(1994, 1, 1)

    q_final = (
        customer_ds.join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
        .filter(pl.col("o_orderdate").is_between(var_1, var_2, closed="left"))
        .filter(pl.col("l_returnflag") == "R")
        .groupby(
            [
                "c_custkey",
                "c_name",
                "c_acctbal",
                "c_phone",
                "n_name",
                "c_address",
                "c_comment",
            ]
        )
        .agg(
            [
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                .sum()
                .round(2)
                .alias("revenue")
            ]
        )
        .with_columns(pl.col("c_address").str.strip(), pl.col("c_comment").str.strip())
        .select(
            [
                "c_custkey",
                "c_name",
                "revenue",
                "c_acctbal",
                "n_name",
                "c_address",
                "c_phone",
                "c_comment",
            ]
        )
        .sort(by="revenue", descending=True)
        .limit(20)
    )
    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
