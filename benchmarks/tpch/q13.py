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


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    var_1 = "special"
    var_2 = "requests"

    customer_ds = polars_utils.get_customer_ds()
    orders_ds = polars_utils.get_orders_ds().filter(
        pl.col("o_comment").str.contains(f"{var_1}.*{var_2}").is_not()
    )
    q_final = (
        customer_ds.join(
            orders_ds, left_on="c_custkey", right_on="o_custkey", how="left"
        )
        .groupby("c_custkey")
        .agg(
            [
                pl.col("o_orderkey").count().alias("c_count"),
                pl.col("o_orderkey").null_count().alias("null_c_count"),
            ]
        )
        .with_columns((pl.col("c_count") - pl.col("null_c_count")).alias("c_count"))
        .groupby("c_count")
        .count()
        .select([pl.col("c_count"), pl.col("count").alias("custdist")])
        .sort(["custdist", "c_count"], descending=[True, True])
    )
    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
