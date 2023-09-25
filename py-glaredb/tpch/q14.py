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
    part_ds = utils.get_part_ds(con)
    line_item_ds = utils.get_line_item_ds(con)

    query_str = f"""
    select
        round(100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
    from
        {line_item_ds},
        {part_ds}
    where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-09-01'
        and l_shipdate < date '1995-09-01' + interval '1' month
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
