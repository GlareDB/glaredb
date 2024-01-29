from datetime import datetime

import polars_queries.utils as polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 13
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q():
    var_1 = "special"
    var_2 = "requests"

    customer_ds = polars_utils.get_customer_ds()
    orders_ds = polars_utils.get_orders_ds().filter(
        pl.col("o_comment").str.contains(f"{var_1}.*{var_2}").not_()
    )
    q_final = (
        customer_ds.join(
            orders_ds, left_on="c_custkey", right_on="o_custkey", how="left"
        )
        .group_by("c_custkey")
        .agg(
            [
                pl.col("o_orderkey").len().alias("c_count"),
                pl.col("o_orderkey").null_count().alias("null_c_count"),
            ]
        )
        .with_columns((pl.col("c_count") - pl.col("null_c_count")).alias("c_count"))
        .group_by("c_count")
        .len()
        .select([pl.col("c_count"), pl.col("len").alias("custdist")])
        .sort(["custdist", "c_count"], descending=[True, True])
    )
    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
