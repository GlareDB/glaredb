from datetime import datetime

import polars_queries.utils as polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 18
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q():
    customer_ds = polars_utils.get_customer_ds()
    line_item_ds = polars_utils.get_line_item_ds()
    orders_ds = polars_utils.get_orders_ds()

    var_1 = 300

    q_final = (
        line_item_ds.group_by("l_orderkey")
        .agg(pl.col("l_quantity").sum().alias("sum_quantity"))
        .filter(pl.col("sum_quantity") > var_1)
        .select([pl.col("l_orderkey").alias("key"), pl.col("sum_quantity")])
        .join(orders_ds, left_on="key", right_on="o_orderkey")
        .join(line_item_ds, left_on="key", right_on="l_orderkey")
        .join(customer_ds, left_on="o_custkey", right_on="c_custkey")
        .group_by("c_name", "o_custkey", "key", "o_orderdate", "o_totalprice")
        .agg(pl.col("l_quantity").sum().alias("col6"))
        .select(
            [
                pl.col("c_name"),
                pl.col("o_custkey").alias("c_custkey"),
                pl.col("key").alias("o_orderkey"),
                pl.col("o_orderdate").alias("o_orderdat"),
                pl.col("o_totalprice"),
                pl.col("col6"),
            ]
        )
        .sort(["o_totalprice", "o_orderdat"], descending=[True, False])
        .limit(100)
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
