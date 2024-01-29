from datetime import datetime

import polars_queries.utils as polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 17
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q():
    var_1 = "Brand#23"
    var_2 = "MED BOX"

    line_item_ds = polars_utils.get_line_item_ds()
    part_ds = polars_utils.get_part_ds()

    res_1 = (
        part_ds.filter(pl.col("p_brand") == var_1)
        .filter(pl.col("p_container") == var_2)
        .join(line_item_ds, how="left", left_on="p_partkey", right_on="l_partkey")
    )

    q_final = (
        res_1.group_by("p_partkey")
        .agg((0.2 * pl.col("l_quantity").mean()).alias("avg_quantity"))
        .select([pl.col("p_partkey").alias("key"), pl.col("avg_quantity")])
        .join(res_1, left_on="key", right_on="p_partkey")
        .filter(pl.col("l_quantity") < pl.col("avg_quantity"))
        .select((pl.col("l_extendedprice").sum() / 7.0).round(2).alias("avg_yearly"))
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
