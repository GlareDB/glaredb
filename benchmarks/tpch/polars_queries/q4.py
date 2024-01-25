from datetime import datetime
import polars_queries.utils as polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 4


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q():
    var_1 = datetime(1993, 7, 1)
    var_2 = datetime(1993, 10, 1)

    line_item_ds = polars_utils.get_line_item_ds()
    orders_ds = polars_utils.get_orders_ds()

    q_final = (
        line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("o_orderdate").is_between(var_1, var_2, closed="left"))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .unique(subset=["o_orderpriority", "l_orderkey"])
        .group_by("o_orderpriority")
        .agg(pl.len().alias("order_count"))
        .sort(by="o_orderpriority")
        .with_columns(pl.col("order_count").cast(pl.datatypes.Int64))
    )
    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
