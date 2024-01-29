from datetime import datetime
import polars_queries.utils as polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

pl.Config().set_tbl_rows(20)
Q_NUM = 3


@linetimer(name="Overall execution of polars Query 2", unit="ms")
def q():
    var_1 = var_2 = datetime(1995, 3, 15)
    var_3 = "BUILDING"

    customer_ds = polars_utils.get_customer_ds()
    line_item_ds = polars_utils.get_line_item_ds()
    orders_ds = polars_utils.get_orders_ds()

    q_final = (
        customer_ds.filter(pl.col("c_mktsegment") == var_3)
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("o_orderdate") < var_2)
        .filter(pl.col("l_shipdate") > var_1)
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .group_by(["o_orderkey", "o_orderdate", "o_shippriority"])
        .agg([pl.sum("revenue")])
        .select(
            [
                pl.col("o_orderkey").alias("l_orderkey"),
                "revenue",
                "o_orderdate",
                "o_shippriority",
            ]
        )
        .sort(by=["revenue", "o_orderdate"], descending=[True, False])
        .limit(10)
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
