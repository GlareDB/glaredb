from datetime import datetime

import polars_queries.utils as polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 16
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q():
    part_supp_ds = polars_utils.get_part_supp_ds()
    part_ds = polars_utils.get_part_ds()
    supplier_ds = (
        polars_utils.get_supplier_ds()
        .filter(pl.col("s_comment").str.contains(".*Customer.*Complaints.*"))
        .select(pl.col("s_suppkey"), pl.col("s_suppkey").alias("ps_suppkey"))
    )

    var_1 = "Brand#45"

    q_final = (
        part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .filter(pl.col("p_brand") != var_1)
        .filter(pl.col("p_type").str.contains("MEDIUM POLISHED*").not_())
        .filter(pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey", how="left")
        .filter(pl.col("ps_suppkey_right").is_null())
        .group_by(["p_brand", "p_type", "p_size"])
        .agg([pl.col("ps_suppkey").n_unique().alias("supplier_cnt")])
        .sort(
            by=["supplier_cnt", "p_brand", "p_type", "p_size"],
            descending=[True, False, False, False],
        )
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
