from datetime import datetime

import polars_queries.utils as polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 11
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q():
    supplier_ds = polars_utils.get_supplier_ds()
    part_supp_ds = polars_utils.get_part_supp_ds()
    nation_ds = polars_utils.get_nation_ds()

    var_1 = "GERMANY"
    var_2 = 0.0001

    res_1 = (
        part_supp_ds.join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("n_name") == var_1)
    )
    res_2 = res_1.select(
        (pl.col("ps_supplycost") * pl.col("ps_availqty")).sum().round(2).alias("tmp")
        * var_2
    ).with_columns(pl.lit(1).alias("lit"))

    q_final = (
        res_1.group_by("ps_partkey")
        .agg(
            (pl.col("ps_supplycost") * pl.col("ps_availqty"))
            .sum()
            .round(2)
            .alias("value")
        )
        .with_columns(pl.lit(1).alias("lit"))
        .join(res_2, on="lit")
        .filter(pl.col("value") > pl.col("tmp"))
        .select(["ps_partkey", "value"])
        .sort("value", descending=True)
    )
    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
