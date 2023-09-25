import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 16
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    part_ds = utils.get_part_ds(con)
    line_item_ds = utils.get_line_item_ds(con)

    query_str = f"""
    select
        round(sum(l_extendedprice) / 7.0, 2) as avg_yearly
    from
        {line_item_ds},
        {part_ds}
    where
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container = 'MED BOX'
        and l_quantity < (
            select
                0.2 * avg(l_quantity)
            from
                {line_item_ds}
            where
                l_partkey = p_partkey
        )
	"""

    utils.run_query(Q_NUM, con, query_str)


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
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
        res_1.groupby("p_partkey")
        .agg((0.2 * pl.col("l_quantity").mean()).alias("avg_quantity"))
        .select([pl.col("p_partkey").alias("key"), pl.col("avg_quantity")])
        .join(res_1, left_on="key", right_on="p_partkey")
        .filter(pl.col("l_quantity") < pl.col("avg_quantity"))
        .select((pl.col("l_extendedprice").sum() / 7.0).round(2).alias("avg_yearly"))
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
