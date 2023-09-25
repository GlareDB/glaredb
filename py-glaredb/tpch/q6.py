import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 6
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()

    line_item_ds = utils.get_line_item_ds(con)

    query_str = """
    select
         sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= timestamp '1994-01-01'
        and l_shipdate < timestamp '1994-01-01' + interval '1' year
        and l_discount between .06 - 0.01 and .06 + 0.01
        and l_quantity < 24
    """

    utils.run_query(Q_NUM, con, query_str)


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    var_1 = datetime(1994, 1, 1)
    var_2 = datetime(1995, 1, 1)
    var_3 = 24

    line_item_ds = polars_utils.get_line_item_ds()

    q_final = (
        line_item_ds.filter(
            pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
        )
        .filter(pl.col("l_discount").is_between(0.05, 0.07))
        .filter(pl.col("l_quantity") < var_3)
        .with_columns(
            (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
        )
        .select(pl.sum("revenue").alias("revenue"))
    )
    print(q_final.explain())
    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
