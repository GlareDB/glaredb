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
    supplier_ds = utils.get_supplier_ds(con)
    part_supp_ds = utils.get_part_supp_ds(con)
    nation_ds = utils.get_nation_ds(con)

    query_str = f"""
    select
        ps_partkey,
        round(sum(ps_supplycost * ps_availqty), 2) as value
    from
        {part_supp_ds},
        {supplier_ds},
        {nation_ds}
    where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
    group by
        ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001
            from
                {part_supp_ds},
                {supplier_ds},
                {nation_ds}
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'GERMANY'
            )
        order by
            value desc
	"""

    utils.run_query(Q_NUM, con, query_str)


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
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
        res_1.groupby("ps_partkey")
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
    q_polars()
