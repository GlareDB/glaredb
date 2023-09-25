import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 20
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    line_item_ds = utils.get_line_item_ds(con)
    nation_ds = utils.get_nation_ds(con)
    supplier_ds = utils.get_supplier_ds(con)
    part_ds = utils.get_part_ds(con)
    part_supp_ds = utils.get_part_supp_ds(con)

    query_str = f"""
    select
        s_name,
        trim(s_address) as s_address
    from
        {supplier_ds},
        {nation_ds}
    where
        s_suppkey in (
            select
                ps_suppkey
            from
                {part_supp_ds}
            where
                ps_partkey in (
                    select
                        p_partkey
                    from
                        {part_ds}
                    where
                        p_name like 'forest%'
                )
                and ps_availqty > (
                    select
                        0.5 * sum(l_quantity)
                    from
                        {line_item_ds}
                    where
                        l_partkey = ps_partkey
                        and l_suppkey = ps_suppkey
                        and l_shipdate >= date '1994-01-01'
                        and l_shipdate < date '1994-01-01' + interval '1' year
                )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
    order by
        s_name
	"""
    utils.run_query(Q_NUM, con, query_str)


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    line_item_ds = polars_utils.get_line_item_ds()
    nation_ds = polars_utils.get_nation_ds()
    supplier_ds = polars_utils.get_supplier_ds()
    part_ds = polars_utils.get_part_ds()
    part_supp_ds = polars_utils.get_part_supp_ds()

    var_1 = datetime(1994, 1, 1)
    var_2 = datetime(1995, 1, 1)
    var_3 = "CANADA"
    var_4 = "forest"

    res_1 = (
        line_item_ds.filter(
            pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
        )
        .groupby("l_partkey", "l_suppkey")
        .agg((pl.col("l_quantity").sum() * 0.5).alias("sum_quantity"))
    )
    res_2 = nation_ds.filter(pl.col("n_name") == var_3)
    res_3 = supplier_ds.join(res_2, left_on="s_nationkey", right_on="n_nationkey")

    q_final = (
        part_ds.filter(pl.col("p_name").str.starts_with(var_4))
        .select(pl.col("p_partkey").unique())
        .join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .join(
            res_1,
            left_on=["ps_suppkey", "p_partkey"],
            right_on=["l_suppkey", "l_partkey"],
        )
        .filter(pl.col("ps_availqty") > pl.col("sum_quantity"))
        .select(pl.col("ps_suppkey").unique())
        .join(res_3, left_on="ps_suppkey", right_on="s_suppkey")
        .with_columns(pl.col("s_address").str.strip())
        .select(["s_name", "s_address"])
        .sort("s_name")
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
