import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 9
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    part_ds = utils.get_part_ds(con)
    supplier_ds = utils.get_supplier_ds(con)
    line_item_ds = utils.get_line_item_ds(con)
    orders_ds = utils.get_orders_ds(con)
    part_supp_ds = utils.get_part_supp_ds(con)
    nation_ds = utils.get_nation_ds(con)

    query_str = f"""
    select
        nation,
        o_year,
        round(sum(amount), 2) as sum_profit
    from
        (
            select
                n_name as nation,
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                {part_ds},
                {supplier_ds},
                {line_item_ds},
                {part_supp_ds},
                {orders_ds},
                {nation_ds}
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%green%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
	"""

    utils.run_query(Q_NUM, con, query_str)


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    part_ds = polars_utils.get_part_ds()
    supplier_ds = polars_utils.get_supplier_ds()
    line_item_ds = polars_utils.get_line_item_ds()
    part_supp_ds = polars_utils.get_part_supp_ds()
    orders_ds = polars_utils.get_orders_ds()
    nation_ds = polars_utils.get_nation_ds()

    q_final = (
        line_item_ds.join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(
            part_supp_ds,
            left_on=["l_suppkey", "l_partkey"],
            right_on=["ps_suppkey", "ps_partkey"],
        )
        .join(part_ds, left_on="l_partkey", right_on="p_partkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("p_name").str.contains("green"))
        .select(
            [
                pl.col("n_name").alias("nation"),
                pl.col("o_orderdate").dt.year().alias("o_year"),
                (
                    pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
                    - pl.col("ps_supplycost") * pl.col("l_quantity")
                ).alias("amount"),
            ]
        )
        .groupby(["nation", "o_year"])
        .agg(pl.sum("amount").round(2).alias("sum_profit"))
        .sort(by=["nation", "o_year"], descending=[False, True])
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
