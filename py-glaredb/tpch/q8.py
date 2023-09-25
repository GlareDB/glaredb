import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 8
pl.Config().set_fmt_float("full")

@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    part_ds = utils.get_part_ds(con)
    supplier_ds = utils.get_supplier_ds(con)
    line_item_ds = utils.get_line_item_ds(con)
    orders_ds = utils.get_orders_ds(con)
    customer_ds = utils.get_customer_ds(con)
    nation_ds = utils.get_nation_ds(con)
    region_ds = utils.get_region_ds(con)

    query_str = f"""
    select
        o_year,
        round(
            sum(case
                when nation = 'BRAZIL' then volume
                else 0
            end) / sum(volume)
        , 2) as mkt_share
    from
        (
            select
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                {part_ds},
                {supplier_ds},
                {line_item_ds},
                {orders_ds},
                {customer_ds},
                {nation_ds} n1,
                {nation_ds} n2,
                {region_ds}
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between timestamp '1995-01-01' and timestamp '1996-12-31'
                and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
    group by
        o_year
    order by
        o_year
	"""


    utils.run_query(Q_NUM, con, query_str)

@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    part_ds = polars_utils.get_part_ds()
    supplier_ds = polars_utils.get_supplier_ds()
    line_item_ds = polars_utils.get_line_item_ds()
    orders_ds = polars_utils.get_orders_ds()
    customer_ds = polars_utils.get_customer_ds()
    nation_ds = polars_utils.get_nation_ds()
    region_ds = polars_utils.get_region_ds()

    n1 = nation_ds.select(["n_nationkey", "n_regionkey"])
    n2 = nation_ds.clone().select(["n_nationkey", "n_name"])

    q_final = (
        part_ds.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .join(customer_ds, left_on="o_custkey", right_on="c_custkey")
        .join(n1, left_on="c_nationkey", right_on="n_nationkey")
        .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("r_name") == "AMERICA")
        .join(n2, left_on="s_nationkey", right_on="n_nationkey")
        .filter(
            pl.col("o_orderdate").is_between(
                datetime(1995, 1, 1), datetime(1996, 12, 31)
            )
        )
        .filter(pl.col("p_type") == "ECONOMY ANODIZED STEEL")
        .select(
            [
                pl.col("o_orderdate").dt.year().alias("o_year"),
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias(
                    "volume"
                ),
                pl.col("n_name").alias("nation"),
            ]
        )
        .with_columns(
            pl.when(pl.col("nation") == "BRAZIL")
            .then(pl.col("volume"))
            .otherwise(0)
            .alias("_tmp")
        )
        .groupby("o_year")
        .agg((pl.sum("_tmp") / pl.sum("volume")).round(2).alias("mkt_share"))
        .sort("o_year")
    )

    polars_utils.run_query(Q_NUM, q_final)

if __name__ == "__main__":
    q()
    q_polars()
    
