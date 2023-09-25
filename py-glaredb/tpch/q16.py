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
    supplier_ds = utils.get_supplier_ds(con)
    part_supp_ds = utils.get_part_supp_ds(con)

    query_str = f"""
    select
        p_brand,
        p_type,
        p_size,
        count(distinct ps_suppkey) as supplier_cnt
    from
        {part_supp_ds},
        {part_ds}
    where
        p_partkey = ps_partkey
        and p_brand <> 'Brand#45'
        and p_type not like 'MEDIUM POLISHED%'
        and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
        and ps_suppkey not in (
            select
                s_suppkey
            from
                {supplier_ds}
            where
                s_comment like '%Customer%Complaints%'
        )
    group by
        p_brand,
        p_type,
        p_size
    order by
        supplier_cnt desc,
        p_brand,
        p_type,
        p_size
	"""

    utils.run_query(Q_NUM, con, query_str)


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    part_supp_ds = polars_utils.get_part_supp_ds()
    part_ds = polars_utils.get_part_ds()
    supplier_ds = (
        polars_utils.get_supplier_ds()
        .filter(pl.col("s_comment").str.contains(f".*Customer.*Complaints.*"))
        .select(pl.col("s_suppkey"), pl.col("s_suppkey").alias("ps_suppkey"))
    )

    var_1 = "Brand#45"

    q_final = (
        part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .filter(pl.col("p_brand") != var_1)
        .filter(pl.col("p_type").str.contains("MEDIUM POLISHED*").is_not())
        .filter(pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey", how="left")
        .filter(pl.col("ps_suppkey_right").is_null())
        .groupby(["p_brand", "p_type", "p_size"])
        .agg([pl.col("ps_suppkey").n_unique().alias("supplier_cnt")])
        .sort(
            by=["supplier_cnt", "p_brand", "p_type", "p_size"],
            descending=[True, False, False, False],
        )
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
