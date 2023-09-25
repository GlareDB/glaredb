import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer


Q_NUM = 2

@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()

    region_ds = utils.get_region_ds(con)
    nation_ds = utils.get_nation_ds(con)
    supplier_ds = utils.get_supplier_ds(con)
    part_ds = utils.get_part_ds(con)
    part_supp_ds = utils.get_part_supp_ds(con)

    query_str = f"""
    select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        trim(s_address) as s_address,
        s_phone,
        trim(s_comment) as s_comment
    from
        {part_ds},
        {supplier_ds},
        {part_supp_ds},
        {nation_ds},
        {region_ds} 
    where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
            select
                min(ps_supplycost)
            from
                {part_supp_ds},
                {supplier_ds},
                {nation_ds},
                {region_ds}
            where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'EUROPE'
        )
    order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey
    limit 100
    """
    utils.run_query(2, con, query_str)

@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    var_1 = 15
    var_2 = "BRASS"
    var_3 = "EUROPE"

    region_ds = polars_utils.get_region_ds()
    nation_ds = polars_utils.get_nation_ds()
    supplier_ds = polars_utils.get_supplier_ds()
    part_ds = polars_utils.get_part_ds()
    part_supp_ds = polars_utils.get_part_supp_ds()

    result_q1 = (
        part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("p_size") == var_1)
        .filter(pl.col("p_type").str.ends_with(var_2))
        .filter(pl.col("r_name") == var_3)
    ).cache()

    final_cols = [
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ]

    q_final = (
        result_q1.groupby("p_partkey")
        .agg(pl.min("ps_supplycost").alias("ps_supplycost"))
        .join(
            result_q1,
            left_on=["p_partkey", "ps_supplycost"],
            right_on=["p_partkey", "ps_supplycost"],
        )
        .select(final_cols)
        .sort(
            by=["s_acctbal", "n_name", "s_name", "p_partkey"],
            descending=[True, False, False, False],
        )
        .limit(100)
        .with_columns(pl.col(pl.datatypes.Utf8).str.strip().keep_name())
    )
    polars_utils.run_query(4, q_final)

if __name__ == "__main__":
    q()
    q_polars()
    

