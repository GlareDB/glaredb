import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 22
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    orders_ds = utils.get_orders_ds(con)
    customer_ds = utils.get_customer_ds(con)

    query_str = f"""
    select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
    from (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            {customer_ds}
        where
            substring(c_phone from 1 for 2) in
                (13, 31, 23, 29, 30, 18, 17)
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    {customer_ds}
                where
                    c_acctbal > 0.00
                    and substring (c_phone from 1 for 2) in
                        (13, 31, 23, 29, 30, 18, 17)
            )
            and not exists (
                select
                    *
                from
                    {orders_ds}
                where
                    o_custkey = c_custkey
            )
        ) as custsale
    group by
        cntrycode
    order by
        cntrycode
	"""

    utils.run_query(Q_NUM, con, query_str)


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    orders_ds = polars_utils.get_orders_ds()
    customer_ds = polars_utils.get_customer_ds()

    res_1 = (
        customer_ds.with_columns(pl.col("c_phone").str.slice(0, 2).alias("cntrycode"))
        .filter(pl.col("cntrycode").str.contains("13|31|23|29|30|18|17"))
        .select(["c_acctbal", "c_custkey", "cntrycode"])
    )

    res_2 = (
        res_1.filter(pl.col("c_acctbal") > 0.0)
        .select(pl.col("c_acctbal").mean().alias("avg_acctbal"))
        .with_columns(pl.lit(1).alias("lit"))
    )

    res_3 = orders_ds.select(pl.col("o_custkey").unique()).with_columns(
        pl.col("o_custkey").alias("c_custkey")
    )

    q_final = (
        res_1.join(res_3, on="c_custkey", how="left")
        .filter(pl.col("o_custkey").is_null())
        .with_columns(pl.lit(1).alias("lit"))
        .join(res_2, on="lit")
        .filter(pl.col("c_acctbal") > pl.col("avg_acctbal"))
        .groupby("cntrycode")
        .agg(
            [
                pl.col("c_acctbal").count().alias("numcust"),
                pl.col("c_acctbal").sum().round(2).alias("totacctbal"),
            ]
        )
        .sort("cntrycode")
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
