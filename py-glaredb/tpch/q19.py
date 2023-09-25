import utils
from datetime import datetime
import glaredb
import polars_utils
import polars as pl
from linetimer import CodeTimer, linetimer

Q_NUM = 19
pl.Config().set_fmt_float("full")


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    part_ds = utils.get_part_ds(con)
    line_item_ds = utils.get_line_item_ds(con)

    query_str = f"""
    select
        round(sum(l_extendedprice* (1 - l_discount)), 2) as revenue
    from
        {line_item_ds},
        {part_ds}
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#12'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#23'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 10 and l_quantity <= 20
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#34'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 20 and l_quantity <= 30
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
	"""
    utils.run_query(Q_NUM, con, query_str)


@linetimer(name=f"Overall execution of polars Query {Q_NUM}", unit="ms")
def q_polars():
    line_item_ds = polars_utils.get_line_item_ds()
    part_ds = polars_utils.get_part_ds()

    q_final = (
        part_ds.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
        .filter(pl.col("l_shipmode").is_in(["AIR", "AIR REG"]))
        .filter(pl.col("l_shipinstruct") == "DELIVER IN PERSON")
        .filter(
            (
                (pl.col("p_brand") == "Brand#12")
                & pl.col("p_container").is_in(
                    ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
                )
                & (pl.col("l_quantity").is_between(1, 11))
                & (pl.col("p_size").is_between(1, 5))
            )
            | (
                (pl.col("p_brand") == "Brand#23")
                & pl.col("p_container").is_in(
                    ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
                )
                & (pl.col("l_quantity").is_between(10, 20))
                & (pl.col("p_size").is_between(1, 10))
            )
            | (
                (pl.col("p_brand") == "Brand#34")
                & pl.col("p_container").is_in(
                    ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
                )
                & (pl.col("l_quantity").is_between(20, 30))
                & (pl.col("p_size").is_between(1, 15))
            )
        )
        .select(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .round(2)
            .alias("revenue")
        )
    )

    polars_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
    q_polars()
