import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 8


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


if __name__ == "__main__":
    q()
