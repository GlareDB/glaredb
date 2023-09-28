import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 9


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


if __name__ == "__main__":
    q()
