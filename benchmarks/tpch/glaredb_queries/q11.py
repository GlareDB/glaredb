import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 11


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


if __name__ == "__main__":
    q()
