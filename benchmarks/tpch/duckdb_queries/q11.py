import duckdb

from duckdb_queries import utils

Q_NUM = 11


def q():
    supplier_ds = utils.get_supplier_ds()
    part_supp_ds = utils.get_part_supp_ds()
    nation_ds = utils.get_nation_ds()

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

    q_final = duckdb.sql(query_str)

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
