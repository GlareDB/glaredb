import duckdb

from duckdb_queries import utils

Q_NUM = 16


def q():
    part_ds = utils.get_part_ds()
    supplier_ds = utils.get_supplier_ds()
    part_supp_ds = utils.get_part_supp_ds()

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

    q_final = duckdb.sql(query_str)

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
