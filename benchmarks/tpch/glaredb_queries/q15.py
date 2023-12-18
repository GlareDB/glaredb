import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 15


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()
    line_item_ds = utils.get_line_item_ds(con)
    supplier_ds = utils.get_supplier_ds(con)

    ddl = f"""
    create or replace temporary view revenue (supplier_no, total_revenue) as
        select
            l_suppkey,
            sum(l_extendedprice * (1 - l_discount))
        from
            {line_item_ds}
        where
            l_shipdate >= date '1996-01-01'
            and l_shipdate < date '1996-01-01' + interval '3' month
        group by
            l_suppkey
    """

    query_str = f"""
    select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
    from
        {supplier_ds},
        revenue
    where
        s_suppkey = supplier_no
        and total_revenue = (
            select
                max(total_revenue)
            from
                revenue
        )
    order by
        s_suppkey
	"""
    con.execute(ddl)

    utils.run_query(Q_NUM, con, query_str)
    con.execute("drop view revenue")


if __name__ == "__main__":
    q()
