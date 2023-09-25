import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer


Q_NUM = 2


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()

    lineitem = utils.get_line_item_ds(con)

    query_str = f"""
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        {lineitem}
    where
        l_shipdate <= '1998-09-02'
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    """
    
    utils.run_query(Q_NUM, con, query_str)


if __name__ == "__main__":
    q()
