import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 6


@linetimer(name=f"Overall execution of glaredb Query {Q_NUM}", unit="ms")
def q():
    con = glaredb.connect()

    line_item_ds = utils.get_line_item_ds(con)

    query_str = """
    select
         sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= timestamp '1994-01-01'
        and l_shipdate < timestamp '1994-01-01' + interval '1' year
        and l_discount between .06 - 0.01 and .06 + 0.01
        and l_quantity < 24
    """

    utils.run_query(Q_NUM, con, query_str)


if __name__ == "__main__":
    q()
