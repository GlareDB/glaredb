import utils
from datetime import datetime
import glaredb
from linetimer import CodeTimer, linetimer

Q_NUM = 22


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


if __name__ == "__main__":
    q()
