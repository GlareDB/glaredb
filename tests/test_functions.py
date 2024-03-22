import psycopg2


def test_scalar_parsing(
    glaredb_connection: psycopg2.extensions.connection,
):
    for operation in ["create table t (x int);", "insert into t values (1), (2), (3);"]:
        with glaredb_connection.cursor() as cur:
            cur.execute(operation)

    with glaredb_connection.cursor() as cur:
        cur.execute("select siphash(x) as actual, siphash(arrow_cast(1, 'Int32')) as one from t")
        result = cur.fetchall()

    assert result[0][0] == result[0][1]
    assert result[1][0] != result[1][1]
    assert result[2][0] != result[2][1]
