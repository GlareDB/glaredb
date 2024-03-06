import great_expectations as gx
import psycopg2.extensions
import psycopg2.extras
import pytest

from tests.fixtures.glaredb import glaredb_connection, binary_path, glaredb_path


def test_great_expectations(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
):
    curr = glaredb_connection.cursor()

    curr.execute("create table my_data (amount int)")
    curr.execute(
        "INSERT INTO my_data (amount) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)"
    )
    curr.execute("select count(*) from my_data;")
    res = curr.fetchone()

    port = glaredb_connection.info.port
    user = glaredb_connection.info.user
    host = glaredb_connection.info.host
    password = glaredb_connection.info.password
    context = gx.get_context()  # gets a great expectations project context
    gx_data_source = context.sources.add_postgres(
        name="glaredb",
        connection_string=f"postgresql://{user}:{password}@{host}:{port}/db",
    )

    gx_data_asset = gx_data_source.add_table_asset("my_data")

    batch_request = gx_data_asset.build_batch_request()

    batch_list = gx_data_asset.get_batch_list_from_batch_request(batch_request=batch_request)

    validator = context.get_validator(batch_list=batch_list)

    assert validator.expect_column_values_to_not_be_null("amount")
