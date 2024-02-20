import psycopg2.extensions
import pytest
import os

from tests.fixtures.glaredb import glaredb_connection, debug_path

from dbt.cli.main import dbtRunner, dbtRunnerResult


@pytest.mark.parametrize("model_name,run_success,query_result",
                         [
                             ("table_materialization", True, 10),
                             pytest.param("view_materialization", True, 10, marks=pytest.mark.xfail),
                         ]
                         )
def test_dbt_glaredb(
    glaredb_connection: psycopg2.extensions.connection,
    model_name,
    run_success,
    query_result
):
    print("LISTDIR", os.listdir('.'))
    dbt: dbtRunner = dbtRunner()

    os.environ["DBT_USER"] = glaredb_connection.info.user

    project_directory: str = "tests/fixtures/dbt_project/"
    dbt_profiles_directory: str = "tests/fixtures/dbt_project/"

    with glaredb_connection.cursor() as curr:
        curr.execute("create table dbt_test (amount int)")
        curr.execute(
            "INSERT INTO dbt_test (amount) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)"
        )
        1 == 1
        
    cli_args: list = [
        "run",
        "--project-dir",
        project_directory,
        "--profiles-dir",
        dbt_profiles_directory,
        "-m",
        model_name
    ]
    #
    res: dbtRunnerResult = dbt.invoke(cli_args)

    assert res.success is run_success

    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from {model_name}")
        result: list = curr.fetchone()

    assert result == (query_result,)
