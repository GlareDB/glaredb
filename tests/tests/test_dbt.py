import pathlib
import tests

import psycopg2.extensions
import pytest
import os

from tests.fixtures.glaredb import glaredb_connection, debug_path

from dbt.cli.main import dbtRunner, dbtRunnerResult

@pytest.fixture
def dbt_project_path() -> pathlib.Path:
    return tests.PKG_DIRECTORY.joinpath("tests", "fixtures", "dbt_project")


@pytest.mark.parametrize("model_name,run_success,query_result",
                         [
                             ("table_materialization", True, 10),
                             ("view_materialization", True, 10),
                         ]
                         )
def test_dbt_glaredb_basic(
    glaredb_connection: psycopg2.extensions.connection,
    dbt_project_path,
    model_name,
    run_success,
    query_result
):
    dbt: dbtRunner = dbtRunner()

    os.environ["DBT_USER"] = glaredb_connection.info.user

    dbt_project_directory: pathlib.Path = dbt_project_path
    dbt_profiles_directory: pathlib.Path = dbt_project_path

    with glaredb_connection.cursor() as curr:
        curr.execute("create table dbt_test (amount int)")
        curr.execute(
            "INSERT INTO dbt_test (amount) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)"
        )
        curr.execute("SELECT * FROM public.dbt_test")
        a = curr.fetchall()
        1==1

    cli_args: list = [
        "run",
        "--project-dir",
        dbt_project_directory,
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
        result: list = curr.fetchone()[0]

    assert result == query_result

def test_dbt_glaredb_external_postgres(
    glaredb_connection: psycopg2.extensions.connection,
    dbt_project_path
):
    dbt: dbtRunner = dbtRunner()
    model_name = "postgres_datasource_view_materialization"
    os.environ["DBT_USER"] = glaredb_connection.info.user

    dbt_project_directory: pathlib.Path = dbt_project_path
    dbt_profiles_directory: pathlib.Path = dbt_project_path

    with glaredb_connection.cursor() as curr:
        curr.execute(
            """
                CREATE EXTERNAL DATABASE my_pg
                FROM postgres
                OPTIONS (
                    host = 'pg.demo.glaredb.com',
                    port = '5432',
                    user = 'demo',
                    password = 'demo',
                    database = 'postgres',
                );
            """
        )

    cli_args: list = [
        "run",
        "--project-dir",
        dbt_project_directory,
        "--profiles-dir",
        dbt_profiles_directory,
        "-m",
        model_name
    ]
    #
    res: dbtRunnerResult = dbt.invoke(cli_args)

    assert res.success is True

    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from {model_name}")
        result: list = curr.fetchone()[0]

    assert result == 5


@pytest.fixture
def test_fixture(a, b):
    return a+b

def test_my_fixture(test_fixture):
    1 ==1