import pathlib
import tests

import psycopg2.extensions
from dbt.cli.main import dbtRunner, dbtRunnerResult
import pytest

import tests.tools


@pytest.fixture
def dbt_project_path() -> pathlib.Path:
    return tests.PKG_DIRECTORY.joinpath("tests", "fixtures", "dbt_project")


@pytest.mark.parametrize(
    "model_name,run_success,query_result",
    [
        ("table_materialization", True, 10),
        ("view_materialization", True, 10),
    ],
)
def test_dbt_glaredb(
    glaredb_connection: psycopg2.extensions.connection,
    dbt_project_path: pathlib.Path,
    model_name: str,
    run_success: bool,
    query_result: int,
):
    with glaredb_connection.cursor() as curr:
        curr.execute("create table dbt_test (amount int)")
        curr.execute(
            "INSERT INTO dbt_test (amount) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)"
        )
        curr.execute("SELECT * FROM public.dbt_test")
        a = curr.fetchall()

    with (
        tests.tools.env("GLAREDB_PORT", str(glaredb_connection.info.port)),
        tests.tools.env("DBT_USER", glaredb_connection.info.user),
    ):
        res: dbtRunnerResult = dbtRunner().invoke(
            [
                "run",
                "--project-dir",
                dbt_project_path,
                "--profiles-dir",
                dbt_project_path,
                "-m",
                model_name,
            ]
        )

        assert res.success is run_success

    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from {model_name}")
        result: list = curr.fetchone()[0]

        assert result == query_result


def test_dbt_glaredb_external_postgres(
    glaredb_connection: psycopg2.extensions.connection,
    dbt_project_path: pathlib.Path,
):
    model_name = "postgres_datasource_view_materialization"

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

    with (
        tests.tools.env("GLAREDB_PORT", str(glaredb_connection.info.port)),
        tests.tools.env("DBT_USER", glaredb_connection.info.user),
    ):
        res: dbtRunnerResult = dbtRunner().invoke(
            [
                "run",
                "--project-dir",
                dbt_project_path,
                "--profiles-dir",
                dbt_project_path,
                "-m",
                model_name,
            ]
        )

        assert res.success is True

    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from {model_name}")
        result: list = curr.fetchone()[0]

    assert result == 5


def test_dbt_join_csv_with_table(
    glaredb_connection: psycopg2.extensions.connection,
    dbt_project_path: pathlib.Path,
):
    model_name = "join_csv_with_table"

    with glaredb_connection.cursor() as curr:
        curr.execute("create table dbt_test (amount int)")
        curr.execute(
            "INSERT INTO dbt_test (amount) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)"
        )
        curr.execute("SELECT * FROM public.dbt_test")
        a = curr.fetchall()

    with (
        tests.tools.env("GLAREDB_PORT", str(glaredb_connection.info.port)),
        tests.tools.env("DBT_USER", glaredb_connection.info.user),
    ):
        res: dbtRunnerResult = dbtRunner().invoke(
            [
                "run",
                "--project-dir",
                dbt_project_path,
                "--profiles-dir",
                dbt_project_path,
                "-m",
                model_name,
            ]
        )

        assert res.success is True

    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from {model_name}")
        result: list = curr.fetchone()[0]

    assert result == 9
