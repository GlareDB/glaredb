import pathlib
import tests
import os

import psycopg2.extensions
from dbt.cli.main import dbtRunner, dbtRunnerResult
import pytest

import tests.tools
from tests.fixtures.glaredb import glaredb_connection, debug_path


@pytest.fixture
def dbt_project_path() -> pathlib.Path:
    return tests.PKG_DIRECTORY.joinpath("tests", "fixtures", "dbt_project")


@pytest.mark.parametrize(
    "model_name,run_success,query_result",
    [
        ("table_materialization", True, 10),
        pytest.param("view_materialization", True, 10, marks=pytest.mark.xfail),
    ],
)
def test_dbt_glaredb(
    glaredb_connection: psycopg2.extensions.connection,
    dbt_project_path,
    model_name,
    run_success,
    query_result,
):
    dbt_project_directory: pathlib.Path = dbt_project_path
    dbt_profiles_directory: pathlib.Path = dbt_project_path

    with glaredb_connection.cursor() as curr:
        curr.execute("create table dbt_test (amount int)")
        curr.execute(
            "INSERT INTO dbt_test (amount) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)"
        )

    with tests.tools.env("DBT_USER", glaredb_connection.info.user):
        res: dbtRunnerResult = dbtRunner().invoke(
            [
                "run",
                "--project-dir",
                dbt_project_directory,
                "--profiles-dir",
                dbt_profiles_directory,
                "-m",
                model_name,
            ]
        )

        assert res.success is run_success

    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from {model_name}")
        result: list = curr.fetchone()[0]

        assert result == query_result
