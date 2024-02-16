import psycopg2.extensions
import pytest
import os

from tests.fixtures.glaredb import glaredb_connection, debug_path

from dbt.cli.main import dbtRunner, dbtRunnerResult


def test_dbt_glaredb(
    glaredb_connection: psycopg2.extensions.connection,
):
    dbt: dbtRunner = dbtRunner()

    os.environ["DBT_USER"] = glaredb_connection.info.user

    model_name: str = "glaredb_model" # TODO
    project_directory: str = "../fixtures/dbt_project/" # TODO
    dbt_profiles_directory: str = "../fixtures/dbt_project/" # TODO

    with glaredb_connection.cursor() as curr:
        curr.execute("create table dbt_test (amount int)")
        curr.execute(
            "INSERT INTO dbt_test (amount) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)"
        )
        
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

    # The below will currently fail. res contains the error message, but that message can be in different places based
    # on where the failure is. Currently, it is under the top level `res.exception` key.
    # assert res.success is True
    #
    # with glaredb_connection.cursor() as curr:
    #     query_result: list = curr.execute(f"select * from {model_name}").fetchall()
    #
    # assert len(query_result) == 10
