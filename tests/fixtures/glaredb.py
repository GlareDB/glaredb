import pytest
import tests


@pytest.fixture
def release():
    return tests.PKG_DIRECTORY.joinpath("target", "release", "glaredb")


@pytest.fixture
def debug():
    return tests.PKG_DIRECTORY.joinpath("target", "debug", "glaredb")
