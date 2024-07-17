import os

import tests.tools


def test_cd():
    cwd = os.getcwd()

    with tests.tools.cd("/tmp"):
        assert not cwd == os.getcwd()

    assert cwd == os.getcwd()


def test_env():
    assert "merlin" not in os.environ

    with tests.tools.env("merlin", "cat"):
        assert "merlin" in os.environ
        assert os.environ["merlin"] == "cat"

    assert "merlin" not in os.environ
