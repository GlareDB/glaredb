from fixtures.glaredb import release, debug


def test_release_exists(release):
    assert release.exists()


def test_debug_exists(debug):
    assert debug.exists()
