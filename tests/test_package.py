import alphadata


def test_version_matches_metadata() -> None:
    assert alphadata.__version__ == "0.1.0"
