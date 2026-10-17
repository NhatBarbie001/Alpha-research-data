from pathlib import Path

import pytest

from alphadata.core.errors import MissingCredential
from alphadata.core.settings import Settings, get_settings


def test_defaults_without_env() -> None:
    s = get_settings(env={}, env_file=None)
    assert s.data_root == Path("data")
    assert s.meta_db_url == "sqlite:///data/meta.db"
    assert s.min_free_gb == 3.0
    assert s.http_timeout == 60.0
    assert s.universe_dir == Path("config/universe")
    assert s.user_agent.startswith("alphadata/")


def test_reads_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "ALPHADATA_ROOT=/data/alphadata\n"
        "ALPHADATA_MIN_FREE_GB=5\n"
        "ALPHADATA_META_DB_URL=postgresql://u@db/alphadata_meta\n"
        "FRED_API_KEY=fred-from-file\n",
        encoding="utf-8",
    )
    s = get_settings(env={}, env_file=env_file)
    assert s.data_root == Path("/data/alphadata")
    assert s.min_free_gb == 5.0
    assert s.meta_db_url == "postgresql://u@db/alphadata_meta"
    assert s.secret("FRED_API_KEY") == "fred-from-file"


def test_real_env_overrides_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALPHADATA_HTTP_TIMEOUT=10\nFRED_API_KEY=from-file\n", encoding="utf-8")
    s = get_settings(env={"FRED_API_KEY": "from-env"}, env_file=env_file)
    assert s.http_timeout == 10.0
    assert s.secret("FRED_API_KEY") == "from-env"


def test_blank_values_are_unset(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALPHADATA_META_DB_URL=\nFRED_API_KEY=   \n", encoding="utf-8")
    s = get_settings(env={}, env_file=env_file)
    assert s.meta_db_url == "sqlite:///data/meta.db"
    with pytest.raises(MissingCredential):
        s.secret("FRED_API_KEY")


def test_meta_db_url_follows_data_root() -> None:
    s = get_settings(env={"ALPHADATA_ROOT": "/srv/lake"}, env_file=None)
    assert s.meta_db_url == "sqlite:////srv/lake/meta.db"


@pytest.mark.parametrize("value", ["abc", "0", "-1"])
def test_invalid_number_is_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="ALPHADATA_MIN_FREE_GB"):
        get_settings(env={"ALPHADATA_MIN_FREE_GB": value}, env_file=None)


def test_missing_and_unknown_secret() -> None:
    s = get_settings(env={}, env_file=None)
    with pytest.raises(MissingCredential, match="ALPACA_KEY_ID"):
        s.secret("ALPACA_KEY_ID")
    with pytest.raises(ValueError, match="unknown secret"):
        s.secret("NOT_A_SECRET")


def test_secret_values_not_in_repr() -> None:
    s = get_settings(env={"FRED_API_KEY": "super-secret-value"}, env_file=None)
    assert "super-secret-value" not in repr(s)


def test_derived_dirs() -> None:
    s = Settings(data_root=Path("/lake"))
    assert s.raw_dir == Path("/lake/raw")
    assert s.silver_dir == Path("/lake/silver")
    assert s.gold_dir == Path("/lake/gold")
    assert s.checkpoint_dir == Path("/lake/checkpoints")
