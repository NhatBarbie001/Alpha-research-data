import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values

from alphadata import __version__
from alphadata.core.errors import MissingCredential

SECRET_NAMES = (
    "FRED_API_KEY",
    "ALPACA_KEY_ID",
    "ALPACA_SECRET_KEY",
    "EIA_API_KEY",
    "BEA_API_KEY",
    "BLS_API_KEY",
)


@dataclass(frozen=True)
class Settings:
    data_root: Path = Path("./data")
    meta_db_url: str = ""
    min_free_gb: float = 3.0
    http_timeout: float = 60.0
    universe_dir: Path = Path("./config/universe")
    user_agent: str = f"alphadata/{__version__} (internal research)"
    secrets: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not self.meta_db_url:
            object.__setattr__(
                self, "meta_db_url", f"sqlite:///{self.data_root.as_posix()}/meta.db"
            )

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def silver_dir(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_root / "gold"

    @property
    def checkpoint_dir(self) -> Path:
        return self.data_root / "checkpoints"

    def secret(self, name: str) -> str:
        if name not in SECRET_NAMES:
            raise ValueError(f"unknown secret name: {name}")
        value = self.secrets.get(name, "")
        if not value:
            raise MissingCredential(f"{name} is not set (see .env.example)")
        return value


def get_settings(
    env: Mapping[str, str] | None = None, env_file: Path | None = Path(".env")
) -> Settings:
    """Biến môi trường thật ưu tiên hơn `.env`. Giá trị rỗng coi như không đặt."""
    merged: dict[str, str] = {}
    if env_file is not None and env_file.is_file():
        merged.update({k: v for k, v in dotenv_values(env_file).items() if v is not None})
    merged.update(os.environ if env is None else env)
    values = {k: v.strip() for k, v in merged.items() if v and v.strip()}

    default = Settings()
    return Settings(
        data_root=Path(values.get("ALPHADATA_ROOT", default.data_root)),
        meta_db_url=values.get("ALPHADATA_META_DB_URL", ""),
        min_free_gb=_positive_float(values, "ALPHADATA_MIN_FREE_GB", default.min_free_gb),
        http_timeout=_positive_float(values, "ALPHADATA_HTTP_TIMEOUT", default.http_timeout),
        universe_dir=Path(values.get("ALPHADATA_UNIVERSE_DIR", default.universe_dir)),
        user_agent=values.get("ALPHADATA_USER_AGENT", default.user_agent),
        secrets={name: values[name] for name in SECRET_NAMES if name in values},
    )


def _positive_float(values: Mapping[str, str], name: str, default: float) -> float:
    if name not in values:
        return default
    try:
        number = float(values[name])
    except ValueError:
        raise ValueError(f"{name} must be a number, got {values[name]!r}") from None
    if number <= 0:
        raise ValueError(f"{name} must be > 0, got {number}")
    return number
