import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    data_root: Path
    meta_db_url: str
    min_free_gb: float
    http_timeout: int

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def silver_dir(self) -> Path:
        return self.data_root / "silver"


def get_settings() -> Settings:
    load_dotenv()
    root = Path(os.getenv("ALPHADATA_ROOT") or "./data").resolve()
    meta_url = os.getenv("ALPHADATA_META_DB_URL") or f"sqlite:///{(root / 'meta.db').as_posix()}"
    return Settings(
        data_root=root,
        meta_db_url=meta_url,
        min_free_gb=float(os.getenv("ALPHADATA_MIN_FREE_GB") or 3),
        http_timeout=int(os.getenv("ALPHADATA_HTTP_TIMEOUT") or 60),
    )
