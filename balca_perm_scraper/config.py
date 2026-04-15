from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    base_dir: Path = Path(__file__).resolve().parents[1]
    raw_dir: Path = base_dir / "data" / "raw"
    processed_dir: Path = base_dir / "data" / "processed"
    database_path: Path = processed_dir / "balca_perm.sqlite"
    user_agent: str = (
        "balca-perm-scraper/0.1 (+https://github.com/your-org/balca-perm-scraper)"
    )
    # Read-only query key embedded in the public DOL search page JS
    azure_query_key: str = os.environ.get("DOL_AZURE_QUERY_KEY", "")
    timeout_seconds: float = 30.0
    max_connections: int = 5
    sleep_seconds: float = 1.0
    pdf_sleep_seconds: float = 2.0
    pdf_sleep_jitter: float = 2.0
    page_size: int = 50


SETTINGS = Settings()
