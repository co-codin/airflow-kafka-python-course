"""Write transformed orders to CSV (and later DuckDB)."""

from __future__ import annotations

import shutil
from pathlib import Path

from include.etl.extract import read_orders_csv
from include.paths import ensure_dirs


def load_orders_csv(source_path: Path | str, dest_path: Path | str) -> str:
    """Idempotent CSV load: overwrite the destination with the transformed file."""
    ensure_dirs()
    source = Path(source_path)
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    read_orders_csv(source)
    shutil.copy2(source, dest)
    return str(dest)
