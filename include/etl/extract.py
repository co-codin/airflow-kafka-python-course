"""Read a source orders CSV and copy it to a staging path."""

from __future__ import annotations

import csv
import shutil
from pathlib import Path

from include.paths import ORDER_COLUMNS, ensure_dirs


def read_orders_csv(path: Path | str) -> list[dict[str, str]]:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"Orders CSV not found: {source}")
    with source.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{source} has no header row")
        missing = [col for col in ORDER_COLUMNS if col not in reader.fieldnames]
        if missing:
            raise ValueError(f"{source} missing columns: {missing}")
        return [dict(row) for row in reader]


def extract_orders(source_path: Path | str, staging_path: Path | str) -> str:
    """Copy the source file to staging and return the staging path.

    Returning a path (not rows) keeps XCom small and matches how file-based
    pipelines pass data between tasks.
    """
    ensure_dirs()
    source = Path(source_path)
    dest = Path(staging_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Validate header before copying so bad extracts fail fast.
    read_orders_csv(source)
    shutil.copy2(source, dest)
    return str(dest)
