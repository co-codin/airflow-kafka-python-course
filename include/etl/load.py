"""Write transformed orders to CSV and DuckDB."""

from __future__ import annotations

import shutil
from pathlib import Path

from include.etl.extract import read_orders_csv
from include.paths import DUCKDB_PATH, ensure_dirs

ORDERS_TABLE = "orders"


def load_orders_csv(source_path: Path | str, dest_path: Path | str) -> str:
    """Idempotent CSV load: overwrite the destination with the transformed file."""
    ensure_dirs()
    source = Path(source_path)
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    read_orders_csv(source)
    shutil.copy2(source, dest)
    return str(dest)


def load_orders_duckdb(
    source_path: Path | str,
    duckdb_path: Path | str | None = None,
    table: str = ORDERS_TABLE,
) -> dict:
    """Replace `table` in a DuckDB file with the transformed CSV.

    CREATE OR REPLACE keeps the load idempotent for the lab: rerunning the
    DAG does not duplicate rows.
    """
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required for load_orders_duckdb") from exc

    ensure_dirs()
    source = Path(source_path)
    db_path = Path(duckdb_path) if duckdb_path else DUCKDB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    read_orders_csv(source)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv_auto(?)",
            [str(source)],
        )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return {"duckdb_path": str(db_path), "table": table, "row_count": int(row_count)}
