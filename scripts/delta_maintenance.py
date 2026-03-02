import logging
import os
from pathlib import Path

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def optimize_delta_table(path: Path, z_order_cols: list[str] | None = None) -> None:
    """Optimize a Delta table with OPTIMIZE, Z-ORDER, and VACUUM."""
    try:
        dt = DeltaTable(str(path))
    except TableNotFoundError:
        logger.warning("Delta table not found, skipping optimization: %s", path)
        return

    if z_order_cols:
        dt.optimize.z_order(z_order_cols)
    else:
        dt.optimize.compact()

    dt.vacuum(
        retention_hours=0,
        enforce_retention_duration=False,
        full=True,
        dry_run=False,
    )
    dt.cleanup_metadata()


def main() -> int:
    data_path = Path(
        os.getenv("AFRR_DATA_PATH", str(Path.home() / "tennet-afrr-data"))
    ).expanduser()
    table_path = data_path / "delta" / "afrr_bid_ladder"
    optimize_delta_table(table_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
