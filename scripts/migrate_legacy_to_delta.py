import argparse
import logging
import os
from pathlib import Path

import polars as pl
from deltalake import write_deltalake

DEFAULT_DATA_ROOT = Path(
    os.getenv("AFRR_DATA_PATH", str(Path.home() / "tennet-afrr-data"))
).expanduser()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate legacy parquet snapshots under data/ into the Delta table."
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("data"),
        help="Path to the legacy parquet archive root.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="Path to the external data root used by the new Delta setup.",
    )
    return parser.parse_args()


def build_day_frame(files: list[Path]) -> pl.DataFrame:
    return (
        pl.scan_parquet([str(path) for path in files])
        .rename({"timestamp": "isp_start"})
        .select(
            [
                pl.col("isp_start")
                .dt.convert_time_zone("UTC")
                .dt.replace_time_zone(None)
                .alias("isp_start_utc"),
                pl.col("capacity_threshold").cast(pl.Int16),
                pl.col("price_down").cast(pl.Float32),
                pl.col("price_up").cast(pl.Float32),
                pl.col("snapshot_timestamp")
                .dt.convert_time_zone("UTC")
                .dt.replace_time_zone(None)
                .alias("snapshot_timestamp_utc"),
                pl.col("minutes_to_delivery").cast(pl.Int16),
                pl.col("isp_start").dt.strftime("%Y-%m-%d").alias("isp_date"),
            ]
        )
        .collect()  # type: ignore[return-value]
    )


def migrate_legacy_archive(source_root: Path, data_root: Path) -> None:
    source_root = source_root.expanduser().resolve()
    data_root = data_root.expanduser().resolve()
    table_path = data_root / "delta" / "afrr_bid_ladder"

    if not source_root.exists():
        raise FileNotFoundError(f"Legacy source root not found: {source_root}")

    data_root.mkdir(parents=True, exist_ok=True)
    date_dirs = sorted(path for path in source_root.glob("????-??-??") if path.is_dir())

    if not date_dirs:
        logging.warning("No dated directories found under %s", source_root)
        return

    total_rows = 0
    migrated_days = 0
    skipped_days = 0

    for date_dir in date_dirs:
        partition_path = table_path / f"isp_date={date_dir.name}"
        if partition_path.exists():
            logging.info("Skipping %s, partition already exists", date_dir.name)
            skipped_days += 1
            continue

        files = sorted(date_dir.rglob("*.parquet"))
        if not files:
            continue

        logging.info("Migrating %s from %s parquet files", date_dir.name, len(files))
        day_frame = build_day_frame(files)
        if day_frame.is_empty():
            logging.info("No rows found for %s", date_dir.name)
            continue

        mode = "append" if (table_path / "_delta_log").exists() else "overwrite"
        write_deltalake(
            str(table_path),
            day_frame,
            mode=mode,
            partition_by=["isp_date"],
        )

        total_rows += day_frame.height
        migrated_days += 1
        logging.info("Wrote %s rows for %s", day_frame.height, date_dir.name)

    logging.info(
        "Migration complete: %s days migrated, %s days skipped, %s total rows",
        migrated_days,
        skipped_days,
        total_rows,
    )


def main() -> int:
    logging.basicConfig(
        format="%(asctime)s - %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    migrate_legacy_archive(args.source_root, args.data_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
