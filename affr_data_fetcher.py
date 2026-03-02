import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from deltalake import write_deltalake
from dotenv import load_dotenv
from tenneteu import TenneTeuClient

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")
DATA_ROOT = Path(
    os.getenv("AFRR_DATA_PATH", str(Path.home() / "tennet-afrr-data"))
).expanduser()
DELTA_TABLE_PATH = DATA_ROOT / "delta" / "afrr_bid_ladder"
LOG_DIR = DATA_ROOT / "logs"

_SNAPSHOT_MINUTES: frozenset[int] = frozenset([
    *range(0, 180, 15),        # 0, 15, ..., 165
    *range(180, 720, 60),      # 180, 240, ..., 660
    *range(720, 1441, 120),    # 720, 840, ..., 1440
])

LOG_DIR.mkdir(parents=True, exist_ok=True)
load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / "afrr_python.log"),
        logging.StreamHandler(),
    ],
)


class AFRRDataFetcher:
    def __init__(self, api_key: str):
        self.client = TenneTeuClient(api_key=api_key)
        DATA_ROOT.mkdir(parents=True, exist_ok=True)
        self.current_time: datetime | None = None
        self.rounded_time: datetime | None = None
        self.df = None

    @staticmethod
    def coerce_ams_datetime(value: object) -> datetime:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return parsed.replace(tzinfo=AMSTERDAM_TZ) if parsed.tzinfo is None else parsed.astimezone(AMSTERDAM_TZ)

    def set_current_time(self):
        self.current_time = datetime.now(AMSTERDAM_TZ)

        minute = self.current_time.minute
        rounded_quarter = (minute + 7) // 15
        if rounded_quarter == 4:
            rounded = (self.current_time + timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0
            )
        else:
            rounded = self.current_time.replace(
                minute=rounded_quarter * 15, second=0, microsecond=0
            )

        self.rounded_time = rounded

    @staticmethod
    def should_store_snapshot(minutes_to_delivery: int) -> bool:
        return minutes_to_delivery in _SNAPSHOT_MINUTES

    def set_current_bid_ladder(self):
        self.set_current_time()
        assert self.rounded_time is not None

        d_from = self.rounded_time - timedelta(minutes=10)
        d_to = self.rounded_time + timedelta(hours=23, minutes=50)

        logging.info("Fetching bid ladder data from %s to %s", d_from, d_to)
        df = self.client.query_merit_order_list(d_from=d_from, d_to=d_to)
        if df is not None and not df.empty:
            logging.info("Successfully fetched %s bid ladder entries", len(df))
            self.df = df

    def build_snapshot_frame(self) -> pl.DataFrame:
        if self.df is None or self.df.empty:
            return pl.DataFrame()
        assert self.current_time is not None
        assert self.rounded_time is not None

        raw_df = self.df.reset_index()
        if raw_df.empty:
            return pl.DataFrame()

        first_col = raw_df.columns[0]
        source = pl.from_pandas(raw_df).rename(
            {
                first_col: "isp_start",
                "Capacity Threshold": "capacity_threshold",
                "Price Down": "price_down",
                "Price Up": "price_up",
            }
        )

        snapshot_timestamp_utc = self.current_time.astimezone(timezone.utc).replace(
            tzinfo=None
        )

        frames: list[pl.DataFrame] = []
        for isp_start_raw in source.get_column("isp_start").unique().to_list():
            isp_start = self.coerce_ams_datetime(isp_start_raw)
            minutes_to_delivery = int((isp_start - self.rounded_time).total_seconds() / 60)

            if not self.should_store_snapshot(minutes_to_delivery):
                continue

            group = source.filter(pl.col("isp_start") == pl.lit(isp_start_raw)).select(
                [
                    pl.col("capacity_threshold").cast(pl.Int16),
                    pl.col("price_down").cast(pl.Float32),
                    pl.col("price_up").cast(pl.Float32),
                ]
            )

            enriched = group.with_columns(
                [
                    pl.lit(isp_start.astimezone(timezone.utc).replace(tzinfo=None)).alias(
                        "isp_start_utc"
                    ),
                    pl.lit(snapshot_timestamp_utc).alias("snapshot_timestamp_utc"),
                    pl.lit(minutes_to_delivery).cast(pl.Int16).alias("minutes_to_delivery"),
                    pl.lit(isp_start.strftime("%Y-%m-%d")).alias("isp_date"),
                ]
            ).select(
                [
                    "isp_start_utc",
                    "capacity_threshold",
                    "price_down",
                    "price_up",
                    "snapshot_timestamp_utc",
                    "minutes_to_delivery",
                    "isp_date",
                ]
            )
            frames.append(enriched)

        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="vertical")

    def write_delta_data(self, df: pl.DataFrame):
        DELTA_TABLE_PATH.parent.mkdir(parents=True, exist_ok=True)
        table_exists = (DELTA_TABLE_PATH / "_delta_log").exists()
        mode = "append" if table_exists else "overwrite"

        write_deltalake(
            str(DELTA_TABLE_PATH),
            df,
            mode=mode,
            partition_by=["isp_date"],
        )
        logging.info("Wrote %s rows to %s", df.height, DELTA_TABLE_PATH)

    def process_and_store_data(self):
        if self.df is None or self.df.empty:
            logging.warning("No data to process")
            return

        snapshot_df = self.build_snapshot_frame()
        if snapshot_df.is_empty():
            logging.warning("No snapshots matched sampling strategy")
            return

        self.write_delta_data(snapshot_df)


def main():
    logging.info("Starting AFRR data fetcher")
    api_key = os.getenv("TENNET_API_KEY")
    if not api_key:
        raise ValueError("TENNET_API_KEY environment variable not set")

    fetcher = AFRRDataFetcher(api_key)
    logging.info("Delta path=%s", DELTA_TABLE_PATH)
    fetcher.set_current_bid_ladder()
    fetcher.process_and_store_data()
    logging.info("AFRR data fetcher completed")


if __name__ == "__main__":
    main()
