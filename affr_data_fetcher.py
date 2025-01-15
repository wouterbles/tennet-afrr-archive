import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tenneteu import TenneTeuClient

log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_dir / "afrr_python.log"),
        logging.StreamHandler(),
    ],
)


class AFRRDataFetcher:
    def __init__(self, api_key: str):
        self.client = TenneTeuClient(api_key=api_key)
        self.base_path = Path("data")
        self.base_path.mkdir(exist_ok=True)
        self.current_time = None
        self.rounded_time = None
        self.df = None

    @staticmethod
    def get_timezone_suffix(timestamp: pd.Timestamp) -> str:
        """Return CET or CEST depending on whether DST is in effect"""
        return "CEST" if timestamp.dst() else "CET"

    def set_current_time(self):
        """Set current time and calculate rounded time to nearest quarter hour"""
        self.current_time = pd.Timestamp.now(tz="Europe/Amsterdam")

        # Round to nearest quarter hour
        minutes = self.current_time.minute
        rounded_minutes = round(minutes / 15) * 15

        # Handle rollover to next hour
        if rounded_minutes == 60:
            self.rounded_time = (self.current_time + pd.Timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0
            )
        else:
            self.rounded_time = self.current_time.replace(
                minute=rounded_minutes, second=0, microsecond=0
            )

    def is_within_tolerance(self, tolerance_minutes: int = 5) -> bool:
        """Check if current time is within tolerance of its rounded quarter hour"""
        diff_minutes = abs((self.current_time - self.rounded_time).total_seconds() / 60)
        return diff_minutes <= tolerance_minutes

    @staticmethod
    def should_store_snapshot(hours_to_delivery: float) -> bool:
        """
        Determine if we should store a snapshot based on hours to delivery.

        Sampling strategy:
        - <3h: Every 15 minutes (0, 0.25, 0.5, 0.75, 1.0, ...)
        - 3h-12h: Every hour (3, 4, 5, ..., 12)
        - >12h: Every 2 hours (14, 16, 18, ..., 24)
        """
        if hours_to_delivery < 0:
            return False

        # Define target hours to delivery
        quarter_hours = [i / 4 for i in range(12)]  # 0, 0.25, 0.5, ... 2.75
        hourly = list(range(3, 12))  # 3, 4, 5, ..., 11
        two_hourly = list(range(12, 25, 2))  # 12, 14, 16, ..., 24
        target_hours = quarter_hours + hourly + two_hourly

        return hours_to_delivery in target_hours

    def set_current_bid_ladder(self):
        """Fetch current and upcoming bid ladders from TenneT API"""
        self.set_current_time()

        if not self.is_within_tolerance():
            raise ValueError(
                f"Current time {self.current_time} is not within tolerance of rounded time {self.rounded_time}"
            )

        d_from = self.current_time - pd.Timedelta(minutes=10)
        d_to = self.current_time + pd.Timedelta(hours=23, minutes=50)

        logging.info(f"Fetching bid ladder data from {d_from} to {d_to}")
        df = self.client.query_merit_order_list(d_from=d_from, d_to=d_to)

        if df is not None:
            logging.info(f"Successfully fetched {len(df)} bid ladder entries")
            self.df = df

    def process_and_store_data(self):
        """Process and store bid ladder data"""
        if self.df is None or self.df.empty:
            logging.warning("No data to process")
            return

        snapshots_stored = 0

        # Group by unique ISP timestamp
        for isp_start, isp_data in self.df.groupby(level=0):
            time_to_delivery = isp_start - self.rounded_time
            minutes_to_delivery = int(time_to_delivery.total_seconds() / 60)
            hours_to_delivery = minutes_to_delivery / 60

            if not self.should_store_snapshot(hours_to_delivery):
                continue

            # Create storage structure
            tz_suffix = self.get_timezone_suffix(isp_start)
            storage_path = (
                self.base_path / f"{isp_start.strftime('%Y-%m-%d/%H%M')}_{tz_suffix}"
            )
            storage_path.mkdir(parents=True, exist_ok=True)

            # Store data with additional time information
            store_data = pd.DataFrame(
                {
                    "capacity_threshold": isp_data["Capacity Threshold"].astype(
                        "int16"
                    ),
                    "price_down": isp_data["Price Down"].astype("float32"),
                    "price_up": isp_data["Price Up"].astype("float32"),
                    "snapshot_timestamp": self.current_time,
                    "minutes_to_delivery": minutes_to_delivery,
                }
            )

            # Save to parquet file using hours to delivery in filename
            filename = (
                f"snapshot_{self.current_time.strftime('%H%M')}_"
                f"htd_{hours_to_delivery:.2f}.parquet"
            )
            filepath = storage_path / filename
            store_data.to_parquet(filepath)
            snapshots_stored += 1

        logging.info(f"Stored {snapshots_stored} snapshots")


def main():
    logging.info("Starting AFRR data fetcher")

    api_key = os.getenv("TENNET_API_KEY")
    if not api_key:
        raise ValueError("TENNET_API_KEY environment variable not set")

    fetcher = AFRRDataFetcher(api_key)
    fetcher.set_current_bid_ladder()
    fetcher.process_and_store_data()

    logging.info("AFRR data fetcher completed")


if __name__ == "__main__":
    main()
