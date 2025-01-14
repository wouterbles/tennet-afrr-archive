import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tenneteu import TenneTeuClient

load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
)


class AFRRDataFetcher:
    def __init__(self, api_key: str):
        self.client = TenneTeuClient(api_key=api_key)
        self.base_path = Path("data")
        self.base_path.mkdir(exist_ok=True)
        self.current_time = None
        self.rounded_time = None
        self.df = None

    def get_timezone_suffix(self, timestamp: pd.Timestamp) -> str:
        """Return CET or CEST depending on whether DST is in effect"""
        return "CEST" if timestamp.dst() else "CET"

    def set_current_time(self):
        """Set current time and calculate rounded time to nearest quarter hour"""
        self.current_time = pd.Timestamp.now(tz="Europe/Amsterdam")

        # Round to nearest quarter
        minutes = self.current_time.minute
        rounded_minutes = round(minutes / 15) * 15

        # Create new timestamp with rounded minutes
        self.rounded_time = self.current_time.replace(
            minute=rounded_minutes, second=0, microsecond=0
        )

    def is_within_tolerance(self, tolerance_minutes: int = 5) -> bool:
        """Check if current time is within tolerance of its rounded quarter hour"""
        diff_minutes = abs((self.current_time - self.rounded_time).total_seconds() / 60)
        return diff_minutes <= tolerance_minutes

    def should_store_snapshot(self, hours_to_delivery: float) -> bool:
        """
        Determine if we should store a snapshot based on sampling strategy.

        Sampling frequency:
        - 24h-12h: Every 2 hours
        - 12h-6h: Every hour
        - 6h-3h: Every 30 minutes
        - <3h: Every 15 minutes
        """
        if not self.is_within_tolerance():
            return False

        # <3h: Every 15 minutes (store all rounded quarters)
        if hours_to_delivery < 3:
            return True

        # 3h-6h: Every 30 minutes (store on :00 and :30)
        elif hours_to_delivery < 6:
            return self.rounded_time.minute in (0, 30)

        # 6h-12h: Every hour (store on :00)
        elif hours_to_delivery < 12:
            return self.rounded_time.minute == 0

        # 12h-24h: Every 2 hours (store on even hours)
        else:
            return self.rounded_time.minute == 0 and self.rounded_time.hour % 2 == 0

    def set_current_bid_ladder(self):
        """Fetch current and upcoming bid ladders from TenneT API"""
        self.set_current_time()
        d_from = self.current_time - pd.Timedelta(minutes=5)
        d_to = self.current_time + pd.Timedelta(hours=24)

        logging.info(f"Fetching bid ladder data from {d_from} to {d_to}")

        try:
            df = self.client.query_merit_order_list(d_from=d_from, d_to=d_to)
        except Exception as e:
            logging.error(f"Error fetching bid ladder: {e}")
            return

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

            if hours_to_delivery <= 0:
                continue  # Skip past ISP periods
            elif not self.should_store_snapshot(hours_to_delivery):
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
        logging.error("TENNET_API_KEY environment variable not set")
        return

    fetcher = AFRRDataFetcher(api_key)
    fetcher.set_current_bid_ladder()
    fetcher.process_and_store_data()

    logging.info("AFRR data fetcher completed")


if __name__ == "__main__":
    main()
