# scraper.py
import os
from pathlib import Path

import pandas as pd
from tenneteu import TenneTeuClient


class AFRRScraper:
    def __init__(self, api_key: str):
        self.client = TenneTeuClient(api_key=api_key)
        self.base_path = Path("data")
        self.base_path.mkdir(exist_ok=True)
        self.current_time = None

    def set_current_time(self):
        """Set current time in Amsterdam timezone"""
        self.current_time = pd.Timestamp.now(tz="Europe/Amsterdam")

    def get_current_bid_ladder(self):
        """Fetch current and upcoming bid ladders from TenneT API"""
        try:
            # Set current time
            self.set_current_time()

            # Query from now until 24 hours ahead
            d_to = self.current_time + pd.Timedelta(hours=24)
            df = self.client.query_merit_order_list(d_from=self.current_time, d_to=d_to)
            return df
        except Exception as e:
            print(f"Error fetching bid ladder: {e}")
            return None

    def process_and_store_data(self, df: pd.DataFrame):
        """Process and store bid ladder data with time-to-delivery tracking"""
        if df is None or df.empty:
            return

        # Group by unique ISP timestamp
        for isp_start, isp_data in df.groupby(level=0):
            # Calculate time to delivery
            time_to_delivery = isp_start - self.current_time
            hours_to_delivery = time_to_delivery.total_seconds() / 3600

            # Only store future ISPs (positive time to delivery)
            if hours_to_delivery <= 0:
                continue

            # Create storage structure
            isp_date = isp_start.strftime("%Y-%m-%d")
            isp_time = isp_start.strftime("%H%M")
            snapshot_time = self.current_time.strftime("%Y%m%d_%H%M")

            # Store data
            storage_path = self.base_path / isp_date / isp_time
            storage_path.mkdir(parents=True, exist_ok=True)

            # Prepare data for storage
            store_data = isp_data.copy()
            store_data["Snapshot Time"] = self.current_time
            store_data["Hours to Delivery"] = hours_to_delivery

            # Select relevant columns for bid ladder
            store_data = store_data[
                [
                    "Isp",
                    "Capacity Threshold",
                    "Price Down",
                    "Price Up",
                    "Snapshot Time",
                    "Hours to Delivery",
                ]
            ]

            # Store as parquet
            filename = f"snapshot_{snapshot_time}_ttd_{hours_to_delivery:.1f}h.parquet"
            filepath = storage_path / filename
            store_data.to_parquet(filepath)


def main():
    api_key = os.getenv("TENNET_API_KEY")
    if not api_key:
        raise ValueError("TENNET_API_KEY environment variable not set")

    scraper = AFRRScraper(api_key)
    df = scraper.get_current_bid_ladder()
    scraper.process_and_store_data(df)


if __name__ == "__main__":
    main()
