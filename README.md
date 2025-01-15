# TenneT AFRR Bid Ladder Archive

A Python application for downloading and archiving TenneT's AFRR (Automatic Frequency Restoration Reserve) bidladders at regular intervals.

## Overview

TenneT's API only provides access to the latest bidladder data. This project automatically downloads and stores bidladder snapshots based on the time-to-delivery, enabling historical analysis of how the ladder changes over time.

## Sampling
To keep the size of the archive manageable and ensure consistent hours-to-delivery across ISPs, the sampling frequency varies based on the time-to-delivery:

- 12h-24h: Every 2 hours, aligned with ISP pattern (e.g., ISP at :00 sampled at 00:00, 02:00, 04:00...)
- 3h-12h: Every 15 minutes, aligned with ISP pattern (sampled when current time matches ISP quarter)
- <3h: Every 15 minutes for all ISPs

The sampling strategy ensures that when comparing different ISPs (e.g., ISP1 at :00 vs ISP2 at :15), they will have consistent hours-to-delivery, making historical analysis more reliable.

> Note: The project runs on a personal VPS using a bash script at regular intervals via cron. This should provide more reliable execution timing compared to the previous GitHub Actions implementation.

## Data Structure
```
data/
└── YYYY-MM-DD/
    └── HHMM_DST/
        └── snapshot_HHMM_htd_X.Xh.parquet
```

The data is stored based on the date and time of the ISP (Imbalance Settlement Period). Each snapshot is saved in a Parquet file with a filename that includes the time of the snapshot and the hours-to-delivery (htd).

- YYYY-MM-DD: The date of the ISP
- HHMM_DST: The time of the ISP in 24-hour format, where DST indicates whether Daylight Saving Time is in effect (e.g., 1400_CET or 1400_CEST)
- snapshot_HHMM_htd_X.XX.parquet: The Parquet file containing the snapshot data, where:
    - HHMM: Time of the snapshot
    - X.XX: Hours-to-delivery

Each Parquet file contains:

- `index` (timestamp): start timestamp of ISP
- `capacity_threshold` (int)
- `price_down` (float)
- `price_up` (float)
- `snapshot_timestamp` (timestamp): timestamp of snapshot (not rounded to nearest quarter hour)
- `minutes_to_delivery` (int): minutes to the start of the ISP (delivery)

    This value is rounded to the nearest quarter hour if within the tolerance (5 minutes). Can be useful for data analysis purposes.

## Acknowledgements

Thanks to [fboerman](https://github.com/fboerman) for developing the [`tenneteu-py`](https://github.com/fboerman/TenneTeu-py) package!