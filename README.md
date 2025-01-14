# TenneT AFRR Bid Ladder Archive

A Python application for downloading and archiving TenneT's AFRR (Automatic Frequency Restoration Reserve) bidladders at regular intervals.

## Overview

TenneT's API only provides access to the latest bidladder data. This project automatically downloads and stores bidladder snapshots based on the time-to-delivery, enabling historical analysis of how the ladder changes over time.

## Sampling
To keep the size of the archive manageable, the sampling frequency varies based on the time-to-delivery:

- 24h-12h: Every 2 hours (stored at even hours, e.g., 00:00, 02:00, 04:00...)
- 12h-6h: Every hour (stored at HH:00)
- <6h: Every 30 minutes (stored at HH:00 and HH:30)

> Note: This project uses GitHub Actions to run the fetcher, which means executions may not occur exactly on the specified intervals. GitHub schedules workflows based on resource availability, with high workload periods typically occurring around the hour mark (HH:00). To work around this, I've implemented a 10-minute tolerance window on either side of the target time and scheduled the jobs to run slightly before the target time (at XX:28 and XX:58).

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
- snapshot_HHMM_htd_X.X.parquet: The Parquet file containing the snapshot data, where:
    - HHMM: Time of the snapshot
    - X.X: Hours-to-delivery

Each Parquet file contains:

- `index` (timestamp): start timestamp of ISP
- `capacity_threshold` (int)
- `price_down` (float)
- `price_up` (float)
- `snapshot_timestamp` (timestamp): timestamp of snapshot (not rounded to nearest half hour)
- `minutes_to_delivery` (int): minutes to the start of the ISP (delivery)

    This value is rounded to the nearest half hour if within the tolerance (10 minutes). Can be useful for data analysis purposes.

## Acknowledgements

Thanks to [fboerman](https://github.com/fboerman) for developing the [`tenneteu-py`](https://github.com/fboerman/TenneTeu-py) package!