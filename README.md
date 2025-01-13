# TenneT AFRR Bid Ladder Archive

A Python application for downloading and archiving TenneT's AFRR (Automatic Frequency Restoration Reserve) bidladders at regular intervals.

## Overview

TenneT's API only provides access to the latest bidladder data. This project automatically downloads and stores bidladder snapshots based on the time-to-delivery, enabling historical analysis of how the ladder changes over time.

## Sampling
To keep the size of the archive manageable, the sampling frequency is based on the time-to-delivery:

- 24h-12h: Every 3 hours
- 12h-6h: Every 2 hours
- 6h-2h: Every hour
- <2h: Every 15 minutes

## Data Structure
```
data/
└── YYYY-MM-DD/
    └── HHMM/
        └── snapshot_HHMM_ttd_X.Xh.parquet
```

The data is stored based on the date and time of the ISP (Imbalance Settlement Period). Each snapshot is saved in a Parquet file with a filename that includes the time of the snapshot and the time-to-delivery (ttd) in hours.

- `YYYY-MM-DD`: The date of the ISP.
- `HHMM`: The time of the ISP in 24-hour format.
- `snapshot_HHMM_ttd_X.Xh.parquet`: The Parquet file containing the snapshot data, where `HHMM` is the time of the snapshot and `X.Xh` is the time-to-delivery in hours.

Each Parquet file contains:

- `index` (timestamp): timestamp of ISP
- `capacity_threshold` (int16)
- `price_down` (float32)
- `price_up` (float32)
- `snapshot_timestamp` (timestamp): timestamp of snapshot
