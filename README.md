# TenneT AFRR Bid Ladder Archive

A Python application for downloading and archiving TenneT's AFRR (Automatic Frequency Restoration Reserve) bidladders at regular intervals.

## Overview

This project automatically downloads and archives bidladder snapshots from TenneT's API, which only provides the latest data. The archive enables historical analysis of ladder changes over time.

## Sampling

To keep the size of the archive manageable the sampling frequency varies by time-to-delivery:

- 12h-24h: Every 2 hours, aligned with ISP pattern
- 3h-12h: Once per hour, aligned with ISP pattern
- <3h: Every 15 minutes for all ISPs

This ensures consistent hours-to-delivery across ISPs.

> Note: Runs on VPS using cron-scheduled bash script for reliable execution timing.

## Data Structure
```
data/
└── YYYY-MM-DD/
    └── HHMM_DST/
        └── snapshot_HHMM_htd_X.Xh.parquet
```

The data is stored based on the date and time of the ISP. Each snapshot is saved in a Parquet file with a filename that includes the time of the snapshot and the hours-to-delivery (htd).

- YYYY-MM-DD: ISP date
- HHMM_DST: ISP time (24h format) with DST indicator (CET or CEST)
- snapshot_HHMM_htd_X.XX.parquet: Snapshot data, where:
    - HHMM: Time of the snapshot
    - X.XX: Hours-to-delivery

Parquet file contents:
- `index`: ISP start timestamp
- `capacity_threshold`
- `price_down` 
- `price_up` 
- `snapshot_timestamp`: Exact snapshot timestamp
- `minutes_to_delivery`: Minutes until ISP start (rounded to nearest quarter hour within 5-minute tolerance)

## Acknowledgements

Thanks to [fboerman](https://github.com/fboerman) for [`tenneteu-py`](https://github.com/fboerman/TenneTeu-py)!