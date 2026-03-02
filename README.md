# TenneT AFRR Bid Ladder Archive

Historical archive of TenneT AFRR bid ladder snapshots, stored as a Delta table.

## What Data Is Captured

Each ingestion run reads current + upcoming ladders and stores sampled snapshots.

Sampling by minutes-to-delivery (MTD):

- `<180 min`: every 15 minutes (`0, 15, ..., 165`)
- `180–719 min`: every 60 minutes (`180, 240, ..., 660`)
- `720–1440 min`: every 120 minutes (`720, 840, ..., 1440`)

This keeps dense coverage close to delivery and lighter coverage further out.

## Table Location

Default base directory:

- `~/tennet-afrr-data`

Delta table path:

- `~/tennet-afrr-data/delta/afrr_bid_ladder`

Partition column:

- `isp_date` (Amsterdam date of the ISP start, `YYYY-MM-DD`)

## Schema

Each row is one bid ladder step for one ISP start at one snapshot time.

- `isp_start_utc` (`timestamp`): ISP start in UTC
- `capacity_threshold` (`int16`): ladder threshold
- `price_down` (`float32`): downward price level
- `price_up` (`float32`): upward price level
- `snapshot_timestamp_utc` (`timestamp`): when the snapshot was captured (UTC)
- `minutes_to_delivery` (`int16`): minutes from snapshot to ISP start
- `isp_date` (`string`): local date used for table partitioning

## Notes For Analysis

- All timestamps are UTC. Convert to `Europe/Amsterdam` where needed.
- `minutes_to_delivery` reflects the sampling grid described above.

## Quick Example (Polars)

```python
import polars as pl

df = (
    pl.scan_delta("~/tennet-afrr-data/delta/afrr_bid_ladder")
    .filter(pl.col("isp_date") == "2026-02-27")
    .sort(["isp_start_utc", "minutes_to_delivery", "capacity_threshold"])
    .collect()
)
```

## Acknowledgements

Thanks to [fboerman](https://github.com/fboerman) for [`tenneteu-py`](https://github.com/fboerman/TenneTeu-py)!
