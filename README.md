# TenneT AFRR Bid Ladder Archive

Historical archive of TenneT AFRR bid ladder snapshots, stored as a Delta table.
The full archive is available as a download from the [GitHub Releases page](../../releases/tag/afrr-delta-backups) — no API key required.

## How It Works

A cron job runs every 15 minutes on a VPS:

1. **Fetch** — pulls the current bid ladder from the TenneT API and stores sampled snapshots into a local Delta table
2. **Backup** — once a day, the Delta table is packed into a `.tar.zst` archive and uploaded to the `afrr-delta-backups` GitHub Release, keeping the last 14 daily snapshots

The repo contains only code. All data lives in the Release assets.

## Getting The Data

Download the latest archive and extract it:

```bash
gh release download afrr-delta-backups \
  --repo wouterbles/tennet-afrr-archive \
  --pattern "*.tar.zst" \
  --dir /tmp/

mkdir -p ~/tennet-afrr-data
tar --zstd -C ~/tennet-afrr-data -xf /tmp/afrr_delta_*.tar.zst
rm /tmp/afrr_delta_*.tar.zst
```

This extracts the Delta table to `~/tennet-afrr-data/delta/afrr_bid_ladder`.

## What Data Is Captured

Each ingestion run reads current + upcoming ladders and stores sampled snapshots.

Sampling by minutes-to-delivery (MTD):

- `<180 min`: every 15 minutes (`0, 15, ..., 165`)
- `180–719 min`: every 60 minutes (`180, 240, ..., 660`)
- `720–1440 min`: every 120 minutes (`720, 840, ..., 1440`)

This keeps dense coverage close to delivery and lighter coverage further out.

## Delta Table Location

After extracting the archive:

- `~/tennet-afrr-data/delta/afrr_bid_ladder`

Partitioned by `isp_date` (Amsterdam date of the ISP start, `YYYY-MM-DD`).

## Schema

Each row is one bid ladder step for one ISP start at one snapshot time.

- `isp_start_utc` (`timestamp`): ISP start in UTC
- `capacity_threshold` (`int16`): ladder threshold
- `price_down` (`float32`): downward price level
- `price_up` (`float32`): upward price level
- `snapshot_timestamp_utc` (`timestamp`): when the snapshot was captured (UTC)
- `minutes_to_delivery` (`int16`): minutes from snapshot to ISP start
- `isp_date` (`string`): partition key — local date of the ISP start

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

All timestamps are UTC — convert to `Europe/Amsterdam` where needed.

## Acknowledgements

Thanks to [fboerman](https://github.com/fboerman) for [`tenneteu-py`](https://github.com/fboerman/TenneTeu-py)!
