# tap-leadgurus

Singer tap for the [LeadGurus](https://clients.leadgurus.com) API, built with
the [Meltano Singer SDK](https://sdk.meltano.com).

LeadGurus is a lead-generation vendor. This tap extracts marketing leads and
the daily spend/cost/conversion rollups Vertex uses for marketing-spend
attribution. The API is **read-only**; a single API key spans every Vertex
brand, and the brand is carried on each record as `client_slug`.

Response shapes were reverse-engineered from the live API (the vendor documents
endpoints but not shapes) — the source of truth is the upstream `leadgurus`
Python client and its `API_NOTES.md` in
`Vertex-Service-Partners/shared-libs`.

## Streams

| Stream | Endpoint | Key | Replication | Notes |
|---|---|---|---|---|
| `clients` | `/clients/` | `id` | full-table | Bare JSON list, no pagination (~13 brands) |
| `leads` | `/leads/` | *(none)* | incremental on `date_created` | DRF-paginated, **200 rows/page** cap; no per-lead id |
| `summary_client` | `/summary/client/` | `id` | incremental on `date` | Spend/cost/sales rollup — only source of spend |
| `summary_vertical` | `/summary/vertical/` | `id` | incremental on `date` | + `vertical` |
| `summary_channel` | `/summary/channel/` | `id` | incremental on `date` | + `channel_name` |
| `summary_territory` | `/summary/territory/` | `id` | incremental on `date` | + `territory_name`, `campaign_id` |
| `summary_dma` | `/summary/dma/` | `id` | incremental on `date` | + `dma_name`, `campaign_id` |
| `summary_state` | `/summary/state/` | `id` | incremental on `date` | + `state`, `campaign_id` |

`/backend-data/` is **not** modeled — it returned zero rows for every key/range
probed so far, so its shape is unknown. Re-add a stream once a key with backend
data exists.

## ⚠️ Behaviors to know (verified against the live API)

- **Auth is the `X-API-Key` header**, not Bearer.
- **Leads have no primary key.** The list exposes no per-lead id and
  `/leads/{id}/` returns SPA HTML. `primary_keys` is empty; **dedup in dbt** on
  a composite of `client_slug` + `phone` + `date_created` + `ad_id`.
- **Summaries require `date_after`** (400 otherwise) — the tap always sends it.
- **Records mutate after creation** — leads gain self-book fields, summary rows
  recompute as conversions land. The only date filter is `date_after` on the
  created/summary date, so the tap re-pulls a trailing `lookback_days` (default
  7) window each run to recover recent updates. Leads updated outside that
  window are not re-fetched; dbt should treat the latest copy as authoritative.
- **Money/rate fields are decimal strings** (`"63.10"`) — kept as strings to
  preserve precision; cast in dbt.

## Config

| Setting | Required | Default | Description |
|---|---|---|---|
| `api_key` | ✅ | — | Sent as `X-API-Key` |
| `start_date` | ✅ | — | Earliest date (YYYY-MM-DD); floor for `date_after` |
| `api_url` | | `https://clients.leadgurus.com/api/v1` | API base URL |
| `lookback_days` | | `7` | Trailing window re-pulled each run |

## Usage

```bash
uv venv && source .venv/bin/activate
uv pip install -e .

# Plugin loads and config is valid
uvx meltano invoke tap-leadgurus --about

# One full run to local JSONL (creds via env, never committed)
export TAP_LEADGURUS_API_KEY=...
export TAP_LEADGURUS_START_DATE=2026-01-01
uvx meltano run tap-leadgurus target-jsonl
```

## Tests

```bash
uv run pytest        # unit tests, no network
```
