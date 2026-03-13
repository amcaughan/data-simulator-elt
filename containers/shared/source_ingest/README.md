Source-facing landing ingestion runtime.

Responsibilities:
- build explicit pull requests generically
- delegate source fetch behavior to a named adapter
- write exact source payloads into the landing bucket
- support both live-hit and backfill execution modes

Adapter-specific behavior lives under `adapters/`.

Each source adapter must implement the abstract base in `adapters/base.py`:
- `adapter_key()`
- `from_ingest_config(...)`
- `fetch(...)`

The runtime turns scheduler intent into explicit request types:
- `LivePullRequest`
- `HistoricalSlicePullRequest`

For `simulator_api`, the adapter is responsible for:
- interpreting pull requests as preset generate requests
- deriving deterministic seeds from workflow + preset + logical date when requested
- signing requests with the task role and calling the private simulator API

Non-goals:
- no processed-data writes
- no Parquet standardization
- no dbt transformations

Those belong in later steps of the ELT flow.
