Source-facing landing ingestion runtime.

Responsibilities:
- plan storage targets for landing objects
- build one adapter-facing fetch request from runtime intent
- delegate source fetch behavior to a named adapter
- map fetch outputs onto planned storage targets and write exact source payloads
- support different request types without baking source behavior into the runtime
- write a sidecar manifest JSON for each landed payload

Adapter-specific behavior lives under `adapters/`.

Each source adapter must implement the abstract base in `adapters/base.py`:
- `adapter_key()`
- `from_ingest_config(...)`
- `_fetch(...)`

Adapter-facing request types are:
- `LiveFetchRequest`
- `ManualFetchRequest`
- `SliceFetchRequest`
- `MultiSliceFetchRequest`

Planning lives outside `IngestConfig`:
- `config.py` holds validated runtime data
- `planning.py` expands runtime intent into a `FetchPlan`
- a `FetchPlan` pairs one source fetch request with one or more storage targets
- `PLANNING_MODE=temporal` uses the slice planner and storage layout config
- `PLANNING_MODE=manual` bypasses slices and lands one adapter output under `MANUAL_STORAGE_PREFIX`
- temporal slice planning is driven by `SLICE_GRANULARITY` plus `SLICE_SELECTOR_MODE`
- selector modes are exclusive:
  - `current` for the slice containing now
  - `pinned` for one explicit slice via `SLICE_PINNED_AT`
  - `range` for slices spanning `SLICE_RANGE_START_AT` to `SLICE_RANGE_END_AT`
  - `relative` for `SLICE_RELATIVE_COUNT` slices from an anchor in `SLICE_RELATIVE_DIRECTION`
- built-in slice granularities are `hour`, `day`, `month`, `quarter`, and `year`

Storage layout is generic:
- `LANDING_BASE_PREFIX` can anchor a whole workflow under a client or project subpath
- `LANDING_PARTITION_FIELDS_JSON` controls which temporal or derived time fields appear in object keys
- built-in derived partition fields include `year_quarter`, `year_month`, and `date`
- `LANDING_PATH_SUFFIX_JSON` can append fixed subpath segments after the temporal partitions
- `MANUAL_STORAGE_PREFIX` can land a one-off object under any explicit bucket prefix
- `MANUAL_OBJECT_NAME` can override the landed object name in manual mode
- the runtime writes both the payload object and a neighboring `.manifest.json` sidecar
- object naming precedence is runner override, then adapter suggestion, then runtime fallback

`FetchResult` is intentionally narrow:
- one or more fetch outputs
- response bytes and content type per output
- adapter-provided metadata per output
- optional logical-date labeling when one fetch returns multiple slice-specific outputs

For `simulator_api`, the adapter is responsible for:
- interpreting fetch requests as simulator generate calls
- handling live, single-slice, and multi-slice requests
- deriving deterministic seeds from workflow + preset + logical date when requested
- signing requests with the task role and calling the private simulator API

Non-goals:
- no processed-data writes
- no Parquet standardization
- no dbt transformations

Those belong in later steps of the ELT flow.
