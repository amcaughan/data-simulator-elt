Source-facing landing ingestion runtime.

Responsibilities:
- plan storage targets for landing objects
- build one adapter-facing fetch request from runtime intent
- delegate source fetch behavior to a named adapter
- map fetch outputs onto planned storage targets and write exact source payloads
- support different request types without baking source behavior into the runtime

Adapter-specific behavior lives under `adapters/`.

Each source adapter must implement the abstract base in `adapters/base.py`:
- `adapter_key()`
- `from_ingest_config(...)`
- `_fetch(...)`

Adapter-facing request types are:
- `LiveFetchRequest`
- `SliceFetchRequest`
- `MultiSliceFetchRequest`

Planning lives outside `IngestConfig`:
- `config.py` holds validated runtime data
- `planning.py` expands runtime intent into a `FetchPlan`
- a `FetchPlan` pairs one source fetch request with one or more storage targets

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
