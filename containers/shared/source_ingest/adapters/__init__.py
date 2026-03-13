from source_ingest.adapters.base import (
    AdapterCapabilities,
    FetchOutput,
    FetchResult,
    LiveFetchRequest,
    ManualFetchRequest,
    MultiSliceFetchRequest,
    RequestedSlice,
    SliceFetchRequest,
    SourceAdapter,
    SourceFetchRequest,
    UnsupportedSourceRequestError,
)
from source_ingest.adapters.registry import build_adapter
from source_ingest.adapters.simulator_api import SimulatorApiAdapter

__all__ = [
    "AdapterCapabilities",
    "FetchOutput",
    "FetchResult",
    "LiveFetchRequest",
    "ManualFetchRequest",
    "MultiSliceFetchRequest",
    "RequestedSlice",
    "SliceFetchRequest",
    "SourceAdapter",
    "SourceFetchRequest",
    "UnsupportedSourceRequestError",
    "build_adapter",
]
