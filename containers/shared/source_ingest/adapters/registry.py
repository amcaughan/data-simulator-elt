from __future__ import annotations

from source_ingest.adapters.base import SourceAdapter
from source_ingest.adapters.simulator_batch_delivery import SimulatorBatchDeliveryAdapter
from source_ingest.adapters.simulator_api import SimulatorApiAdapter
from source_ingest.config import IngestConfig

ADAPTER_TYPES: tuple[type[SourceAdapter], ...] = (
    SimulatorApiAdapter,
    SimulatorBatchDeliveryAdapter,
)
ADAPTERS_BY_KEY = {
    adapter_type.adapter_key(): adapter_type for adapter_type in ADAPTER_TYPES
}


def build_adapter(config: IngestConfig) -> SourceAdapter:
    try:
        adapter_type = ADAPTERS_BY_KEY[config.source_adapter]
    except KeyError as exc:
        supported_adapters = ", ".join(sorted(ADAPTERS_BY_KEY))
        raise ValueError(
            f"Unsupported source adapter '{config.source_adapter}'. "
            f"Supported adapters: {supported_adapters}"
        )
    return adapter_type.from_ingest_config(config)
