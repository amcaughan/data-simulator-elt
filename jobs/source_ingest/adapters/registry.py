from __future__ import annotations

from source_ingest.adapters.base import SourceAdapter
from source_ingest.adapters.simulator_api import (
    SimulatorApiAdapter,
    SimulatorApiConfig,
)
from source_ingest.config import IngestConfig


def build_adapter(config: IngestConfig) -> SourceAdapter:
    if config.source_adapter == "simulator_api":
        adapter_config = SimulatorApiConfig.from_dict(config.source_adapter_config)
        return SimulatorApiAdapter(
            workflow_name=config.workflow_name,
            aws_region=config.aws_region,
            source_base_url=config.source_base_url,
            adapter_config=adapter_config,
        )

    raise ValueError(
        f"Unsupported source adapter '{config.source_adapter}'. Supported adapters: simulator_api"
    )
