from __future__ import annotations

from standardize.config import StandardizeConfig
from standardize.parsers.base import LandingParser
from standardize.parsers.simulator_api import (
    SimulatorApiParser,
    SimulatorApiParserConfig,
)


def build_parser(config: StandardizeConfig) -> LandingParser:
    if config.source_adapter == "simulator_api":
        parser_config = SimulatorApiParserConfig.from_dict(config.source_adapter_config)
        return SimulatorApiParser(parser_config)

    raise ValueError(
        f"Unsupported standardize parser '{config.source_adapter}'. Supported parsers: simulator_api"
    )
