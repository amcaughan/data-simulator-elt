from __future__ import annotations

from standardize.config import StandardizeConfig
from standardize.strategies.base import StandardizeStrategy
from standardize.strategies.batch_delivery_csv import (
    BatchDeliveryCsvStandardizeStrategy,
)
from standardize.strategies.simulator_api import (
    SimulatorApiStandardizeStrategy,
)


STRATEGIES_BY_KEY: dict[str, type[StandardizeStrategy]] = {
    strategy_type.strategy_key(): strategy_type
    for strategy_type in (
        BatchDeliveryCsvStandardizeStrategy,
        SimulatorApiStandardizeStrategy,
    )
}


def build_strategy(config: StandardizeConfig) -> StandardizeStrategy:
    try:
        strategy_type = STRATEGIES_BY_KEY[config.standardize_strategy]
    except KeyError as exc:
        supported_strategies = ", ".join(sorted(STRATEGIES_BY_KEY))
        raise ValueError(
            f"Unsupported standardize strategy '{config.standardize_strategy}'. "
            f"Supported strategies: {supported_strategies}"
        ) from exc

    return strategy_type.from_standardize_config(config)
