from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from common.slices import LogicalSlice
from standardize.config import StandardizeConfig
from standardize.strategies.base import (
    StandardizeInputObject,
    StandardizeOutput,
    StandardizeResult,
    StandardizeStrategy,
)


@dataclass(frozen=True)
class SimulatorApiStandardizeConfig:
    preset_id: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SimulatorApiStandardizeConfig":
        preset_id = value.get("preset_id")
        if not isinstance(preset_id, str) or preset_id == "":
            raise ValueError(
                "Simulator API standardize strategy requires a non-empty 'preset_id'"
            )
        return cls(preset_id=preset_id)


class SimulatorApiStandardizeStrategy(StandardizeStrategy):
    @classmethod
    def strategy_key(cls) -> str:
        return "simulator_api"

    @classmethod
    def from_standardize_config(
        cls,
        config: StandardizeConfig,
    ) -> "SimulatorApiStandardizeStrategy":
        strategy_config = SimulatorApiStandardizeConfig.from_dict(
            config.standardize_strategy_config
        )
        return cls(strategy_config)

    def __init__(self, strategy_config: SimulatorApiStandardizeConfig):
        self.strategy_config = strategy_config

    def process_slice(
        self,
        output_slice: LogicalSlice,
        input_objects: list[StandardizeInputObject],
    ) -> StandardizeResult:
        return self._process_input_objects(
            input_objects=input_objects,
            output_slice=output_slice,
        )

    def process_manual(
        self,
        input_objects: list[StandardizeInputObject],
    ) -> StandardizeResult:
        return self._process_input_objects(
            input_objects=input_objects,
            output_slice=None,
        )

    def _process_input_objects(
        self,
        input_objects: list[StandardizeInputObject],
        output_slice: LogicalSlice | None,
    ) -> StandardizeResult:
        rows: list[dict[str, Any]] = []

        for input_object in input_objects:
            rows.extend(
                self._parse_input_object(
                    output_slice=output_slice,
                    input_object=input_object,
                )
            )

        if not rows:
            return StandardizeResult(outputs=())

        return StandardizeResult(
            outputs=(
                StandardizeOutput(
                    rows=rows,
                    metadata={
                        "preset_id": self.strategy_config.preset_id,
                        "input_object_count": str(len(input_objects)),
                    },
                ),
            )
        )

    def _parse_input_object(
        self,
        output_slice: LogicalSlice | None,
        input_object: StandardizeInputObject,
    ) -> list[dict]:
        parsed = json.loads(input_object.payload.decode("utf-8"))
        rows = parsed.get("rows", [])
        if not isinstance(rows, list):
            raise ValueError(
                f"Landing object {input_object.key} does not contain a valid 'rows' array"
            )

        result_row_count = parsed.get("row_count")
        schema_version = parsed.get("schema_version")
        scenario_name = parsed.get("scenario_name")

        parsed_rows: list[dict] = []
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError(
                    f"Landing object {input_object.key} contains a non-object row entry"
                )

            flattened = dict(row)
            flattened["_landing_key"] = input_object.key
            flattened["_standardize_strategy"] = self.strategy_key()
            flattened["_source_preset_id"] = input_object.metadata.get(
                "preset_id",
                self.strategy_config.preset_id,
            )
            flattened["_logical_date"] = input_object.metadata.get(
                "logical_date",
                None if output_slice is None else output_slice.logical_date.isoformat(),
            )
            flattened["_ingested_at"] = input_object.metadata.get("ingested_at")
            flattened["_schema_version"] = schema_version
            flattened["_scenario_name"] = scenario_name
            flattened["_response_row_count"] = result_row_count
            parsed_rows.append(flattened)

        return parsed_rows
