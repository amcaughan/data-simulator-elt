from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from common.slices import LogicalSlice


@dataclass(frozen=True)
class SimulatorApiParserConfig:
    preset_id: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SimulatorApiParserConfig":
        preset_id = value.get("preset_id")
        if not isinstance(preset_id, str) or preset_id == "":
            raise ValueError(
                "Simulator API parser config requires a non-empty 'preset_id'"
            )
        return cls(preset_id=preset_id)


class SimulatorApiParser:
    def __init__(self, config: SimulatorApiParserConfig):
        self.config = config

    def parse_landing_object(
        self,
        logical_slice: LogicalSlice,
        key: str,
        payload: bytes,
        metadata: dict[str, str],
    ) -> list[dict]:
        parsed = json.loads(payload.decode("utf-8"))
        rows = parsed.get("rows", [])
        if not isinstance(rows, list):
            raise ValueError(
                f"Landing object {key} does not contain a valid 'rows' array"
            )

        result_row_count = parsed.get("row_count")
        schema_version = parsed.get("schema_version")
        scenario_name = parsed.get("scenario_name")

        parsed_rows: list[dict] = []
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError(f"Landing object {key} contains a non-object row entry")

            flattened = dict(row)
            flattened["_landing_key"] = key
            flattened["_source_adapter"] = "simulator_api"
            flattened["_source_preset_id"] = metadata.get("preset_id", self.config.preset_id)
            flattened["_logical_date"] = metadata.get(
                "logical_date", logical_slice.logical_date.isoformat()
            )
            flattened["_ingested_at"] = metadata.get("ingested_at")
            flattened["_schema_version"] = schema_version
            flattened["_scenario_name"] = scenario_name
            flattened["_response_row_count"] = result_row_count
            parsed_rows.append(flattened)

        return parsed_rows
