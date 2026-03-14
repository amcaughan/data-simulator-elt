from __future__ import annotations

import csv
from dataclasses import dataclass
import io
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
class BatchDeliveryCsvConfig:
    preset_id: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BatchDeliveryCsvConfig":
        preset_id = value.get("preset_id")
        if not isinstance(preset_id, str) or preset_id == "":
            raise ValueError(
                "Batch delivery CSV standardize strategy requires a non-empty 'preset_id'"
            )
        return cls(preset_id=preset_id)


class BatchDeliveryCsvStandardizeStrategy(StandardizeStrategy):
    @classmethod
    def strategy_key(cls) -> str:
        return "batch_delivery_csv"

    @classmethod
    def from_standardize_config(
        cls,
        config: StandardizeConfig,
    ) -> "BatchDeliveryCsvStandardizeStrategy":
        return cls(BatchDeliveryCsvConfig.from_dict(config.standardize_strategy_config))

    def __init__(self, strategy_config: BatchDeliveryCsvConfig):
        self.strategy_config = strategy_config

    def process_slice(
        self,
        output_slice: LogicalSlice,
        input_objects: list[StandardizeInputObject],
    ) -> StandardizeResult:
        return self._process_input_objects(input_objects, output_slice)

    def process_manual(
        self,
        input_objects: list[StandardizeInputObject],
    ) -> StandardizeResult:
        return self._process_input_objects(input_objects, None)

    def _process_input_objects(
        self,
        input_objects: list[StandardizeInputObject],
        output_slice: LogicalSlice | None,
    ) -> StandardizeResult:
        rows: list[dict[str, Any]] = []
        for input_object in input_objects:
            rows.extend(self._parse_input_object(output_slice, input_object))

        if not rows:
            return StandardizeResult(outputs=())

        return StandardizeResult(
            outputs=(
                StandardizeOutput(
                    rows=rows,
                    metadata={
                        "preset_id": self.strategy_config.preset_id,
                        "input_object_count": str(len(input_objects)),
                        "source_format": "csv",
                    },
                ),
            )
        )

    def _parse_input_object(
        self,
        output_slice: LogicalSlice | None,
        input_object: StandardizeInputObject,
    ) -> list[dict[str, Any]]:
        reader = csv.DictReader(io.StringIO(input_object.payload.decode("utf-8")))
        parsed_rows: list[dict[str, Any]] = []
        for row in reader:
            flattened = {
                key: (value if value != "" else None)
                for key, value in row.items()
            }
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
            flattened["_source_system_id"] = input_object.metadata.get("source_system_id")
            flattened["_delivery_id"] = input_object.metadata.get("delivery_id")
            flattened["_delivery_date"] = input_object.metadata.get("delivery_date")
            flattened["_feed_type"] = input_object.metadata.get("feed_type")
            flattened["_response_row_count"] = input_object.metadata.get("row_count")
            parsed_rows.append(flattened)
        return parsed_rows
