from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any
from uuid import uuid4

from common.slices import GRANULARITY_ORDER, LogicalSlice, SliceWindowConfig


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _optional_int(name: str) -> int | None:
    value = os.environ.get(name)
    if value in {None, ""}:
        return None
    return int(value)


def _json_env(name: str, default: str = "{}") -> dict[str, Any]:
    raw_value = os.environ.get(name, default)
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be valid JSON") from exc

    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must decode to a JSON object")
    return parsed


@dataclass(frozen=True)
class StandardizeConfig:
    workflow_name: str
    source_adapter: str
    landing_bucket_name: str
    processed_bucket_name: str
    aws_region: str
    landing_partition_granularity: str
    output_partition_granularity: str
    processed_output_prefix: str
    landing_input_prefix: str | None
    slice_window: SliceWindowConfig
    source_adapter_config: dict[str, Any]

    @classmethod
    def from_env(cls) -> "StandardizeConfig":
        config = cls(
            workflow_name=_require_env("WORKFLOW_NAME"),
            source_adapter=_require_env("SOURCE_ADAPTER"),
            landing_bucket_name=_require_env("LANDING_BUCKET_NAME"),
            processed_bucket_name=_require_env("PROCESSED_BUCKET_NAME"),
            aws_region=_require_env("AWS_REGION"),
            landing_partition_granularity=os.environ.get(
                "LANDING_PARTITION_GRANULARITY", "day"
            ),
            output_partition_granularity=os.environ.get(
                "OUTPUT_PARTITION_GRANULARITY", "day"
            ),
            processed_output_prefix=os.environ.get("PROCESSED_OUTPUT_PREFIX", "raw"),
            landing_input_prefix=os.environ.get("LANDING_INPUT_PREFIX"),
            slice_window=SliceWindowConfig(
                partition_granularity=os.environ.get(
                    "OUTPUT_PARTITION_GRANULARITY", "day"
                ),
                mode=os.environ.get("MODE", "live_hit"),
                logical_date=os.environ.get("LOGICAL_DATE"),
                start_at=os.environ.get("START_AT"),
                end_at=os.environ.get("END_AT"),
                backfill_days=_optional_int("BACKFILL_DAYS"),
            ),
            source_adapter_config=_json_env("SOURCE_ADAPTER_CONFIG_JSON"),
        )
        config.validate()
        return config

    def validate(self) -> None:
        self.slice_window.validate()
        if self.landing_partition_granularity not in GRANULARITY_ORDER:
            raise ValueError(
                f"LANDING_PARTITION_GRANULARITY must be one of {sorted(GRANULARITY_ORDER)}"
            )
        if self.output_partition_granularity not in GRANULARITY_ORDER:
            raise ValueError(
                f"OUTPUT_PARTITION_GRANULARITY must be one of {sorted(GRANULARITY_ORDER)}"
            )
        if (
            GRANULARITY_ORDER[self.output_partition_granularity]
            > GRANULARITY_ORDER[self.landing_partition_granularity]
        ):
            raise ValueError(
                "OUTPUT_PARTITION_GRANULARITY cannot be finer than LANDING_PARTITION_GRANULARITY"
            )

    @property
    def mode(self) -> str:
        return self.slice_window.mode

    def iter_slices(self, now=None) -> list[LogicalSlice]:
        return [
            LogicalSlice(logical_date=logical_date, run_id=str(uuid4()))
            for logical_date in self.slice_window.iter_slices(now)
        ]
