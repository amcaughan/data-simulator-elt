from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any

from common.slices import SliceWindowConfig


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
class IngestConfig:
    workflow_name: str
    source_adapter: str
    landing_bucket_name: str
    aws_region: str
    slice_window: SliceWindowConfig
    source_adapter_config: dict[str, Any]

    @classmethod
    def from_env(cls) -> "IngestConfig":
        config = cls(
            workflow_name=_require_env("WORKFLOW_NAME"),
            source_adapter=os.environ.get("SOURCE_ADAPTER", "simulator_api"),
            landing_bucket_name=_require_env("LANDING_BUCKET_NAME"),
            aws_region=_require_env("AWS_REGION"),
            slice_window=SliceWindowConfig(
                partition_granularity=os.environ.get("PARTITION_GRANULARITY", "day"),
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

    @property
    def partition_granularity(self) -> str:
        return self.slice_window.partition_granularity

    @property
    def mode(self) -> str:
        return self.slice_window.mode
