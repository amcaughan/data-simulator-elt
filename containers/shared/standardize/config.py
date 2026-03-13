from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any
from uuid import uuid4

from common.slices import GRANULARITY_ORDER, LogicalSlice, SliceWindowConfig
from common.storage_layout import (
    StorageLayoutConfig,
    default_partition_fields,
    validate_path_segments,
    validate_partition_fields,
    validate_partition_fields_for_granularity,
)


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


def _json_list_env(name: str, default: list[str]) -> tuple[str, ...]:
    raw_value = os.environ.get(name, json.dumps(default))
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be valid JSON") from exc

    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise ValueError(f"{name} must decode to a JSON array of strings")
    return tuple(parsed)


@dataclass(frozen=True)
class StandardizeConfig:
    workflow_name: str
    standardize_strategy: str
    landing_bucket_name: str
    processed_bucket_name: str
    aws_region: str
    landing_slice_granularity: str
    landing_layout: StorageLayoutConfig
    output_slice_granularity: str
    processed_layout: StorageLayoutConfig
    landing_input_prefix: str | None
    slice_window: SliceWindowConfig
    standardize_strategy_config: dict[str, Any]

    @classmethod
    def from_env(cls) -> "StandardizeConfig":
        config = cls(
            workflow_name=_require_env("WORKFLOW_NAME"),
            standardize_strategy=_require_env("STANDARDIZE_STRATEGY"),
            landing_bucket_name=_require_env("LANDING_BUCKET_NAME"),
            processed_bucket_name=_require_env("PROCESSED_BUCKET_NAME"),
            aws_region=_require_env("AWS_REGION"),
            landing_slice_granularity=os.environ.get(
                "LANDING_SLICE_GRANULARITY", "day"
            ),
            landing_layout=StorageLayoutConfig(
                base_prefix=os.environ.get("LANDING_BASE_PREFIX"),
                partition_fields=_json_list_env(
                    "LANDING_PARTITION_FIELDS_JSON",
                    list(
                        default_partition_fields(
                            os.environ.get("LANDING_SLICE_GRANULARITY", "day")
                        )
                    ),
                ),
                path_suffix=_json_list_env("LANDING_PATH_SUFFIX_JSON", []),
            ),
            output_slice_granularity=os.environ.get(
                "OUTPUT_SLICE_GRANULARITY", "day"
            ),
            processed_layout=StorageLayoutConfig(
                base_prefix=os.environ.get("PROCESSED_BASE_PREFIX", "raw"),
                partition_fields=_json_list_env(
                    "PROCESSED_PARTITION_FIELDS_JSON",
                    list(
                        default_partition_fields(
                            os.environ.get("OUTPUT_SLICE_GRANULARITY", "day")
                        )
                    ),
                ),
                path_suffix=_json_list_env("PROCESSED_PATH_SUFFIX_JSON", []),
            ),
            landing_input_prefix=os.environ.get("LANDING_INPUT_PREFIX"),
            slice_window=SliceWindowConfig(
                slice_granularity=os.environ.get(
                    "OUTPUT_SLICE_GRANULARITY", "day"
                ),
                selector_mode=os.environ.get("SLICE_SELECTOR_MODE", "current"),
                pinned_at=os.environ.get("SLICE_PINNED_AT"),
                range_start_at=os.environ.get("SLICE_RANGE_START_AT"),
                range_end_at=os.environ.get("SLICE_RANGE_END_AT"),
                relative_count=_optional_int("SLICE_RELATIVE_COUNT"),
                relative_direction=os.environ.get("SLICE_RELATIVE_DIRECTION"),
                relative_anchor_at=os.environ.get("SLICE_RELATIVE_ANCHOR_AT"),
                timestamp_alignment_policy=os.environ.get(
                    "SLICE_ALIGNMENT_POLICY",
                    "floor",
                ),
                range_inclusion_policy=os.environ.get(
                    "SLICE_RANGE_POLICY",
                    "overlap",
                ),
            ),
            standardize_strategy_config=_json_env("STANDARDIZE_STRATEGY_CONFIG_JSON"),
        )
        config.validate()
        return config

    def validate(self) -> None:
        self.slice_window.validate()
        if self.landing_slice_granularity not in GRANULARITY_ORDER:
            raise ValueError(
                f"LANDING_SLICE_GRANULARITY must be one of {sorted(GRANULARITY_ORDER)}"
            )
        validate_partition_fields(self.landing_layout.partition_fields)
        validate_partition_fields_for_granularity(
            self.landing_layout.partition_fields,
            self.landing_slice_granularity,
        )
        validate_path_segments(self.landing_layout.path_suffix)
        if self.output_slice_granularity not in GRANULARITY_ORDER:
            raise ValueError(
                f"OUTPUT_SLICE_GRANULARITY must be one of {sorted(GRANULARITY_ORDER)}"
            )
        if (
            GRANULARITY_ORDER[self.output_slice_granularity]
            > GRANULARITY_ORDER[self.landing_slice_granularity]
        ):
            raise ValueError(
                "OUTPUT_SLICE_GRANULARITY cannot be finer than LANDING_SLICE_GRANULARITY"
            )
        validate_partition_fields(self.processed_layout.partition_fields)
        validate_partition_fields_for_granularity(
            self.processed_layout.partition_fields,
            self.output_slice_granularity,
        )
        validate_path_segments(self.processed_layout.path_suffix)

    @property
    def iter_slices(self, now=None) -> list[LogicalSlice]:
        return [
            logical_slice.with_run_id(str(uuid4()))
            for logical_slice in self.slice_window.iter_slices(now)
        ]
