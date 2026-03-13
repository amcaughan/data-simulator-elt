from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any

from common.slices import SliceWindowConfig
from common.storage_layout import (
    StorageLayoutConfig,
    default_partition_fields,
    validate_path_segments,
    validate_partition_fields,
    validate_partition_fields_for_granularity,
)


VALID_PLANNING_MODES = {"temporal", "manual"}


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


def _validate_manual_object_name(object_name: str | None) -> None:
    if object_name is None:
        return
    if object_name.strip() == "" or "/" in object_name:
        raise ValueError("MANUAL_OBJECT_NAME must be a non-empty filename without '/'")


@dataclass(frozen=True)
class TemporalPlanningConfig:
    slice_window: SliceWindowConfig
    landing_layout: StorageLayoutConfig

    def validate(self) -> None:
        self.slice_window.validate()
        validate_partition_fields(self.landing_layout.partition_fields)
        validate_partition_fields_for_granularity(
            self.landing_layout.partition_fields,
            self.slice_window.slice_granularity,
        )
        validate_path_segments(self.landing_layout.path_suffix)


@dataclass(frozen=True)
class ManualPlanningConfig:
    request_payload: dict[str, Any]
    storage_prefix: str | None
    object_name: str | None

    def validate(self) -> None:
        _validate_manual_object_name(self.object_name)


@dataclass(frozen=True)
class IngestConfig:
    workflow_name: str
    source_adapter: str
    landing_bucket_name: str
    aws_region: str
    planning_mode: str
    source_adapter_config: dict[str, Any]
    temporal_config: TemporalPlanningConfig | None = None
    manual_config: ManualPlanningConfig | None = None

    @classmethod
    def from_env(cls) -> "IngestConfig":
        planning_mode = os.environ.get("PLANNING_MODE", "temporal")
        temporal_config = None
        manual_config = None
        if planning_mode == "temporal":
            temporal_config = TemporalPlanningConfig(
                slice_window=SliceWindowConfig(
                    slice_granularity=os.environ.get("SLICE_GRANULARITY", "day"),
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
                landing_layout=StorageLayoutConfig(
                    base_prefix=os.environ.get("LANDING_BASE_PREFIX"),
                    partition_fields=_json_list_env(
                        "LANDING_PARTITION_FIELDS_JSON",
                        list(
                            default_partition_fields(
                                os.environ.get("SLICE_GRANULARITY", "day")
                            )
                        ),
                    ),
                    path_suffix=_json_list_env("LANDING_PATH_SUFFIX_JSON", []),
                ),
            )
        elif planning_mode == "manual":
            manual_config = ManualPlanningConfig(
                request_payload=_json_env("MANUAL_REQUEST_JSON"),
                storage_prefix=os.environ.get("MANUAL_STORAGE_PREFIX"),
                object_name=os.environ.get("MANUAL_OBJECT_NAME"),
            )
        config = cls(
            workflow_name=_require_env("WORKFLOW_NAME"),
            source_adapter=os.environ.get("SOURCE_ADAPTER", "simulator_api"),
            landing_bucket_name=_require_env("LANDING_BUCKET_NAME"),
            aws_region=_require_env("AWS_REGION"),
            planning_mode=planning_mode,
            source_adapter_config=_json_env("SOURCE_ADAPTER_CONFIG_JSON"),
            temporal_config=temporal_config,
            manual_config=manual_config,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.planning_mode not in VALID_PLANNING_MODES:
            raise ValueError(
                f"PLANNING_MODE must be one of {sorted(VALID_PLANNING_MODES)}"
            )
        if self.planning_mode == "temporal":
            if self.temporal_config is None or self.manual_config is not None:
                raise ValueError(
                    "Temporal planning mode requires temporal_config and forbids manual_config"
                )
            self.temporal_config.validate()
            return
        if self.manual_config is None or self.temporal_config is not None:
            raise ValueError(
                "Manual planning mode requires manual_config and forbids temporal_config"
            )
        self.manual_config.validate()

    @property
    def is_manual(self) -> bool:
        return self.planning_mode == "manual"

    @property
    def temporal(self) -> TemporalPlanningConfig:
        if self.temporal_config is None:
            raise ValueError("Temporal planning config is unavailable in manual mode")
        return self.temporal_config

    @property
    def manual(self) -> ManualPlanningConfig:
        if self.manual_config is None:
            raise ValueError("Manual planning config is unavailable in temporal mode")
        return self.manual_config

    @property
    def slice_granularity(self) -> str:
        return self.temporal.slice_window.slice_granularity

    @property
    def slice_selector_mode(self) -> str:
        return self.temporal.slice_window.selector_mode

    @property
    def slice_window(self) -> SliceWindowConfig:
        return self.temporal.slice_window

    @property
    def landing_layout(self) -> StorageLayoutConfig:
        return self.temporal.landing_layout
