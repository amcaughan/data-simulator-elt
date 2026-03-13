from __future__ import annotations

from dataclasses import dataclass

from common.slices import LogicalSlice


SUPPORTED_PARTITION_FIELDS = {
    "workflow",
    "adapter",
    "year",
    "quarter",
    "month",
    "day",
    "hour",
}

TEMPORAL_PARTITION_FIELDS_BY_GRANULARITY = {
    "year": {"year"},
    "quarter": {"year", "quarter"},
    "month": {"year", "month"},
    "day": {"year", "month", "day"},
    "hour": {"year", "month", "day", "hour"},
}


@dataclass(frozen=True)
class PartitionComponent:
    key: str
    value: str

    def to_path_segment(self) -> str:
        return f"{self.key}={self.value}"


@dataclass(frozen=True)
class StorageLayoutConfig:
    base_prefix: str | None
    partition_fields: tuple[str, ...]
    path_suffix: tuple[str, ...] = ()


def default_partition_fields(slice_granularity: str) -> tuple[str, ...]:
    default_fields_by_granularity = {
        "year": ("year",),
        "quarter": ("year", "quarter"),
        "month": ("year", "month"),
        "day": ("year", "month", "day"),
        "hour": ("year", "month", "day", "hour"),
    }
    return default_fields_by_granularity[slice_granularity]


def validate_partition_fields(partition_fields: tuple[str, ...]) -> None:
    invalid_fields = sorted(set(partition_fields) - SUPPORTED_PARTITION_FIELDS)
    if invalid_fields:
        raise ValueError(
            f"Unsupported partition fields: {', '.join(invalid_fields)}. "
            f"Supported fields: {', '.join(sorted(SUPPORTED_PARTITION_FIELDS))}"
        )


def validate_path_segments(path_segments: tuple[str, ...]) -> None:
    invalid_segments = [
        segment
        for segment in path_segments
        if segment.strip() == "" or "/" in segment
    ]
    if invalid_segments:
        raise ValueError(
            "Path suffix segments must be non-empty strings without '/': "
            + ", ".join(repr(segment) for segment in invalid_segments)
        )


def trim_partition_fields_for_granularity(
    partition_fields: tuple[str, ...],
    slice_granularity: str,
) -> tuple[str, ...]:
    allowed_temporal_fields = TEMPORAL_PARTITION_FIELDS_BY_GRANULARITY[slice_granularity]
    trimmed_fields: list[str] = []
    for field_name in partition_fields:
        if field_name in {"workflow", "adapter"} or field_name in allowed_temporal_fields:
            trimmed_fields.append(field_name)
    return tuple(trimmed_fields)


def build_partition_components(
    partition_fields: tuple[str, ...],
    workflow_name: str,
    source_adapter: str,
    logical_slice: LogicalSlice,
) -> tuple[PartitionComponent, ...]:
    field_values = {
        "workflow": workflow_name,
        "adapter": source_adapter,
        "year": logical_slice.year,
        "quarter": logical_slice.quarter,
        "month": logical_slice.month,
        "day": logical_slice.day,
        "hour": logical_slice.hour,
    }
    return tuple(
        PartitionComponent(key=field_name, value=field_values[field_name])
        for field_name in partition_fields
    )


def join_storage_path(
    base_prefix: str | None,
    partition_components: tuple[PartitionComponent, ...],
    path_suffix: tuple[str, ...] = (),
    object_name: str | None = None,
) -> str:
    parts: list[str] = []
    if base_prefix is not None and base_prefix.strip("/") != "":
        parts.append(base_prefix.strip("/"))
    parts.extend(component.to_path_segment() for component in partition_components)
    parts.extend(segment.strip("/") for segment in path_suffix if segment.strip("/") != "")
    if object_name is not None:
        parts.append(object_name)
    return "/".join(parts)
