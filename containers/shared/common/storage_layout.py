from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from common.slices import GRANULARITY_ORDER, LogicalSlice


PartitionValueBuilder = Callable[[LogicalSlice], str]


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


@dataclass(frozen=True)
class PartitionFieldSpec:
    value_builder: PartitionValueBuilder
    minimum_slice_granularity: str | None = None

    def is_available_for(self, slice_granularity: str) -> bool:
        if self.minimum_slice_granularity is None:
            return True
        return (
            GRANULARITY_ORDER[slice_granularity]
            <= GRANULARITY_ORDER[self.minimum_slice_granularity]
        )


PARTITION_FIELD_SPECS = {
    "year": PartitionFieldSpec(
        value_builder=lambda logical_slice: logical_slice.year,
        minimum_slice_granularity="year",
    ),
    "quarter": PartitionFieldSpec(
        value_builder=lambda logical_slice: logical_slice.quarter,
        minimum_slice_granularity="quarter",
    ),
    "year_quarter": PartitionFieldSpec(
        value_builder=lambda logical_slice: f"{logical_slice.year}{logical_slice.quarter}",
        minimum_slice_granularity="quarter",
    ),
    "month": PartitionFieldSpec(
        value_builder=lambda logical_slice: logical_slice.month,
        minimum_slice_granularity="month",
    ),
    "year_month": PartitionFieldSpec(
        value_builder=lambda logical_slice: f"{logical_slice.year}_{logical_slice.month}",
        minimum_slice_granularity="month",
    ),
    "day": PartitionFieldSpec(
        value_builder=lambda logical_slice: logical_slice.day,
        minimum_slice_granularity="day",
    ),
    "date": PartitionFieldSpec(
        value_builder=lambda logical_slice: (
            f"{logical_slice.year}_{logical_slice.month}_{logical_slice.day}"
        ),
        minimum_slice_granularity="day",
    ),
    "hour": PartitionFieldSpec(
        value_builder=lambda logical_slice: logical_slice.hour,
        minimum_slice_granularity="hour",
    ),
}

SUPPORTED_PARTITION_FIELDS = frozenset(PARTITION_FIELD_SPECS)


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


def validate_partition_fields_for_granularity(
    partition_fields: tuple[str, ...],
    slice_granularity: str,
) -> None:
    unavailable_fields = sorted(
        field_name
        for field_name in partition_fields
        if not PARTITION_FIELD_SPECS[field_name].is_available_for(slice_granularity)
    )
    if unavailable_fields:
        raise ValueError(
            f"Partition fields are too fine-grained for slice granularity "
            f"'{slice_granularity}': {', '.join(unavailable_fields)}"
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
    return tuple(
        field_name
        for field_name in partition_fields
        if PARTITION_FIELD_SPECS[field_name].is_available_for(slice_granularity)
    )


def build_partition_components(
    partition_fields: tuple[str, ...],
    logical_slice: LogicalSlice,
) -> tuple[PartitionComponent, ...]:
    return tuple(
        PartitionComponent(
            key=field_name,
            value=PARTITION_FIELD_SPECS[field_name].value_builder(logical_slice),
        )
        for field_name in partition_fields
    )


def join_storage_path(
    base_prefix: str | None,
    partition_components: tuple[PartitionComponent, ...] = (),
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
