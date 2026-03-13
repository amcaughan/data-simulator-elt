from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from common.slices import LogicalSlice
from common.storage_layout import PartitionComponent, build_partition_components
from source_ingest.adapters.base import (
    LiveFetchRequest,
    ManualFetchRequest,
    MultiSliceFetchRequest,
    RequestedSlice,
    SliceFetchRequest,
    SourceFetchRequest,
)
from source_ingest.config import IngestConfig


@dataclass(frozen=True)
class StorageTarget:
    logical_slice: LogicalSlice | None
    object_stem: str
    partition_components: tuple[PartitionComponent, ...] = ()
    storage_prefix: str | None = None
    object_name_override: str | None = None


@dataclass(frozen=True)
class FetchPlan:
    request: SourceFetchRequest
    storage_targets: tuple[StorageTarget, ...]


def build_storage_targets(config: IngestConfig, now=None) -> tuple[StorageTarget, ...]:
    if config.is_manual:
        return (
            StorageTarget(
                logical_slice=None,
                object_stem=f"run_id={uuid4()}",
                storage_prefix=config.manual.storage_prefix or "",
                object_name_override=config.manual.object_name,
            ),
        )
    return tuple(
        build_storage_target(config, logical_slice)
        for logical_slice in config.slice_window.iter_slices(now)
    )


def build_storage_target(config: IngestConfig, logical_slice: LogicalSlice) -> StorageTarget:
    slice_with_run_id = logical_slice.with_run_id(str(uuid4()))
    return StorageTarget(
        logical_slice=slice_with_run_id,
        partition_components=build_partition_components(
            partition_fields=config.landing_layout.partition_fields,
            logical_slice=slice_with_run_id,
        ),
        object_stem=f"run_id={slice_with_run_id.run_id}",
    )


def build_requested_slice(logical_slice: LogicalSlice) -> RequestedSlice:
    return RequestedSlice(
        logical_date=logical_slice.logical_date,
        slice_start=logical_slice.slice_start,
        slice_end=logical_slice.slice_end,
        granularity=logical_slice.granularity,
    )


def build_fetch_plan(config: IngestConfig, now=None) -> FetchPlan:
    storage_targets = build_storage_targets(config, now)
    if config.is_manual:
        return FetchPlan(
            request=ManualFetchRequest(payload=dict(config.manual.request_payload)),
            storage_targets=storage_targets,
        )
    if config.slice_selector_mode == "current":
        return FetchPlan(
            request=LiveFetchRequest(),
            storage_targets=storage_targets,
        )

    if config.slice_selector_mode == "pinned":
        return FetchPlan(
            request=SliceFetchRequest(
                slice=build_requested_slice(storage_targets[0].logical_slice)
            ),
            storage_targets=storage_targets,
        )

    return FetchPlan(
        request=MultiSliceFetchRequest(
            slices=tuple(
                build_requested_slice(target.logical_slice)
                for target in storage_targets
            )
        ),
        storage_targets=storage_targets,
    )
