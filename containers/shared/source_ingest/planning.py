from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from common.slices import LogicalSlice, granularity_step
from source_ingest.adapters.base import (
    LiveFetchRequest,
    MultiSliceFetchRequest,
    RequestedSlice,
    SliceFetchRequest,
    SourceFetchRequest,
)
from source_ingest.config import IngestConfig


@dataclass(frozen=True)
class StorageTarget:
    logical_slice: LogicalSlice


@dataclass(frozen=True)
class FetchPlan:
    request: SourceFetchRequest
    storage_targets: tuple[StorageTarget, ...]


def build_storage_targets(config: IngestConfig, now=None) -> tuple[StorageTarget, ...]:
    return tuple(
        StorageTarget(
            logical_slice=LogicalSlice(logical_date=logical_date, run_id=str(uuid4()))
        )
        for logical_date in config.slice_window.iter_slices(now)
    )


def build_requested_slice(
    logical_slice: LogicalSlice,
    partition_granularity: str,
) -> RequestedSlice:
    step = granularity_step(partition_granularity)
    return RequestedSlice(
        logical_date=logical_slice.logical_date,
        slice_start=logical_slice.logical_date,
        slice_end=logical_slice.logical_date + step,
    )


def build_fetch_plan(config: IngestConfig, now=None) -> FetchPlan:
    storage_targets = build_storage_targets(config, now)
    if config.mode == "live_hit" and config.slice_window.logical_date is None:
        return FetchPlan(
            request=LiveFetchRequest(),
            storage_targets=storage_targets,
        )

    if config.mode == "live_hit":
        return FetchPlan(
            request=SliceFetchRequest(
                slice=build_requested_slice(
                    storage_targets[0].logical_slice,
                    config.partition_granularity,
                )
            ),
            storage_targets=storage_targets,
        )

    return FetchPlan(
        request=MultiSliceFetchRequest(
            slices=tuple(
                build_requested_slice(target.logical_slice, config.partition_granularity)
                for target in storage_targets
            )
        ),
        storage_targets=storage_targets,
    )
