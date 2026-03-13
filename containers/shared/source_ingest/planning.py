from __future__ import annotations

from uuid import uuid4

from common.slices import LogicalSlice, granularity_step
from source_ingest.adapters.base import (
    HistoricalSlicePullRequest,
    LivePullRequest,
    SourcePullRequest,
)
from source_ingest.config import IngestConfig


def build_logical_slices(config: IngestConfig, now=None) -> list[LogicalSlice]:
    return [
        LogicalSlice(logical_date=logical_date, run_id=str(uuid4()))
        for logical_date in config.slice_window.iter_slices(now)
    ]


def build_pull_requests(config: IngestConfig, now=None) -> list[SourcePullRequest]:
    logical_slices = build_logical_slices(config, now)
    if config.mode == "live_hit":
        return [LivePullRequest(logical_slice=logical_slice) for logical_slice in logical_slices]

    step = granularity_step(config.partition_granularity)
    return [
        HistoricalSlicePullRequest(
            logical_slice=logical_slice,
            slice_start=logical_slice.logical_date,
            slice_end=logical_slice.logical_date + step,
        )
        for logical_slice in logical_slices
    ]
