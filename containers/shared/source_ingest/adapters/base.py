from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from common.slices import LogicalSlice


@dataclass(frozen=True)
class AdapterCapabilities:
    supports_backfill: bool = False


@dataclass(frozen=True)
class FetchResult:
    body: bytes
    content_type: str
    row_count: int | None
    route: str | None = None
    source_metadata: dict[str, str] = field(default_factory=dict)


class SourceAdapter(Protocol):
    capabilities: AdapterCapabilities

    def fetch(self, logical_slice: LogicalSlice) -> FetchResult:
        ...
