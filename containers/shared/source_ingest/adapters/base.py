from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from common.slices import LogicalSlice

if TYPE_CHECKING:
    from source_ingest.config import IngestConfig


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


class UnsupportedSourceModeError(ValueError):
    pass


class SourceAdapter(ABC):
    capabilities: ClassVar[AdapterCapabilities] = AdapterCapabilities()

    @classmethod
    @abstractmethod
    def adapter_key(cls) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_ingest_config(cls, config: "IngestConfig") -> "SourceAdapter":
        raise NotImplementedError

    def ensure_supported(self, mode: str) -> None:
        if mode == "backfill" and not self.capabilities.supports_backfill:
            raise UnsupportedSourceModeError(
                f"Source adapter '{self.adapter_key()}' does not support MODE=backfill. "
                "Use MODE=live_hit for an immediate pull or choose an adapter that can "
                "interpret logical date ranges."
            )

    @abstractmethod
    def fetch(self, logical_slice: LogicalSlice) -> FetchResult:
        raise NotImplementedError
