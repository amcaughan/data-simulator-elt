from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Literal, TypeAlias

from common.slices import LogicalSlice

if TYPE_CHECKING:
    from source_ingest.config import IngestConfig


@dataclass(frozen=True)
class LivePullRequest:
    logical_slice: LogicalSlice
    mode: Literal["live_hit"] = field(init=False, default="live_hit")


@dataclass(frozen=True)
class HistoricalSlicePullRequest:
    logical_slice: LogicalSlice
    slice_start: datetime
    slice_end: datetime
    mode: Literal["backfill"] = field(init=False, default="backfill")


SourcePullRequest: TypeAlias = LivePullRequest | HistoricalSlicePullRequest
SourcePullRequestType: TypeAlias = (
    type[LivePullRequest] | type[HistoricalSlicePullRequest]
)


@dataclass(frozen=True)
class AdapterCapabilities:
    supported_pull_request_types: tuple[SourcePullRequestType, ...] = (LivePullRequest,)


@dataclass(frozen=True)
class FetchResult:
    body: bytes
    content_type: str
    metadata: dict[str, str] = field(default_factory=dict)


class UnsupportedSourcePullRequestError(ValueError):
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

    def fetch(self, pull_request: SourcePullRequest) -> FetchResult:
        self.validate_pull_request(pull_request)
        return self._fetch(pull_request)

    def validate_pull_request(self, pull_request: SourcePullRequest) -> None:
        if isinstance(pull_request, self.capabilities.supported_pull_request_types):
            return

        supported_types = ", ".join(
            pull_type.__name__
            for pull_type in self.capabilities.supported_pull_request_types
        )
        raise UnsupportedSourcePullRequestError(
            f"Source adapter '{self.adapter_key()}' does not support pull request type "
            f"'{type(pull_request).__name__}' for mode '{pull_request.mode}'. "
            f"Supported pull request types: {supported_types}"
        )

    @abstractmethod
    def _fetch(self, pull_request: SourcePullRequest) -> FetchResult:
        raise NotImplementedError

    def unsupported_pull_request_error(
        self,
        pull_request: SourcePullRequest,
        detail: str,
    ) -> UnsupportedSourcePullRequestError:
        return UnsupportedSourcePullRequestError(
            f"Source adapter '{self.adapter_key()}' cannot handle "
            f"'{type(pull_request).__name__}' for mode '{pull_request.mode}': {detail}"
        )
