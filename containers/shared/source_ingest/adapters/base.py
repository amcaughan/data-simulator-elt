from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Literal, TypeAlias

if TYPE_CHECKING:
    from source_ingest.config import IngestConfig


@dataclass(frozen=True)
class RequestedSlice:
    logical_date: datetime
    slice_start: datetime
    slice_end: datetime
    granularity: str


@dataclass(frozen=True)
class LiveFetchRequest:
    kind: Literal["live"] = field(init=False, default="live")


@dataclass(frozen=True)
class SliceFetchRequest:
    slice: RequestedSlice
    kind: Literal["slice"] = field(init=False, default="slice")


@dataclass(frozen=True)
class MultiSliceFetchRequest:
    slices: tuple[RequestedSlice, ...]
    kind: Literal["multi_slice"] = field(init=False, default="multi_slice")


SourceFetchRequest: TypeAlias = LiveFetchRequest | SliceFetchRequest | MultiSliceFetchRequest
SourceFetchRequestType: TypeAlias = (
    type[LiveFetchRequest] | type[SliceFetchRequest] | type[MultiSliceFetchRequest]
)


@dataclass(frozen=True)
class AdapterCapabilities:
    supported_request_types: tuple[SourceFetchRequestType, ...] = (LiveFetchRequest,)


@dataclass(frozen=True)
class FetchOutput:
    body: bytes
    content_type: str
    metadata: dict[str, str] = field(default_factory=dict)
    logical_date: datetime | None = None


@dataclass(frozen=True)
class FetchResult:
    outputs: tuple[FetchOutput, ...]

    @classmethod
    def single(
        cls,
        body: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
        logical_date: datetime | None = None,
    ) -> "FetchResult":
        return cls(
            outputs=(
                FetchOutput(
                    body=body,
                    content_type=content_type,
                    metadata=metadata or {},
                    logical_date=logical_date,
                ),
            )
        )


class UnsupportedSourceRequestError(ValueError):
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

    def fetch(self, request: SourceFetchRequest) -> FetchResult:
        self.validate_request(request)
        return self._fetch(request)

    def validate_request(self, request: SourceFetchRequest) -> None:
        if isinstance(request, self.capabilities.supported_request_types):
            return

        supported_types = ", ".join(
            request_type.__name__
            for request_type in self.capabilities.supported_request_types
        )
        raise UnsupportedSourceRequestError(
            f"Source adapter '{self.adapter_key()}' does not support request type "
            f"'{type(request).__name__}' for kind '{request.kind}'. "
            f"Supported request types: {supported_types}"
        )

    @abstractmethod
    def _fetch(self, request: SourceFetchRequest) -> FetchResult:
        raise NotImplementedError

    def unsupported_request_error(
        self,
        request: SourceFetchRequest,
        detail: str,
    ) -> UnsupportedSourceRequestError:
        return UnsupportedSourceRequestError(
            f"Source adapter '{self.adapter_key()}' cannot handle "
            f"'{type(request).__name__}' for kind '{request.kind}': {detail}"
        )
