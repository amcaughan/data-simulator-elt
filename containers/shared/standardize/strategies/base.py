from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from common.slices import LogicalSlice

if TYPE_CHECKING:
    from standardize.config import StandardizeConfig


@dataclass(frozen=True)
class StandardizeInputObject:
    key: str
    payload: bytes
    metadata: dict[str, str]


@dataclass(frozen=True)
class StandardizeOutput:
    rows: list[dict[str, Any]]
    metadata: dict[str, str] = field(default_factory=dict)
    suggested_object_name: str | None = None


@dataclass(frozen=True)
class StandardizeResult:
    outputs: tuple[StandardizeOutput, ...]

    @classmethod
    def single(
        cls,
        rows: list[dict[str, Any]],
        metadata: dict[str, str] | None = None,
        suggested_object_name: str | None = None,
    ) -> "StandardizeResult":
        return cls(
            outputs=(
                StandardizeOutput(
                    rows=rows,
                    metadata=metadata or {},
                    suggested_object_name=suggested_object_name,
                ),
            )
        )


class StandardizeStrategy(ABC):
    @classmethod
    @abstractmethod
    def strategy_key(cls) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_standardize_config(
        cls,
        config: "StandardizeConfig",
    ) -> "StandardizeStrategy":
        raise NotImplementedError

    @abstractmethod
    def process_slice(
        self,
        output_slice: LogicalSlice,
        input_objects: list[StandardizeInputObject],
    ) -> StandardizeResult:
        raise NotImplementedError

    @abstractmethod
    def process_manual(
        self,
        input_objects: list[StandardizeInputObject],
    ) -> StandardizeResult:
        raise NotImplementedError
