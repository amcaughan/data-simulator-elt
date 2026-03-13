from __future__ import annotations

from typing import Protocol

from common.slices import LogicalSlice


class LandingParser(Protocol):
    def parse_landing_object(
        self,
        logical_slice: LogicalSlice,
        key: str,
        payload: bytes,
        metadata: dict[str, str],
    ) -> list[dict]:
        ...
