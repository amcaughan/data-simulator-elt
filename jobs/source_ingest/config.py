from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from typing import Any
from uuid import uuid4


VALID_GRANULARITIES = {"day", "hour"}
VALID_MODES = {"single_run", "backfill"}
VALID_SEED_STRATEGIES = {"derived", "fixed", "none"}


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _optional_int(name: str) -> int | None:
    value = os.environ.get(name)
    if value in {None, ""}:
        return None
    return int(value)


def _parse_iso_datetime(raw: str) -> datetime:
    if "T" not in raw:
        return datetime.fromisoformat(raw).replace(tzinfo=UTC)

    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _truncate_datetime(value: datetime, granularity: str) -> datetime:
    truncated = value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    if granularity == "day":
        return truncated.replace(hour=0)
    return truncated


def _json_env(name: str, default: str = "{}") -> dict[str, Any]:
    raw_value = os.environ.get(name, default)
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be valid JSON") from exc

    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must decode to a JSON object")
    return parsed


@dataclass(frozen=True)
class LogicalSlice:
    logical_date: datetime
    run_id: str
    seed: int | None

    @property
    def year(self) -> str:
        return f"{self.logical_date.year:04d}"

    @property
    def month(self) -> str:
        return f"{self.logical_date.month:02d}"

    @property
    def day(self) -> str:
        return f"{self.logical_date.day:02d}"

    @property
    def hour(self) -> str:
        return f"{self.logical_date.hour:02d}"


@dataclass(frozen=True)
class IngestConfig:
    workflow_name: str
    source_adapter: str
    simulator_api_url: str
    preset_id: str
    row_count: int
    landing_bucket_name: str
    aws_region: str
    partition_granularity: str
    mode: str
    logical_date: str | None
    start_at: str | None
    end_at: str | None
    backfill_days: int | None
    seed_strategy: str
    fixed_seed: int | None
    request_overrides: dict[str, Any]

    @classmethod
    def from_env(cls) -> "IngestConfig":
        config = cls(
            workflow_name=_require_env("WORKFLOW_NAME"),
            source_adapter=os.environ.get("SOURCE_ADAPTER", "simulator_api"),
            simulator_api_url=_require_env("SIMULATOR_API_URL"),
            preset_id=_require_env("PRESET_ID"),
            row_count=int(_require_env("ROW_COUNT")),
            landing_bucket_name=_require_env("LANDING_BUCKET_NAME"),
            aws_region=_require_env("AWS_REGION"),
            partition_granularity=os.environ.get("PARTITION_GRANULARITY", "day"),
            mode=os.environ.get("MODE", "single_run"),
            logical_date=os.environ.get("LOGICAL_DATE"),
            start_at=os.environ.get("START_AT"),
            end_at=os.environ.get("END_AT"),
            backfill_days=_optional_int("BACKFILL_DAYS"),
            seed_strategy=os.environ.get("SEED_STRATEGY", "derived"),
            fixed_seed=_optional_int("FIXED_SEED"),
            request_overrides=_json_env("REQUEST_OVERRIDES_JSON"),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.partition_granularity not in VALID_GRANULARITIES:
            raise ValueError(
                f"PARTITION_GRANULARITY must be one of {sorted(VALID_GRANULARITIES)}"
            )

        if self.mode not in VALID_MODES:
            raise ValueError(f"MODE must be one of {sorted(VALID_MODES)}")

        if self.seed_strategy not in VALID_SEED_STRATEGIES:
            raise ValueError(
                f"SEED_STRATEGY must be one of {sorted(VALID_SEED_STRATEGIES)}"
            )

        if self.mode == "single_run" and any(
            value is not None for value in (self.start_at, self.end_at, self.backfill_days)
        ):
            raise ValueError(
                "Single-run mode cannot use START_AT, END_AT, or BACKFILL_DAYS"
            )

        if self.mode == "backfill":
            has_range = self.start_at is not None or self.end_at is not None
            has_days = self.backfill_days is not None
            if not has_range and not has_days:
                raise ValueError(
                    "Backfill mode requires BACKFILL_DAYS or both START_AT and END_AT"
                )
            if has_range and (self.start_at is None or self.end_at is None):
                raise ValueError("START_AT and END_AT must be provided together")
            if self.backfill_days is not None and self.backfill_days < 1:
                raise ValueError("BACKFILL_DAYS must be greater than zero")

        if self.seed_strategy == "fixed" and self.fixed_seed is None:
            raise ValueError("FIXED_SEED is required when SEED_STRATEGY=fixed")

    def iter_slices(self, now: datetime | None = None) -> list[LogicalSlice]:
        current_time = now or datetime.now(UTC)
        step = timedelta(days=1) if self.partition_granularity == "day" else timedelta(hours=1)

        if self.mode == "single_run":
            logical_date = _truncate_datetime(
                _parse_iso_datetime(self.logical_date) if self.logical_date else current_time,
                self.partition_granularity,
            )
            return [self._build_slice(logical_date)]

        if self.backfill_days is not None:
            end_at = _truncate_datetime(current_time, self.partition_granularity)
            start_at = end_at - (step * (self.backfill_days - 1))
        else:
            start_at = _truncate_datetime(
                _parse_iso_datetime(self.start_at), self.partition_granularity
            )
            end_at = _truncate_datetime(
                _parse_iso_datetime(self.end_at), self.partition_granularity
            )

        if start_at > end_at:
            raise ValueError("START_AT must be less than or equal to END_AT")

        slices: list[LogicalSlice] = []
        cursor = start_at
        while cursor <= end_at:
            slices.append(self._build_slice(cursor))
            cursor += step
        return slices

    def _build_slice(self, logical_date: datetime) -> LogicalSlice:
        return LogicalSlice(
            logical_date=logical_date,
            run_id=str(uuid4()),
            seed=self._seed_for(logical_date),
        )

    def _seed_for(self, logical_date: datetime) -> int | None:
        if self.seed_strategy == "none":
            return None
        if self.seed_strategy == "fixed":
            return self.fixed_seed

        namespace = "|".join(
            [
                self.workflow_name,
                self.source_adapter,
                self.preset_id,
                logical_date.isoformat(),
            ]
        )
        digest = hashlib.sha256(namespace.encode("utf-8")).hexdigest()
        return int(digest[:16], 16) % (2**31)
