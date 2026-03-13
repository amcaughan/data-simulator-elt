from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


VALID_GRANULARITIES = {"day", "hour"}
VALID_MODES = {"live_hit", "backfill"}
GRANULARITY_ORDER = {"day": 0, "hour": 1}


def parse_iso_datetime(raw: str) -> datetime:
    if "T" not in raw:
        return datetime.fromisoformat(raw).replace(tzinfo=UTC)

    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def truncate_datetime(value: datetime, granularity: str) -> datetime:
    truncated = value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    if granularity == "day":
        return truncated.replace(hour=0)
    return truncated


def granularity_step(granularity: str) -> timedelta:
    if granularity == "day":
        return timedelta(days=1)
    if granularity == "hour":
        return timedelta(hours=1)
    raise ValueError(f"Unsupported granularity: {granularity}")


@dataclass(frozen=True)
class LogicalSlice:
    logical_date: datetime
    run_id: str

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
class SliceWindowConfig:
    partition_granularity: str
    mode: str
    logical_date: str | None
    start_at: str | None
    end_at: str | None
    backfill_days: int | None

    def validate(self) -> None:
        if self.partition_granularity not in VALID_GRANULARITIES:
            raise ValueError(
                f"PARTITION_GRANULARITY must be one of {sorted(VALID_GRANULARITIES)}"
            )

        if self.mode not in VALID_MODES:
            raise ValueError(f"MODE must be one of {sorted(VALID_MODES)}")

        if self.mode == "live_hit" and any(
            value is not None for value in (self.start_at, self.end_at, self.backfill_days)
        ):
            raise ValueError(
                "Live-hit mode cannot use START_AT, END_AT, or BACKFILL_DAYS. "
                "Use LOGICAL_DATE to pin a one-off run to a specific logical slice."
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

    def iter_slices(self, now: datetime | None = None) -> list[datetime]:
        current_time = now or datetime.now(UTC)
        step = granularity_step(self.partition_granularity)

        if self.mode == "live_hit":
            logical_date = truncate_datetime(
                parse_iso_datetime(self.logical_date) if self.logical_date else current_time,
                self.partition_granularity,
            )
            return [logical_date]

        if self.backfill_days is not None:
            end_at = truncate_datetime(current_time, self.partition_granularity)
            start_at = end_at - (step * (self.backfill_days - 1))
        else:
            start_at = truncate_datetime(
                parse_iso_datetime(self.start_at), self.partition_granularity
            )
            end_at = truncate_datetime(
                parse_iso_datetime(self.end_at), self.partition_granularity
            )

        if start_at > end_at:
            raise ValueError("START_AT must be less than or equal to END_AT")

        slices: list[datetime] = []
        cursor = start_at
        while cursor <= end_at:
            slices.append(cursor)
            cursor += step
        return slices
