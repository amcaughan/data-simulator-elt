from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


VALID_MODES = {"live_hit", "backfill"}


def parse_iso_datetime(raw: str) -> datetime:
    if "T" not in raw:
        return datetime.fromisoformat(raw).replace(tzinfo=UTC)

    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _add_months(value: datetime, months: int) -> datetime:
    total_month_index = (value.year * 12 + (value.month - 1)) + months
    year = total_month_index // 12
    month = total_month_index % 12 + 1
    return value.replace(year=year, month=month)


@dataclass(frozen=True)
class LogicalSlice:
    slice_start: datetime
    slice_end: datetime
    granularity: str
    run_id: str | None = None

    @property
    def logical_date(self) -> datetime:
        return self.slice_start

    @property
    def year(self) -> str:
        return f"{self.slice_start.year:04d}"

    @property
    def quarter(self) -> str:
        return f"Q{((self.slice_start.month - 1) // 3) + 1}"

    @property
    def month(self) -> str:
        return f"{self.slice_start.month:02d}"

    @property
    def day(self) -> str:
        return f"{self.slice_start.day:02d}"

    @property
    def hour(self) -> str:
        return f"{self.slice_start.hour:02d}"

    def contains(self, value: datetime) -> bool:
        normalized_value = _normalize_to_utc(value)
        return self.slice_start <= normalized_value < self.slice_end

    def with_run_id(self, run_id: str) -> "LogicalSlice":
        return LogicalSlice(
            slice_start=self.slice_start,
            slice_end=self.slice_end,
            granularity=self.granularity,
            run_id=run_id,
        )


class SliceGranularity(ABC):
    name: str
    order: int

    @abstractmethod
    def floor(self, value: datetime) -> datetime:
        raise NotImplementedError

    @abstractmethod
    def shift(self, slice_start: datetime, count: int) -> datetime:
        raise NotImplementedError

    def build_slice(
        self,
        value: datetime,
        *,
        treat_as_slice_start: bool = False,
        run_id: str | None = None,
    ) -> LogicalSlice:
        normalized_value = _normalize_to_utc(value)
        slice_start = normalized_value if treat_as_slice_start else self.floor(normalized_value)
        return LogicalSlice(
            slice_start=slice_start,
            slice_end=self.shift(slice_start, 1),
            granularity=self.name,
            run_id=run_id,
        )


class HourGranularity(SliceGranularity):
    name = "hour"
    order = 0

    def floor(self, value: datetime) -> datetime:
        normalized = _normalize_to_utc(value)
        return normalized.replace(minute=0, second=0, microsecond=0)

    def shift(self, slice_start: datetime, count: int) -> datetime:
        return slice_start + timedelta(hours=count)


class DayGranularity(SliceGranularity):
    name = "day"
    order = 1

    def floor(self, value: datetime) -> datetime:
        normalized = _normalize_to_utc(value)
        return normalized.replace(hour=0, minute=0, second=0, microsecond=0)

    def shift(self, slice_start: datetime, count: int) -> datetime:
        return slice_start + timedelta(days=count)


class MonthGranularity(SliceGranularity):
    name = "month"
    order = 2

    def floor(self, value: datetime) -> datetime:
        normalized = _normalize_to_utc(value)
        return normalized.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def shift(self, slice_start: datetime, count: int) -> datetime:
        return _add_months(slice_start, count)


class QuarterGranularity(SliceGranularity):
    name = "quarter"
    order = 3

    def floor(self, value: datetime) -> datetime:
        normalized = _normalize_to_utc(value)
        quarter_start_month = ((normalized.month - 1) // 3) * 3 + 1
        return normalized.replace(
            month=quarter_start_month,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    def shift(self, slice_start: datetime, count: int) -> datetime:
        return _add_months(slice_start, count * 3)


class YearGranularity(SliceGranularity):
    name = "year"
    order = 4

    def floor(self, value: datetime) -> datetime:
        normalized = _normalize_to_utc(value)
        return normalized.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    def shift(self, slice_start: datetime, count: int) -> datetime:
        return slice_start.replace(year=slice_start.year + count)


GRANULARITIES = {
    granularity.name: granularity
    for granularity in (
        HourGranularity(),
        DayGranularity(),
        MonthGranularity(),
        QuarterGranularity(),
        YearGranularity(),
    )
}
VALID_GRANULARITIES = frozenset(GRANULARITIES)
GRANULARITY_ORDER = {
    name: granularity.order for name, granularity in GRANULARITIES.items()
}


def get_granularity(name: str) -> SliceGranularity:
    try:
        return GRANULARITIES[name]
    except KeyError as exc:
        raise ValueError(
            f"SLICE_GRANULARITY must be one of {sorted(VALID_GRANULARITIES)}"
        ) from exc


def build_logical_slice(
    value: datetime,
    granularity: str,
    *,
    treat_as_slice_start: bool = False,
    run_id: str | None = None,
) -> LogicalSlice:
    return get_granularity(granularity).build_slice(
        value,
        treat_as_slice_start=treat_as_slice_start,
        run_id=run_id,
    )


@dataclass(frozen=True)
class SliceWindowConfig:
    slice_granularity: str
    mode: str
    logical_date: str | None
    start_at: str | None
    end_at: str | None
    backfill_count: int | None

    def validate(self) -> None:
        get_granularity(self.slice_granularity)

        if self.mode not in VALID_MODES:
            raise ValueError(f"MODE must be one of {sorted(VALID_MODES)}")

        if self.mode == "live_hit" and any(
            value is not None for value in (self.start_at, self.end_at, self.backfill_count)
        ):
            raise ValueError(
                "Live-hit mode cannot use START_AT, END_AT, or BACKFILL_COUNT. "
                "Use LOGICAL_DATE to pin a one-off run to a specific logical slice."
            )

        if self.mode == "backfill":
            has_range = self.start_at is not None or self.end_at is not None
            has_count = self.backfill_count is not None
            if not has_range and not has_count:
                raise ValueError(
                    "Backfill mode requires BACKFILL_COUNT or both START_AT and END_AT"
                )
            if has_range and (self.start_at is None or self.end_at is None):
                raise ValueError("START_AT and END_AT must be provided together")
            if self.backfill_count is not None and self.backfill_count < 1:
                raise ValueError("BACKFILL_COUNT must be greater than zero")

    def iter_slices(self, now: datetime | None = None) -> list[LogicalSlice]:
        granularity = get_granularity(self.slice_granularity)
        current_time = now or datetime.now(UTC)

        if self.mode == "live_hit":
            anchor = parse_iso_datetime(self.logical_date) if self.logical_date else current_time
            return [granularity.build_slice(anchor)]

        if self.backfill_count is not None:
            end_slice = granularity.build_slice(current_time)
            first_slice_start = granularity.shift(
                end_slice.slice_start,
                -(self.backfill_count - 1),
            )
            start_slice = granularity.build_slice(first_slice_start, treat_as_slice_start=True)
        else:
            start_slice = granularity.build_slice(parse_iso_datetime(self.start_at))
            end_slice = granularity.build_slice(parse_iso_datetime(self.end_at))

        if start_slice.slice_start > end_slice.slice_start:
            raise ValueError("START_AT must be less than or equal to END_AT")

        slices: list[LogicalSlice] = []
        cursor = start_slice.slice_start
        while cursor <= end_slice.slice_start:
            slices.append(granularity.build_slice(cursor, treat_as_slice_start=True))
            cursor = granularity.shift(cursor, 1)
        return slices
