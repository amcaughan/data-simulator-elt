from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


VALID_SELECTOR_MODES = {"current", "pinned", "range", "relative"}
VALID_RELATIVE_DIRECTIONS = {"backward", "forward"}
VALID_TIMESTAMP_ALIGNMENT_POLICIES = {"floor", "ceil", "strict"}
VALID_RANGE_INCLUSION_POLICIES = {"overlap", "contained", "strict"}


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

    def ceil(self, value: datetime) -> datetime:
        normalized_value = _normalize_to_utc(value)
        floored_value = self.floor(normalized_value)
        if floored_value == normalized_value:
            return floored_value
        return self.shift(floored_value, 1)

    def is_aligned(self, value: datetime) -> bool:
        normalized_value = _normalize_to_utc(value)
        return self.floor(normalized_value) == normalized_value

    def resolve_slice_start(
        self,
        value: datetime,
        alignment_policy: str,
    ) -> datetime:
        normalized_value = _normalize_to_utc(value)
        if alignment_policy == "floor":
            return self.floor(normalized_value)
        if alignment_policy == "ceil":
            return self.ceil(normalized_value)
        if alignment_policy == "strict":
            if not self.is_aligned(normalized_value):
                raise ValueError(
                    f"Timestamp {normalized_value.isoformat()} must align to "
                    f"slice granularity '{self.name}' when SLICE_ALIGNMENT_POLICY='strict'"
                )
            return normalized_value
        raise ValueError(
            f"SLICE_ALIGNMENT_POLICY must be one of "
            f"{sorted(VALID_TIMESTAMP_ALIGNMENT_POLICIES)}"
        )

    def build_slice(
        self,
        value: datetime,
        *,
        alignment_policy: str = "floor",
        treat_as_slice_start: bool = False,
        run_id: str | None = None,
    ) -> LogicalSlice:
        normalized_value = _normalize_to_utc(value)
        slice_start = (
            normalized_value
            if treat_as_slice_start
            else self.resolve_slice_start(normalized_value, alignment_policy)
        )
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
    alignment_policy: str = "floor",
    treat_as_slice_start: bool = False,
    run_id: str | None = None,
) -> LogicalSlice:
    return get_granularity(granularity).build_slice(
        value,
        alignment_policy=alignment_policy,
        treat_as_slice_start=treat_as_slice_start,
        run_id=run_id,
    )


@dataclass(frozen=True)
class SliceWindowConfig:
    slice_granularity: str
    selector_mode: str
    pinned_at: str | None
    range_start_at: str | None
    range_end_at: str | None
    relative_count: int | None
    relative_direction: str | None
    relative_anchor_at: str | None = None
    timestamp_alignment_policy: str = "floor"
    range_inclusion_policy: str = "overlap"

    @classmethod
    def current(
        cls,
        *,
        slice_granularity: str,
        timestamp_alignment_policy: str = "floor",
    ) -> "SliceWindowConfig":
        return cls(
            slice_granularity=slice_granularity,
            selector_mode="current",
            pinned_at=None,
            range_start_at=None,
            range_end_at=None,
            relative_count=None,
            relative_direction=None,
            relative_anchor_at=None,
            timestamp_alignment_policy=timestamp_alignment_policy,
        )

    @classmethod
    def pinned(
        cls,
        *,
        slice_granularity: str,
        pinned_at: str,
        timestamp_alignment_policy: str = "floor",
    ) -> "SliceWindowConfig":
        return cls(
            slice_granularity=slice_granularity,
            selector_mode="pinned",
            pinned_at=pinned_at,
            range_start_at=None,
            range_end_at=None,
            relative_count=None,
            relative_direction=None,
            relative_anchor_at=None,
            timestamp_alignment_policy=timestamp_alignment_policy,
        )

    @classmethod
    def range(
        cls,
        *,
        slice_granularity: str,
        range_start_at: str,
        range_end_at: str,
        timestamp_alignment_policy: str = "floor",
        range_inclusion_policy: str = "overlap",
    ) -> "SliceWindowConfig":
        return cls(
            slice_granularity=slice_granularity,
            selector_mode="range",
            pinned_at=None,
            range_start_at=range_start_at,
            range_end_at=range_end_at,
            relative_count=None,
            relative_direction=None,
            relative_anchor_at=None,
            timestamp_alignment_policy=timestamp_alignment_policy,
            range_inclusion_policy=range_inclusion_policy,
        )

    @classmethod
    def relative(
        cls,
        *,
        slice_granularity: str,
        relative_count: int,
        relative_direction: str,
        relative_anchor_at: str | None = None,
        timestamp_alignment_policy: str = "floor",
    ) -> "SliceWindowConfig":
        return cls(
            slice_granularity=slice_granularity,
            selector_mode="relative",
            pinned_at=None,
            range_start_at=None,
            range_end_at=None,
            relative_count=relative_count,
            relative_direction=relative_direction,
            relative_anchor_at=relative_anchor_at,
            timestamp_alignment_policy=timestamp_alignment_policy,
        )

    def validate(self) -> None:
        get_granularity(self.slice_granularity)

        if self.selector_mode not in VALID_SELECTOR_MODES:
            raise ValueError(
                f"SLICE_SELECTOR_MODE must be one of {sorted(VALID_SELECTOR_MODES)}"
            )

        if self.timestamp_alignment_policy not in VALID_TIMESTAMP_ALIGNMENT_POLICIES:
            raise ValueError(
                "SLICE_ALIGNMENT_POLICY must be one of "
                f"{sorted(VALID_TIMESTAMP_ALIGNMENT_POLICIES)}"
            )
        if self.range_inclusion_policy not in VALID_RANGE_INCLUSION_POLICIES:
            raise ValueError(
                "SLICE_RANGE_POLICY must be one of "
                f"{sorted(VALID_RANGE_INCLUSION_POLICIES)}"
            )

        if self.selector_mode == "current":
            return

        if self.selector_mode == "pinned":
            _validate_selector_field_is_present("pinned", "SLICE_PINNED_AT", self.pinned_at)
            return

        if self.selector_mode == "range":
            _validate_selector_field_is_present(
                "range",
                "SLICE_RANGE_START_AT",
                self.range_start_at,
            )
            _validate_selector_field_is_present(
                "range",
                "SLICE_RANGE_END_AT",
                self.range_end_at,
            )
            return

        _validate_selector_field_is_present(
            "relative",
            "SLICE_RELATIVE_COUNT",
            self.relative_count,
        )
        _validate_selector_field_is_present(
            "relative",
            "SLICE_RELATIVE_DIRECTION",
            self.relative_direction,
        )
        if self.relative_count < 1:
            raise ValueError("SLICE_RELATIVE_COUNT must be greater than zero")
        if self.relative_direction not in VALID_RELATIVE_DIRECTIONS:
            raise ValueError(
                "SLICE_RELATIVE_DIRECTION must be one of "
                f"{sorted(VALID_RELATIVE_DIRECTIONS)}"
            )

    def iter_slices(self, now: datetime | None = None) -> list[LogicalSlice]:
        granularity = get_granularity(self.slice_granularity)
        current_time = now or datetime.now(UTC)

        if self.selector_mode == "current":
            return [
                granularity.build_slice(
                    current_time,
                    alignment_policy=self.timestamp_alignment_policy,
                )
            ]

        if self.selector_mode == "pinned":
            return [
                granularity.build_slice(
                    parse_iso_datetime(self.pinned_at),
                    alignment_policy=self.timestamp_alignment_policy,
                )
            ]

        if self.selector_mode == "relative":
            anchor_time = (
                parse_iso_datetime(self.relative_anchor_at)
                if self.relative_anchor_at is not None
                else current_time
            )
            anchor_slice = granularity.build_slice(
                anchor_time,
                alignment_policy=self.timestamp_alignment_policy,
            )
            if self.relative_direction == "backward":
                first_slice_start = granularity.shift(
                    anchor_slice.slice_start,
                    -(self.relative_count - 1),
                )
                end_slice = anchor_slice
            else:
                first_slice_start = anchor_slice.slice_start
                end_slice = granularity.build_slice(
                    granularity.shift(anchor_slice.slice_start, self.relative_count - 1),
                    treat_as_slice_start=True,
                )
            start_slice = granularity.build_slice(first_slice_start, treat_as_slice_start=True)
        else:
            range_start = parse_iso_datetime(self.range_start_at)
            range_end = parse_iso_datetime(self.range_end_at)

            if range_start > range_end:
                raise ValueError(
                    "SLICE_RANGE_START_AT must be less than or equal to SLICE_RANGE_END_AT"
                )

            if self.range_inclusion_policy == "strict":
                if not granularity.is_aligned(range_start) or not granularity.is_aligned(range_end):
                    raise ValueError(
                        "SLICE_RANGE_START_AT and SLICE_RANGE_END_AT must align to slice boundaries when "
                        "SLICE_RANGE_POLICY='strict'"
                    )
                start_slice = granularity.build_slice(range_start, treat_as_slice_start=True)
                end_slice = granularity.build_slice(range_end, treat_as_slice_start=True)
            else:
                start_slice = granularity.build_slice(
                    range_start,
                    alignment_policy="floor",
                )
                end_slice = granularity.build_slice(
                    range_end,
                    alignment_policy="floor",
                )

        if start_slice.slice_start > end_slice.slice_start:
            raise ValueError(
                "SLICE_RANGE_START_AT must be less than or equal to SLICE_RANGE_END_AT"
            )

        slices: list[LogicalSlice] = []
        cursor = start_slice.slice_start
        while cursor <= end_slice.slice_start:
            logical_slice = granularity.build_slice(cursor, treat_as_slice_start=True)
            if self.selector_mode == "range":
                if self.range_inclusion_policy == "overlap":
                    if not _slice_overlaps_range(logical_slice, range_start, range_end):
                        cursor = granularity.shift(cursor, 1)
                        continue
                elif self.range_inclusion_policy == "contained":
                    if not _slice_is_contained_in_range(logical_slice, range_start, range_end):
                        cursor = granularity.shift(cursor, 1)
                        continue
            slices.append(logical_slice)
            cursor = granularity.shift(cursor, 1)

        if not slices:
            raise ValueError(
                "The configured range and boundary policy do not include any logical slices"
            )
        return slices

    @property
    def request_kind(self) -> str:
        if self.selector_mode == "current":
            return "live"
        if self.selector_mode == "pinned":
            return "slice"
        return "multi_slice"


def _validate_selector_field_is_present(
    selector_mode: str,
    field_name: str,
    value: object | None,
) -> None:
    if value is None:
        raise ValueError(
            f"{field_name} is required when SLICE_SELECTOR_MODE='{selector_mode}'"
        )


def _slice_overlaps_range(
    logical_slice: LogicalSlice,
    range_start: datetime,
    range_end: datetime,
) -> bool:
    return logical_slice.slice_end > range_start and logical_slice.slice_start <= range_end


def _slice_is_contained_in_range(
    logical_slice: LogicalSlice,
    range_start: datetime,
    range_end: datetime,
) -> bool:
    return logical_slice.slice_start >= range_start and logical_slice.slice_end <= range_end
