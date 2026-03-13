from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "containers" / "shared"))

from common.slices import SliceWindowConfig, build_logical_slice
from common.storage_layout import default_partition_fields, trim_partition_fields_for_granularity


class CommonSliceTests(unittest.TestCase):
    def test_month_slice_floors_to_month_boundary(self):
        logical_slice = build_logical_slice(
            datetime(2026, 3, 18, 14, 25, tzinfo=UTC),
            "month",
        )

        self.assertEqual(logical_slice.slice_start, datetime(2026, 3, 1, 0, 0, tzinfo=UTC))
        self.assertEqual(logical_slice.slice_end, datetime(2026, 4, 1, 0, 0, tzinfo=UTC))

    def test_quarter_slice_floors_to_quarter_boundary(self):
        logical_slice = build_logical_slice(
            datetime(2026, 5, 11, 9, 30, tzinfo=UTC),
            "quarter",
        )

        self.assertEqual(logical_slice.slice_start, datetime(2026, 4, 1, 0, 0, tzinfo=UTC))
        self.assertEqual(logical_slice.slice_end, datetime(2026, 7, 1, 0, 0, tzinfo=UTC))
        self.assertEqual(logical_slice.quarter, "Q2")

    def test_backfill_count_iterates_quarter_slices(self):
        window = SliceWindowConfig(
            slice_granularity="quarter",
            mode="backfill",
            logical_date=None,
            start_at=None,
            end_at=None,
            backfill_count=3,
        )

        slices = window.iter_slices(now=datetime(2026, 5, 11, 9, 30, tzinfo=UTC))

        self.assertEqual(
            [logical_slice.slice_start.isoformat() for logical_slice in slices],
            [
                "2025-10-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-04-01T00:00:00+00:00",
            ],
        )

    def test_default_partition_fields_match_slice_granularity(self):
        self.assertEqual(
            default_partition_fields("quarter"),
            ("year", "quarter"),
        )
        self.assertEqual(
            default_partition_fields("year"),
            ("year",),
        )

    def test_trim_partition_fields_for_month_removes_finer_components(self):
        trimmed_fields = trim_partition_fields_for_granularity(
            ("workflow", "adapter", "year", "quarter", "month", "day", "hour"),
            "month",
        )

        self.assertEqual(
            trimmed_fields,
            ("workflow", "adapter", "year", "month"),
        )


if __name__ == "__main__":
    unittest.main()
