from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "containers" / "shared"))

from common.slices import SliceWindowConfig, build_logical_slice
from common.storage_layout import (
    build_partition_components,
    default_partition_fields,
    trim_partition_fields_for_granularity,
    validate_partition_fields_for_granularity,
)


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

    def test_build_logical_slice_can_ceil_to_next_boundary(self):
        logical_slice = build_logical_slice(
            datetime(2026, 3, 18, 14, 25, tzinfo=UTC),
            "day",
            alignment_policy="ceil",
        )

        self.assertEqual(logical_slice.slice_start, datetime(2026, 3, 19, 0, 0, tzinfo=UTC))
        self.assertEqual(logical_slice.slice_end, datetime(2026, 3, 20, 0, 0, tzinfo=UTC))

    def test_relative_backward_selector_iterates_quarter_slices(self):
        window = SliceWindowConfig.relative(
            slice_granularity="quarter",
            relative_count=3,
            relative_direction="backward",
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

    def test_relative_forward_selector_iterates_from_anchor_forward(self):
        window = SliceWindowConfig.relative(
            slice_granularity="month",
            relative_count=3,
            relative_direction="forward",
            relative_anchor_at="2026-03-18T12:00:00Z",
        )

        slices = window.iter_slices()

        self.assertEqual(
            [logical_slice.slice_start.isoformat() for logical_slice in slices],
            [
                "2026-03-01T00:00:00+00:00",
                "2026-04-01T00:00:00+00:00",
                "2026-05-01T00:00:00+00:00",
            ],
        )

    def test_range_overlap_policy_includes_partial_edge_slices(self):
        window = SliceWindowConfig.range(
            slice_granularity="day",
            range_start_at="2026-03-13T12:00:00Z",
            range_end_at="2026-03-16T12:00:00Z",
            range_inclusion_policy="overlap",
        )

        slices = window.iter_slices()

        self.assertEqual(
            [logical_slice.slice_start.isoformat() for logical_slice in slices],
            [
                "2026-03-13T00:00:00+00:00",
                "2026-03-14T00:00:00+00:00",
                "2026-03-15T00:00:00+00:00",
                "2026-03-16T00:00:00+00:00",
            ],
        )

    def test_range_contained_policy_excludes_partial_edge_slices(self):
        window = SliceWindowConfig.range(
            slice_granularity="day",
            range_start_at="2026-03-13T12:00:00Z",
            range_end_at="2026-03-16T12:00:00Z",
            range_inclusion_policy="contained",
        )

        slices = window.iter_slices()

        self.assertEqual(
            [logical_slice.slice_start.isoformat() for logical_slice in slices],
            [
                "2026-03-14T00:00:00+00:00",
                "2026-03-15T00:00:00+00:00",
            ],
        )

    def test_range_strict_policy_rejects_unaligned_bounds(self):
        window = SliceWindowConfig.range(
            slice_granularity="day",
            range_start_at="2026-03-13T12:00:00Z",
            range_end_at="2026-03-16T12:00:00Z",
            range_inclusion_policy="strict",
        )

        with self.assertRaisesRegex(ValueError, "SLICE_RANGE_POLICY='strict'"):
            window.iter_slices()

    def test_selector_modes_ignore_irrelevant_selector_fields(self):
        window = SliceWindowConfig(
            slice_granularity="day",
            selector_mode="current",
            pinned_at="2026-03-13",
            range_start_at=None,
            range_end_at=None,
            relative_count=None,
            relative_direction=None,
        )

        window.validate()

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
            (
                "year",
                "quarter",
                "year_quarter",
                "month",
                "year_month",
                "day",
                "date",
                "hour",
            ),
            "month",
        )

        self.assertEqual(
            trimmed_fields,
            ("year", "quarter", "year_quarter", "month", "year_month"),
        )

    def test_build_partition_components_supports_derived_fields(self):
        logical_slice = build_logical_slice(
            datetime(2025, 3, 16, 12, 0, tzinfo=UTC),
            "day",
        )

        components = build_partition_components(
            partition_fields=("year_quarter", "date", "year_month"),
            logical_slice=logical_slice,
        )

        self.assertEqual(
            [(component.key, component.value) for component in components],
            [
                ("year_quarter", "2025Q1"),
                ("date", "2025_03_16"),
                ("year_month", "2025_03"),
            ],
        )

    def test_validate_partition_fields_for_granularity_rejects_finer_fields(self):
        with self.assertRaisesRegex(ValueError, "too fine-grained"):
            validate_partition_fields_for_granularity(
                ("year_quarter", "date"),
                "quarter",
            )


if __name__ == "__main__":
    unittest.main()
