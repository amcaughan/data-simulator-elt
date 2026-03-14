from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "containers" / "shared"))

from common.slices import SliceWindowConfig
from common.storage_layout import StorageLayoutConfig
from standardize.config import (
    ManualPlanningConfig,
    StandardizeConfig,
    TemporalPlanningConfig,
)


class StandardizeConfigTests(unittest.TestCase):
    def build_temporal_config(self, **overrides) -> StandardizeConfig:
        values = {
            "workflow_name": "sample-api-polling-01",
            "standardize_strategy": "simulator_api",
            "landing_bucket_name": "landing-bucket",
            "processed_bucket_name": "processed-bucket",
            "aws_region": "us-east-2",
            "planning_mode": "temporal",
            "standardize_strategy_config": {"preset_id": "transaction_benchmark"},
            "temporal_config": TemporalPlanningConfig(
                landing_slice_granularity="hour",
                landing_layout=StorageLayoutConfig(
                    base_prefix="client=acme",
                    partition_fields=("year", "month", "day", "hour"),
                ),
                output_slice_granularity="day",
                processed_layout=StorageLayoutConfig(
                    base_prefix="raw",
                    partition_fields=("year", "month", "day"),
                ),
                landing_input_prefix=None,
                slice_window=SliceWindowConfig.pinned(
                    slice_granularity="day",
                    pinned_at="2026-03-12",
                ),
            ),
            "manual_config": None,
        }
        values.update(overrides)
        config = StandardizeConfig(**values)
        config.validate()
        return config

    def build_manual_config(self, **overrides) -> StandardizeConfig:
        values = {
            "workflow_name": "sample-api-polling-01",
            "standardize_strategy": "simulator_api",
            "landing_bucket_name": "landing-bucket",
            "processed_bucket_name": "processed-bucket",
            "aws_region": "us-east-2",
            "planning_mode": "manual",
            "standardize_strategy_config": {"preset_id": "transaction_benchmark"},
            "temporal_config": None,
            "manual_config": ManualPlanningConfig(
                input_prefix="client=acme/emergency",
                output_prefix="raw/manual",
                object_name="emergency.parquet",
            ),
        }
        values.update(overrides)
        config = StandardizeConfig(**values)
        config.validate()
        return config

    def test_temporal_output_granularity_may_be_coarser_than_landing(self):
        self.build_temporal_config(
            temporal_config=TemporalPlanningConfig(
                landing_slice_granularity="hour",
                landing_layout=StorageLayoutConfig(
                    base_prefix="client=acme",
                    partition_fields=("year", "month", "day", "hour"),
                ),
                output_slice_granularity="day",
                processed_layout=StorageLayoutConfig(
                    base_prefix="raw",
                    partition_fields=("year", "month", "day"),
                ),
                landing_input_prefix=None,
                slice_window=SliceWindowConfig.pinned(
                    slice_granularity="day",
                    pinned_at="2026-03-12",
                ),
            ),
        )

    def test_manual_mode_disables_slice_iteration(self):
        config = self.build_manual_config()

        with self.assertRaisesRegex(ValueError, "manual planning mode"):
            config.iter_slices()

    def test_manual_mode_rejects_empty_input_prefix(self):
        config = StandardizeConfig(
            workflow_name="sample-api-polling-01",
            standardize_strategy="simulator_api",
            landing_bucket_name="landing-bucket",
            processed_bucket_name="processed-bucket",
            aws_region="us-east-2",
            planning_mode="manual",
            standardize_strategy_config={"preset_id": "transaction_benchmark"},
            temporal_config=None,
            manual_config=ManualPlanningConfig(
                input_prefix="  ",
                output_prefix="raw/manual",
                object_name="emergency.parquet",
            ),
        )

        with self.assertRaisesRegex(ValueError, "MANUAL_INPUT_PREFIX"):
            config.validate()


if __name__ == "__main__":
    unittest.main()
