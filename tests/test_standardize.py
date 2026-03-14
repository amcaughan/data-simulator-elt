from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "containers" / "shared"))

try:
    import pyarrow  # noqa: F401
except ImportError:  # pragma: no cover - environment-dependent
    pyarrow = None

from common.slices import SliceWindowConfig
from common.storage_layout import StorageLayoutConfig

if pyarrow is not None:
    from standardize.config import (
        ManualPlanningConfig,
        StandardizeConfig,
        TemporalPlanningConfig,
    )
    from standardize.runtime import (
        build_landing_prefix,
        build_manual_landing_prefix,
        build_manual_processed_key,
        build_processed_key,
        run_standardize,
    )


class FakeBody:
    def __init__(self, payload: bytes):
        self.payload = payload

    def read(self):
        return self.payload


class FakeS3Client:
    def __init__(self, objects=None):
        self.calls = []
        self.objects = objects or {}

    def put_object(self, **kwargs):
        self.calls.append(kwargs)

    def get_object(self, Bucket, Key):
        stored = self.objects[(Bucket, Key)]
        return {"Body": FakeBody(stored["body"]), "Metadata": stored.get("metadata", {})}

    def get_paginator(self, name):
        client = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                contents = [
                    {"Key": key}
                    for (bucket, key), value in client.objects.items()
                    if bucket == Bucket and key.startswith(Prefix)
                ]
                return [{"Contents": contents}]

        return Paginator()


@unittest.skipUnless(pyarrow is not None, "pyarrow is required for standardize runtime tests")
class StandardizeTests(unittest.TestCase):
    def build_config(self, **overrides) -> "StandardizeConfig":
        values = {
            "workflow_name": "polling-generated-events",
            "standardize_strategy": "simulator_api",
            "landing_bucket_name": "landing-bucket",
            "processed_bucket_name": "processed-bucket",
            "aws_region": "us-east-2",
            "planning_mode": "temporal",
            "standardize_strategy_config": {"preset_id": "transaction_benchmark"},
            "temporal_config": TemporalPlanningConfig(
                landing_slice_granularity="hour",
                landing_layout=StorageLayoutConfig(
                    base_prefix="client=acme/project=finance",
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

    def build_manual_config(self, **overrides) -> "StandardizeConfig":
        values = {
            "workflow_name": "polling-generated-events",
            "standardize_strategy": "simulator_api",
            "landing_bucket_name": "landing-bucket",
            "processed_bucket_name": "processed-bucket",
            "aws_region": "us-east-2",
            "planning_mode": "manual",
            "standardize_strategy_config": {"preset_id": "transaction_benchmark"},
            "temporal_config": None,
            "manual_config": ManualPlanningConfig(
                input_prefix="client=acme/project=finance/emergency",
                output_prefix="raw/manual",
                object_name="emergency.parquet",
            ),
        }
        values.update(overrides)
        config = StandardizeConfig(**values)
        config.validate()
        return config

    def test_standardize_prefix_rolls_hourly_landing_into_day(self):
        config = self.build_config()
        logical_slice = config.iter_slices()[0]

        prefix = build_landing_prefix(config, logical_slice)

        self.assertEqual(
            prefix,
            (
                "client=acme/project=finance/"
                "year=2026/month=03/day=12/"
            ),
        )

    def test_processed_key_uses_raw_prefix(self):
        config = self.build_config()
        logical_slice = config.iter_slices()[0]

        key = build_processed_key(
            config,
            logical_slice,
            object_name=f"slice_id={logical_slice.run_id}.parquet",
        )

        self.assertTrue(
            key.startswith(
                "raw/year=2026/month=03/day=12/"
            )
        )
        self.assertTrue(key.endswith(".parquet"))

    def test_output_granularity_may_be_coarser_than_landing(self):
        config = self.build_config(
            temporal_config=TemporalPlanningConfig(
                landing_slice_granularity="hour",
                landing_layout=StorageLayoutConfig(
                    base_prefix="client=acme/project=finance",
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

        config.validate()

    def test_run_standardize_aggregates_landing_rows(self):
        import json

        config = self.build_config()
        logical_slice = config.iter_slices()[0]
        key_one = (
            "client=acme/project=finance/year=2026/month=03/day=12/hour=00/run_id=a.json"
        )
        key_two = (
            "client=acme/project=finance/year=2026/month=03/day=12/hour=01/run_id=b.json"
        )
        manifest_key = (
            "client=acme/project=finance/year=2026/month=03/day=12/hour=00/run_id=a.manifest.json"
        )
        landing_payload = {
            "row_count": 1,
            "schema_version": "v1",
            "scenario_name": "transaction_benchmark",
            "rows": [{"amount": 10.0, "__row_index": 0}],
        }
        s3_client = FakeS3Client(
            objects={
                ("landing-bucket", key_one): {
                    "body": json.dumps(landing_payload).encode("utf-8"),
                    "metadata": {
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "preset_id": "transaction_benchmark",
                    },
                },
                ("landing-bucket", manifest_key): {
                    "body": b"{}",
                    "metadata": {},
                },
                ("landing-bucket", key_two): {
                    "body": json.dumps(landing_payload).encode("utf-8"),
                    "metadata": {
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "preset_id": "transaction_benchmark",
                    },
                },
            }
        )

        with unittest.mock.patch("builtins.print"):
            results = run_standardize(config=config, s3_client=s3_client)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].row_count, 2)
        self.assertEqual(results[0].source_object_count, 2)
        self.assertTrue(results[0].manifest_key.endswith(".manifest.json"))
        put_calls = [call for call in s3_client.calls if call["Bucket"] == "processed-bucket"]
        self.assertEqual(len(put_calls), 2)
        parquet_call = next(call for call in put_calls if call["Key"].endswith(".parquet"))
        manifest_call = next(
            call for call in put_calls if call["Key"].endswith(".manifest.json")
        )
        self.assertTrue(
            parquet_call["Key"].startswith(
                "raw/year=2026/month=03/day=12/"
            )
        )
        self.assertEqual(parquet_call["Metadata"]["input_object_count"], "2")
        self.assertEqual(
            parquet_call["Metadata"]["standardize_strategy"],
            "simulator_api",
        )
        self.assertEqual(manifest_call["ContentType"], "application/json")
        manifest = json.loads(manifest_call["Body"].decode("utf-8"))
        self.assertEqual(manifest["planning_mode"], "temporal")
        self.assertEqual(manifest["input"]["object_count"], 2)
        self.assertEqual(len(manifest["input"]["keys"]), 2)

    def test_manual_mode_reads_targeted_prefix_and_writes_manifest(self):
        import json

        config = self.build_manual_config()
        self.assertEqual(
            build_manual_landing_prefix(config),
            "client=acme/project=finance/emergency/",
        )
        self.assertEqual(
            build_manual_processed_key(config, "emergency.parquet"),
            "raw/manual/emergency.parquet",
        )
        s3_client = FakeS3Client(
            objects={
                (
                    "landing-bucket",
                    "client=acme/project=finance/emergency/file-a.json",
                ): {
                    "body": json.dumps(
                        {
                            "row_count": 1,
                            "schema_version": "v1",
                            "scenario_name": "transaction_benchmark",
                            "rows": [{"amount": 10.0, "__row_index": 0}],
                        }
                    ).encode("utf-8"),
                    "metadata": {"preset_id": "transaction_benchmark"},
                },
                (
                    "landing-bucket",
                    "client=acme/project=finance/emergency/file-a.manifest.json",
                ): {
                    "body": b"{}",
                    "metadata": {},
                },
            }
        )

        with unittest.mock.patch("builtins.print"):
            results = run_standardize(config=config, s3_client=s3_client)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].key, "raw/manual/emergency.parquet")
        self.assertEqual(
            results[0].manifest_key,
            "raw/manual/_emergency.parquet.manifest.json",
        )
        put_calls = [call for call in s3_client.calls if call["Bucket"] == "processed-bucket"]
        self.assertEqual(len(put_calls), 2)
        manifest_call = next(
            call for call in put_calls if call["Key"].endswith(".manifest.json")
        )
        manifest = json.loads(manifest_call["Body"].decode("utf-8"))
        self.assertEqual(manifest["planning_mode"], "manual")
        self.assertEqual(
            manifest["input"]["input_prefix"],
            "client=acme/project=finance/emergency/",
        )
        self.assertEqual(manifest["output"]["output_prefix"], "raw/manual")


if __name__ == "__main__":
    unittest.main()
