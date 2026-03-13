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
    from standardize.config import StandardizeConfig
    from standardize.runtime import (
        build_landing_prefix,
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
            "source_adapter": "simulator_api",
            "landing_bucket_name": "landing-bucket",
            "processed_bucket_name": "processed-bucket",
            "aws_region": "us-east-2",
            "landing_slice_granularity": "hour",
            "landing_layout": StorageLayoutConfig(
                base_prefix="client=acme/project=finance",
                partition_fields=("year", "month", "day", "hour"),
            ),
            "output_slice_granularity": "day",
            "processed_output_prefix": "raw",
            "landing_input_prefix": None,
            "slice_window": SliceWindowConfig(
                slice_granularity="day",
                mode="live_hit",
                logical_date="2026-03-12",
                start_at=None,
                end_at=None,
                backfill_count=None,
            ),
            "source_adapter_config": {"preset_id": "transaction_benchmark"},
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

        key = build_processed_key(config, logical_slice)

        self.assertTrue(
            key.startswith(
                "raw/year=2026/month=03/day=12/"
            )
        )
        self.assertTrue(key.endswith(".parquet"))

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
        put_calls = [call for call in s3_client.calls if call["Bucket"] == "processed-bucket"]
        self.assertEqual(len(put_calls), 1)
        self.assertTrue(
            put_calls[0]["Key"].startswith(
                "raw/year=2026/month=03/day=12/"
            )
        )
        self.assertEqual(put_calls[0]["Metadata"]["landing_object_count"], "2")


if __name__ == "__main__":
    unittest.main()
