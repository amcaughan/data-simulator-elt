from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from source_ingest.adapters.simulator_api import FetchResult, build_generate_payload
from source_ingest.config import IngestConfig
from source_ingest.runtime import build_landing_key, run_source_ingest


class FakeAdapter:
    def fetch(self, logical_slice):
        return FetchResult(
            body=b'{"row_count": 3, "rows": []}',
            content_type="application/json",
            row_count=3,
            route="/v1/presets/transaction_benchmark/generate",
        )


class FakeS3Client:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


class SourceIngestTests(unittest.TestCase):
    def build_config(self, **overrides) -> IngestConfig:
        values = {
            "workflow_name": "polling-generated-events",
            "source_adapter": "simulator_api",
            "simulator_api_url": "https://example.execute-api.us-east-2.amazonaws.com/dev",
            "preset_id": "transaction_benchmark",
            "row_count": 250,
            "landing_bucket_name": "landing-bucket",
            "aws_region": "us-east-2",
            "partition_granularity": "day",
            "mode": "single_run",
            "logical_date": None,
            "start_at": None,
            "end_at": None,
            "backfill_days": None,
            "seed_strategy": "derived",
            "fixed_seed": None,
            "request_overrides": {},
        }
        values.update(overrides)
        config = IngestConfig(**values)
        config.validate()
        return config

    def test_single_run_uses_truncated_logical_date(self):
        config = self.build_config(partition_granularity="hour")

        slices = config.iter_slices(now=datetime(2026, 3, 12, 10, 49, tzinfo=UTC))

        self.assertEqual(len(slices), 1)
        self.assertEqual(
            slices[0].logical_date, datetime(2026, 3, 12, 10, 0, tzinfo=UTC)
        )

    def test_backfill_days_expands_date_range(self):
        config = self.build_config(mode="backfill", backfill_days=3)

        slices = config.iter_slices(now=datetime(2026, 3, 12, 9, 30, tzinfo=UTC))

        self.assertEqual(
            [item.logical_date.isoformat() for item in slices],
            [
                "2026-03-10T00:00:00+00:00",
                "2026-03-11T00:00:00+00:00",
                "2026-03-12T00:00:00+00:00",
            ],
        )

    def test_seed_derivation_is_deterministic(self):
        config = self.build_config(
            mode="single_run",
            logical_date="2026-03-12",
            seed_strategy="derived",
        )

        first = config.iter_slices()[0].seed
        second = config.iter_slices()[0].seed

        self.assertEqual(first, second)

    def test_build_generate_payload_applies_defaults(self):
        payload = build_generate_payload(
            request_overrides={"description": "test"},
            row_count=25,
            seed=17,
        )

        self.assertEqual(
            payload,
            {"description": "test", "row_count": 25, "seed": 17},
        )

    def test_build_landing_key_includes_hour_partition(self):
        config = self.build_config(
            partition_granularity="hour",
            logical_date="2026-03-12T08:00:00Z",
        )
        logical_slice = config.iter_slices()[0]

        key = build_landing_key(config, logical_slice)

        self.assertIn("adapter=simulator_api", key)
        self.assertIn("preset_id=transaction_benchmark", key)
        self.assertIn("year=2026/month=03/day=12/hour=08", key)
        self.assertTrue(key.endswith(".json"))

    def test_run_source_ingest_writes_landing_object(self):
        config = self.build_config(logical_date="2026-03-12")
        s3_client = FakeS3Client()

        with (
            patch("source_ingest.runtime.build_adapter", return_value=FakeAdapter()),
            patch("builtins.print"),
        ):
            results = run_source_ingest(config=config, s3_client=s3_client)

        self.assertEqual(len(results), 1)
        self.assertEqual(len(s3_client.calls), 1)
        put_call = s3_client.calls[0]
        self.assertEqual(put_call["Bucket"], "landing-bucket")
        self.assertEqual(put_call["ContentType"], "application/json")
        self.assertEqual(put_call["Metadata"]["workflow_name"], "polling-generated-events")


if __name__ == "__main__":
    unittest.main()
