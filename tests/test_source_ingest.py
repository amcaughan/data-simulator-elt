from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "containers" / "shared"))

from source_ingest.adapters.base import (
    AdapterCapabilities,
    FetchResult,
    HistoricalSlicePullRequest,
    LivePullRequest,
    SourceAdapter,
)
from source_ingest.adapters.simulator_api import (
    SimulatorApiConfig,
    build_generate_payload,
    derive_seed,
)
from source_ingest.config import IngestConfig
from source_ingest.runtime import build_landing_key, run_source_ingest


class FakeAdapter(SourceAdapter):
    capabilities = AdapterCapabilities(
        supported_pull_request_types=(LivePullRequest, HistoricalSlicePullRequest)
    )

    @classmethod
    def adapter_key(cls) -> str:
        return "fake"

    @classmethod
    def from_ingest_config(cls, config):
        return cls()

    def _fetch(self, pull_request):
        return FetchResult(
            body=b'{"row_count": 3, "rows": []}',
            content_type="application/json",
            metadata={
                "row_count": "3",
                "preset_id": "transaction_benchmark",
                "source_route": "/v1/presets/transaction_benchmark/generate",
            },
        )


class LiveOnlyAdapter(SourceAdapter):
    capabilities = AdapterCapabilities()

    @classmethod
    def adapter_key(cls) -> str:
        return "live_only"

    @classmethod
    def from_ingest_config(cls, config):
        return cls()

    def _fetch(self, pull_request):
        raise AssertionError("fetch should not be called when backfill is unsupported")


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


class SourceIngestTests(unittest.TestCase):
    def build_config(self, **overrides) -> IngestConfig:
        values = {
            "workflow_name": "polling-generated-events",
            "source_adapter": "simulator_api",
            "landing_bucket_name": "landing-bucket",
            "aws_region": "us-east-2",
            "source_base_url": "https://example.execute-api.us-east-2.amazonaws.com/dev",
            "slice_window": None,
            "source_adapter_config": {
                "preset_id": "transaction_benchmark",
                "row_count": 250,
                "seed_strategy": "derived",
                "request_overrides": {},
            },
        }
        values.update(overrides)
        if values["slice_window"] is None:
            from common.slices import SliceWindowConfig

            values["slice_window"] = SliceWindowConfig(
                partition_granularity="day",
                mode="live_hit",
                logical_date=None,
                start_at=None,
                end_at=None,
                backfill_days=None,
            )
        config = IngestConfig(**values)
        config.validate()
        return config

    def test_live_hit_uses_truncated_logical_date(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="hour",
                mode="live_hit",
                logical_date=None,
                start_at=None,
                end_at=None,
                backfill_days=None,
            )
        )

        slices = config.iter_slices(now=datetime(2026, 3, 12, 10, 49, tzinfo=UTC))

        self.assertEqual(len(slices), 1)
        self.assertEqual(
            slices[0].logical_date, datetime(2026, 3, 12, 10, 0, tzinfo=UTC)
        )

    def test_backfill_days_expands_date_range(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="day",
                mode="backfill",
                logical_date=None,
                start_at=None,
                end_at=None,
                backfill_days=3,
            )
        )

        slices = config.iter_slices(now=datetime(2026, 3, 12, 9, 30, tzinfo=UTC))

        self.assertEqual(
            [item.logical_date.isoformat() for item in slices],
            [
                "2026-03-10T00:00:00+00:00",
                "2026-03-11T00:00:00+00:00",
                "2026-03-12T00:00:00+00:00",
            ],
        )

    def test_live_hit_builds_live_pull_request(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="hour",
                mode="live_hit",
                logical_date=None,
                start_at=None,
                end_at=None,
                backfill_days=None,
            )
        )

        pull_requests = config.iter_pull_requests(
            now=datetime(2026, 3, 12, 10, 49, tzinfo=UTC)
        )

        self.assertEqual(len(pull_requests), 1)
        self.assertIsInstance(pull_requests[0], LivePullRequest)
        self.assertEqual(pull_requests[0].mode, "live_hit")
        self.assertEqual(
            pull_requests[0].logical_slice.logical_date,
            datetime(2026, 3, 12, 10, 0, tzinfo=UTC),
        )

    def test_backfill_builds_historical_pull_requests(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="day",
                mode="backfill",
                logical_date=None,
                start_at=None,
                end_at=None,
                backfill_days=2,
            )
        )

        pull_requests = config.iter_pull_requests(
            now=datetime(2026, 3, 12, 9, 30, tzinfo=UTC)
        )

        self.assertEqual(len(pull_requests), 2)
        self.assertIsInstance(pull_requests[0], HistoricalSlicePullRequest)
        self.assertEqual(pull_requests[0].mode, "backfill")
        self.assertEqual(
            pull_requests[0].slice_start.isoformat(),
            "2026-03-11T00:00:00+00:00",
        )
        self.assertEqual(
            pull_requests[0].slice_end.isoformat(),
            "2026-03-12T00:00:00+00:00",
        )

    def test_seed_derivation_is_deterministic(self):
        logical_slice = self.build_config().iter_slices(
            now=datetime(2026, 3, 12, 9, 30, tzinfo=UTC)
        )[0]

        first = derive_seed(
            workflow_name="polling-generated-events",
            preset_id="transaction_benchmark",
            logical_slice=logical_slice,
            strategy="derived",
            fixed_seed=None,
        )
        second = derive_seed(
            workflow_name="polling-generated-events",
            preset_id="transaction_benchmark",
            logical_slice=logical_slice,
            strategy="derived",
            fixed_seed=None,
        )

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

    def test_simulator_adapter_config_requires_preset_and_row_count(self):
        with self.assertRaisesRegex(ValueError, "preset_id"):
            SimulatorApiConfig.from_dict({"row_count": 1})

        with self.assertRaisesRegex(ValueError, "row_count"):
            SimulatorApiConfig.from_dict({"preset_id": "foo", "row_count": 0})

    def test_build_landing_key_uses_content_type_suffix(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="hour",
                mode="live_hit",
                logical_date="2026-03-12T08:00:00Z",
                start_at=None,
                end_at=None,
                backfill_days=None,
            )
        )
        logical_slice = config.iter_slices()[0]

        key = build_landing_key(config, logical_slice, "application/json")

        self.assertIn("workflow=polling-generated-events", key)
        self.assertIn("adapter=simulator_api", key)
        self.assertIn("year=2026/month=03/day=12/hour=08", key)
        self.assertTrue(key.endswith(".json"))

    def test_build_landing_key_falls_back_to_binary_suffix(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="day",
                mode="live_hit",
                logical_date="2026-03-12",
                start_at=None,
                end_at=None,
                backfill_days=None,
            )
        )

        key = build_landing_key(
            config,
            config.iter_slices()[0],
            "application/octet-stream",
        )

        self.assertTrue(key.endswith(".bin"))

    def test_build_landing_key_uses_json_suffix_for_vendor_json_types(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="day",
                mode="live_hit",
                logical_date="2026-03-12",
                start_at=None,
                end_at=None,
                backfill_days=None,
            )
        )

        key = build_landing_key(
            config,
            config.iter_slices()[0],
            "application/vnd.acme.dataset+json",
        )

        self.assertTrue(key.endswith(".json"))

    def test_run_source_ingest_writes_landing_object(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="day",
                mode="live_hit",
                logical_date="2026-03-12",
                start_at=None,
                end_at=None,
                backfill_days=None,
            )
        )
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
        self.assertEqual(put_call["Metadata"]["preset_id"], "transaction_benchmark")
        self.assertEqual(
            put_call["Metadata"]["source_route"],
            "/v1/presets/transaction_benchmark/generate",
        )
        self.assertEqual(put_call["Metadata"]["row_count"], "3")

    def test_run_source_ingest_rejects_unsupported_pull_request_type(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            source_adapter="unsupported_source",
            slice_window=SliceWindowConfig(
                partition_granularity="day",
                mode="backfill",
                logical_date=None,
                start_at=None,
                end_at=None,
                backfill_days=2,
            ),
        )

        with patch(
            "source_ingest.runtime.build_adapter", return_value=LiveOnlyAdapter()
        ):
            with self.assertRaisesRegex(
                ValueError,
                "does not support pull request type 'HistoricalSlicePullRequest'",
            ):
                run_source_ingest(config=config, s3_client=FakeS3Client())


if __name__ == "__main__":
    unittest.main()
