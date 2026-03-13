from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "containers" / "shared"))

from source_ingest.adapters.base import (
    AdapterCapabilities,
    FetchOutput,
    FetchResult,
    LiveFetchRequest,
    MultiSliceFetchRequest,
    RequestedSlice,
    SliceFetchRequest,
    SourceAdapter,
)
from source_ingest.adapters.simulator_api import (
    SimulatorApiAdapter,
    SimulatorApiConfig,
    build_generate_payload,
    derive_seed,
)
from source_ingest.config import IngestConfig
from source_ingest.planning import FetchPlan, build_fetch_plan, build_storage_targets
from source_ingest.runtime import build_landing_key, map_fetch_outputs, run_source_ingest


class FakeAdapter(SourceAdapter):
    capabilities = AdapterCapabilities(
        supported_request_types=(
            LiveFetchRequest,
            SliceFetchRequest,
            MultiSliceFetchRequest,
        )
    )

    @classmethod
    def adapter_key(cls) -> str:
        return "fake"

    @classmethod
    def from_ingest_config(cls, config):
        return cls()

    def _fetch(self, request):
        metadata = {
            "row_count": "3",
            "preset_id": "transaction_benchmark",
            "source_route": "/v1/presets/transaction_benchmark/generate",
        }
        if isinstance(request, MultiSliceFetchRequest):
            return FetchResult(
                outputs=tuple(
                    FetchOutput(
                        body=b'{"row_count": 3, "rows": []}',
                        content_type="application/json",
                        metadata=metadata,
                        logical_date=requested_slice.logical_date,
                    )
                    for requested_slice in request.slices
                )
            )

        logical_date = request.slice.logical_date if isinstance(request, SliceFetchRequest) else None
        return FetchResult.single(
            body=b'{"row_count": 3, "rows": []}',
            content_type="application/json",
            metadata=metadata,
            logical_date=logical_date,
        )


class LiveOnlyAdapter(SourceAdapter):
    capabilities = AdapterCapabilities()

    @classmethod
    def adapter_key(cls) -> str:
        return "live_only"

    @classmethod
    def from_ingest_config(cls, config):
        return cls()

    def _fetch(self, request):
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

    def test_live_hit_builds_live_request_and_storage_target(self):
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

        plan = build_fetch_plan(config, now=datetime(2026, 3, 12, 10, 49, tzinfo=UTC))

        self.assertIsInstance(plan.request, LiveFetchRequest)
        self.assertEqual(len(plan.storage_targets), 1)
        self.assertEqual(
            plan.storage_targets[0].logical_slice.logical_date,
            datetime(2026, 3, 12, 10, 0, tzinfo=UTC),
        )

    def test_logical_date_builds_slice_request_and_storage_target(self):
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

        plan = build_fetch_plan(config)

        self.assertIsInstance(plan.request, SliceFetchRequest)
        self.assertEqual(len(plan.storage_targets), 1)
        self.assertEqual(
            plan.request.slice.logical_date.isoformat(),
            "2026-03-12T00:00:00+00:00",
        )
        self.assertEqual(
            plan.storage_targets[0].logical_slice.logical_date.isoformat(),
            "2026-03-12T00:00:00+00:00",
        )

    def test_backfill_builds_multi_slice_request_and_storage_targets(self):
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

        plan = build_fetch_plan(config, now=datetime(2026, 3, 12, 9, 30, tzinfo=UTC))

        self.assertIsInstance(plan.request, MultiSliceFetchRequest)
        self.assertEqual(len(plan.storage_targets), 3)
        self.assertEqual(
            [requested_slice.logical_date.isoformat() for requested_slice in plan.request.slices],
            [
                "2026-03-10T00:00:00+00:00",
                "2026-03-11T00:00:00+00:00",
                "2026-03-12T00:00:00+00:00",
            ],
        )

    def test_seed_derivation_is_deterministic(self):
        logical_date = datetime(2026, 3, 12, 0, 0, tzinfo=UTC)

        first = derive_seed(
            workflow_name="polling-generated-events",
            preset_id="transaction_benchmark",
            logical_date=logical_date,
            strategy="derived",
            fixed_seed=None,
        )
        second = derive_seed(
            workflow_name="polling-generated-events",
            preset_id="transaction_benchmark",
            logical_date=logical_date,
            strategy="derived",
            fixed_seed=None,
        )

        self.assertEqual(first, second)

    def test_live_seed_derivation_omits_seed_for_derived_strategy(self):
        seed = derive_seed(
            workflow_name="polling-generated-events",
            preset_id="transaction_benchmark",
            logical_date=None,
            strategy="derived",
            fixed_seed=None,
        )

        self.assertIsNone(seed)

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

    def test_simulator_adapter_requires_source_base_url_env(self):
        config = self.build_config()

        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(ValueError, "SOURCE_BASE_URL"):
                SimulatorApiAdapter.from_ingest_config(config)

    def test_simulator_adapter_reads_source_base_url_from_env(self):
        config = self.build_config()

        with patch.dict(
            "os.environ",
            {"SOURCE_BASE_URL": "https://example.execute-api.us-east-2.amazonaws.com/dev"},
            clear=True,
        ):
            adapter = SimulatorApiAdapter.from_ingest_config(config)

        self.assertEqual(
            adapter.runtime_config.source_base_url,
            "https://example.execute-api.us-east-2.amazonaws.com/dev",
        )

    def test_map_fetch_outputs_matches_multi_slice_results_to_storage_targets(self):
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
        storage_targets = build_storage_targets(
            config,
            now=datetime(2026, 3, 12, 9, 30, tzinfo=UTC),
        )
        requested_slices = tuple(
            RequestedSlice(
                logical_date=target.logical_slice.logical_date,
                slice_start=target.logical_slice.logical_date,
                slice_end=target.logical_slice.logical_date,
            )
            for target in storage_targets
        )
        plan = FetchPlan(
            request=MultiSliceFetchRequest(slices=requested_slices),
            storage_targets=storage_targets,
        )
        fetched = FetchResult(
            outputs=tuple(
                FetchOutput(
                    body=b"{}",
                    content_type="application/json",
                    logical_date=target.logical_slice.logical_date,
                )
                for target in reversed(storage_targets)
            )
        )

        assignments = map_fetch_outputs(plan, fetched)

        self.assertEqual(
            [target.logical_slice.logical_date for target, _ in assignments],
            [target.logical_slice.logical_date for target in storage_targets],
        )

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
        logical_slice = build_storage_targets(config)[0].logical_slice

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
            build_storage_targets(config)[0].logical_slice,
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
            build_storage_targets(config)[0].logical_slice,
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

    def test_run_source_ingest_writes_multiple_objects_for_multi_slice_request(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig(
                partition_granularity="day",
                mode="backfill",
                logical_date=None,
                start_at=None,
                end_at=None,
                backfill_days=2,
            ),
        )
        s3_client = FakeS3Client()

        with (
            patch("source_ingest.runtime.build_adapter", return_value=FakeAdapter()),
            patch("builtins.print"),
        ):
            results = run_source_ingest(config=config, s3_client=s3_client)

        self.assertEqual(len(results), 2)
        self.assertEqual(len(s3_client.calls), 2)

    def test_run_source_ingest_rejects_unsupported_request_type(self):
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
                "does not support request type 'MultiSliceFetchRequest'",
            ):
                run_source_ingest(config=config, s3_client=FakeS3Client())


if __name__ == "__main__":
    unittest.main()
