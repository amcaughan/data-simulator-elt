from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "containers" / "shared"))

from common.storage_layout import StorageLayoutConfig, default_partition_fields
from source_ingest.adapters.base import (
    AdapterCapabilities,
    FetchOutput,
    FetchResult,
    LiveFetchRequest,
    ManualFetchRequest,
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
from source_ingest.config import (
    IngestConfig,
    ManualPlanningConfig,
    TemporalPlanningConfig,
)
from source_ingest.planning import FetchPlan, build_fetch_plan, build_storage_targets
from source_ingest.runtime import build_landing_key, map_fetch_outputs, run_source_ingest


class FakeAdapter(SourceAdapter):
    capabilities = AdapterCapabilities(
        supported_request_types=(
            LiveFetchRequest,
            ManualFetchRequest,
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
        if isinstance(request, ManualFetchRequest):
            return FetchResult.single(
                body=b'{"row_count": 3, "rows": []}',
                content_type="application/json",
                metadata=metadata,
                suggested_object_name="adapter-suggested.json",
            )
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
        slice_window_override = overrides.pop("slice_window", None)
        landing_layout_override = overrides.pop("landing_layout", None)
        values = {
            "workflow_name": "polling-generated-events",
            "source_adapter": "simulator_api",
            "landing_bucket_name": "landing-bucket",
            "aws_region": "us-east-2",
            "planning_mode": "temporal",
            "temporal_config": None,
            "manual_config": None,
            "source_adapter_config": {
                "preset_id": "transaction_benchmark",
                "row_count": 250,
                "seed_strategy": "derived",
                "request_overrides": {},
            },
        }
        values.update(overrides)
        if values["planning_mode"] == "temporal" and values["temporal_config"] is None:
            from common.slices import SliceWindowConfig

            slice_window = slice_window_override or SliceWindowConfig.current(
                slice_granularity="day",
            )
            values["temporal_config"] = TemporalPlanningConfig(
                slice_window=slice_window,
                landing_layout=landing_layout_override
                or StorageLayoutConfig(
                    base_prefix=None,
                    partition_fields=default_partition_fields(slice_window.slice_granularity),
                ),
            )
        if values["planning_mode"] == "manual" and values["manual_config"] is None:
            values["manual_config"] = ManualPlanningConfig(
                request_payload={},
                storage_prefix="client=acme/emergency",
                object_name=None,
            )
        config = IngestConfig(**values)
        config.validate()
        return config

    def test_live_hit_builds_live_request_and_storage_target(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig.current(
                slice_granularity="hour",
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
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="day",
                pinned_at="2026-03-12",
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
            slice_window=SliceWindowConfig.relative(
                slice_granularity="day",
                relative_count=3,
                relative_direction="backward",
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

    def test_manual_mode_builds_manual_request_and_storage_target(self):
        config = self.build_config(
            planning_mode="manual",
            temporal_config=None,
            manual_config=ManualPlanningConfig(
                request_payload={"row_count": 10},
                storage_prefix="client=acme/emergency",
                object_name=None,
            ),
        )

        plan = build_fetch_plan(config)

        self.assertIsInstance(plan.request, ManualFetchRequest)
        self.assertEqual(plan.request.payload, {"row_count": 10})
        self.assertEqual(len(plan.storage_targets), 1)
        self.assertIsNone(plan.storage_targets[0].logical_slice)
        self.assertEqual(
            plan.storage_targets[0].storage_prefix,
            "client=acme/emergency",
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
            slice_window=SliceWindowConfig.relative(
                slice_granularity="day",
                relative_count=3,
                relative_direction="backward",
            )
        )
        storage_targets = build_storage_targets(
            config,
            now=datetime(2026, 3, 12, 9, 30, tzinfo=UTC),
        )
        requested_slices = tuple(
            RequestedSlice(
                logical_date=target.logical_slice.logical_date,
                slice_start=target.logical_slice.slice_start,
                slice_end=target.logical_slice.slice_end,
                granularity=target.logical_slice.granularity,
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
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="hour",
                pinned_at="2026-03-12T08:00:00Z",
            )
        )
        storage_target = build_storage_targets(config)[0]

        key = build_landing_key(config, storage_target, "application/json")

        self.assertIn("year=2026/month=03/day=12/hour=08", key)
        self.assertTrue(key.endswith(".json"))

    def test_build_landing_key_falls_back_to_binary_suffix(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="day",
                pinned_at="2026-03-12",
            )
        )

        key = build_landing_key(
            config,
            build_storage_targets(config)[0],
            "application/octet-stream",
        )

        self.assertTrue(key.endswith(".bin"))

    def test_build_landing_key_uses_json_suffix_for_vendor_json_types(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="day",
                pinned_at="2026-03-12",
            )
        )

        key = build_landing_key(
            config,
            build_storage_targets(config)[0],
            "application/vnd.acme.dataset+json",
        )

        self.assertTrue(key.endswith(".json"))

    def test_build_landing_key_uses_base_prefix_and_custom_partition_fields(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            landing_layout=StorageLayoutConfig(
                base_prefix="client=acme/project=finance",
                partition_fields=("year_month",),
            ),
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="hour",
                pinned_at="2026-03-12T08:00:00Z",
            ),
        )

        key = build_landing_key(
            config,
            build_storage_targets(config)[0],
            "application/json",
        )

        self.assertTrue(key.startswith("client=acme/project=finance/"))
        self.assertIn("year_month=2026_03", key)
        self.assertNotIn("/day=12/", key)

    def test_build_landing_key_supports_derived_partition_fields(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            landing_layout=StorageLayoutConfig(
                base_prefix="client=acme",
                partition_fields=("year_quarter", "date"),
            ),
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="day",
                pinned_at="2026-03-12",
            ),
        )

        key = build_landing_key(
            config,
            build_storage_targets(config)[0],
            "application/json",
        )

        self.assertIn("client=acme/year_quarter=2026Q1/date=2026_03_12/", key)

    def test_build_landing_key_appends_path_suffix_after_time_partitions(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            landing_layout=StorageLayoutConfig(
                base_prefix="client=acme",
                partition_fields=("year", "month", "day"),
                path_suffix=("api=test_api", "files"),
            ),
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="day",
                pinned_at="2026-03-12",
            ),
        )

        key = build_landing_key(
            config,
            build_storage_targets(config)[0],
            "application/json",
        )

        self.assertIn("client=acme/year=2026/month=03/day=12/api=test_api/files/", key)

    def test_build_landing_key_uses_manual_storage_prefix(self):
        config = self.build_config(
            planning_mode="manual",
            temporal_config=None,
            manual_config=ManualPlanningConfig(
                request_payload={},
                storage_prefix="client=acme/emergency/manual-run",
                object_name=None,
            ),
        )

        key = build_landing_key(
            config,
            build_storage_targets(config)[0],
            "application/json",
        )

        self.assertTrue(key.startswith("client=acme/emergency/manual-run/"))
        self.assertTrue(key.endswith(".json"))

    def test_config_rejects_path_suffix_segments_with_slashes(self):
        with self.assertRaisesRegex(ValueError, "Path suffix segments"):
            self.build_config(
                landing_layout=StorageLayoutConfig(
                    base_prefix=None,
                    partition_fields=("year", "month", "day"),
                    path_suffix=("api/test",),
                )
            )

    def test_config_rejects_partition_fields_finer_than_slice_granularity(self):
        from common.slices import SliceWindowConfig

        with self.assertRaisesRegex(ValueError, "too fine-grained"):
            self.build_config(
                landing_layout=StorageLayoutConfig(
                    base_prefix="client=acme",
                    partition_fields=("year_quarter", "day"),
                ),
                slice_window=SliceWindowConfig.relative(
                    slice_granularity="quarter",
                    relative_count=2,
                    relative_direction="backward",
                ),
            )

    def test_config_rejects_manual_object_names_with_slashes(self):
        with self.assertRaisesRegex(ValueError, "MANUAL_OBJECT_NAME"):
            self.build_config(
                planning_mode="manual",
                temporal_config=None,
                manual_config=ManualPlanningConfig(
                    request_payload={},
                    storage_prefix="client=acme/emergency",
                    object_name="bad/path.json",
                ),
            )

    def test_run_source_ingest_writes_landing_object_and_manifest(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="day",
                pinned_at="2026-03-12",
            )
        )
        s3_client = FakeS3Client()

        with (
            patch("source_ingest.runtime.build_adapter", return_value=FakeAdapter()),
            patch("builtins.print"),
        ):
            results = run_source_ingest(config=config, s3_client=s3_client)

        self.assertEqual(len(results), 1)
        self.assertEqual(len(s3_client.calls), 2)
        payload_call = next(
            call for call in s3_client.calls if not call["Key"].endswith(".manifest.json")
        )
        manifest_call = next(
            call for call in s3_client.calls if call["Key"].endswith(".manifest.json")
        )
        self.assertEqual(payload_call["Bucket"], "landing-bucket")
        self.assertEqual(payload_call["ContentType"], "application/json")
        self.assertEqual(
            payload_call["Metadata"]["workflow_name"], "polling-generated-events"
        )
        self.assertEqual(payload_call["Metadata"]["preset_id"], "transaction_benchmark")
        self.assertEqual(
            payload_call["Metadata"]["source_route"],
            "/v1/presets/transaction_benchmark/generate",
        )
        self.assertEqual(payload_call["Metadata"]["row_count"], "3")
        self.assertEqual(payload_call["Metadata"]["slice_selector_mode"], "pinned")
        manifest = json.loads(manifest_call["Body"].decode("utf-8"))
        self.assertEqual(manifest["request"]["kind"], "slice")
        self.assertEqual(manifest["storage"]["payload_key"], payload_call["Key"])
        self.assertEqual(manifest["storage"]["manifest_key"], manifest_call["Key"])
        self.assertEqual(manifest["storage"]["slice_selector_mode"], "pinned")
        self.assertEqual(
            manifest["storage"]["partition_fields"],
            ["year", "month", "day"],
        )
        self.assertEqual(manifest["storage"]["path_suffix"], [])
        self.assertEqual(
            manifest["adapter_config"]["preset_id"],
            "transaction_benchmark",
        )
        self.assertEqual(
            manifest["payload"]["metadata"]["source_route"],
            "/v1/presets/transaction_benchmark/generate",
        )

    def test_run_source_ingest_manual_mode_uses_adapter_suggested_name(self):
        config = self.build_config(
            planning_mode="manual",
            temporal_config=None,
            manual_config=ManualPlanningConfig(
                request_payload={},
                storage_prefix="client=acme/emergency",
                object_name=None,
            ),
        )
        s3_client = FakeS3Client()

        with (
            patch("source_ingest.runtime.build_adapter", return_value=FakeAdapter()),
            patch("builtins.print"),
        ):
            results = run_source_ingest(config=config, s3_client=s3_client)

        self.assertEqual(len(results), 1)
        payload_call = next(
            call for call in s3_client.calls if not call["Key"].endswith(".manifest.json")
        )
        manifest_call = next(
            call for call in s3_client.calls if call["Key"].endswith(".manifest.json")
        )
        self.assertTrue(
            payload_call["Key"].startswith("client=acme/emergency/adapter-suggested.json")
        )
        self.assertTrue(
            manifest_call["Key"].startswith(
                "client=acme/emergency/adapter-suggested.json.manifest.json"
            )
        )
        self.assertEqual(payload_call["Metadata"]["planning_mode"], "manual")
        self.assertNotIn("logical_date", payload_call["Metadata"])
        manifest = json.loads(manifest_call["Body"].decode("utf-8"))
        self.assertEqual(manifest["planning_mode"], "manual")
        self.assertEqual(manifest["request"]["kind"], "manual")
        self.assertEqual(manifest["storage"]["mode"], "manual")
        self.assertEqual(
            manifest["storage"]["resolved_storage_prefix"],
            "client=acme/emergency",
        )

    def test_run_source_ingest_manual_mode_prefers_runner_object_name_override(self):
        config = self.build_config(
            planning_mode="manual",
            temporal_config=None,
            manual_config=ManualPlanningConfig(
                request_payload={},
                storage_prefix="client=acme/emergency",
                object_name="runner-override.json",
            ),
        )
        s3_client = FakeS3Client()

        with (
            patch("source_ingest.runtime.build_adapter", return_value=FakeAdapter()),
            patch("builtins.print"),
        ):
            run_source_ingest(config=config, s3_client=s3_client)

        payload_call = next(
            call for call in s3_client.calls if not call["Key"].endswith(".manifest.json")
        )
        self.assertTrue(
            payload_call["Key"].startswith("client=acme/emergency/runner-override.json")
        )

    def test_run_source_ingest_writes_multiple_objects_for_multi_slice_request(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            slice_window=SliceWindowConfig.relative(
                slice_granularity="day",
                relative_count=2,
                relative_direction="backward",
            ),
        )
        s3_client = FakeS3Client()

        with (
            patch("source_ingest.runtime.build_adapter", return_value=FakeAdapter()),
            patch("builtins.print"),
        ):
            results = run_source_ingest(config=config, s3_client=s3_client)

        self.assertEqual(len(results), 2)
        self.assertEqual(len(s3_client.calls), 4)
        payload_calls = [call for call in s3_client.calls if not call["Key"].endswith(".manifest.json")]
        manifest_calls = [call for call in s3_client.calls if call["Key"].endswith(".manifest.json")]
        self.assertEqual(len(payload_calls), 2)
        self.assertEqual(len(manifest_calls), 2)
        self.assertEqual(
            sorted(result.manifest_key for result in results),
            sorted(call["Key"] for call in manifest_calls),
        )

    def test_run_source_ingest_rejects_unsupported_request_type(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            source_adapter="unsupported_source",
            slice_window=SliceWindowConfig.relative(
                slice_granularity="day",
                relative_count=2,
                relative_direction="backward",
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
