from __future__ import annotations

from datetime import UTC, datetime
import io
import json
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import patch
import urllib.error

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
from source_ingest.adapters.simulator_batch_delivery import (
    DeliverySpec,
    SimulatorBatchDeliveryAdapter,
    SimulatorBatchDeliveryConfig,
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


class FakeHttpResponse:
    def __init__(self, body: bytes, content_type: str = "application/json"):
        self._body = body
        self.headers = self
        self._content_type = content_type

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._body

    def get_content_type(self):
        return self._content_type


class SourceIngestTests(unittest.TestCase):
    def build_config(self, **overrides) -> IngestConfig:
        slice_window_override = overrides.pop("slice_window", None)
        landing_layout_override = overrides.pop("landing_layout", None)
        values = {
            "workflow_name": "sample-api-polling-01",
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
            workflow_name="sample-api-polling-01",
            preset_id="transaction_benchmark",
            logical_date=logical_date,
            strategy="derived",
            fixed_seed=None,
        )
        second = derive_seed(
            workflow_name="sample-api-polling-01",
            preset_id="transaction_benchmark",
            logical_date=logical_date,
            strategy="derived",
            fixed_seed=None,
        )

        self.assertEqual(first, second)

    def test_live_seed_derivation_omits_seed_for_derived_strategy(self):
        seed = derive_seed(
            workflow_name="sample-api-polling-01",
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

    def test_simulator_adapter_retries_transient_http_errors(self):
        adapter = SimulatorApiAdapter(
            workflow_name="sample-api-polling-01",
            aws_region="us-east-2",
            runtime_config=type("RuntimeConfig", (), {"source_base_url": "https://example.com"})(),
            adapter_config=SimulatorApiConfig.from_dict(
                {
                    "preset_id": "transaction_benchmark",
                    "row_count": 2,
                    "seed_strategy": "derived",
                    "request_overrides": {},
                }
            ),
        )
        http_error = urllib.error.HTTPError(
            url="https://example.com/v1/presets/transaction_benchmark/generate",
            code=500,
            msg="Internal Server Error",
            hdrs=None,
            fp=io.BytesIO(b'{"message":"Internal server error"}'),
        )
        success_response = FakeHttpResponse(
            body=b'{"row_count": 2, "rows": []}',
            content_type="application/json",
        )
        fake_botocore = ModuleType("botocore")
        fake_botocore_auth = ModuleType("botocore.auth")
        fake_botocore_awsrequest = ModuleType("botocore.awsrequest")
        fake_botocore_session = ModuleType("botocore.session")

        class FakeSigV4Auth:
            def __init__(self, credentials, service_name, region_name):
                self.credentials = credentials
                self.service_name = service_name
                self.region_name = region_name

            def add_auth(self, request):
                return None

        class FakeAWSRequest:
            def __init__(self, method, url, data, headers):
                self.method = method
                self.url = url
                self.data = data
                self.headers = headers

            def prepare(self):
                return type("PreparedRequest", (), {"headers": self.headers})()

        class FakeCredentials:
            def get_frozen_credentials(self):
                return object()

        class FakeSession:
            def get_credentials(self):
                return FakeCredentials()

        fake_botocore_auth.SigV4Auth = FakeSigV4Auth
        fake_botocore_awsrequest.AWSRequest = FakeAWSRequest
        fake_botocore_session.get_session = lambda: FakeSession()
        fake_botocore.auth = fake_botocore_auth
        fake_botocore.awsrequest = fake_botocore_awsrequest
        fake_botocore.session = fake_botocore_session

        with patch(
            "source_ingest.adapters.simulator_api.urllib.request.urlopen",
            side_effect=[http_error, success_response],
        ) as mock_urlopen, patch(
            "source_ingest.adapters.simulator_api.time.sleep"
        ) as mock_sleep, patch.dict(
            sys.modules,
            {
                "botocore": fake_botocore,
                "botocore.auth": fake_botocore_auth,
                "botocore.awsrequest": fake_botocore_awsrequest,
                "botocore.session": fake_botocore_session,
            },
        ):
            output = adapter._fetch_slice(
                SliceFetchRequest(
                    slice=RequestedSlice(
                        logical_date=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
                        slice_start=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
                        slice_end=datetime(2026, 3, 1, 0, 59, 59, tzinfo=UTC),
                        granularity="hour",
                    )
                )
            )

        self.assertEqual(mock_urlopen.call_count, 2)
        mock_sleep.assert_called_once()
        self.assertEqual(output.outputs[0].content_type, "application/json")

    def test_batch_delivery_adapter_config_requires_deliveries(self):
        with self.assertRaisesRegex(ValueError, "deliveries"):
            SimulatorBatchDeliveryConfig.from_dict(
                {
                    "preset_id": "batch_delivery_benchmark",
                    "row_count": 10,
                    "seed_strategy": "derived",
                    "request_overrides": {},
                    "deliveries": [],
                }
            )

    def test_batch_delivery_adapter_returns_two_csv_files_for_one_slice(self):
        adapter = SimulatorBatchDeliveryAdapter(
            workflow_name="sample-file-delivery-01",
            aws_region="us-east-2",
            runtime_config=type("RuntimeConfig", (), {"source_base_url": "https://example.com"})(),
            adapter_config=SimulatorBatchDeliveryConfig(
                preset_id="batch_delivery_benchmark",
                row_count=2,
                seed_strategy="derived",
                fixed_seed=None,
                request_overrides={},
                deliveries=(
                    DeliverySpec(
                        source_system_id="location_1",
                        feed_type="member_snapshot",
                        object_name="location_1.csv",
                    ),
                    DeliverySpec(
                        source_system_id="location_2",
                        feed_type="member_snapshot",
                        object_name="location_2.csv",
                    ),
                ),
            ),
        )

        with patch.object(
            adapter,
            "_signed_post",
            return_value=(
                json.dumps(
                    {
                        "row_count": 2,
                        "fields": ["source_system_id", "delivery_id", "record_number"],
                        "rows": [
                            {
                                "source_system_id": "location_1",
                                "delivery_id": "delivery_a",
                                "record_number": 1,
                            },
                            {
                                "source_system_id": "location_1",
                                "delivery_id": "delivery_a",
                                "record_number": 2,
                            },
                        ],
                    }
                ).encode("utf-8"),
                "application/json",
            ),
        ):
            result = adapter._fetch_slice(
                SliceFetchRequest(
                    slice=RequestedSlice(
                        logical_date=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
                        slice_start=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
                        slice_end=datetime(2026, 3, 1, 23, 59, 59, tzinfo=UTC),
                        granularity="day",
                    )
                )
            )

        self.assertEqual(len(result.outputs), 2)
        self.assertEqual(
            [output.suggested_object_name for output in result.outputs],
            ["location_1.csv", "location_2.csv"],
        )
        self.assertTrue(all(output.content_type == "text/csv" for output in result.outputs))

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

    def test_map_fetch_outputs_allows_multiple_outputs_for_one_slice(self):
        from common.slices import SliceWindowConfig

        config = self.build_config(
            workflow_name="sample-file-delivery-01",
            source_adapter="simulator_batch_delivery",
            source_adapter_config={
                "preset_id": "batch_delivery_benchmark",
                "row_count": 2,
                "seed_strategy": "derived",
                "request_overrides": {},
                "deliveries": [
                    {
                        "source_system_id": "location_1",
                        "feed_type": "member_snapshot",
                    },
                    {
                        "source_system_id": "location_2",
                        "feed_type": "member_snapshot",
                    },
                ],
            },
            slice_window=SliceWindowConfig.pinned(
                slice_granularity="day",
                pinned_at="2026-03-01",
            ),
        )
        storage_targets = build_storage_targets(config)
        target = storage_targets[0]
        plan = FetchPlan(
            request=SliceFetchRequest(
                slice=RequestedSlice(
                    logical_date=target.logical_slice.logical_date,
                    slice_start=target.logical_slice.slice_start,
                    slice_end=target.logical_slice.slice_end,
                    granularity=target.logical_slice.granularity,
                )
            ),
            storage_targets=storage_targets,
        )
        fetched = FetchResult(
            outputs=(
                FetchOutput(
                    body=b"a,b\n1,2\n",
                    content_type="text/csv",
                    logical_date=target.logical_slice.logical_date,
                    suggested_object_name="location_1.csv",
                ),
                FetchOutput(
                    body=b"a,b\n3,4\n",
                    content_type="text/csv",
                    logical_date=target.logical_slice.logical_date,
                    suggested_object_name="location_2.csv",
                ),
            )
        )

        assignments = map_fetch_outputs(plan, fetched)

        self.assertEqual(len(assignments), 2)
        self.assertTrue(all(assigned_target == target for assigned_target, _ in assignments))

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
            payload_call["Metadata"]["workflow_name"], "sample-api-polling-01"
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
                "client=acme/emergency/_adapter-suggested.json.manifest.json"
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
