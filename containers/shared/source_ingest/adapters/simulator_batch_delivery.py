from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import io
import json
from typing import Any
import urllib.parse

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
    SourceFetchRequest,
)
from source_ingest.adapters.simulator_api import (
    DEFAULT_HTTP_RETRY_ATTEMPTS,
    DEFAULT_HTTP_RETRY_BACKOFF_SECONDS,
    TRANSIENT_HTTP_STATUS_CODES,
    SimulatorApiRuntimeConfig,
    build_generate_payload,
    derive_seed,
)
from source_ingest.config import IngestConfig


def _derive_delivery_seed(
    *,
    workflow_name: str,
    preset_id: str,
    logical_date: datetime | None,
    source_system_id: str,
    feed_type: str,
    strategy: str,
    fixed_seed: int | None,
) -> int | None:
    base_seed = derive_seed(
        workflow_name=workflow_name,
        preset_id=preset_id,
        logical_date=logical_date,
        strategy=strategy,
        fixed_seed=fixed_seed,
    )
    if base_seed is None:
        return None
    namespace = f"{base_seed}|{source_system_id}|{feed_type}"
    digest = hashlib.sha256(namespace.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % (2**31)


def _default_delivery_date(logical_date: datetime | None) -> str:
    if logical_date is None:
        logical_date = datetime.now(UTC)
    return logical_date.date().isoformat()


def _csv_bytes(rows: list[dict[str, Any]], fieldnames: list[str]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                field: json.dumps(value) if isinstance(value, (dict, list)) else value
                for field, value in row.items()
            }
        )
    return buffer.getvalue().encode("utf-8")


@dataclass(frozen=True)
class DeliverySpec:
    source_system_id: str
    feed_type: str
    row_count: int | None = None
    object_name: str | None = None
    request_overrides: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DeliverySpec":
        source_system_id = value.get("source_system_id")
        feed_type = value.get("feed_type")
        row_count = value.get("row_count")
        object_name = value.get("object_name")
        request_overrides = value.get("request_overrides", {})
        if not isinstance(source_system_id, str) or source_system_id == "":
            raise ValueError("Delivery spec requires a non-empty 'source_system_id'")
        if not isinstance(feed_type, str) or feed_type == "":
            raise ValueError("Delivery spec requires a non-empty 'feed_type'")
        if row_count is not None and (not isinstance(row_count, int) or row_count < 1):
            raise ValueError("Delivery spec 'row_count' must be a positive integer when provided")
        if object_name is not None and (
            not isinstance(object_name, str)
            or object_name.strip() == ""
            or "/" in object_name
        ):
            raise ValueError("Delivery spec 'object_name' must be a non-empty filename without '/'")
        if not isinstance(request_overrides, dict):
            raise ValueError("Delivery spec 'request_overrides' must be a JSON object")
        return cls(
            source_system_id=source_system_id,
            feed_type=feed_type,
            row_count=row_count,
            object_name=object_name,
            request_overrides=request_overrides,
        )


@dataclass(frozen=True)
class SimulatorBatchDeliveryConfig:
    preset_id: str
    row_count: int
    seed_strategy: str
    fixed_seed: int | None
    request_overrides: dict[str, Any]
    deliveries: tuple[DeliverySpec, ...]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SimulatorBatchDeliveryConfig":
        preset_id = value.get("preset_id")
        row_count = value.get("row_count")
        seed_strategy = value.get("seed_strategy", "derived")
        fixed_seed = value.get("fixed_seed")
        request_overrides = value.get("request_overrides", {})
        deliveries = value.get("deliveries", [])

        if not isinstance(preset_id, str) or preset_id == "":
            raise ValueError("Batch delivery adapter config requires a non-empty 'preset_id'")
        if not isinstance(row_count, int) or row_count < 1:
            raise ValueError("Batch delivery adapter config requires positive integer 'row_count'")
        if seed_strategy not in {"derived", "fixed", "none"}:
            raise ValueError("Batch delivery adapter config has invalid 'seed_strategy'")
        if seed_strategy == "fixed" and not isinstance(fixed_seed, int):
            raise ValueError("Batch delivery adapter config requires integer 'fixed_seed' for fixed seeds")
        if not isinstance(request_overrides, dict):
            raise ValueError("Batch delivery adapter config 'request_overrides' must be an object")
        if not isinstance(deliveries, list) or not deliveries:
            raise ValueError("Batch delivery adapter config requires a non-empty 'deliveries' array")

        return cls(
            preset_id=preset_id,
            row_count=row_count,
            seed_strategy=seed_strategy,
            fixed_seed=fixed_seed,
            request_overrides=request_overrides,
            deliveries=tuple(DeliverySpec.from_dict(item) for item in deliveries),
        )


class SimulatorBatchDeliveryAdapter(SourceAdapter):
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
        return "simulator_batch_delivery"

    @classmethod
    def from_ingest_config(cls, config: IngestConfig) -> "SimulatorBatchDeliveryAdapter":
        return cls(
            workflow_name=config.workflow_name,
            aws_region=config.aws_region,
            runtime_config=SimulatorApiRuntimeConfig.from_env(),
            adapter_config=SimulatorBatchDeliveryConfig.from_dict(config.source_adapter_config),
        )

    def __init__(
        self,
        workflow_name: str,
        aws_region: str,
        runtime_config: SimulatorApiRuntimeConfig,
        adapter_config: SimulatorBatchDeliveryConfig,
    ):
        self.workflow_name = workflow_name
        self.aws_region = aws_region
        self.runtime_config = runtime_config
        self.adapter_config = adapter_config

    def _fetch(self, request: SourceFetchRequest) -> FetchResult:
        if isinstance(request, LiveFetchRequest):
            return self._fetch_live(request)
        if isinstance(request, ManualFetchRequest):
            return self._fetch_manual(request)
        if isinstance(request, SliceFetchRequest):
            return self._fetch_slice(request)
        if isinstance(request, MultiSliceFetchRequest):
            return self._fetch_multi_slice(request)
        raise TypeError(f"Unsupported request type: {type(request)!r}")

    def _fetch_live(self, request: LiveFetchRequest) -> FetchResult:
        return self._fetch_for_logical_date(logical_date=None)

    def _fetch_manual(self, request: ManualFetchRequest) -> FetchResult:
        return self._fetch_for_logical_date(
            logical_date=None,
            manual_payload=request.payload,
        )

    def _fetch_slice(self, request: SliceFetchRequest) -> FetchResult:
        return self._fetch_for_logical_date(logical_date=request.slice.logical_date)

    def _fetch_multi_slice(self, request: MultiSliceFetchRequest) -> FetchResult:
        outputs: list[FetchOutput] = []
        for requested_slice in request.slices:
            outputs.extend(
                self._fetch_for_logical_date(
                    logical_date=requested_slice.logical_date
                ).outputs
            )
        return FetchResult(outputs=tuple(outputs))

    def _fetch_for_logical_date(
        self,
        *,
        logical_date: datetime | None,
        manual_payload: dict[str, Any] | None = None,
    ) -> FetchResult:
        route = f"/v1/presets/{self.adapter_config.preset_id}/generate"
        url = urllib.parse.urljoin(
            self.runtime_config.source_base_url.rstrip("/") + "/",
            route.lstrip("/"),
        )
        deliveries = self._resolve_deliveries(manual_payload)
        outputs = []
        for delivery in deliveries:
            payload = self._build_generate_payload(
                logical_date=logical_date,
                delivery=delivery,
                manual_payload=manual_payload,
            )
            response_bytes, _ = self._signed_post(url, payload)
            parsed = json.loads(response_bytes.decode("utf-8"))
            rows = parsed.get("rows", [])
            if not isinstance(rows, list):
                raise ValueError("Batch delivery simulator response missing valid 'rows' array")
            fieldnames = [
                str(field)
                for field in parsed.get("fields", [])
                if isinstance(field, str)
            ]
            if not fieldnames and rows:
                fieldnames = list(rows[0].keys())
            csv_bytes = _csv_bytes(rows, fieldnames)
            delivery_date = str(
                payload.get("overrides", {}).get(
                    "delivery_date",
                    _default_delivery_date(logical_date),
                )
            )
            delivery_id = str(
                payload.get("overrides", {}).get(
                    "delivery_id",
                    f"{delivery.source_system_id}_{delivery.feed_type}_{delivery_date}",
                )
            )
            outputs.append(
                FetchOutput(
                    body=csv_bytes,
                    content_type="text/csv",
                    logical_date=logical_date,
                    suggested_object_name=delivery.object_name or f"{delivery_id}.csv",
                    metadata={
                        "preset_id": self.adapter_config.preset_id,
                        "source_route": route,
                        "source_system_id": delivery.source_system_id,
                        "delivery_id": delivery_id,
                        "delivery_date": delivery_date,
                        "feed_type": delivery.feed_type,
                        "row_count": str(parsed.get("row_count", len(rows))),
                        "file_format": "csv",
                    },
                )
            )
        return FetchResult(outputs=tuple(outputs))

    def _resolve_deliveries(
        self,
        manual_payload: dict[str, Any] | None,
    ) -> tuple[DeliverySpec, ...]:
        if manual_payload is None or "deliveries" not in manual_payload:
            return self.adapter_config.deliveries
        deliveries = manual_payload["deliveries"]
        if not isinstance(deliveries, list) or not deliveries:
            raise ValueError("Manual batch delivery request field 'deliveries' must be a non-empty array")
        return tuple(DeliverySpec.from_dict(item) for item in deliveries)

    def _build_generate_payload(
        self,
        *,
        logical_date: datetime | None,
        delivery: DeliverySpec,
        manual_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        payload = build_generate_payload(
            request_overrides=self.adapter_config.request_overrides,
            row_count=delivery.row_count or self.adapter_config.row_count,
            seed=_derive_delivery_seed(
                workflow_name=self.workflow_name,
                preset_id=self.adapter_config.preset_id,
                logical_date=logical_date,
                source_system_id=delivery.source_system_id,
                feed_type=delivery.feed_type,
                strategy=self.adapter_config.seed_strategy,
                fixed_seed=self.adapter_config.fixed_seed,
            ),
        )
        payload["overrides"] = dict(payload.get("overrides", {}))
        payload["overrides"].update(
            {
                "source_system_id": delivery.source_system_id,
                "feed_type": delivery.feed_type,
                "delivery_date": _default_delivery_date(logical_date),
            }
        )
        if delivery.request_overrides:
            payload["overrides"].update(delivery.request_overrides)
        if manual_payload is None:
            return payload

        request_overrides = manual_payload.get("request_overrides", {})
        if not isinstance(request_overrides, dict):
            raise ValueError("Manual batch delivery request field 'request_overrides' must be an object")
        payload["overrides"].update(request_overrides)

        manual_row_count = manual_payload.get("row_count")
        if manual_row_count is not None:
            if not isinstance(manual_row_count, int) or manual_row_count < 1:
                raise ValueError("Manual batch delivery request field 'row_count' must be a positive integer")
            payload["row_count"] = manual_row_count

        if "seed" in manual_payload:
            manual_seed = manual_payload["seed"]
            if manual_seed is None:
                payload.pop("seed", None)
            elif not isinstance(manual_seed, int):
                raise ValueError("Manual batch delivery request field 'seed' must be an integer")
            else:
                payload["seed"] = manual_seed
        return payload

    def _signed_post(self, url: str, payload: dict[str, Any]) -> tuple[bytes, str]:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        import botocore.session
        import time
        import urllib.error
        import urllib.request

        request_body = json.dumps(payload, sort_keys=True).encode("utf-8")
        session = botocore.session.get_session()
        credentials = session.get_credentials()
        if credentials is None:
            raise RuntimeError("Unable to resolve AWS credentials for simulator API request")

        request = AWSRequest(
            method="POST",
            url=url,
            data=request_body,
            headers={"Content-Type": "application/json"},
        )
        SigV4Auth(
            credentials=credentials.get_frozen_credentials(),
            service_name="execute-api",
            region_name=self.aws_region,
        ).add_auth(request)
        prepared_request = request.prepare()

        http_request = urllib.request.Request(
            url=url,
            data=request_body,
            method="POST",
            headers=dict(prepared_request.headers.items()),
        )

        last_error: RuntimeError | None = None
        for attempt in range(1, DEFAULT_HTTP_RETRY_ATTEMPTS + 1):
            try:
                with urllib.request.urlopen(http_request) as response:
                    return response.read(), response.headers.get_content_type()
            except urllib.error.HTTPError as exc:
                error_body = exc.read().decode("utf-8", errors="replace")
                last_error = RuntimeError(
                    f"Simulator API request failed with HTTP {exc.code}: {error_body}"
                )
                if (
                    exc.code not in TRANSIENT_HTTP_STATUS_CODES
                    or attempt == DEFAULT_HTTP_RETRY_ATTEMPTS
                ):
                    raise last_error from exc
            except urllib.error.URLError as exc:
                last_error = RuntimeError(
                    f"Simulator API request failed before receiving a response: {exc.reason}"
                )
                if attempt == DEFAULT_HTTP_RETRY_ATTEMPTS:
                    raise last_error from exc

            time.sleep(DEFAULT_HTTP_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)))

        if last_error is not None:
            raise last_error
        raise RuntimeError("Simulator API request failed before issuing an HTTP request")
