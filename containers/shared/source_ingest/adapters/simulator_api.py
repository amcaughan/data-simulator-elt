from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from source_ingest.adapters.base import (
    AdapterCapabilities,
    FetchOutput,
    FetchResult,
    LiveFetchRequest,
    MultiSliceFetchRequest,
    RequestedSlice,
    SliceFetchRequest,
    SourceAdapter,
    SourceFetchRequest,
)
from source_ingest.config import IngestConfig


VALID_SEED_STRATEGIES = {"derived", "fixed", "none"}


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def build_generate_payload(
    request_overrides: dict[str, Any],
    row_count: int,
    seed: int | None,
) -> dict[str, Any]:
    payload = dict(request_overrides)
    payload.setdefault("row_count", row_count)
    if seed is not None:
        payload.setdefault("seed", seed)
    return payload


def derive_seed(
    workflow_name: str,
    preset_id: str,
    logical_date: datetime | None,
    strategy: str,
    fixed_seed: int | None,
) -> int | None:
    if strategy == "none":
        return None
    if strategy == "fixed":
        return fixed_seed
    if logical_date is None:
        return None

    namespace = "|".join(
        [
            workflow_name,
            preset_id,
            logical_date.isoformat(),
        ]
    )
    digest = hashlib.sha256(namespace.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % (2**31)


@dataclass(frozen=True)
class SimulatorApiConfig:
    preset_id: str
    row_count: int
    seed_strategy: str
    fixed_seed: int | None
    request_overrides: dict[str, Any]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SimulatorApiConfig":
        preset_id = value.get("preset_id")
        row_count = value.get("row_count")
        seed_strategy = value.get("seed_strategy", "derived")
        fixed_seed = value.get("fixed_seed")
        request_overrides = value.get("request_overrides", {})

        if not isinstance(preset_id, str) or preset_id == "":
            raise ValueError(
                "Simulator API adapter config requires a non-empty 'preset_id'"
            )
        if not isinstance(row_count, int) or row_count < 1:
            raise ValueError(
                "Simulator API adapter config requires 'row_count' to be a positive integer"
            )
        if seed_strategy not in VALID_SEED_STRATEGIES:
            raise ValueError(
                f"Simulator API adapter config 'seed_strategy' must be one of {sorted(VALID_SEED_STRATEGIES)}"
            )
        if seed_strategy == "fixed" and not isinstance(fixed_seed, int):
            raise ValueError(
                "Simulator API adapter config requires integer 'fixed_seed' when seed_strategy='fixed'"
            )
        if not isinstance(request_overrides, dict):
            raise ValueError(
                "Simulator API adapter config field 'request_overrides' must be a JSON object"
            )

        return cls(
            preset_id=preset_id,
            row_count=row_count,
            seed_strategy=seed_strategy,
            fixed_seed=fixed_seed,
            request_overrides=request_overrides,
        )


@dataclass(frozen=True)
class SimulatorApiRuntimeConfig:
    source_base_url: str

    @classmethod
    def from_env(cls) -> "SimulatorApiRuntimeConfig":
        return cls(source_base_url=_require_env("SOURCE_BASE_URL"))


class SimulatorApiAdapter(SourceAdapter):
    capabilities = AdapterCapabilities(
        supported_request_types=(
            LiveFetchRequest,
            SliceFetchRequest,
            MultiSliceFetchRequest,
        )
    )

    @classmethod
    def adapter_key(cls) -> str:
        return "simulator_api"

    @classmethod
    def from_ingest_config(cls, config: IngestConfig) -> "SimulatorApiAdapter":
        adapter_config = SimulatorApiConfig.from_dict(config.source_adapter_config)
        runtime_config = SimulatorApiRuntimeConfig.from_env()
        return cls(
            workflow_name=config.workflow_name,
            aws_region=config.aws_region,
            runtime_config=runtime_config,
            adapter_config=adapter_config,
        )

    def __init__(
        self,
        workflow_name: str,
        aws_region: str,
        runtime_config: SimulatorApiRuntimeConfig,
        adapter_config: SimulatorApiConfig,
    ):
        self.workflow_name = workflow_name
        self.aws_region = aws_region
        self.runtime_config = runtime_config
        self.adapter_config = adapter_config

    def _fetch(self, request: SourceFetchRequest) -> FetchResult:
        if isinstance(request, LiveFetchRequest):
            return self._fetch_live(request)
        if isinstance(request, SliceFetchRequest):
            return self._fetch_slice(request)
        if isinstance(request, MultiSliceFetchRequest):
            return self._fetch_multi_slice(request)
        raise TypeError(f"Unsupported request type: {type(request)!r}")

    def _fetch_live(self, request: LiveFetchRequest) -> FetchResult:
        output = self._fetch_generate_output(logical_date=None)
        return FetchResult(outputs=(output,))

    def _fetch_slice(self, request: SliceFetchRequest) -> FetchResult:
        output = self._fetch_generate_output(logical_date=request.slice.logical_date)
        return FetchResult(outputs=(output,))

    def _fetch_multi_slice(self, request: MultiSliceFetchRequest) -> FetchResult:
        return FetchResult(
            outputs=tuple(
                self._fetch_generate_output(logical_date=requested_slice.logical_date)
                for requested_slice in request.slices
            )
        )

    def _fetch_generate_output(self, logical_date: datetime | None) -> FetchOutput:
        route = f"/v1/presets/{self.adapter_config.preset_id}/generate"
        url = urllib.parse.urljoin(
            self.runtime_config.source_base_url.rstrip("/") + "/",
            route.lstrip("/"),
        )
        payload = self._build_generate_payload(logical_date)
        response_bytes, content_type = self._signed_post(url, payload)
        parsed = json.loads(response_bytes.decode("utf-8"))
        return FetchOutput(
            body=response_bytes,
            content_type=content_type,
            metadata=self._build_response_metadata(parsed=parsed, route=route),
            logical_date=logical_date,
        )

    def _build_response_metadata(
        self,
        parsed: dict[str, Any],
        route: str,
    ) -> dict[str, str]:
        metadata = {
            "preset_id": self.adapter_config.preset_id,
            "source_route": route,
        }
        row_count = parsed.get("row_count")
        if isinstance(row_count, int):
            metadata["row_count"] = str(row_count)
        return metadata

    def _build_generate_payload(self, logical_date: datetime | None) -> dict[str, Any]:
        return build_generate_payload(
            request_overrides=self.adapter_config.request_overrides,
            row_count=self.adapter_config.row_count,
            seed=derive_seed(
                workflow_name=self.workflow_name,
                preset_id=self.adapter_config.preset_id,
                logical_date=logical_date,
                strategy=self.adapter_config.seed_strategy,
                fixed_seed=self.adapter_config.fixed_seed,
            ),
        )

    def _signed_post(self, url: str, payload: dict[str, Any]) -> tuple[bytes, str]:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        import botocore.session

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

        try:
            with urllib.request.urlopen(http_request) as response:
                return response.read(), response.headers.get_content_type()
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Simulator API request failed with HTTP {exc.code}: {error_body}"
            ) from exc
