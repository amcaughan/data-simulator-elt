from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from common.slices import LogicalSlice
from source_ingest.adapters.base import (
    AdapterCapabilities,
    FetchResult,
    HistoricalSlicePullRequest,
    LivePullRequest,
    SourceAdapter,
    SourcePullRequest,
)
from source_ingest.config import IngestConfig


VALID_SEED_STRATEGIES = {"derived", "fixed", "none"}


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
    logical_slice: LogicalSlice,
    strategy: str,
    fixed_seed: int | None,
) -> int | None:
    if strategy == "none":
        return None
    if strategy == "fixed":
        return fixed_seed

    namespace = "|".join(
        [
            workflow_name,
            preset_id,
            logical_slice.logical_date.isoformat(),
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


class SimulatorApiAdapter(SourceAdapter):
    capabilities = AdapterCapabilities(
        supported_pull_request_types=(LivePullRequest, HistoricalSlicePullRequest)
    )

    @classmethod
    def adapter_key(cls) -> str:
        return "simulator_api"

    @classmethod
    def from_ingest_config(cls, config: IngestConfig) -> "SimulatorApiAdapter":
        adapter_config = SimulatorApiConfig.from_dict(config.source_adapter_config)
        return cls(
            workflow_name=config.workflow_name,
            aws_region=config.aws_region,
            source_base_url=config.source_base_url,
            adapter_config=adapter_config,
        )

    def __init__(
        self,
        workflow_name: str,
        aws_region: str,
        source_base_url: str | None,
        adapter_config: SimulatorApiConfig,
    ):
        if source_base_url in {None, ""}:
            raise ValueError(
                "Source adapter 'simulator_api' requires SOURCE_BASE_URL to be set"
            )

        self.workflow_name = workflow_name
        self.aws_region = aws_region
        self.source_base_url = source_base_url
        self.adapter_config = adapter_config

    def _fetch(self, pull_request: SourcePullRequest) -> FetchResult:
        route = f"/v1/presets/{self.adapter_config.preset_id}/generate"
        url = urllib.parse.urljoin(
            self.source_base_url.rstrip("/") + "/",
            route.lstrip("/"),
        )
        payload = self._build_payload(pull_request)
        response_bytes, content_type = self._signed_post(url, payload)
        parsed = json.loads(response_bytes.decode("utf-8"))
        return FetchResult(
            body=response_bytes,
            content_type=content_type,
            metadata=self._build_response_metadata(parsed=parsed, route=route),
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

    def _build_payload(self, pull_request: SourcePullRequest) -> dict[str, Any]:
        if isinstance(pull_request, LivePullRequest):
            return self._build_live_payload(pull_request)
        if isinstance(pull_request, HistoricalSlicePullRequest):
            return self._build_historical_payload(pull_request)
        raise TypeError(f"Unsupported pull request type: {type(pull_request)!r}")

    def _build_live_payload(self, pull_request: LivePullRequest) -> dict[str, Any]:
        return self._build_generate_payload_for_slice(pull_request.logical_slice)

    def _build_historical_payload(
        self,
        pull_request: HistoricalSlicePullRequest,
    ) -> dict[str, Any]:
        return self._build_generate_payload_for_slice(pull_request.logical_slice)

    def _build_generate_payload_for_slice(
        self,
        logical_slice: LogicalSlice,
    ) -> dict[str, Any]:
        return build_generate_payload(
            request_overrides=self.adapter_config.request_overrides,
            row_count=self.adapter_config.row_count,
            seed=derive_seed(
                workflow_name=self.workflow_name,
                preset_id=self.adapter_config.preset_id,
                logical_slice=logical_slice,
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
