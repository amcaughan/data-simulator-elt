from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from source_ingest.config import IngestConfig, LogicalSlice


@dataclass(frozen=True)
class FetchResult:
    body: bytes
    content_type: str
    row_count: int | None
    route: str


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


class SimulatorApiAdapter:
    def __init__(self, config: IngestConfig):
        self.config = config

    def fetch(self, logical_slice: LogicalSlice) -> FetchResult:
        route = f"/v1/presets/{self.config.preset_id}/generate"
        url = urllib.parse.urljoin(
            self.config.simulator_api_url.rstrip("/") + "/",
            route.lstrip("/"),
        )
        payload = build_generate_payload(
            request_overrides=self.config.request_overrides,
            row_count=self.config.row_count,
            seed=logical_slice.seed,
        )
        response_bytes, content_type = self._signed_post(url, payload)
        parsed = json.loads(response_bytes.decode("utf-8"))
        row_count = parsed.get("row_count")
        return FetchResult(
            body=response_bytes,
            content_type=content_type,
            row_count=row_count if isinstance(row_count, int) else None,
            route=route,
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
            region_name=self.config.aws_region,
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
