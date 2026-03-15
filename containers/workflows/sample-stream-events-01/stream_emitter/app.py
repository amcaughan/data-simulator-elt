from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from typing import Any
import urllib.error
import urllib.parse
import urllib.request
import uuid

def utc_now() -> datetime:
    return datetime.now(UTC)


def to_iso8601(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def derive_seed(
    *,
    workflow_name: str,
    preset_id: str,
    batch_started_at: str,
    emission_index: int,
) -> int:
    digest = hashlib.sha256(
        f"{workflow_name}|{preset_id}|{batch_started_at}|{emission_index}".encode("utf-8")
    ).hexdigest()
    return int(digest[:8], 16)


def build_sample_url(*, base_url: str, preset_id: str) -> str:
    route = f"/v1/presets/{preset_id}/sample"
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", route.lstrip("/"))


def signed_post(url: str, payload: dict[str, Any]) -> dict[str, Any]:
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
        region_name=require_env("AWS_REGION"),
    ).add_auth(request)

    prepared = request.prepare()
    http_request = urllib.request.Request(
        prepared.url,
        data=request_body,
        method="POST",
        headers=dict(prepared.headers.items()),
    )
    try:
        with urllib.request.urlopen(http_request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Simulator API request failed with {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Simulator API request failed: {exc.reason}") from exc


def build_stream_event(
    *,
    workflow_name: str,
    preset_id: str,
    sample_payload: dict[str, Any],
    source_seed: int,
    batch_started_at: str,
    emitted_at: str,
    emission_index: int,
) -> dict[str, Any]:
    row = sample_payload.get("row")
    if not isinstance(row, dict):
        raise RuntimeError("Simulator sample payload did not include an object 'row'")

    event = {
        "workflow_name": workflow_name,
        "source_preset_id": preset_id,
        "source_schema_version": sample_payload.get("schema_version"),
        "source_scenario_name": sample_payload.get("scenario_name"),
        "source_seed": source_seed,
        "emitter_event_id": str(uuid.uuid4()),
        "emission_batch_started_at": batch_started_at,
        "emitted_at": emitted_at,
        "emission_index": emission_index,
    }
    event.update(row)
    return event


def build_kinesis_record(event: dict[str, Any]) -> dict[str, str | bytes]:
    partition_key = str(event.get("device_id") or event.get("site_id") or event["emitter_event_id"])
    return {
        "Data": (json.dumps(event, sort_keys=True) + "\n").encode("utf-8"),
        "PartitionKey": partition_key,
    }


@dataclass(frozen=True)
class RuntimeConfig:
    workflow_name: str
    preset_id: str
    emission_rate_per_minute: int
    stream_name: str
    simulator_api_url: str
    aws_region: str

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        emission_rate = int(require_env("EMISSION_RATE_PER_MINUTE"))
        if emission_rate < 1:
            raise RuntimeError("EMISSION_RATE_PER_MINUTE must be a positive integer")
        return cls(
            workflow_name=require_env("WORKFLOW_NAME"),
            preset_id=require_env("PRESET_ID"),
            emission_rate_per_minute=emission_rate,
            stream_name=require_env("STREAM_NAME"),
            simulator_api_url=require_env("SIMULATOR_API_URL"),
            aws_region=require_env("AWS_REGION"),
        )


def emit_batch(config: RuntimeConfig) -> int:
    import boto3

    sample_url = build_sample_url(base_url=config.simulator_api_url, preset_id=config.preset_id)
    batch_started_at = to_iso8601(utc_now())
    records = []

    for emission_index in range(config.emission_rate_per_minute):
        source_seed = derive_seed(
            workflow_name=config.workflow_name,
            preset_id=config.preset_id,
            batch_started_at=batch_started_at,
            emission_index=emission_index,
        )
        sample_payload = signed_post(sample_url, {"seed": source_seed})
        event = build_stream_event(
            workflow_name=config.workflow_name,
            preset_id=config.preset_id,
            sample_payload=sample_payload,
            source_seed=source_seed,
            batch_started_at=batch_started_at,
            emitted_at=to_iso8601(utc_now()),
            emission_index=emission_index,
        )
        records.append(build_kinesis_record(event))

    client = boto3.client("kinesis", region_name=config.aws_region)
    response = client.put_records(StreamName=config.stream_name, Records=records)
    failed_count = int(response.get("FailedRecordCount", 0))
    if failed_count:
        raise RuntimeError(f"Failed to publish {failed_count} records to stream {config.stream_name}")
    return len(records)


def main() -> int:
    config = RuntimeConfig.from_env()
    record_count = emit_batch(config)
    print(
        json.dumps(
            {
                "workflow_name": config.workflow_name,
                "preset_id": config.preset_id,
                "stream_name": config.stream_name,
                "record_count": record_count,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
