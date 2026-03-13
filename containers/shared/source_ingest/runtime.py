from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import mimetypes

from source_ingest.adapters import build_adapter
from source_ingest.config import IngestConfig, LogicalSlice


@dataclass(frozen=True)
class LandingObject:
    bucket_name: str
    key: str
    body: bytes
    content_type: str
    metadata: dict[str, str]


def build_object_suffix(content_type: str) -> str:
    normalized_content_type = content_type.split(";", maxsplit=1)[0].strip().lower()
    if normalized_content_type.endswith("+json"):
        return ".json"
    suffix = mimetypes.guess_extension(normalized_content_type, strict=False)
    if suffix is not None:
        return suffix
    return ".bin"


def build_landing_key(
    config: IngestConfig,
    logical_slice: LogicalSlice,
    content_type: str,
) -> str:
    parts = [
        f"workflow={config.workflow_name}",
        f"adapter={config.source_adapter}",
        f"year={logical_slice.year}",
        f"month={logical_slice.month}",
        f"day={logical_slice.day}",
    ]
    if config.partition_granularity == "hour":
        parts.append(f"hour={logical_slice.hour}")
    parts.append(f"run_id={logical_slice.run_id}{build_object_suffix(content_type)}")
    return "/".join(parts)


def build_landing_metadata(
    config: IngestConfig,
    logical_slice: LogicalSlice,
    ingested_at: datetime,
    adapter_metadata: dict[str, str],
) -> dict[str, str]:
    metadata = {
        "workflow_name": config.workflow_name,
        "source_adapter": config.source_adapter,
        "logical_date": logical_slice.logical_date.isoformat(),
        "ingested_at": ingested_at.astimezone(UTC).isoformat(),
        "mode": config.mode,
        "partition_granularity": config.partition_granularity,
    }
    metadata.update(adapter_metadata)
    return metadata


class LandingWriter:
    def __init__(self, config: IngestConfig, s3_client):
        self.config = config
        self.s3_client = s3_client

    def write(
        self,
        logical_slice: LogicalSlice,
        body: bytes,
        content_type: str,
        adapter_metadata: dict[str, str],
    ) -> LandingObject:
        ingested_at = datetime.now(UTC)
        key = build_landing_key(self.config, logical_slice, content_type)
        metadata = build_landing_metadata(
            config=self.config,
            logical_slice=logical_slice,
            ingested_at=ingested_at,
            adapter_metadata=adapter_metadata,
        )
        self.s3_client.put_object(
            Bucket=self.config.landing_bucket_name,
            Key=key,
            Body=body,
            ContentType=content_type,
            Metadata=metadata,
        )
        return LandingObject(
            bucket_name=self.config.landing_bucket_name,
            key=key,
            body=body,
            content_type=content_type,
            metadata=metadata,
        )


def run_source_ingest(config: IngestConfig, s3_client) -> list[LandingObject]:
    adapter = build_adapter(config)
    writer = LandingWriter(config=config, s3_client=s3_client)
    results: list[LandingObject] = []

    for pull_request in config.iter_pull_requests():
        logical_slice = pull_request.logical_slice
        fetched = adapter.fetch(pull_request)
        landing_object = writer.write(
            logical_slice=logical_slice,
            body=fetched.body,
            content_type=fetched.content_type,
            adapter_metadata=fetched.metadata,
        )
        print(
            json.dumps(
                {
                    "event": "landing_object_written",
                    "bucket": landing_object.bucket_name,
                    "key": landing_object.key,
                    "logical_date": logical_slice.logical_date.isoformat(),
                    "mode": pull_request.mode,
                    "content_type": fetched.content_type,
                }
            )
        )
        results.append(landing_object)

    return results
