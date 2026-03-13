from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json

from source_ingest.adapters import build_adapter
from source_ingest.config import IngestConfig, LogicalSlice


@dataclass(frozen=True)
class LandingObject:
    bucket_name: str
    key: str
    body: bytes
    content_type: str
    metadata: dict[str, str]


def build_landing_key(config: IngestConfig, logical_slice: LogicalSlice) -> str:
    parts = [
        f"workflow={config.workflow_name}",
        f"adapter={config.source_adapter}",
        f"year={logical_slice.year}",
        f"month={logical_slice.month}",
        f"day={logical_slice.day}",
    ]
    if config.partition_granularity == "hour":
        parts.append(f"hour={logical_slice.hour}")
    parts.append(f"run_id={logical_slice.run_id}.json")
    return "/".join(parts)


def build_landing_metadata(
    config: IngestConfig,
    logical_slice: LogicalSlice,
    ingested_at: datetime,
    result_row_count: int | None,
    source_metadata: dict[str, str],
    route: str | None,
) -> dict[str, str]:
    metadata = {
        "workflow_name": config.workflow_name,
        "source_adapter": config.source_adapter,
        "logical_date": logical_slice.logical_date.isoformat(),
        "ingested_at": ingested_at.astimezone(UTC).isoformat(),
        "mode": config.mode,
        "partition_granularity": config.partition_granularity,
    }
    if result_row_count is not None:
        metadata["row_count"] = str(result_row_count)
    if route is not None:
        metadata["route"] = route
    metadata.update(source_metadata)
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
        result_row_count: int | None,
        source_metadata: dict[str, str],
        route: str | None,
    ) -> LandingObject:
        ingested_at = datetime.now(UTC)
        key = build_landing_key(self.config, logical_slice)
        metadata = build_landing_metadata(
            config=self.config,
            logical_slice=logical_slice,
            ingested_at=ingested_at,
            result_row_count=result_row_count,
            source_metadata=source_metadata,
            route=route,
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
            result_row_count=fetched.row_count,
            source_metadata=fetched.source_metadata,
            route=fetched.route,
        )
        print(
            json.dumps(
                {
                    "event": "landing_object_written",
                    "bucket": landing_object.bucket_name,
                    "key": landing_object.key,
                    "logical_date": logical_slice.logical_date.isoformat(),
                    "route": fetched.route,
                    "row_count": fetched.row_count,
                }
            )
        )
        results.append(landing_object)

    return results
