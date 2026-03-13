from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import mimetypes

from common.slices import LogicalSlice
from source_ingest.adapters import build_adapter
from source_ingest.adapters.base import FetchOutput, FetchResult
from source_ingest.config import IngestConfig
from source_ingest.planning import FetchPlan, StorageTarget, build_fetch_plan


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
    return ""


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


def map_fetch_outputs(
    plan: FetchPlan,
    fetched: FetchResult,
) -> list[tuple[StorageTarget, FetchOutput]]:
    if len(plan.storage_targets) == 1 and len(fetched.outputs) == 1:
        output = fetched.outputs[0]
        target = plan.storage_targets[0]
        if output.logical_date not in {None, target.logical_slice.logical_date}:
            raise ValueError(
                "Single-output fetch result logical_date does not match the planned "
                "storage target"
            )
        return [(target, output)]

    outputs_by_date: dict[datetime, FetchOutput] = {}
    for output in fetched.outputs:
        if output.logical_date is None:
            raise ValueError(
                "Multi-target fetch results must label each output with logical_date"
            )
        if output.logical_date in outputs_by_date:
            raise ValueError(
                f"Duplicate fetch output logical_date: {output.logical_date.isoformat()}"
            )
        outputs_by_date[output.logical_date] = output

    assignments: list[tuple[StorageTarget, FetchOutput]] = []
    for target in plan.storage_targets:
        logical_date = target.logical_slice.logical_date
        try:
            output = outputs_by_date.pop(logical_date)
        except KeyError as exc:
            raise ValueError(
                f"Missing fetch output for logical_date {logical_date.isoformat()}"
            ) from exc
        assignments.append((target, output))

    if outputs_by_date:
        unexpected_dates = ", ".join(
            logical_date.isoformat() for logical_date in sorted(outputs_by_date)
        )
        raise ValueError(f"Received unexpected fetch outputs for logical dates: {unexpected_dates}")

    return assignments


def run_source_ingest(config: IngestConfig, s3_client) -> list[LandingObject]:
    adapter = build_adapter(config)
    writer = LandingWriter(config=config, s3_client=s3_client)
    results: list[LandingObject] = []

    plan = build_fetch_plan(config)
    fetched = adapter.fetch(plan.request)

    for storage_target, output in map_fetch_outputs(plan, fetched):
        logical_slice = storage_target.logical_slice
        landing_object = writer.write(
            logical_slice=logical_slice,
            body=output.body,
            content_type=output.content_type,
            adapter_metadata=output.metadata,
        )
        print(
            json.dumps(
                {
                    "event": "landing_object_written",
                    "bucket": landing_object.bucket_name,
                    "key": landing_object.key,
                    "logical_date": logical_slice.logical_date.isoformat(),
                    "request_kind": plan.request.kind,
                    "content_type": output.content_type,
                }
            )
        )
        results.append(landing_object)

    return results
