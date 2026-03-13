from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import mimetypes

from common.slices import LogicalSlice
from common.storage_layout import join_storage_path
from source_ingest.adapters import build_adapter
from source_ingest.adapters.base import (
    FetchOutput,
    FetchResult,
    LiveFetchRequest,
    MultiSliceFetchRequest,
    RequestedSlice,
    SliceFetchRequest,
    SourceFetchRequest,
)
from source_ingest.config import IngestConfig
from source_ingest.planning import FetchPlan, StorageTarget, build_fetch_plan


@dataclass(frozen=True)
class LandingObject:
    bucket_name: str
    key: str
    manifest_key: str
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


def build_landing_key(config: IngestConfig, storage_target: StorageTarget, content_type: str) -> str:
    return join_storage_path(
        base_prefix=config.landing_layout.base_prefix,
        partition_components=storage_target.partition_components,
        path_suffix=config.landing_layout.path_suffix,
        object_name=f"{storage_target.object_stem}{build_object_suffix(content_type)}",
    )


def build_manifest_key(config: IngestConfig, storage_target: StorageTarget) -> str:
    return join_storage_path(
        base_prefix=config.landing_layout.base_prefix,
        partition_components=storage_target.partition_components,
        path_suffix=config.landing_layout.path_suffix,
        object_name=f"{storage_target.object_stem}.manifest.json",
    )


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
        "slice_granularity": config.slice_granularity,
    }
    metadata.update(adapter_metadata)
    return metadata


class LandingWriter:
    def __init__(self, config: IngestConfig, s3_client):
        self.config = config
        self.s3_client = s3_client

    def write(
        self,
        request: SourceFetchRequest,
        storage_target: StorageTarget,
        body: bytes,
        content_type: str,
        adapter_metadata: dict[str, str],
    ) -> LandingObject:
        ingested_at = datetime.now(UTC)
        logical_slice = storage_target.logical_slice
        key = build_landing_key(self.config, storage_target, content_type)
        manifest_key = build_manifest_key(self.config, storage_target)
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
        manifest_body = build_manifest_body(
            config=self.config,
            request=request,
            storage_target=storage_target,
            payload_key=key,
            manifest_key=manifest_key,
            content_type=content_type,
            ingested_at=ingested_at,
            adapter_metadata=adapter_metadata,
        )
        self.s3_client.put_object(
            Bucket=self.config.landing_bucket_name,
            Key=manifest_key,
            Body=manifest_body,
            ContentType="application/json",
        )
        return LandingObject(
            bucket_name=self.config.landing_bucket_name,
            key=key,
            manifest_key=manifest_key,
            body=body,
            content_type=content_type,
            metadata=metadata,
        )


def serialize_requested_slice(requested_slice: RequestedSlice) -> dict[str, str]:
    return {
        "logical_date": requested_slice.logical_date.isoformat(),
        "slice_start": requested_slice.slice_start.isoformat(),
        "slice_end": requested_slice.slice_end.isoformat(),
        "granularity": requested_slice.granularity,
    }


def serialize_request(
    request: SourceFetchRequest,
    matched_logical_date: datetime,
) -> dict[str, object]:
    if isinstance(request, LiveFetchRequest):
        return {"kind": request.kind}
    if isinstance(request, SliceFetchRequest):
        return {
            "kind": request.kind,
            "slice": serialize_requested_slice(request.slice),
        }
    if isinstance(request, MultiSliceFetchRequest):
        requested_slices = request.slices
        matched_slice = next(
            requested_slice
            for requested_slice in requested_slices
            if requested_slice.logical_date == matched_logical_date
        )
        return {
            "kind": request.kind,
            "slice_count": len(requested_slices),
            "first_slice": serialize_requested_slice(requested_slices[0]),
            "last_slice": serialize_requested_slice(requested_slices[-1]),
            "matched_slice": serialize_requested_slice(matched_slice),
        }
    raise TypeError(f"Unsupported request type: {type(request)!r}")


def build_manifest_body(
    config: IngestConfig,
    request: SourceFetchRequest,
    storage_target: StorageTarget,
    payload_key: str,
    manifest_key: str,
    content_type: str,
    ingested_at: datetime,
    adapter_metadata: dict[str, str],
) -> bytes:
    logical_slice = storage_target.logical_slice
    manifest = {
        "schema_version": "1",
        "workflow_name": config.workflow_name,
        "source_adapter": config.source_adapter,
        "ingested_at": ingested_at.astimezone(UTC).isoformat(),
        "request": serialize_request(request, logical_slice.logical_date),
        "storage": {
            "bucket_name": config.landing_bucket_name,
            "payload_key": payload_key,
            "manifest_key": manifest_key,
            "base_prefix": config.landing_layout.base_prefix,
            "partition_fields": list(config.landing_layout.partition_fields),
            "path_suffix": list(config.landing_layout.path_suffix),
            "partition_components": [
                {"key": component.key, "value": component.value}
                for component in storage_target.partition_components
            ],
            "logical_date": logical_slice.logical_date.isoformat(),
            "slice_start": logical_slice.slice_start.isoformat(),
            "slice_end": logical_slice.slice_end.isoformat(),
            "slice_granularity": logical_slice.granularity,
            "run_id": logical_slice.run_id,
        },
        "payload": {
            "content_type": content_type,
            "metadata": adapter_metadata,
        },
        "adapter_config": config.source_adapter_config,
    }
    return json.dumps(manifest, sort_keys=True).encode("utf-8")


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
            request=plan.request,
            storage_target=storage_target,
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
                    "manifest_key": landing_object.manifest_key,
                    "logical_date": logical_slice.logical_date.isoformat(),
                    "request_kind": plan.request.kind,
                    "content_type": output.content_type,
                }
            )
        )
        results.append(landing_object)

    return results
