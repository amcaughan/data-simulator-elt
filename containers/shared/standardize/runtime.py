from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import io
import json
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from common.slices import LogicalSlice, parse_iso_datetime
from common.storage_layout import (
    build_partition_components,
    join_storage_path,
    trim_partition_fields_for_granularity,
)
from standardize.config import StandardizeConfig
from standardize.strategies import build_strategy
from standardize.strategies.base import (
    StandardizeInputObject,
    StandardizeOutput,
)


@dataclass(frozen=True)
class StandardizedSliceResult:
    bucket_name: str
    key: str
    manifest_key: str
    row_count: int
    source_object_count: int


def build_landing_prefix(config: StandardizeConfig, logical_slice: LogicalSlice) -> str:
    if config.landing_input_prefix is not None:
        prefix = config.landing_input_prefix.rstrip("/")
        return prefix + "/"

    partition_fields = trim_partition_fields_for_granularity(
        partition_fields=config.landing_layout.partition_fields,
        slice_granularity=config.output_slice_granularity,
    )
    partition_components = build_partition_components(
        partition_fields=partition_fields,
        logical_slice=logical_slice,
    )
    prefix = join_storage_path(
        base_prefix=config.landing_layout.base_prefix,
        partition_components=partition_components,
        path_suffix=config.landing_layout.path_suffix,
    )
    return prefix.rstrip("/") + "/"


def build_manual_landing_prefix(config: StandardizeConfig) -> str:
    return config.manual.input_prefix.rstrip("/") + "/"


def _is_manifest_key(key: str) -> bool:
    return key.endswith(".manifest.json")


def _is_within_output_slice(
    logical_slice: LogicalSlice,
    candidate_logical_date: datetime,
) -> bool:
    return logical_slice.contains(candidate_logical_date)


def build_processed_key(
    config: StandardizeConfig,
    logical_slice: LogicalSlice,
    object_name: str,
) -> str:
    partition_components = build_partition_components(
        partition_fields=config.processed_layout.partition_fields,
        logical_slice=logical_slice,
    )
    return join_storage_path(
        base_prefix=config.processed_layout.base_prefix,
        partition_components=partition_components,
        path_suffix=config.processed_layout.path_suffix,
        object_name=object_name,
    )


def build_manual_processed_key(
    config: StandardizeConfig,
    object_name: str,
) -> str:
    output_prefix = config.manual.output_prefix
    if output_prefix is None:
        return object_name
    return join_storage_path(
        base_prefix=output_prefix,
        partition_components=(),
        object_name=object_name,
    )


def build_processed_manifest_key(
    config: StandardizeConfig,
    logical_slice: LogicalSlice,
    object_name: str,
) -> str:
    return build_processed_key(
        config=config,
        logical_slice=logical_slice,
        object_name=f"_{object_name}.manifest.json",
    )


def build_manual_processed_manifest_key(
    config: StandardizeConfig,
    object_name: str,
) -> str:
    return build_manual_processed_key(
        config=config,
        object_name=f"_{object_name}.manifest.json",
    )


def _normalize_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return value


def _normalize_rows(rows: list[dict]) -> list[dict]:
    return [
        {key: _normalize_value(value) for key, value in row.items()}
        for row in rows
    ]


def _annotate_output_rows(
    output: StandardizeOutput,
    *,
    bundle_id: str,
    processed_key: str,
    manifest_key: str,
    standardized_at: datetime,
    input_object_count: int,
    logical_slice: LogicalSlice | None,
    row_count: int,
) -> list[dict]:
    bundle_metadata = {
        "_raw_bundle_id": bundle_id,
        "_raw_bundle_key": processed_key,
        "_raw_bundle_manifest_key": manifest_key,
        "_raw_standardized_at": standardized_at.astimezone(UTC).isoformat(),
        "_raw_bundle_logical_date": (
            None
            if logical_slice is None
            else logical_slice.logical_date.astimezone(UTC).isoformat()
        ),
        "_raw_bundle_granularity": (
            "manual" if logical_slice is None else logical_slice.granularity
        ),
        "_raw_input_object_count": input_object_count,
        "_raw_bundle_row_count": row_count,
    }
    return [
        {
            **row,
            **bundle_metadata,
        }
        for row in output.rows
    ]


def _write_parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(_normalize_rows(rows))
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def _list_landing_keys(
    config: StandardizeConfig,
    logical_slice: LogicalSlice,
    s3_client,
) -> list[str]:
    prefix = build_landing_prefix(config, logical_slice)
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=config.landing_bucket_name, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if _is_manifest_key(key):
                continue
            keys.append(key)
    return keys


def _list_manual_landing_keys(
    config: StandardizeConfig,
    s3_client,
) -> list[str]:
    prefix = build_manual_landing_prefix(config)
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=config.landing_bucket_name, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if _is_manifest_key(key):
                continue
            keys.append(key)
    return keys


def _build_standardize_inputs(
    config: StandardizeConfig,
    logical_slice: LogicalSlice,
    s3_client,
) -> list[StandardizeInputObject]:
    input_objects: list[StandardizeInputObject] = []
    for key in _list_landing_keys(config, logical_slice, s3_client):
        response = s3_client.get_object(Bucket=config.landing_bucket_name, Key=key)
        metadata = response.get("Metadata", {})
        raw_logical_date = metadata.get("logical_date")
        if raw_logical_date is not None and not _is_within_output_slice(
            logical_slice=logical_slice,
            candidate_logical_date=parse_iso_datetime(raw_logical_date),
        ):
            continue

        input_objects.append(
            StandardizeInputObject(
                key=key,
                payload=response["Body"].read(),
                metadata=metadata,
            )
        )
    return input_objects


def _build_manual_standardize_inputs(
    config: StandardizeConfig,
    s3_client,
) -> list[StandardizeInputObject]:
    input_objects: list[StandardizeInputObject] = []
    for key in _list_manual_landing_keys(config, s3_client):
        response = s3_client.get_object(Bucket=config.landing_bucket_name, Key=key)
        input_objects.append(
            StandardizeInputObject(
                key=key,
                payload=response["Body"].read(),
                metadata=response.get("Metadata", {}),
            )
        )
    return input_objects


def _resolve_output_object_name(
    logical_slice: LogicalSlice,
    output: StandardizeOutput,
    output_index: int,
    output_count: int,
) -> str:
    if output.suggested_object_name:
        return output.suggested_object_name
    if output_count == 1:
        return f"slice_id={logical_slice.run_id}.parquet"
    return f"slice_id={logical_slice.run_id}.part={output_index:02d}.parquet"


def _resolve_manual_output_object_name(
    config: StandardizeConfig,
    output: StandardizeOutput,
    output_index: int,
    output_count: int,
    run_id: str,
) -> str:
    if output_count > 1 and config.manual.object_name is not None:
        raise ValueError(
            "MANUAL_OBJECT_NAME may only be used when the standardize strategy returns "
            "exactly one output"
        )
    if config.manual.object_name is not None:
        return config.manual.object_name
    if output.suggested_object_name:
        return output.suggested_object_name
    if output_count == 1:
        return f"run_id={run_id}.parquet"
    return f"run_id={run_id}.part={output_index:02d}.parquet"


def build_manifest_body(
    config: StandardizeConfig,
    input_objects: list[StandardizeInputObject],
    output: StandardizeOutput,
    payload_key: str,
    manifest_key: str,
    standardized_at: datetime,
    row_count: int,
    logical_slice: LogicalSlice | None = None,
) -> bytes:
    if logical_slice is None:
        input_section: dict[str, object] = {
            "mode": "manual",
            "bucket_name": config.landing_bucket_name,
            "input_prefix": build_manual_landing_prefix(config),
            "object_count": len(input_objects),
            "keys": [input_object.key for input_object in input_objects],
        }
        output_section: dict[str, object] = {
            "mode": "manual",
            "bucket_name": config.processed_bucket_name,
            "payload_key": payload_key,
            "manifest_key": manifest_key,
            "output_prefix": config.manual.output_prefix,
        }
    else:
        input_section = {
            "mode": "temporal",
            "bucket_name": config.landing_bucket_name,
            "input_prefix": build_landing_prefix(config, logical_slice),
            "object_count": len(input_objects),
            "keys": [input_object.key for input_object in input_objects],
            "slice_selector_mode": config.slice_window.selector_mode,
            "landing_slice_granularity": config.landing_slice_granularity,
            "logical_date": logical_slice.logical_date.isoformat(),
            "slice_start": logical_slice.slice_start.isoformat(),
            "slice_end": logical_slice.slice_end.isoformat(),
            "slice_granularity": logical_slice.granularity,
            "run_id": logical_slice.run_id,
        }
        output_section = {
            "mode": "temporal",
            "bucket_name": config.processed_bucket_name,
            "payload_key": payload_key,
            "manifest_key": manifest_key,
            "base_prefix": config.processed_layout.base_prefix,
            "partition_fields": list(config.processed_layout.partition_fields),
            "path_suffix": list(config.processed_layout.path_suffix),
        }

    manifest = {
        "schema_version": "1",
        "workflow_name": config.workflow_name,
        "standardize_strategy": config.standardize_strategy,
        "planning_mode": config.planning_mode,
        "standardized_at": standardized_at.astimezone(UTC).isoformat(),
        "strategy_config": config.standardize_strategy_config,
        "input": input_section,
        "output": {
            **output_section,
            "row_count": row_count,
            "metadata": output.metadata,
            "suggested_object_name": output.suggested_object_name,
            "content_type": "application/x-parquet",
        },
    }
    return json.dumps(manifest, sort_keys=True).encode("utf-8")


def _write_processed_object(
    config: StandardizeConfig,
    s3_client,
    input_objects: list[StandardizeInputObject],
    output: StandardizeOutput,
    processed_key: str,
    manifest_key: str,
    metadata: dict[str, str],
    row_count: int,
    body: bytes,
    standardized_at: datetime,
    logical_slice: LogicalSlice | None = None,
) -> None:
    s3_client.put_object(
        Bucket=config.processed_bucket_name,
        Key=processed_key,
        Body=body,
        ContentType="application/octet-stream",
        Metadata=metadata,
    )
    s3_client.put_object(
        Bucket=config.processed_bucket_name,
        Key=manifest_key,
        Body=build_manifest_body(
            config=config,
            input_objects=input_objects,
            output=output,
            payload_key=processed_key,
            manifest_key=manifest_key,
            standardized_at=standardized_at,
            row_count=row_count,
            logical_slice=logical_slice,
        ),
        ContentType="application/json",
    )


def run_standardize(config: StandardizeConfig, s3_client) -> list[StandardizedSliceResult]:
    strategy = build_strategy(config)
    results: list[StandardizedSliceResult] = []

    if config.is_manual:
        input_objects = _build_manual_standardize_inputs(config, s3_client)
        if not input_objects:
            print(
                json.dumps(
                    {
                        "event": "standardize_manual_skipped",
                        "reason": "no_input_objects",
                        "input_prefix": build_manual_landing_prefix(config),
                    }
                )
            )
            return results

        strategy_result = strategy.process_manual(input_objects=input_objects)
        if not strategy_result.outputs:
            print(
                json.dumps(
                    {
                        "event": "standardize_manual_skipped",
                        "reason": "no_outputs_generated",
                        "input_object_count": len(input_objects),
                    }
                )
            )
            return results

        manual_run_id = str(uuid4())
        for output_index, output in enumerate(strategy_result.outputs, start=1):
            if not output.rows:
                continue

            object_name = _resolve_manual_output_object_name(
                config=config,
                output=output,
                output_index=output_index,
                output_count=len(strategy_result.outputs),
                run_id=manual_run_id,
            )
            processed_key = build_manual_processed_key(
                config=config,
                object_name=object_name,
            )
            manifest_key = build_manual_processed_manifest_key(
                config=config,
                object_name=object_name,
            )
            standardized_at = datetime.now(UTC)
            annotated_rows = _annotate_output_rows(
                output=output,
                bundle_id=manual_run_id,
                processed_key=processed_key,
                manifest_key=manifest_key,
                standardized_at=standardized_at,
                input_object_count=len(input_objects),
                logical_slice=None,
                row_count=len(output.rows),
            )
            annotated_output = StandardizeOutput(
                rows=annotated_rows,
                metadata=output.metadata,
                suggested_object_name=output.suggested_object_name,
            )
            body = _write_parquet_bytes(annotated_rows)
            metadata = {
                "workflow_name": config.workflow_name,
                "standardize_strategy": config.standardize_strategy,
                "planning_mode": config.planning_mode,
                "input_object_count": str(len(input_objects)),
                "row_count": str(len(annotated_rows)),
                "standardized_at": standardized_at.isoformat(),
                **output.metadata,
            }
            _write_processed_object(
                config=config,
                s3_client=s3_client,
                input_objects=input_objects,
                output=annotated_output,
                processed_key=processed_key,
                manifest_key=manifest_key,
                metadata=metadata,
                row_count=len(annotated_rows),
                body=body,
                standardized_at=standardized_at,
            )
            print(
                json.dumps(
                    {
                        "event": "processed_object_written",
                        "mode": "manual",
                        "bucket": config.processed_bucket_name,
                        "key": processed_key,
                        "input_object_count": len(input_objects),
                        "row_count": len(annotated_rows),
                    }
                )
            )
            results.append(
                StandardizedSliceResult(
                    bucket_name=config.processed_bucket_name,
                    key=processed_key,
                    manifest_key=manifest_key,
                    row_count=len(annotated_rows),
                    source_object_count=len(input_objects),
                )
            )
        return results

    for logical_slice in config.iter_slices():
        input_objects = _build_standardize_inputs(config, logical_slice, s3_client)
        if not input_objects:
            print(
                json.dumps(
                    {
                        "event": "standardize_slice_skipped",
                        "reason": "no_input_objects",
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "prefix": build_landing_prefix(config, logical_slice),
                    }
                )
            )
            continue

        strategy_result = strategy.process_slice(
            output_slice=logical_slice,
            input_objects=input_objects,
        )
        if not strategy_result.outputs:
            print(
                json.dumps(
                    {
                        "event": "standardize_slice_skipped",
                        "reason": "no_outputs_generated",
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "input_object_count": len(input_objects),
                    }
                )
            )
            continue

        for output_index, output in enumerate(strategy_result.outputs, start=1):
            if not output.rows:
                continue

            processed_key = build_processed_key(
                config=config,
                logical_slice=logical_slice,
                object_name=_resolve_output_object_name(
                    logical_slice=logical_slice,
                    output=output,
                    output_index=output_index,
                    output_count=len(strategy_result.outputs),
                ),
            )
            manifest_key = build_processed_manifest_key(
                config=config,
                logical_slice=logical_slice,
                object_name=processed_key.rsplit("/", maxsplit=1)[-1],
            )
            standardized_at = datetime.now(UTC)
            annotated_rows = _annotate_output_rows(
                output=output,
                bundle_id=logical_slice.run_id,
                processed_key=processed_key,
                manifest_key=manifest_key,
                standardized_at=standardized_at,
                input_object_count=len(input_objects),
                logical_slice=logical_slice,
                row_count=len(output.rows),
            )
            annotated_output = StandardizeOutput(
                rows=annotated_rows,
                metadata=output.metadata,
                suggested_object_name=output.suggested_object_name,
            )
            body = _write_parquet_bytes(annotated_rows)
            metadata = {
                "workflow_name": config.workflow_name,
                "standardize_strategy": config.standardize_strategy,
                "planning_mode": config.planning_mode,
                "logical_date": logical_slice.logical_date.astimezone(UTC).isoformat(),
                "output_slice_granularity": config.output_slice_granularity,
                "input_object_count": str(len(input_objects)),
                "row_count": str(len(annotated_rows)),
                "standardized_at": standardized_at.isoformat(),
                **output.metadata,
            }
            _write_processed_object(
                config=config,
                s3_client=s3_client,
                input_objects=input_objects,
                output=annotated_output,
                processed_key=processed_key,
                manifest_key=manifest_key,
                metadata=metadata,
                row_count=len(annotated_rows),
                body=body,
                standardized_at=standardized_at,
                logical_slice=logical_slice,
            )

            print(
                json.dumps(
                    {
                        "event": "processed_object_written",
                        "bucket": config.processed_bucket_name,
                        "key": processed_key,
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "input_object_count": len(input_objects),
                        "row_count": len(annotated_rows),
                    }
                )
            )
            results.append(
                StandardizedSliceResult(
                    bucket_name=config.processed_bucket_name,
                    key=processed_key,
                    manifest_key=manifest_key,
                    row_count=len(annotated_rows),
                    source_object_count=len(input_objects),
                )
            )

    return results
