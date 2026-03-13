from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import io
import json

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


def _normalize_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return value


def _normalize_rows(rows: list[dict]) -> list[dict]:
    return [
        {key: _normalize_value(value) for key, value in row.items()}
        for row in rows
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


def run_standardize(config: StandardizeConfig, s3_client) -> list[StandardizedSliceResult]:
    strategy = build_strategy(config)
    results: list[StandardizedSliceResult] = []

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
            body = _write_parquet_bytes(output.rows)
            metadata = {
                "workflow_name": config.workflow_name,
                "standardize_strategy": config.standardize_strategy,
                "logical_date": logical_slice.logical_date.astimezone(UTC).isoformat(),
                "output_slice_granularity": config.output_slice_granularity,
                "input_object_count": str(len(input_objects)),
                "row_count": str(len(output.rows)),
                "standardized_at": datetime.now(UTC).isoformat(),
                **output.metadata,
            }
            s3_client.put_object(
                Bucket=config.processed_bucket_name,
                Key=processed_key,
                Body=body,
                ContentType="application/octet-stream",
                Metadata=metadata,
            )

            print(
                json.dumps(
                    {
                        "event": "processed_object_written",
                        "bucket": config.processed_bucket_name,
                        "key": processed_key,
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "input_object_count": len(input_objects),
                        "row_count": len(output.rows),
                    }
                )
            )
            results.append(
                StandardizedSliceResult(
                    bucket_name=config.processed_bucket_name,
                    key=processed_key,
                    row_count=len(output.rows),
                    source_object_count=len(input_objects),
                )
            )

    return results
