from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq

from common.slices import LogicalSlice, parse_iso_datetime
from common.storage_layout import build_partition_components, default_partition_fields, join_storage_path, trim_partition_fields_for_granularity
from standardize.config import StandardizeConfig
from standardize.parsers import build_parser


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


def build_processed_key(config: StandardizeConfig, logical_slice: LogicalSlice) -> str:
    partition_components = build_partition_components(
        partition_fields=default_partition_fields(config.output_slice_granularity),
        logical_slice=logical_slice,
    )
    return join_storage_path(
        base_prefix=config.processed_output_prefix,
        partition_components=partition_components,
        object_name=f"slice_id={logical_slice.run_id}.parquet",
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


def _list_landing_keys(config: StandardizeConfig, logical_slice: LogicalSlice, s3_client) -> list[str]:
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


def run_standardize(config: StandardizeConfig, s3_client) -> list[StandardizedSliceResult]:
    parser = build_parser(config)
    results: list[StandardizedSliceResult] = []

    for logical_slice in config.iter_slices():
        landing_keys = _list_landing_keys(config, logical_slice, s3_client)
        if not landing_keys:
            print(
                json.dumps(
                    {
                        "event": "standardize_slice_skipped",
                        "reason": "no_landing_objects",
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "prefix": build_landing_prefix(config, logical_slice),
                    }
                )
            )
            continue

        rows: list[dict] = []
        matched_landing_key_count = 0
        for key in landing_keys:
            response = s3_client.get_object(Bucket=config.landing_bucket_name, Key=key)
            metadata = response.get("Metadata", {})
            raw_logical_date = metadata.get("logical_date")
            if raw_logical_date is not None and not _is_within_output_slice(
                logical_slice=logical_slice,
                candidate_logical_date=parse_iso_datetime(raw_logical_date),
            ):
                continue
            matched_landing_key_count += 1
            body = response["Body"].read()
            rows.extend(
                parser.parse_landing_object(
                    logical_slice=logical_slice,
                    key=key,
                    payload=body,
                    metadata=metadata,
                )
            )

        if not rows:
            print(
                json.dumps(
                    {
                        "event": "standardize_slice_skipped",
                        "reason": "no_rows_parsed",
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "landing_object_count": matched_landing_key_count,
                    }
                )
            )
            continue

        processed_key = build_processed_key(config, logical_slice)
        body = _write_parquet_bytes(rows)
        metadata = {
            "workflow_name": config.workflow_name,
            "source_adapter": config.source_adapter,
            "logical_date": logical_slice.logical_date.astimezone(UTC).isoformat(),
            "output_slice_granularity": config.output_slice_granularity,
            "landing_object_count": str(matched_landing_key_count),
            "row_count": str(len(rows)),
            "standardized_at": datetime.now(UTC).isoformat(),
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
                    "landing_object_count": matched_landing_key_count,
                    "row_count": len(rows),
                }
            )
        )
        results.append(
            StandardizedSliceResult(
                bucket_name=config.processed_bucket_name,
                key=processed_key,
                row_count=len(rows),
                source_object_count=matched_landing_key_count,
            )
        )

    return results
