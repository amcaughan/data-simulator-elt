from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq

from common.slices import GRANULARITY_ORDER, LogicalSlice
from standardize.config import StandardizeConfig
from standardize.parsers import build_parser


@dataclass(frozen=True)
class StandardizedSliceResult:
    bucket_name: str
    key: str
    row_count: int
    source_object_count: int


def build_landing_prefix(config: StandardizeConfig, logical_slice: LogicalSlice) -> str:
    prefix = config.landing_input_prefix or "/".join(
        [
            f"workflow={config.workflow_name}",
            f"adapter={config.source_adapter}",
        ]
    )

    parts = [
        prefix.rstrip("/"),
        f"year={logical_slice.year}",
        f"month={logical_slice.month}",
        f"day={logical_slice.day}",
    ]

    if (
        config.output_partition_granularity == "hour"
        and config.landing_partition_granularity == "hour"
    ):
        parts.append(f"hour={logical_slice.hour}")

    return "/".join(parts) + "/"


def build_processed_key(config: StandardizeConfig, logical_slice: LogicalSlice) -> str:
    parts = [
        config.processed_output_prefix.strip("/"),
        f"workflow={config.workflow_name}",
        f"adapter={config.source_adapter}",
        f"year={logical_slice.year}",
        f"month={logical_slice.month}",
        f"day={logical_slice.day}",
    ]
    if config.output_partition_granularity == "hour":
        parts.append(f"hour={logical_slice.hour}")
    parts.append(f"slice_id={logical_slice.run_id}.parquet")
    return "/".join(parts)


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
            keys.append(item["Key"])
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
        for key in landing_keys:
            response = s3_client.get_object(Bucket=config.landing_bucket_name, Key=key)
            body = response["Body"].read()
            rows.extend(
                parser.parse_landing_object(
                    logical_slice=logical_slice,
                    key=key,
                    payload=body,
                    metadata=response.get("Metadata", {}),
                )
            )

        if not rows:
            print(
                json.dumps(
                    {
                        "event": "standardize_slice_skipped",
                        "reason": "no_rows_parsed",
                        "logical_date": logical_slice.logical_date.isoformat(),
                        "landing_object_count": len(landing_keys),
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
            "output_partition_granularity": config.output_partition_granularity,
            "landing_object_count": str(len(landing_keys)),
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
                    "landing_object_count": len(landing_keys),
                    "row_count": len(rows),
                }
            )
        )
        results.append(
            StandardizedSliceResult(
                bucket_name=config.processed_bucket_name,
                key=processed_key,
                row_count=len(rows),
                source_object_count=len(landing_keys),
            )
        )

    return results
