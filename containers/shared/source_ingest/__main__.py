from __future__ import annotations

from source_ingest.config import IngestConfig
from source_ingest.runtime import run_source_ingest


def main() -> int:
    import boto3

    config = IngestConfig.from_env()
    s3_client = boto3.client("s3", region_name=config.aws_region)
    run_source_ingest(config=config, s3_client=s3_client)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
