from __future__ import annotations

import boto3

from standardize.config import StandardizeConfig
from standardize.runtime import run_standardize


def main() -> None:
    config = StandardizeConfig.from_env()
    s3_client = boto3.client("s3", region_name=config.aws_region)
    run_standardize(config=config, s3_client=s3_client)


if __name__ == "__main__":
    main()
