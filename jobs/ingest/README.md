# ingest

Ingestion job code will live here.

Responsibilities:
- read the private simulator API URL from SSM
- call the simulator API from inside the shared dev VPC
- write raw API responses to the landing zone in S3
