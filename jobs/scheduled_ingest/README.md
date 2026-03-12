Scheduled pull-based ingestion runtime.

Responsibilities:
- read the private simulator API URL from SSM
- call the simulator API from inside the shared VPC
- write workflow-specific landing data to isolated storage
- perform lightweight normalization where that belongs in the ingest path
