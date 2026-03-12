Frequent scheduled polling workflow.

Intent:
- repeatedly pull generated benchmark data from the simulator API
- land exact source responses into the landing bucket
- support later standardization into processed snapshots
- support dbt models for polling-style ingestion, repeated snapshots, and rolled-up event summaries
