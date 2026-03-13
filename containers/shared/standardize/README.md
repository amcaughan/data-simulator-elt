Normalize partitioned landing data into partitioned Parquet in the `processed`
layer.

This runtime is intentionally generic at the orchestration level:
- iterate output slices
- read input objects from the configured landing layout
- hand those objects to a source-specific standardize strategy
- write one or more Parquet outputs into the configured processed layout

The source-specific behavior lives under `strategies/`.
