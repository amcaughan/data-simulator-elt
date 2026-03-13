Normalize partitioned landing data into partitioned Parquet in the `processed`
layer.

This runtime is intentionally generic at the orchestration level:
- iterate output slices
- read input objects from the configured landing layout
- hand those objects to a source-specific standardize strategy
- write one or more Parquet outputs into the configured processed layout
- write a sidecar manifest for each processed output

The source-specific behavior lives under `strategies/`.

`standardize` supports two planning modes:
- `temporal`: gather landing objects for each output slice, standardize them, and
  write partitioned Parquet outputs
- `manual`: read all non-manifest objects under `MANUAL_INPUT_PREFIX`, hand them
  to the strategy once, and write the resulting Parquet output(s) under
  `MANUAL_OUTPUT_PREFIX`
