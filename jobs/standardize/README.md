Normalize landing-zone source payloads into Parquet in the `processed` layer.

This runtime is intentionally generic at the orchestration level:
- iterate logical slices
- read landing objects for each slice
- parse them through a source-specific parser
- write one Parquet file per output slice into `processed/raw`

The source-specific shape lives under `parsers/`.
