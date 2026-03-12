# jobs

Containerized ELT job code lives here.

The initial job split is:
- `ingest/`
  calls the private simulator API and writes raw payloads to the landing zone
- `standardize/`
  validates landing data and writes normalized Parquet to the raw zone

Direct Python dependencies for these jobs should be managed through:
- `requirements.in`
- `requirements.txt`
