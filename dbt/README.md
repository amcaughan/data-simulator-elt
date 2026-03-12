# dbt

The dbt project for Athena transforms lives here.

The expected model flow is:
- source and staging models over the raw standardized zone
- intermediate models for business logic
- mart models for UI and analytics consumption

Direct dbt dependencies should be managed through:
- `requirements.in`
- `requirements.txt`
