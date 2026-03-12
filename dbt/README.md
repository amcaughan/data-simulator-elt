# dbt

dbt projects and shared model scaffolding live here.

The initial expectation is:
- one shared dbt runtime pattern
- workflow-specific project configuration where isolation is useful
- staging, intermediate, and mart layers over curated workflow data

The first pass should treat workflow datasets as isolated sources rather than
one giant shared lake.

Direct dbt dependencies should be managed through:
- `requirements.in`
- `requirements.txt`
