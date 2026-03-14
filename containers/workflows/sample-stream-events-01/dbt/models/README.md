# models

dbt models will be organized here by layer:
- `staging/`
- `intermediate/`
- `marts/`

Within those layers, workflow-specific model groupings should stay clear enough
that one workload can be reasoned about without reading all of the others.
