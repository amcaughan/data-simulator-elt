Stream-oriented sampled-event workflow.

Intent:
- bridge simulator preset sampling into a stream transport
- keep the workflow name focused on transport shape rather than fake business domain
- support downstream processing of sampled events and late processed aggregation

This workflow includes a simulated upstream producer.
The stream emitter should be understood as test-source plumbing, not the core
ELT transformation logic.

This workflow owns:
- its simulated upstream emitter under `containers/workflows/sample-stream-events-01/stream_emitter/`
- its dbt project under `containers/workflows/sample-stream-events-01/dbt/`

The current sample uses the simulator API's `iot_sensor_benchmark` preset in
`/sample` mode, pushes newline-delimited JSON events through Kinesis and
Firehose, and then builds queryable Athena marts from the landed stream files.
