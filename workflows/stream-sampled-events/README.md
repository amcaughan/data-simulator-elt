Stream-oriented sampled-event workflow.

Intent:
- bridge simulator preset sampling into a stream transport
- keep the workflow name focused on transport shape rather than fake business domain
- support downstream processing of sampled events and late processed aggregation

This workflow includes a simulated upstream producer.
The stream emitter should be understood as test-source plumbing, not the core
ELT transformation logic.
