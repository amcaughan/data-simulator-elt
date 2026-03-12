Stream-emitting ingestion runtime.

Responsibilities:
- pull sample-sized records from the simulator API
- emit those records into the workflow's stream transport
- preserve workflow metadata needed for downstream processing
- keep the generator and the transport responsibilities separated
