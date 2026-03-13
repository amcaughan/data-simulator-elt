Stream-emitting ingestion runtime.

Responsibilities:
- pull sample-sized records from the simulator API
- emit those records into the workflow's stream transport
- preserve workflow metadata needed for downstream processing
- keep the generator and the transport responsibilities separated

This runtime is intentionally upstream of the ELT path.
It exists to simulate the external producer a real pipeline would normally
consume from, not to act like the transformation layer itself.
