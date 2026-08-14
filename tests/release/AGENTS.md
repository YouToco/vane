# Release fixture agent contract

Fixtures must be deterministic, credential-free, and bound to exact revisions
and digests. They may prove compatibility or rejection behavior, but must not
invoke production providers, mutate durable production state, or weaken signed
manifest requirements.
