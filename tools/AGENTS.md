# Tools agent contract

- Tools have no production permission and contain no credentials.
- Pin both version and upstream integrity; never invent a checksum.
- Use `UNRESOLVED` for unknown integrity and keep `vane doctor` failing until it
  is verified from a primary source.
- Updating a version requires updating its installer, lock, policy, and tests in
  the same change.
- Do not add another package lock or executable operations entry point here.
