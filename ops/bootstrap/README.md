# Tool bootstrap

Bootstrap scripts install only integrity-pinned build/deployment tools below a
caller-owned private temporary directory. Version-only or unresolved pins are
not sufficient for release; `vane doctor` enforces `tools/toolchain.lock.json`.
