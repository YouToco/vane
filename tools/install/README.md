# Locked acquisition

`locked_tool.py` downloads only URLs named by `toolchain.lock.json`, verifies
their checksum before extraction, rejects unsafe archive members, and installs
atomically. It is an internal module, not a second operator CLI.
