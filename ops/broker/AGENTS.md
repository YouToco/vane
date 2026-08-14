# Broker agent contract

The SSH command set is an exact allowlist. All mutating admissions hold the
global lock and re-check current-release CAS. Never expose a shell or accept
arbitrary paths/commands from request JSON.
