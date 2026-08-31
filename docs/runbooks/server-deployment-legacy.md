# Legacy server deployment notes

This page preserves only the compatibility facts from the former standalone
server repository. It is not an executable deployment authority.

- Production dependencies are PostgreSQL 18, Temporal Server 1.29.7, Temporal
  UI, and Caddy. Their canonical pinned configuration is under
  `../../infra/production/`.
- The application listens on `127.0.0.1:8080`; Caddy is the public ingress.
- `vane-migrate` owns forward-only schema/role provision. Long-running server
  and research-gateway processes use distinct non-owner identities.
- The research gateway uses a mode `0660` Unix socket and separate systemd
  credentials. Provider credentials never enter the server environment.
- Old Temporal workflow implementations, route generations, and version
  branches remain until the retention gate proves they are no longer needed.
- A previous binary is not a valid rollback merely because its file exists. It
  must be proven compatible with the already-migrated schema; otherwise recover
  by roll-forward.

Production deployment is the GitHub Actions `Deploy` workflow
(`../../.github/workflows/deploy.yml`) running
`../../tools/release/remote-atomic-release.sh`; human documentation must not
duplicate the release shell state machine.
