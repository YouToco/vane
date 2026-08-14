# Development Compose

Local development uses an explicit generated/temporary overlay against the
canonical dependency versions in `../../production/compose/docker-compose.yml`.
Do not copy the production Compose file here. The local orchestration command
creates isolated projects, ports, and volumes per test run.
