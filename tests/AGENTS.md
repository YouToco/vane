# Cross-component test rules

This directory contains black-box contracts, E2E tests, real Temporal history
replay, release drills, and shared fixtures only. Keep Go package tests in
`server/` and browser component tests in `web/tests/`.

Tests here must not import `server/internal` or read private Web implementation
state to simulate a black-box assertion. A suite that discovers zero tests,
does not reach a terminal result, or omits required coverage is a failure.
