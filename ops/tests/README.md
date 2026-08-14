# Operations tests

Tests here cover the repository-side control plane and the imported release,
recovery, provider, and artifact primitives. They must use temporary local
state and fake provider binaries; no test may read production credentials.
