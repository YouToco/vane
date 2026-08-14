FROM ubuntu@sha256:561618e2c15bf2397621dd04f96926663a3b5616c189cf7e38db7e82f5c538ea

ARG TARGETARCH
RUN test "$TARGETARCH" = "amd64" -o "$TARGETARCH" = "arm64" \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       python3=3.12.3-0ubuntu2.1 \
       git=1:2.43.0-1ubuntu7.3 \
       build-essential=12.10ubuntu1 \
       ca-certificates=20260601~24.04.1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --home-dir /home/vane-build --uid 10001 vane-build

USER 10001:10001
WORKDIR /workspace
ENTRYPOINT ["/usr/bin/python3"]
