# Multi-stage build for `query_engine serve`.
#
# UNVALIDATED-ON-CLUSTER: this file has never been built. The development
# machine has no docker/podman, no passwordless sudo, and unprivileged user
# namespaces are blocked (kernel.apparmor_restrict_unprivileged_userns=1), so
# no container image was produced or run. It is written to be correct by
# construction; treat the first `docker build` as the validation step.
#
# Data is MOUNTED, never baked. A TPC-H SF=10 image would be ~10 GB, every
# `kind load docker-image` would copy it into every node, and the data would be
# frozen at build time. `k8s/statefulset.yaml` mounts /data instead.

ARG RUST_VERSION=1.93
ARG DEBIAN_RELEASE=bookworm

# ---------------------------------------------------------------------------
# Stage 1 — build
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder

WORKDIR /build

# Dependency layer: copy only the manifests, build a stub, and let Docker cache
# the ~400-crate dependency compile across source edits. Cargo.lock is copied
# verbatim and never regenerated — the arrow-53 pin is load-bearing (Lance
# requires it) and a floating resolve in CI would silently move it.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src benches \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && echo 'fn main() {}' > benches/tpch.rs \
    && cargo build --release --locked --bin query_engine \
    && rm -rf src benches

COPY src ./src
COPY benches ./benches
# Touch so cargo does not reuse the stub's fingerprint.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked --bin query_engine \
    && strip target/release/query_engine

# ---------------------------------------------------------------------------
# Stage 2 — runtime
# ---------------------------------------------------------------------------
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# libssl3: reqwest 0.11 (the metastore client) uses native-tls, which links
# OpenSSL dynamically. ca-certificates: for HTTPS object stores.
# No curl/wget on purpose — Kubernetes `httpGet` probes need nothing inside the
# container, and a shell-less image has less to exploit.
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 qe \
    && useradd --system --uid 10001 --gid qe --no-create-home --shell /usr/sbin/nologin qe

COPY --from=builder /build/target/release/query_engine /usr/local/bin/query_engine

# Mount points. Both are declared so a `docker run` without volumes fails
# visibly on a missing dataset rather than starting an empty node.
RUN mkdir -p /data /spill && chown qe:qe /spill
VOLUME ["/data"]

USER 10001:10001
EXPOSE 7777

ENTRYPOINT ["/usr/local/bin/query_engine"]
CMD ["serve", "--bind", "0.0.0.0:7777", "--data", "/data"]
