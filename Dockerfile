# syntax=docker/dockerfile:1
#
# Self-contained CUDA GPU miner image for cloud rental (vast.ai / RunPod / Lambda).
# Composes the miner (lattice-miner) and CUDA PoW worker built here with the Lattice
# node from the published node image. See deploy/gpu-entrypoint.sh for the run model.
#
# Run:  docker run --gpus all ghcr.io/0xlattice/lattice-miner-gpu:main
# libcuda is provided by the host driver via the NVIDIA container runtime.

# ── Stage 1: build the miner and the CUDA PoW worker (Rust) ──────────────────
# cudarc dynamically loads libcuda at run time and NVRTC-compiles the kernel, so the
# build needs only Rust — no CUDA toolkit (mirrors the linux-gpu-features CI job).
FROM rust:1-bookworm AS worker
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p lattice-miner -p lattice-miner-gpu --features lattice-miner-gpu/cuda

# ── The Lattice node (already built, static-swift-stdlib) ─────────────────────
FROM ghcr.io/0xlattice/lattice-node:4.0.0@sha256:dad33fefeb953d24de712e608e76f62464696a445e009dcd0f1916d2fbe8adc8 AS node

# ── Stage 2: the self-contained GPU miner ─────────────────────────────────────
# RUNTIME base (not devel): the only CUDA piece needed at run time is libnvrtc (cudarc
# NVRTC-compiles the kernel); libcuda.so.1 is injected from the host driver by the NVIDIA
# container runtime. The devel base drags in the whole toolkit (~7GB unpacked → slow cold
# pulls on every fresh rental host); runtime + the single cuda-nvrtc package is ~half that.
# The Swift binaries are static-swift-stdlib but still link the node's shared apt deps, so
# install the same set the node image uses.
FROM nvidia/cuda:12.6.1-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    cuda-nvrtc-12-6 \
    curl \
    dnsutils \
    jq \
    libatomic1 \
    libcurl4 \
    libjavascriptcoregtk-4.1-0 \
    libsqlite3-0 \
    libxml2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=node /usr/local/bin/lattice-node            /usr/local/bin/lattice-node
COPY --from=node /usr/local/bin/lattice                 /usr/local/bin/lattice
# The miner: rounds, recipients per chain from RECIPIENTS, and the worker below.
COPY --from=worker /src/target/release/lattice-miner     /usr/local/bin/lattice-miner
COPY --from=worker /src/target/release/lattice-miner-gpu /usr/local/bin/lattice-miner-gpu
COPY deploy/gpu-entrypoint.sh /usr/local/bin/gpu-entrypoint
RUN chmod +x /usr/local/bin/gpu-entrypoint

VOLUME /data
ENTRYPOINT ["/usr/local/bin/gpu-entrypoint"]
