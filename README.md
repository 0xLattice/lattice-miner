# lattice-miner

Mining for the [Lattice](https://github.com/0xLattice/lattice-node) network. A
node serves block templates and accepts work; it runs no miner. Everything that
mines lives here:

| Crate | Binary | What it is |
| --- | --- | --- |
| [`coordinator/`](coordinator) | `lattice-miner` | The miner: a client of one node. It fetches templates, splits nonce ranges across workers, follows the node's template digest, submits what is found, and runs the rounds. |
| `.` (root) | `lattice-miner-gpu` | The worker: CPU, Metal, CUDA and OpenCL nonce search. |
| [`core/`](core) | | The worker contract both sides share: target spelling, nonce ranges, the worker's result. |

## The miner

```bash
cargo build --release --workspace
lattice-miner run --node http://10.0.0.5:8090 --token-file miner.token --config miner.json
lattice-miner run --node http://127.0.0.1:8080 --cookie-file <root>/chains/Nexus/.cookie
```

`--node` is a node's remote operator listener with a bearer token
(`--token-file`), or its loopback API with the cookie it writes at every start
(`--cookie-file`). Either file is read again for every request. A token for
mining needs `POST /mining/templates`, `POST /mining/work` and `GET /status`,
and `GET /mining/events` to be pushed template changes rather than poll for
them:

```bash
# Beside the node:
lattice tokens create miner --out miner.token --root /var/lib/lattice \
  --route "POST /mining/templates" --route "POST /mining/work" \
  --route "GET /status" --route "GET /mining/events"
```

Its settings are its own file, never the node's (default `miner.json`):

```json
{
  "worker": "/usr/local/bin/lattice-miner-gpu",
  "workers": 1,
  "batchSize": 2000000000,
  "recipients": {
    "Nexus": "<address>",
    "Nexus/Alpha": "<address>"
  },
  "minWork": {"Nexus": "2^32"},
  "minBlockIntervalSeconds": 10
}
```

- `worker`: `cpu` (the built-in search, the default) or the path of any
  executable implementing the worker contract, such as `lattice-miner-gpu`. Its
  backend is chosen by `LATTICE_MINER_BACKEND` in the miner's environment.
- `workers` (default 1) and `batchSize` (nonces per round across all workers,
  default 2,000,000,000).
- `recipients`: where each chain's block reward and fees go, by chain path.
  Every template request carries them. A chain left out mines to no one: its
  reward and fees burn.
- `minWork`: per chain, `2^N` or a decimal integer. A local search filter: the
  miner neither searches for nor submits hashes below it; blocks still commit
  their scheduled target.
- `minBlockIntervalSeconds`: a floor on the spacing of rounds that produce a
  parent block, measured from the round's start; a slower round waits not at
  all.
- `templateTimeoutSeconds` (1 to 60, default 60): how long one request to the
  node may take, a template request including the node building it.
- `roundDeadlineMultiplier` (default 10): the round deadline is 600 s until a
  round completes, then the longest completed round (counted at most 600 s)
  times this, never less than 600 s. It only catches a hung round; work never
  expires on a clock.

An unknown key is refused (a misspelt `recipients` would burn every reward).
The file is read again before every round: a changed recipient applies to the
next round with no restart, and a file that no longer checks is logged while
the last good settings are kept. Only a bad file at start is fatal.

A round is one batch: fetch a template, search it, submit hits. A hit that
clears only some of the template's targets (a child chain's, `childOnly`) is
submitted and the search goes on over the same range toward the rest. Work is
valid until the node's template digest changes: the miner follows
`GET /mining/events` and ends a batch when a pushed digest differs from its
work's. A dropped stream says nothing about the chain: the search goes on,
`GET /status` is polled every 2 s meanwhile, and the stream is opened again
after a wait doubling from 250 ms to 5 s. A round without a template, a refused
submission or a failed worker is logged and retried after 5 s. SIGINT or
SIGTERM ends the round in flight and exits.

`coordinator/tools/node-integration.py` mines real blocks through a local
lattice-node, including a child chain under a much harder parent; see
[PORTING.md](PORTING.md#evidence).

## The worker

The worker implements the **Mining Worker Protocol** — the miner hands it a
nonce range and the nonce-independent PoW prefix; it searches for a nonce where

```
SHA256( prefix || nonce_be64 )  ≤  target
```

and prints one `WorkerResult` JSON object. A worker needs only SHA-256 and the
protocol — no Lattice/block-parsing code. See
`lattice-node/docs/mining-workers.md` for the normative contract.

## Status

Built in phases, each gated on reproducing the previous one bit-for-bit:

- [x] **Phase 1 — CPU golden reference** (`--backend cpu`). The oracle every
      GPU kernel is tested against.
- [x] **Phase 2 — Metal kernel** (`--backend metal`, Apple Silicon). Resumes
      SHA-256 from a host-computed prefix midstate over the final block(s).
      Verified bit-for-bit against the CPU oracle. **~230 MH/s** un-optimized on
      M-series (vs ~10 MH/s single CPU thread).
- [x] **Phase 3 — kernel optimization** — full unroll, op-reduced Ch/Maj,
      host-precomputed message words. **~834 MH/s** (3.6× the first kernel).
      Remaining headroom (message-schedule precompute, high-word early-exit)
      toward ~1 GH/s.
- [x] **Phase 4 — CUDA** (`--backend cuda`, NVIDIA). `cudarc` + NVRTC compiles
      the kernel at runtime; `libcuda` is loaded dynamically, so the host binary
      builds without the CUDA toolkit and runs on any NVIDIA driver (incl. cloud
      rental: vast.ai / RunPod / Lambda). Same midstate-resume kernel as Metal.
- [x] **Phase 5 — OpenCL** (`--backend opencl`, AMD / NVIDIA / Intel). One
      backend for every non-Apple GPU via the platform OpenCL ICD.

The throughput figures above are inherited benchmarks, not measurements
revalidated by this port. See [PORTING.md](PORTING.md) for the dated Metal
hardware result and the remaining CUDA/OpenCL hardware coverage gaps.

Backend kernels are intended to reproduce the CPU oracle bit-for-bit, and the host
**re-verifies every hit** against `sha256::finalize_from_midstate` before
reporting it. Metal initialization, execution and invalid-result failures return
a nonzero worker exit; they never report an exhausted range. CUDA/OpenCL retain
their existing hit assertions.

Backends are a thin Rust host calling per-vendor native kernels (not one portable
shader): SHA-256's hot path is rotates and 3-input booleans, which native ISAs do
in one instruction. **Metal runs only on macOS** (Apple GPU isn't visible inside
Linux containers); **CUDA/OpenCL** are how the network is mined on commodity and
cloud GPUs — essential for hashpower beyond Apple Silicon.

## Download

Prebuilt workers are attached to each
[release](https://github.com/0xLattice/lattice-miner/releases); archives of
releases after 0.1.0 also carry the miner (`lattice-miner`) in `bin/`:

| Archive | Backends | Backend flag |
| --- | --- | --- |
| `lattice-miner-gpu-<version>-linux-amd64.tar.gz` | OpenCL, CUDA, CPU | `--backend opencl` (AMD / Intel / NVIDIA) or `--backend cuda` (NVIDIA) |
| `lattice-miner-gpu-<version>-windows-amd64.tar.gz` | OpenCL, CUDA, CPU | `--backend opencl` (AMD / Intel / NVIDIA) or `--backend cuda` (NVIDIA) |
| `lattice-miner-gpu-<version>-macos-arm64.tar.gz` | Metal, CPU | `--backend metal` (the default on macOS) |

The Linux binaries need glibc 2.31 or newer (Ubuntu 20.04, Debian 11 and later).

Verify the archive against its checksum file, then unpack it:

```bash
sha256sum -c lattice-miner-gpu-<version>-<platform>.tar.gz.sha256       # Linux, Windows (Git Bash)
shasum -a 256 -c lattice-miner-gpu-<version>-<platform>.tar.gz.sha256   # macOS
tar -xzf lattice-miner-gpu-<version>-<platform>.tar.gz
./lattice-miner-gpu-<version>-<platform>/bin/lattice-miner-gpu --help
```

On Linux and Windows, `auto` picks OpenCL. On an NVIDIA machine that mines with
CUDA, run the miner with `LATTICE_MINER_BACKEND=cuda`. The worker loads its
GPU libraries at run time:

- OpenCL: `libOpenCL.so.1` on Linux (`ocl-icd-libopencl1` plus the graphics
  driver's OpenCL support), `OpenCL.dll` on Windows (installed with the driver).
- CUDA: the driver library, `libcuda.so` on Linux (if the driver installed only
  `libcuda.so.1`, add a `libcuda.so` symlink to it) or `nvcuda.dll` on Windows,
  and NVRTC from CUDA 12 (`libnvrtc.so.12`, for example the `cuda-nvrtc-12-6`
  package, or `nvrtc64_120_0.dll`), on the library search path. Use an NVRTC no
  newer than the CUDA version `nvidia-smi` reports for the driver.

Each worker process mines on one GPU: CUDA device 0, the first OpenCL GPU
found, or the system default Metal device.

On macOS, a downloaded binary may be quarantined by Gatekeeper;
`xattr -d com.apple.quarantine <path>` clears it.

## Build & run

```bash
cargo build --release           # macOS: Metal + CPU
./target/release/lattice-miner-gpu \
  --work-id w1 --prefix-hex deadbeef \
  --target ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  --start-nonce 0 --count 1000000 --backend metal
```

Non-Apple GPUs (opt-in features; neither needs its toolkit to build):

```bash
# AMD / NVIDIA / Intel, on Linux and Windows. The OpenCL library is loaded at
# runtime: libOpenCL.so.1 on Linux (ocl-icd-libopencl1, plus the graphics
# driver's OpenCL package), OpenCL.dll on Windows (installed with the driver).
cargo build --release --features opencl
lattice-miner-gpu ... --backend opencl

# NVIDIA — libcuda is loaded at runtime, and NVRTC (from the CUDA toolkit)
# compiles the kernel, so the machine that runs it needs the toolkit.
cargo build --release --features cuda
lattice-miner-gpu ... --backend cuda
```

Without `--backend` the worker uses the build's GPU backend for the platform:
Metal on macOS, otherwise OpenCL, otherwise CUDA. The miner passes no
backend, so to force one set `LATTICE_MINER_BACKEND` in its environment. A
backend that cannot search (no library, no device, a kernel that does not
build) exits non-zero with the reason on stderr: the range is not reported as
exhausted.

```bash
cargo test   # reference PoW vectors + midstate/target oracle (all platforms)
# The OpenCL kernel against the CPU oracle, on this machine's OpenCL device.
# LATTICE_MINER_OPENCL_DEVICES=all also admits a CPU device (PoCL), for a
# machine with no graphics chip; it is for testing, not mining.
cargo test --features opencl -- --ignored
```

## Use with a node

Name the worker in the miner's `miner.json` (`"worker": "./target/release/lattice-miner-gpu"`)
and run `lattice-miner run` as above.

## Docker (cloud GPU rental)

A self-contained CUDA image (`ghcr.io/0xlattice/lattice-miner-gpu:main`, built by CI)
bundles a Lattice node (`lattice-node` and `lattice` from the node image), the miner and
this worker. One node process hosts Nexus and the configured child levels; once Nexus
catches up to the network, `lattice-miner run` mines through the node's loopback API with
the CUDA worker, its settings written to `/data/miner.json` and its log to
`/data/mining.log`:

```bash
docker run --gpus all -v lattice-data:/data \
  -e RECIPIENTS=Nexus=<addr>,Nexus/testnet=<addr> \
  ghcr.io/0xlattice/lattice-miner-gpu:main
```

`libcuda` is provided by the host driver through the NVIDIA container runtime
(`--gpus all`). Each block names its own reward recipient: `RECIPIENTS` (required)
maps `<chain path>=<address>`; a hosted chain without an entry burns its reward and
fees. No key or pre-signed reward file lives on the box. Other tunables via `-e`:
`HOSTED_CHAINS` (merge-mined child paths, parent first; default `Nexus/testnet`, empty
= Nexus only), `NEXUS_PEERS`, `WORKERS`, `BATCH_SIZE`, `MINER_BACKEND`
(cuda|opencl|cpu). See `deploy/gpu-entrypoint.sh`.
