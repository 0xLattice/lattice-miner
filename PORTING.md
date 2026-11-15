# Platform worker integration

This existing Rust repository is reused at baseline
`0xLattice/lattice-miner-gpu@74285a9d4fe5f4eeb25ce6acb251519cc6f014ed`.
Its assignment/result contract was checked against
`0xLattice/lattice-node@3df9e0a786c7b103a3da88d1c3881efc81706255`, specifically
`docs/mining-workers.md`, `MiningWorkerProcess.swift` and
`LatticeMinerCore/MinerLoopLogic.swift`.

Target parsing now accepts nonempty one-to-64 digit hex, including odd-length
short targets, matching the coordinator parser. An optional lowercase `0x`
prefix is accepted; hex digits may be uppercase. Empty strings previously
silently became zero and are now rejected. The byte-prefix parser remains
separate because its input must contain whole bytes.

Metal device/kernel/pipeline failures return explicit assignment errors. After
waiting, command status must be Completed before reading the result. An error
must never masquerade as an exhausted range. Returned indices are checked
against the dispatched range and the digest is recomputed on the host.

Validation in this checkout:

- CPU target/range tests and real worker-process JSON contract tests pass.
- An actual Metal run matches CPU results for 52 cases across 13 prefix lengths,
  multiple-winner searches, zero targets, and wrapping ranges; empty ranges also
  return without opening a GPU device. The lowest winning assignment offset is
  required so merged-target searches can resume without skipping candidates.
- Default strict Clippy passes. CUDA and OpenCL optional features compile together.
- Actual OpenCL oracle passed all 42 prefix/start cases, a 40-million-attempt wrapped range, and a 100,000-attempt exhausted range on this host (122.62 seconds).
- CUDA runtime validation requires NVIDIA hardware and remains unverified here.

This is a worker compatibility slice. Coordinator template polling, staleness,
merged-target continuation, submission and bounded child-process teardown remain
separate platform work. No live mining, external submission or deployment was
performed. The inherited Metal dependency currently reports a future Rust
compatibility warning in transitive `block` 0.1.6; migration is still outstanding.

## Hardware validation checkpoint — 2026-11-02

The original checkout matched all 21 tracked files in the canonical publication
checkout before this documentation update, including `Cargo.toml` and
`Cargo.lock`.

On macOS arm64, the system Metal device reported **Apple M5** with unified
memory. This existing offline test passed in 0.28 seconds:

```sh
cargo test --offline --locked --release metal_matches_cpu_padding_boundaries_and_lowest_winners -- --ignored --exact contract_tests::metal_matches_cpu_padding_boundaries_and_lowest_winners
```

The test compares 52 Metal searches against the CPU oracle across 13 prefix
lengths, covering SHA-256 padding boundaries, lowest winning assignment offsets,
multiple winners, wrapping nonce ranges and zero-target exhaustion. It also
checks an empty range, which returns without opening the GPU. The executed
release test artifact had SHA-256:

```text
7ee2c0a6053380eb59777d0eb6be2945abfc2b390e3b23e018addeb533bc9efc
```

`cargo test --offline --locked --release` separately passed seven unit tests and
one worker-process contract test; it skips the Metal test unless explicitly
requested as above. No live mining, network submission or deployment occurred.

Remaining hardware coverage:

- CUDA remains compile-checked in CI, without NVIDIA hardware parity evidence.
- OpenCL CI runs the kernel on PoCL's CPU device. The earlier local OpenCL result
  recorded above does not identify its device; this checkpoint did not rerun it
  and does not establish physical-GPU OpenCL coverage.
- The small Metal test does not cross the 16-million-nonce dispatch boundary or
  cover sustained load, device failure injection or GPU worker cancellation.
  CI currently skips this ignored hardware test.
- The inherited throughput figures in the README were not revalidated by this
  port or this correctness checkpoint.

# The miner (coordinator and round loop)

`coordinator/` is `0xLattice/lattice-mining-rs@3ba1c5b` merged into this
repository (that repository's content now lives here). It was a Rust port of
`0xLattice/lattice-node@3df9e0a` — `LatticeMinerCore` (`MinerLoopLogic.swift`,
`ProofOfWork.swift`), `LatticeMiningCoordinator` (`MiningCoordinator.swift`,
`MiningWorkerProcess.swift`) and the coordinator tool — with consensus types
from `0xLattice/lattice-rs` and `0xLattice/cashew-rs`, now git dependencies
pinned by revision (their own sibling path dependencies are patched to the
revisions `lattice-rs/platform-revisions.json` pins).

## Ported from lattice-node after 3df9e0a (to 1486706)

- No clock expiry (`2739e73`, `ae654df`, `8244c83`): templates carry no
  lifetime the miner reads (`expiresInMilliseconds` is ignored), and neither a
  batch nor a worker search ends on a timer. Only staleness, a submission's
  definitive answer, or the batch's end cancels a search.
- Pushed templates (`2739e73`, `86880e5`, `8cdda19`, `02ca1b9`, `6ef67b3`,
  `877ce7a`, `09220e5`): the node's push became one node-wide stream of the
  template digest, `GET /mining/events`, before the Swift coordinator
  consumed it (`ae654df` left the consumer for later). The miner follows it:
  a pushed digest other than the work's ends the batch as stale; a
  reconnect's first push is compared at once; a dropped stream ends nothing
  (the search on pushed work goes on and `GET /status` is polled meanwhile);
  resubscribing waits 250 ms doubling to 5 s, reset by a push. Each batch
  still makes the one immediate `GET /status` probe and the pre-submit check
  the Swift coordinator makes.
- Bearer tokens (lattice-node PR #67, `MiningCoordinator.swift`): a token
  file for a node's remote operator listener, read again per request like the
  cookie. Plain HTTP is allowed, as that listener serves it.
- The round loop of `lattice mine run` (PR #67 `Mine.swift`, `MinerConfig`,
  `MiningRoundDeadline`) as `lattice-miner run`: the same `miner.json` fields
  and checks, re-read every round with the last good settings kept, pacing,
  and the round deadline. The coordinator runs in-process instead of as a
  `--once` child per round, so the probe that the node answers templates
  before a round is the round's own template request, and the coordinator
  (with its nonce cursor) lives across rounds while its settings do.
- Shutdown (new): SIGINT/SIGTERM cancels the round, but a submission
  already in flight is still answered, for at most 10 s (`SUBMIT_GRACE`), so
  a found block is not dropped; a found nonce is submitted without the
  pre-submit staleness probe once cancelled.
- Recipients (new): the effective recipient map is logged at start and on
  every change, with a warning when it is empty or a fetched template mines a
  chain it does not name (that chain's reward and fees burn), once per
  change. A chain named twice in `miner.json` is refused.
- A template request the node refuses keeps the node's own reason
  (`{"error":{"message":...}}`) in the log, as submissions already did.
- The `lattice.json` `mine` fallback (`fcc7bee`, `83a5d9d`) is not ported:
  that file is the node's, and the miner reads only its own.

## Deliberate differences

From lattice-mining-rs (kept): the process adapter fails closed on unknown
status, wrong work ID or range, a missing or out-of-range found nonce, or an
exhausted result with a nonce; stdout is bounded in memory and stderr drained
to a 4-KiB tail; Unix workers run in their own process group, torn down on
cancellation; HTTP has byte bounds, deadlines and no redirects; worker count
is capped at 1024.

New: the one-batch CLI (`--once`, `--no-stale-probe`, flags per setting) is
replaced by `lattice-miner run` and `miner.json`. A round that gets no
template reports why (the request's error, or HTTP 409/503).

## Evidence

- Differential: `coordinator/tests/swift-vectors.json` is regenerated from the
  actual, unchanged Swift `MinerLoopLogic.swift`, `ProofOfWork.swift` and
  `MiningCoordinator.swift` at lattice-node `1486706`
  (`coordinator/tools/swift-fixtures.py <lattice-node checkout>`; hashes in
  `swift-evidence.json`). The Rust coordinator matches 72 range cases, 18
  target/minimum-work parses and 13 cycle transcripts (assignments,
  submissions, results, six metrics), the old expiry cycle now a slow search
  that is not cut short.
- Native tests: event stream parsing (chunk boundaries, CRLF, keepalive
  comments), refusal and reconnect, bearer headers; a pushed digest ending a
  batch while a disconnect and a same-digest reconnect do not; `miner.json`
  checks, pacing and deadline arithmetic; `lattice-miner run` against a
  fixture node re-reading recipients each round and keeping the last good
  file, and SIGTERM.
- Real node: `coordinator/tools/node-integration.py --node-bin <dir>` against
  lattice-node built from main `1486706` (4.0.0+dev, Lattice 56.1.2) on an
  Apple M-series Mac, passing (report in `target/node-integration.json`). The
  miner reaches the node only through the remote operator listener with a
  bearer token, and is never restarted:
  - Token scoped to `POST /mining/templates`, `POST /mining/work`,
    `GET /status`: `GET /mining/events` answers 403 and the miner polls
    `/status`; it mines Nexus, then `lattice child create` makes Nexus/C,
    Nexus/P (1 h blocks, halfLife 1) and Nexus/P/G, and every block on each
    pays the recipient `miner.json` names for that chain.
  - The token file gains `GET /mining/events`: the miner logs it follows the
    stream, with no restart.
  - Hard parent: P is hardened to 2^30 work per block (G and C at 2^0). With
    no minimum work and the external worker (`lattice-miner-gpu`, CPU
    backend), over 60 s: G +977, C +977, Nexus +237, P +0; G's longest gap
    0.5 s. Every G and C block pays its configured recipient. No round
    deadline, worker failure or NOT MINING; SIGTERM exits 0 with
    `mining stopped`.
  - `minWork` on Nexus is applied from `miner.json` mid-run (the warm-up
    holds it at P's work so every round's hit also lands P).
