//! `lattice-miner-gpu` — a proof-of-work mining worker for the Lattice network.
//!
//! Implements the Mining Worker Protocol (see `lattice-node/docs/
//! mining-workers.md`): search one immutable nonce range for a nonce
//! such that `SHA256(prefix || nonce_be64) <= target`, and print one
//! `WorkerResult` JSON object. With `--stream` it instead follows jobs on
//! stdin and prints every hit, setting the backend up once.
//!
//! Phase 1 is this pure-Rust CPU search — the **golden reference**. The Metal
//! (and later CUDA) backends must reproduce it bit-for-bit; the CPU path stays
//! as the oracle the GPU kernels are tested against.

#[cfg(any(test, target_os = "macos", feature = "cuda", feature = "opencl"))]
mod sha256;

#[cfg(target_os = "macos")]
mod metal_backend;

#[cfg(feature = "cuda")]
mod cuda_backend;

#[cfg(feature = "opencl")]
mod opencl_backend;

use clap::Parser;
use lattice_miner_core::{parse_target, StreamHit, StreamJob, WorkerOutput};
use sha2::{Digest, Sha256};
use std::io::{BufRead, Write};

/// Search one nonce range for a PoW solution.
#[derive(Parser, Debug)]
#[command(name = "lattice-miner-gpu", version, about)]
struct Args {
    /// Opaque work identifier; echoed back in the result.
    #[arg(long, required_unless_present = "stream")]
    work_id: Option<String>,

    /// Nonce-independent PoW preimage prefix (hex). Preferred input.
    #[arg(long)]
    prefix_hex: Option<String>,

    /// Serialized nonce-0 block (hex). Accepted for contract compatibility but
    /// unused: deriving the prefix from a block needs Lattice, which this worker
    /// deliberately avoids. Supply `--prefix-hex`.
    #[arg(long)]
    block_hex: Option<String>,

    /// PoW target as 256-bit big-endian hex. A nonce wins iff `digest <= target`.
    #[arg(long, required_unless_present = "stream")]
    target: Option<String>,

    /// First nonce in this assignment.
    #[arg(long, default_value_t = 0)]
    start_nonce: u64,

    /// Number of nonces to search: `[start_nonce, start_nonce + count)`.
    #[arg(long, required_unless_present = "stream")]
    count: Option<u64>,

    /// Stream mode: set the backend up once, then read one JSON job per line
    /// on stdin (`{"workId","prefixHex","target","startNonce"}`) and print one
    /// JSON line per winning nonce (`{"workId","nonce","hash"}`), lowest
    /// first, searching on until stdin closes. A job with a new `workId`
    /// starts a search at its `startNonce`; the same `workId` again changes
    /// only the target and keeps the position. A job sent while searching
    /// takes effect at the next hit or chunk of nonces. A target of zero,
    /// which no hash meets, idles the worker where it is until the next job.
    #[arg(long, conflicts_with_all = ["work_id", "prefix_hex", "block_hex", "target", "start_nonce", "count"])]
    stream: bool,

    /// Search backend: `auto` (default: this build's GPU backend for the
    /// platform, Metal on macOS, otherwise OpenCL, otherwise CUDA), `metal`
    /// (Apple GPU), `cuda` (NVIDIA), `opencl` (AMD/NVIDIA/Intel), or `cpu` (the
    /// reference search). cuda/opencl require the matching build feature. The
    /// coordinator passes no backend, so one that must be forced is set in the
    /// environment.
    #[arg(long, env = "LATTICE_MINER_BACKEND", default_value = "auto")]
    backend: String,
}

/// The backend `auto` stands for in this build, if it has a GPU backend.
fn automatic_backend() -> Option<&'static str> {
    if cfg!(target_os = "macos") {
        Some("metal")
    } else if cfg!(feature = "opencl") {
        Some("opencl")
    } else if cfg!(feature = "cuda") {
        Some("cuda")
    } else {
        None
    }
}

/// The backend `--backend` names, `auto` resolved.
fn chosen_backend(name: &str) -> &str {
    match name {
        // An empty value is a variable set to nothing, not a backend's name.
        "auto" | "" => automatic_backend().unwrap_or_else(|| {
            eprintln!("error: this build has no GPU backend for this platform (rebuild with --features opencl or cuda, or use --backend cpu)");
            std::process::exit(2);
        }),
        named => named,
    }
}

/// A backend that could not search is a failed assignment, never an exhausted
/// range: the coordinator must not take the nonces as searched.
#[cfg(any(target_os = "macos", feature = "cuda", feature = "opencl"))]
fn searched(
    backend: &str,
    outcome: Result<Option<(u64, [u8; 32])>, String>,
) -> Option<(u64, [u8; 32])> {
    outcome.unwrap_or_else(|e| {
        eprintln!("error: {backend} backend failed: {e}");
        std::process::exit(1);
    })
}

/// `digest <= target` as unsigned 256-bit big-endian integers.
#[inline]
pub(crate) fn meets_target(digest: &[u8; 32], target: &[u8; 32]) -> bool {
    for i in 0..32 {
        if digest[i] != target[i] {
            return digest[i] < target[i];
        }
    }
    true
}

/// One PoW hash: `SHA256(prefix || nonce_be64)`, resumed from the prefix midstate.
#[inline]
fn pow_hash(midstate: &Sha256, nonce: u64) -> [u8; 32] {
    let mut h = midstate.clone();
    h.update(nonce.to_be_bytes());
    h.finalize().into()
}

/// Golden-reference CPU search over `[start, start+count)`. Returns the first
/// winning `(nonce, digest)`, or `None` if the range is exhausted.
fn search_cpu(prefix: &[u8], target: &[u8; 32], start: u64, count: u64) -> Option<(u64, [u8; 32])> {
    let mut midstate = Sha256::new();
    midstate.update(prefix);

    let mut nonce = start;
    let mut remaining = count;
    while remaining > 0 {
        let digest = pow_hash(&midstate, nonce);
        if meets_target(&digest, target) {
            return Some((nonce, digest));
        }
        nonce = nonce.wrapping_add(1);
        remaining -= 1;
    }
    None
}

/// Nonces per chunk in stream mode: a job sent while searching takes effect
/// at the next hit or the next chunk. A GPU chunk is one kernel launch.
const GPU_CHUNK: u64 = 16_000_000;
const CPU_CHUNK: u64 = 1 << 16;

/// A job in stream mode: where its search is, and where its chunk ends.
struct Job {
    work_id: String,
    prefix: Vec<u8>,
    target: [u8; 32],
    nonce: u64,
    end: u64,
}

fn fail(code: i32, message: &str) -> ! {
    eprintln!("error: {message}");
    std::process::exit(code);
}

/// Stream mode (see `Args::stream`). `search` is the backend, set up once:
/// the first winning `(nonce, digest)` in `[start, start+count)`. Returns
/// when stdin closes.
fn stream(
    chunk: u64,
    mut search: impl FnMut(&[u8], &[u8; 32], u64, u64) -> Result<Option<(u64, [u8; 32])>, String>,
) {
    let (lines, jobs) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let mut stdout = std::io::stdout().lock();
    let mut job: Option<Job> = None;
    loop {
        // Every job sent since the last hit or chunk; with nothing to
        // search, or a zero target that nothing meets, wait.
        loop {
            let line = match &job {
                None => match jobs.recv() {
                    Ok(line) => line,
                    Err(_) => return,
                },
                Some(idle) if idle.target == [0; 32] => match jobs.recv() {
                    Ok(line) => line,
                    Err(_) => return,
                },
                Some(_) => match jobs.try_recv() {
                    Ok(line) => line,
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                },
            };
            let next: StreamJob = serde_json::from_str(&line)
                .unwrap_or_else(|e| fail(2, &format!("invalid job line: {e}")));
            let target = parse_target(&next.target)
                .unwrap_or_else(|| fail(2, &format!("invalid target in job {}", next.work_id)));
            match job.as_mut() {
                Some(current) if current.work_id == next.work_id => current.target = target,
                _ => {
                    let hex = next
                        .prefix_hex
                        .strip_prefix("0x")
                        .unwrap_or(&next.prefix_hex);
                    let prefix = hex::decode(hex).unwrap_or_else(|_| {
                        fail(2, &format!("invalid prefixHex in job {}", next.work_id))
                    });
                    job = Some(Job {
                        work_id: next.work_id,
                        prefix,
                        target,
                        nonce: next.start_nonce,
                        end: next.start_nonce,
                    });
                }
            }
        }
        let current = job.as_mut().expect("a job to search");
        if current.nonce == current.end {
            current.end = current.nonce.wrapping_add(chunk);
        }
        let found = search(
            &current.prefix,
            &current.target,
            current.nonce,
            current.end.wrapping_sub(current.nonce),
        )
        .unwrap_or_else(|e| fail(1, &format!("backend failed: {e}")));
        let Some((nonce, digest)) = found else {
            current.nonce = current.end;
            continue;
        };
        current.nonce = nonce.wrapping_add(1);
        let hit = StreamHit {
            work_id: current.work_id.clone(),
            nonce,
            hash: hex::encode(digest),
        };
        // A closed stdout is a coordinator that has gone.
        if writeln!(stdout, "{}", serde_json::to_string(&hit).unwrap())
            .and_then(|_| stdout.flush())
            .is_err()
        {
            return;
        }
    }
}

fn main() {
    let args = Args::parse();

    if args.stream {
        match chosen_backend(&args.backend) {
            "cpu" => stream(CPU_CHUNK, |p, t, s, c| Ok(search_cpu(p, t, s, c))),
            #[cfg(target_os = "macos")]
            "metal" => {
                let mut searcher = metal_backend::Searcher::new()
                    .unwrap_or_else(|e| fail(1, &format!("metal backend failed: {e}")));
                stream(GPU_CHUNK, |p, t, s, c| searcher.search(p, t, s, c))
            }
            #[cfg(feature = "cuda")]
            "cuda" => {
                let mut searcher = cuda_backend::Searcher::new()
                    .unwrap_or_else(|e| fail(1, &format!("cuda backend failed: {e}")));
                stream(GPU_CHUNK, |p, t, s, c| searcher.search(p, t, s, c))
            }
            #[cfg(feature = "opencl")]
            "opencl" => {
                let mut searcher = opencl_backend::Searcher::new()
                    .unwrap_or_else(|e| fail(1, &format!("opencl backend failed: {e}")));
                stream(GPU_CHUNK, |p, t, s, c| searcher.search(p, t, s, c))
            }
            other => fail(
                2,
                &format!("backend '{other}' is unknown or not in this build (use auto|metal|cuda|opencl|cpu)"),
            ),
        }
        return;
    }
    let work_id = args.work_id.expect("clap requires it without --stream");
    let count = args.count.expect("clap requires it without --stream");

    let prefix = args
        .prefix_hex
        .as_deref()
        .map(|h| h.strip_prefix("0x").unwrap_or(h))
        .and_then(|h| hex::decode(h).ok())
        .unwrap_or_else(|| {
            eprintln!("error: --prefix-hex is required (this worker does not parse blocks)");
            std::process::exit(2);
        });

    let target = parse_target(&args.target.expect("clap requires it without --stream"))
        .unwrap_or_else(|| {
            eprintln!("error: invalid --target");
            std::process::exit(2);
        });

    let backend = chosen_backend(&args.backend);

    let found = match backend {
        "cpu" => search_cpu(&prefix, &target, args.start_nonce, count),
        "metal" => {
            #[cfg(target_os = "macos")]
            {
                searched(
                    "metal",
                    metal_backend::search_metal(&prefix, &target, args.start_nonce, count),
                )
            }
            #[cfg(not(target_os = "macos"))]
            {
                eprintln!("error: metal backend requires macOS");
                std::process::exit(2);
            }
        }
        "cuda" => {
            #[cfg(feature = "cuda")]
            {
                searched(
                    "cuda",
                    cuda_backend::search_cuda(&prefix, &target, args.start_nonce, count),
                )
            }
            #[cfg(not(feature = "cuda"))]
            {
                eprintln!("error: cuda backend not compiled in (rebuild with --features cuda)");
                std::process::exit(2);
            }
        }
        "opencl" => {
            #[cfg(feature = "opencl")]
            {
                searched(
                    "opencl",
                    opencl_backend::search_opencl(&prefix, &target, args.start_nonce, count),
                )
            }
            #[cfg(not(feature = "opencl"))]
            {
                eprintln!("error: opencl backend not compiled in (rebuild with --features opencl)");
                std::process::exit(2);
            }
        }
        other => {
            eprintln!("error: unknown backend '{other}' (use auto|metal|cuda|opencl|cpu)");
            std::process::exit(2);
        }
    };

    let result = match found {
        Some((nonce, digest)) => WorkerOutput {
            work_id,
            status: "found".into(),
            nonce: Some(nonce),
            hash: Some(hex::encode(digest)),
            range_start: args.start_nonce,
            range_count: count,
        },
        None => WorkerOutput {
            work_id,
            status: "exhausted".into(),
            nonce: None,
            hash: None,
            range_start: args.start_nonce,
            range_count: count,
        },
    };

    println!("{}", serde_json::to_string(&result).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    // Vectors cross-checked against the Swift reference worker and an
    // independent SHA-256: SHA256("deadbeef" || nonce_be64).
    #[test]
    fn pow_hash_matches_reference_vectors() {
        let mut ms = Sha256::new();
        ms.update(hex::decode("deadbeef").unwrap());

        assert_eq!(
            hex::encode(pow_hash(&ms, 0)),
            "40657e5cc8e75162df7ea33a8fd55daa6e1a46d96502febf77af0883cdf365a4"
        );
        assert_eq!(
            hex::encode(pow_hash(&ms, 7)),
            "c425e8763ce8ea8cf50dbe9bee6505e6af66b13e85012bb29e5a07c747e0001f"
        );
    }

    #[test]
    fn max_target_accepts_first_nonce() {
        let target = [0xff_u8; 32];
        let prefix = hex::decode("deadbeef").unwrap();
        let (nonce, _) = search_cpu(&prefix, &target, 0, 1).unwrap();
        assert_eq!(nonce, 0);
    }

    #[test]
    fn meets_target_is_big_endian() {
        let mut t = [0u8; 32];
        t[0] = 0x01; // target = 2^248
        let mut below = [0u8; 32];
        below[1] = 0xff; // < target
        let mut above = [0u8; 32];
        above[0] = 0x02; // > target
        assert!(meets_target(&below, &t));
        assert!(!meets_target(&above, &t));
        assert!(meets_target(&t, &t)); // equal counts
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    #[test]
    fn coordinator_target_spellings_are_supported_without_empty_zero_aliases() {
        assert_eq!(parse_target("1"), parse_target("01"));
        assert_eq!(parse_target("0xabc"), parse_target("0ABC"));
        assert_eq!(parse_target("0"), Some([0; 32]));
        assert_eq!(parse_target(&"f".repeat(64)), Some([255; 32]));
        for invalid in ["", "0x", "0X1", "-1", "+1", " 1", "1 ", "gg"] {
            assert_eq!(parse_target(invalid), None, "{invalid}");
        }
        assert!(parse_target(&"f".repeat(65)).is_none());
    }
    #[test]
    fn cpu_returns_first_hit_in_assigned_order_and_handles_empty_ranges() {
        let prefix = b"merged-work";
        let target = [127; 32];
        for start in [0, 17, u64::MAX - 5] {
            let expected = (0..32u64).find_map(|offset| {
                let nonce = start.wrapping_add(offset);
                let digest: [u8; 32] = Sha256::new()
                    .chain_update(prefix)
                    .chain_update(nonce.to_be_bytes())
                    .finalize()
                    .into();
                (digest <= target).then_some((nonce, digest))
            });
            assert_eq!(search_cpu(prefix, &target, start, 32), expected);
        }
        assert_eq!(search_cpu(prefix, &[255; 32], 0, 0), None);
        assert_eq!(search_cpu(prefix, &[0; 32], 0, 100), None);
    }
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "requires an available Metal device"]
    fn metal_matches_cpu_padding_boundaries_and_lowest_winners() {
        for length in [0, 1, 47, 48, 55, 56, 63, 64, 65, 119, 120, 127, 128] {
            let prefix: Vec<u8> = (0..length).map(|i| (i * 13 + 7) as u8).collect();
            for (start, count, target) in [
                (0, 257, [127; 32]),
                (91, 17, [255; 32]),
                (u64::MAX - 4, 17, [127; 32]),
                (0, 20, [0; 32]),
            ] {
                assert_eq!(
                    metal_backend::search_metal(&prefix, &target, start, count).unwrap(),
                    search_cpu(&prefix, &target, start, count),
                    "length {length}, start {start}"
                );
            }
        }
        assert_eq!(
            metal_backend::search_metal(&[], &[255; 32], 0, 0).unwrap(),
            None
        );
    }
}
