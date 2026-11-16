//! `lattice-miner run`: mining rounds against one node until stopped.
//!
//! A round is one coordinator batch: fetch a template carrying this round's
//! recipients and minimum work, search it with the workers, submit what is
//! found. `miner.json` is read again before every round; a file that no
//! longer checks is reported and the settings read before are kept.
use crate::*;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// The wait before retrying a round that got no template, or failed.
pub const RETRY_WAIT: Duration = Duration::from_secs(5);

/// A template body may carry many hosted levels' blocks.
const MAXIMUM_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

pub struct RunOptions {
    pub node: String,
    pub auth: Auth,
    pub config: PathBuf,
}

fn log(message: impl AsRef<str>) {
    println!("{}", message.as_ref());
}

/// The recipients in effect, said whenever they change. None at all is loud:
/// every block then pays no one.
fn log_recipients(recipients: &Option<BTreeMap<String, String>>) {
    match recipients.as_ref().filter(|r| !r.is_empty()) {
        None => log("WARNING: miner.json names no recipients: every block's reward and fees burn"),
        Some(r) => log(format!(
            "recipients: {}",
            r.iter()
                .map(|(chain, address)| format!("{chain}={address}"))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// The workers a configuration names.
fn workers(config: &MinerConfig) -> Vec<Arc<dyn Worker>> {
    (0..config.workers())
        .map(|i| match config.worker.as_deref() {
            Some(path) if path != "cpu" => {
                Arc::new(ProcessWorker::new(format!("worker-{i}"), path.into())) as Arc<dyn Worker>
            }
            _ => Arc::new(CpuWorker::new(format!("cpu-{i}"))) as Arc<dyn Worker>,
        })
        .collect()
}

/// Rounds until `cancel`. Fails only when the first read of the settings or
/// the node URL does not check; after that every failure is logged and the
/// next round retries.
pub async fn run(options: RunOptions, cancel: CancellationToken) -> Result<(), String> {
    let mut config = MinerConfig::load(&options.config)?;
    let client = |config: &MinerConfig| {
        HttpNodeClient::new(
            &options.node,
            config.template_request().expect("checked when loaded"),
            options.auth.clone(),
            config.template_timeout(),
            MAXIMUM_RESPONSE_BYTES,
        )
        .map_err(|e| e.to_string())
    };
    let digests = client(&config)?.digest_feed(cancel.child_token());
    log(format!(
        "mining through {}{}",
        options.node,
        config
            .min_block_interval_seconds
            .map(|s| format!(", pacing parent blocks at least {s}s apart"))
            .unwrap_or_default()
    ));
    // The coordinator is rebuilt only when what it is built from changes, so
    // its nonce cursor carries across rounds on the same work.
    let mut built: Option<(MinerConfig, HttpNodeClient, Coordinator)> = None;
    let mut longest_completed = Duration::ZERO;
    let mut without_template_since: Option<Instant> = None;
    let mut logged_recipients = None;
    let mut warned_unpaid: Vec<String> = vec![];
    while !cancel.is_cancelled() {
        match MinerConfig::load(&options.config) {
            Ok(read) => config = read,
            Err(e) => log(format!("mining on with the settings read before: {e}")),
        }
        if logged_recipients.as_ref() != Some(&config.recipients) {
            log_recipients(&config.recipients);
            logged_recipients = Some(config.recipients.clone());
        }
        let stale = built.as_ref().is_none_or(|(was, _, _)| {
            (
                &was.worker,
                was.workers(),
                was.batch_size(),
                was.template_timeout(),
            ) != (
                &config.worker,
                config.workers(),
                config.batch_size(),
                config.template_timeout(),
            )
        });
        if stale {
            let node = match client(&config) {
                Ok(node) => node,
                Err(e) => {
                    log(format!(
                        "cannot reach the node with these settings ({e}); retrying"
                    ));
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(RETRY_WAIT) => continue,
                    }
                }
            };
            let coordinator = Coordinator::new(
                Arc::new(node.clone()),
                workers(&config),
                CoordinatorOptions {
                    total_batch_size: config.batch_size(),
                    digests: Some(digests.clone()),
                    ..Default::default()
                },
            );
            built = Some((config.clone(), node, coordinator));
        }
        let (_, node, coordinator) = built.as_mut().expect("built above");
        // Recipients apply from this round on, with no restart.
        node.set_request(config.template_request().expect("checked when loaded"));
        let deadline = config.round_deadline(longest_completed);
        let started = Instant::now();
        let cycle = tokio::select! {
            cycle = coordinator.run_batch(&cancel) => cycle,
            _ = tokio::time::sleep(deadline) => {
                // Dropping the batch cancels its searches and stops worker
                // processes. Loud by construction: only a hung round gets here.
                log(format!(
                    "ROUND DEADLINE EXCEEDED after {}s: the round was abandoned and its workers stopped. Raise roundDeadlineMultiplier in miner.json if rounds here legitimately run this long.",
                    deadline.as_secs()
                ));
                continue;
            }
        };
        let elapsed = started.elapsed();
        let unpaid = node.unpaid_chains();
        if unpaid != warned_unpaid {
            if !unpaid.is_empty() {
                log(format!(
                    "WARNING: the node's template mines {} with no recipient in miner.json: their rewards and fees burn",
                    unpaid.join(", ")
                ));
            }
            warned_unpaid = unpaid;
        }
        if !matches!(cycle, Cycle::Backoff { .. }) {
            without_template_since = None;
        }
        if matches!(
            cycle,
            Cycle::Submitted { .. } | Cycle::NoSolution { .. } | Cycle::Stale { .. }
        ) {
            longest_completed = longest_completed.max(elapsed);
        }
        let wait = match cycle {
            Cycle::Cancelled => break,
            Cycle::NoSolution { .. } | Cycle::Stale { .. } => Duration::ZERO,
            Cycle::Submitted {
                submission:
                    Submission {
                        accepted: true,
                        tip_cid,
                        ..
                    },
                ..
            } => {
                log(format!("accepted tip={}", tip_cid.unwrap_or_default()));
                let hold = config.pacing_hold(elapsed);
                if !hold.is_zero() {
                    log(format!(
                        "pacing: holding {}s so the next parent block is at least minBlockIntervalSeconds from this one",
                        hold.as_secs()
                    ));
                }
                hold
            }
            Cycle::Submitted { submission, .. } if submission.disposition == "childOnly" => {
                // A child chain advanced; no parent block, so no pacing.
                log("accepted child block(s) (childOnly)");
                Duration::ZERO
            }
            Cycle::Submitted { submission, .. } => {
                log(format!(
                    "the node refused this round's work ({}); retrying",
                    submission.disposition
                ));
                RETRY_WAIT
            }
            Cycle::Backoff { error } => {
                // A standstill, stated as what was observed: it retries
                // forever, and a passing-hiccup wording would hide that.
                let since = *without_template_since.get_or_insert(started);
                log(format!(
                    "NOT MINING: no usable template from the node ({}). Stopped for {}s so far; check POST /mining/templates on this node.",
                    error.unwrap_or_else(|| "it has none yet: HTTP 409 or 503".into()),
                    since.elapsed().as_secs()
                ));
                RETRY_WAIT
            }
            Cycle::NodeFailed { error } => {
                log(format!(
                    "the node refused the miner ({error}); check the token or cookie; retrying"
                ));
                RETRY_WAIT
            }
            Cycle::WorkerFailed {
                worker_id, error, ..
            } => {
                log(format!("worker {worker_id} failed ({error}); retrying"));
                RETRY_WAIT
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(wait) => {}
        }
    }
    log("mining stopped");
    Ok(())
}
