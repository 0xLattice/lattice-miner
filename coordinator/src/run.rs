//! `lattice-miner run`: mining against one node until stopped.
//!
//! One [`Session`] keeps the workers searching the node's current template
//! and submits what they find. `miner.json` is read again before every
//! template fetch, so the template carries its recipients and minimum work;
//! a file that no longer checks is reported and the settings read before are
//! kept.
use crate::*;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// The wait before a failed template fetch or worker is tried again.
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

/// Says what a submission was answered; true for a block that became the
/// tip (`canonicalized`), the only answer pacing holds for: a side block
/// (`acceptedSide`, say on old work) leaves the current work as it was.
fn log_submitted(work_id: &str, nonce: u64, result: &Result<Submission, Error>) -> bool {
    match result {
        Ok(Submission {
            accepted: true,
            disposition,
            tip_cid,
        }) => {
            log(format!(
                "accepted tip={}",
                tip_cid.as_deref().unwrap_or_default()
            ));
            return disposition == "canonicalized";
        }
        // A child chain advanced; no parent block, so no pacing.
        Ok(submission) if submission.disposition == "childOnly" => {
            log("accepted child block(s) (childOnly)")
        }
        Ok(submission) => log(format!(
            "the node refused work {work_id} nonce {nonce} ({})",
            submission.disposition
        )),
        Err(e @ Error::Unauthorized(_)) => log(format!(
            "the node refused the miner ({e}); check the token or cookie"
        )),
        Err(e) => log(format!(
            "submitting work {work_id} nonce {nonce} failed ({e})"
        )),
    }
    false
}

/// The workers a configuration names.
fn workers(config: &MinerConfig) -> Vec<Arc<dyn StreamWorker>> {
    (0..config.workers())
        .map(|_| match config.worker.as_deref() {
            Some(path) if path != "cpu" => {
                Arc::new(ProcessWorker::new(path.into())) as Arc<dyn StreamWorker>
            }
            _ => Arc::new(CpuWorker) as Arc<dyn StreamWorker>,
        })
        .collect()
}

/// Mines until `cancel`, then submits every hit found unless `force` comes
/// first. Fails only when the first read of the settings or the node URL
/// does not check; after that every failure is logged and mining goes on.
pub async fn run(
    options: RunOptions,
    cancel: CancellationToken,
    force: CancellationToken,
) -> Result<(), String> {
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
    let mut node = client(&config)?;
    let digests = node.digest_feed(cancel.child_token());
    log(format!(
        "mining through {}{}",
        options.node,
        config
            .min_block_interval_seconds
            .map(|s| format!(", pacing parent blocks at least {s}s apart"))
            .unwrap_or_default()
    ));
    // The workers and the node client are replaced only when what they are
    // built from changes; workers otherwise live for the whole run.
    let mut built = config.clone();
    let mut session = Session::new(
        Arc::new(node.clone()),
        workers(&config),
        Some(digests),
        SessionOptions {
            retry_wait: RETRY_WAIT,
            ..Default::default()
        },
    );
    let mut without_template_since: Option<Instant> = None;
    let mut logged_recipients = None;
    let mut warned_unpaid: Vec<String> = vec![];
    loop {
        let event = tokio::select! {
            _ = cancel.cancelled() => break,
            event = session.next() => event,
        };
        match event {
            Event::FetchDue => {
                // Settings are read again before every template.
                match MinerConfig::load(&options.config) {
                    Ok(read) => config = read,
                    Err(e) => log(format!("mining on with the settings read before: {e}")),
                }
                if logged_recipients.as_ref() != Some(&config.recipients) {
                    log_recipients(&config.recipients);
                    logged_recipients = Some(config.recipients.clone());
                }
                if (&built.worker, built.workers(), built.template_timeout())
                    != (&config.worker, config.workers(), config.template_timeout())
                {
                    match client(&config) {
                        Ok(replaced) => {
                            node = replaced;
                            session.replace(Arc::new(node.clone()), workers(&config));
                            built = config.clone();
                        }
                        Err(e) => log(format!(
                            "cannot reach the node with these settings ({e}); mining on with the settings read before"
                        )),
                    }
                }
                // Recipients apply from this template on, with no restart.
                node.set_request(config.template_request().expect("checked when loaded"));
                session.fetch();
            }
            Event::Fetched(fetched) => {
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
                match fetched {
                    Ok(Some(_)) => without_template_since = None,
                    Err(e @ Error::Unauthorized(_)) => log(format!(
                        "the node refused the miner ({e}); check the token or cookie; retrying"
                    )),
                    Ok(None) | Err(_) => {
                        // A standstill, stated as what was observed: it retries
                        // forever, and a passing-hiccup wording would hide that.
                        let since = *without_template_since.get_or_insert_with(Instant::now);
                        log(format!(
                            "NOT MINING: no usable template from the node ({}). Stopped for {}s so far; check POST /mining/templates on this node.",
                            fetched
                                .err()
                                .map(|e| e.to_string())
                                .unwrap_or_else(|| "it has none yet: HTTP 409 or 503".into()),
                            since.elapsed().as_secs()
                        ));
                    }
                }
            }
            Event::Submitted {
                work_id,
                nonce,
                result,
            } => {
                // Measured from the parent block's acceptance; a hold running
                // already is not extended.
                if log_submitted(&work_id, nonce, &result) && !session.paused() {
                    let hold = config.pacing_hold();
                    if !hold.is_zero() {
                        log(format!(
                            "pacing: holding {}s so the next parent block is at least minBlockIntervalSeconds from this one",
                            hold.as_secs()
                        ));
                        session.pause(hold);
                    }
                }
            }
            Event::WorkerFailed { worker, error } => {
                log(format!("worker {worker} failed ({error}); retrying"))
            }
        }
    }
    // A found block is not dropped on shutdown: the workers stop, and every
    // hit they found is submitted first, unless asked again to stop or the
    // node stops answering; a hit not sent is logged.
    log("stopping: submitting what the workers have found (signal again to stop now)");
    tokio::select! {
        unsent = session.finish(|event| {
            if let Event::Submitted { work_id, nonce, result } = event {
                log_submitted(&work_id, nonce, &result);
            }
        }) => {
            for hit in unsent {
                log(format!("not submitted: work {} nonce {}", hit.work_id, hit.nonce));
            }
        }
        _ = force.cancelled() => log("stopped without waiting"),
    }
    log("mining stopped");
    Ok(())
}
