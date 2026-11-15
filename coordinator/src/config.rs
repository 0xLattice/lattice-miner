//! The miner's own settings, `miner.json`. The node knows nothing of them:
//! recipients and minimum work go with each template request, and nothing
//! is read from the node.
use crate::*;
use serde::Deserialize;
use std::{collections::BTreeMap, path::Path, time::Duration};

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MinerConfig {
    /// `cpu` (the built-in search) or the path of a worker executable
    /// implementing lattice-node's docs/mining-workers.md.
    pub worker: Option<String>,
    pub workers: Option<usize>,
    /// Nonces per round, across all workers.
    pub batch_size: Option<u64>,
    /// Where each chain's block reward and fees go, by chain path. A chain
    /// left out mines to no one: its reward and fees burn.
    pub recipients: Option<BTreeMap<String, String>>,
    /// Minimum work per block by chain path (`2^N` or a decimal integer): a
    /// local search filter; blocks still commit their scheduled target.
    pub min_work: Option<BTreeMap<String, String>>,
    /// Shortest gap between the starts of two rounds that produced parent
    /// blocks. A floor, never a ceiling: a round that already ran longer
    /// waits not at all.
    pub min_block_interval_seconds: Option<u64>,
    /// How long a request to the node may take, a template request
    /// including the node building it.
    pub template_timeout_seconds: Option<u64>,
    /// Headroom on the round deadline, which catches only a hung round.
    pub round_deadline_multiplier: Option<u64>,
}

pub const DEFAULT_WORKERS: usize = 1;
pub const DEFAULT_BATCH_SIZE: u64 = 2_000_000_000;
pub const DEFAULT_TEMPLATE_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_ROUND_DEADLINE_MULTIPLIER: u64 = 10;
/// The round deadline before any round has completed, and the most one
/// measured round counts for.
pub const FIRST_ROUND_ALLOWANCE: Duration = Duration::from_secs(600);

impl MinerConfig {
    /// The settings `file` holds, checked. A key this release does not know
    /// is refused: a misspelt `recipients` would burn every reward.
    pub fn load(file: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(file)
            .map_err(|e| format!("no miner settings at {}: {e}", file.display()))?;
        let config: Self =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", file.display()))?;
        config
            .validated()
            .map_err(|e| format!("{}: {e}", file.display()))
    }

    pub fn validated(self) -> Result<Self, String> {
        if self.workers == Some(0) {
            return Err("workers must be at least 1".into());
        }
        if self.workers.is_some_and(|n| n > 1024) {
            return Err("workers must be at most 1024".into());
        }
        if self.batch_size == Some(0) {
            return Err("batchSize must be at least 1".into());
        }
        if template_request(&self.recipient_entries(), &[]).is_err() {
            return Err("recipients maps chain paths (Nexus/...) to addresses".into());
        }
        if template_request(&[], &self.minimum_work_entries()).is_err() {
            return Err(
                "minWork maps chain paths to work per block, as 2^N or a positive decimal integer"
                    .into(),
            );
        }
        if let Some(seconds) = self.template_timeout_seconds
            && !(1..=DEFAULT_TEMPLATE_TIMEOUT_SECONDS).contains(&seconds)
        {
            return Err(format!(
                "templateTimeoutSeconds must be from 1 to {DEFAULT_TEMPLATE_TIMEOUT_SECONDS}"
            ));
        }
        if self.round_deadline_multiplier == Some(0) {
            return Err("roundDeadlineMultiplier must be at least 1".into());
        }
        if let Some(path) = self.worker.as_deref().filter(|w| *w != "cpu")
            && !is_executable(Path::new(path))
        {
            return Err(format!("worker is not an executable file: {path}"));
        }
        Ok(self)
    }

    pub fn recipient_entries(&self) -> Vec<String> {
        entries(&self.recipients)
    }

    pub fn minimum_work_entries(&self) -> Vec<String> {
        entries(&self.min_work)
    }

    /// The `POST /mining/templates` body: recipients and minimum work.
    pub fn template_request(&self) -> Result<serde_json::Value, Error> {
        template_request(&self.recipient_entries(), &self.minimum_work_entries())
    }

    pub fn workers(&self) -> usize {
        self.workers.unwrap_or(DEFAULT_WORKERS)
    }

    pub fn batch_size(&self) -> u64 {
        self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE)
    }

    pub fn template_timeout(&self) -> Duration {
        Duration::from_secs(
            self.template_timeout_seconds
                .unwrap_or(DEFAULT_TEMPLATE_TIMEOUT_SECONDS),
        )
    }

    /// How long to hold the next round so that rounds producing parent
    /// blocks start at least `minBlockIntervalSeconds` apart, given how long
    /// the round that just produced one took. From the round's start,
    /// because a block's timestamp is fixed when its template is built.
    pub fn pacing_hold(&self, elapsed: Duration) -> Duration {
        self.min_block_interval_seconds
            .map(|s| Duration::from_secs(s).saturating_sub(elapsed))
            .unwrap_or_default()
    }

    /// The deadline for the next round: the first-round allowance until a
    /// round completes, then the longest completed round (counted at most
    /// the allowance, so slow rounds cannot ratchet it) times the
    /// multiplier, never less than the allowance. A hung round never
    /// completes, so it never widens the bound that catches it.
    pub fn round_deadline(&self, longest_completed: Duration) -> Duration {
        let multiplier = self
            .round_deadline_multiplier
            .unwrap_or(DEFAULT_ROUND_DEADLINE_MULTIPLIER)
            .max(1);
        let measured = longest_completed.min(FIRST_ROUND_ALLOWANCE).as_secs();
        Duration::from_secs(measured.saturating_mul(multiplier).min(u64::MAX / 4))
            .max(FIRST_ROUND_ALLOWANCE)
    }
}

fn entries(map: &Option<BTreeMap<String, String>>) -> Vec<String> {
    map.iter()
        .flatten()
        .map(|(path, value)| format!("{path}={value}"))
        .collect()
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}
