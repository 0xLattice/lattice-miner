//! The contract between the coordinator and a worker (lattice-node
//! `docs/mining-workers.md`): how a target is spelled, how a nonce range is
//! split, and the one JSON object a worker prints. Both sides use these, so
//! they cannot drift apart.
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

/// A 256-bit target, big-endian. A hash wins when `hash <= target`.
pub type Target = [u8; 32];

/// A target from one to 64 hex digits (an optional `0x`, either case),
/// right-aligned. An empty string is not zero: it is refused.
pub fn parse_target(s: &str) -> Option<Target> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.is_empty() || s.len() > 64 || !s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    hex::decode(format!("{s:0>64}")).ok()?.try_into().ok()
}

/// `count` nonces from `start_nonce`, wrapping past `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonceRange {
    pub start_nonce: u64,
    pub count: u64,
}

impl NonceRange {
    pub fn contains(&self, nonce: u64) -> bool {
        nonce.wrapping_sub(self.start_nonce) < self.count
    }
}

/// `total` nonces from `offset` split into contiguous ranges, one per worker,
/// the remainder going one each to the first workers. Empty ranges are left
/// out; at least one nonce and one worker are assumed.
pub fn allocate_ranges(total: u64, workers: usize, offset: u64) -> Vec<NonceRange> {
    let workers = workers.max(1);
    let total = total.max(1);
    let base = total / (workers as u64);
    let rem = total % (workers as u64);
    let mut at = offset;
    let mut ranges = vec![];
    for i in 0..workers {
        let count = base + u64::from((i as u64) < rem);
        if count > 0 {
            ranges.push(NonceRange {
                start_nonce: at,
                count,
            });
            at = at.wrapping_add(count);
        }
    }
    ranges
}

/// The one JSON object a worker prints for an assignment. `status` is
/// `found` (with the lowest winning `nonce` and its `hash`) or `exhausted`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerOutput {
    pub work_id: String,
    pub status: String,
    pub nonce: Option<u64>,
    pub hash: Option<String>,
    pub range_start: u64,
    pub range_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_spellings() {
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
    fn ranges_wrap_and_spread_the_remainder() {
        assert_eq!(
            allocate_ranges(5, 3, u64::MAX - 1),
            vec![
                NonceRange { start_nonce: u64::MAX - 1, count: 2 },
                NonceRange { start_nonce: 0, count: 2 },
                NonceRange { start_nonce: 2, count: 1 },
            ]
        );
        assert_eq!(allocate_ranges(0, 0, 7), vec![NonceRange { start_nonce: 7, count: 1 }]);
        assert_eq!(allocate_ranges(2, 9, 0).len(), 2);
        assert!(NonceRange { start_nonce: u64::MAX, count: 2 }.contains(0));
    }

    #[test]
    fn output_wire_form() {
        let found = WorkerOutput {
            work_id: "w".into(),
            status: "found".into(),
            nonce: Some(7),
            hash: Some("00".into()),
            range_start: 0,
            range_count: 8,
        };
        assert_eq!(
            serde_json::to_string(&found).unwrap(),
            r#"{"workId":"w","status":"found","nonce":7,"hash":"00","rangeStart":0,"rangeCount":8}"#
        );
    }
}
