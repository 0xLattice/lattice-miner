use serde_json::Value;
use std::process::Command;
fn worker(target: &str, start: u64, count: u64) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_lattice-miner-gpu"))
        .args([
            "--backend",
            "cpu",
            "--work-id",
            "opaque-work",
            "--prefix-hex",
            "deadbeef",
            "--block-hex",
            "unused",
            "--target",
            target,
            "--start-nonce",
            &start.to_string(),
            "--count",
            &count.to_string(),
        ])
        .output()
        .unwrap()
}
#[test]
fn process_contract_reports_found_and_exhausted_without_extra_stdout() {
    let hit = worker(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        12345,
        1,
    );
    assert!(hit.status.success());
    assert!(hit.stderr.is_empty());
    let value: Value = serde_json::from_slice(&hit.stdout).unwrap();
    assert_eq!(value["workId"], "opaque-work");
    assert_eq!(value["status"], "found");
    assert_eq!(value["nonce"], 12345);
    assert_eq!(value["rangeStart"], 12345);
    assert_eq!(value["rangeCount"], 1);
    let exhausted = worker("1", 17, 0);
    assert!(exhausted.status.success());
    let value: Value = serde_json::from_slice(&exhausted.stdout).unwrap();
    assert_eq!(value["status"], "exhausted");
    assert!(value["nonce"].is_null());
    assert!(value["hash"].is_null());
    for invalid in ["", "0x", "xyz"] {
        let result = worker(invalid, 0, 1);
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
    }
}

/// `--stream` against the CPU backend: one process, jobs on stdin, hits out.
mod stream {
    use serde_json::Value;
    use sha2::{Digest, Sha256};
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, ChildStdout, Command, Stdio};

    /// The CPU backend's chunk.
    const CHUNK: u64 = 1 << 16;
    const HALF: &str = "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    const SIXTEENTH: &str = "0fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    fn spawn() -> (Child, BufReader<ChildStdout>) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_lattice-miner-gpu"))
            .args(["--stream", "--backend", "cpu"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let out = BufReader::new(child.stdout.take().unwrap());
        (child, out)
    }

    fn send(child: &mut Child, work_id: &str, prefix: &str, target: &str, start: u64) {
        let job = serde_json::json!({"workId": work_id, "prefixHex": prefix, "target": target, "startNonce": start});
        writeln!(child.stdin.as_mut().unwrap(), "{job}").unwrap();
    }

    /// The next hit as (workId, nonce), checked against its own hash.
    fn next(out: &mut BufReader<ChildStdout>, prefixes: &[(&str, &str)]) -> Option<(String, u64)> {
        let mut line = String::new();
        if out.read_line(&mut line).unwrap() == 0 {
            return None;
        }
        let hit: Value = serde_json::from_str(&line).unwrap();
        let work_id = hit["workId"].as_str().unwrap().to_owned();
        let nonce = hit["nonce"].as_u64().unwrap();
        let prefix = prefixes.iter().find(|(w, _)| *w == work_id).unwrap().1;
        assert_eq!(
            hit["hash"].as_str().unwrap(),
            hex::encode(hash(prefix, nonce))
        );
        Some((work_id, nonce))
    }

    fn hash(prefix: &str, nonce: u64) -> [u8; 32] {
        Sha256::new()
            .chain_update(hex::decode(prefix).unwrap())
            .chain_update(nonce.to_be_bytes())
            .finalize()
            .into()
    }

    /// Every winning nonce in `[from, to)`, lowest first.
    fn hits(prefix: &str, target: &str, from: u64, to: u64) -> Vec<u64> {
        let target: [u8; 32] = hex::decode(target).unwrap().try_into().unwrap();
        (from..to).filter(|&n| hash(prefix, n) <= target).collect()
    }

    fn finish(
        mut child: Child,
        mut out: BufReader<ChildStdout>,
        prefixes: &[(&str, &str)],
    ) -> Vec<(String, u64)> {
        drop(child.stdin.take());
        let mut rest = vec![];
        while let Some(hit) = next(&mut out, prefixes) {
            rest.push(hit);
        }
        assert!(child.wait().unwrap().success());
        rest
    }

    #[test]
    fn prints_every_hit_lowest_first_and_exits_when_stdin_closes() {
        let (mut child, mut out) = spawn();
        send(&mut child, "a", "deadbeef", HALF, 5);
        let mut seen: Vec<u64> = (0..40)
            .map(|_| next(&mut out, &[("a", "deadbeef")]).unwrap().1)
            .collect();
        // Closed stdin ends the search; what it printed until then is every
        // hit from the start, in order.
        seen.extend(
            finish(child, out, &[("a", "deadbeef")])
                .into_iter()
                .map(|(_, n)| n),
        );
        assert_eq!(seen, hits("deadbeef", HALF, 5, seen.last().unwrap() + 1));
    }

    #[test]
    fn the_same_work_with_a_new_target_keeps_its_position() {
        let (mut child, mut out) = spawn();
        let p = [("a", "deadbeef")];
        send(&mut child, "a", "deadbeef", HALF, 0);
        let mut seen: Vec<u64> = (0..10).map(|_| next(&mut out, &p).unwrap().1).collect();
        send(&mut child, "a", "deadbeef", SIXTEENTH, 0);
        while *seen.last().unwrap() < 3 * CHUNK {
            seen.push(next(&mut out, &p).unwrap().1);
        }
        finish(child, out, &p);
        let to = seen.last().unwrap() + 1;
        // The target changes after some hit (or at a chunk boundary with no
        // hit before it); the nonces never restart and none is skipped.
        let (half, sixteenth) = (
            hits("deadbeef", HALF, 0, to),
            hits("deadbeef", SIXTEENTH, 0, to),
        );
        let switched = (10..seen.len()).any(|k| {
            let at = seen[k - 1] + 1;
            seen[..k] == half[..half.partition_point(|&n| n < at)]
                && seen[k..] == sixteenth[sixteenth.partition_point(|&n| n < at)..]
        });
        assert!(switched, "no switch point explains {} hits", seen.len());
    }

    #[test]
    fn a_new_work_starts_at_its_start_nonce() {
        let (mut child, mut out) = spawn();
        let p = [("a", "deadbeef"), ("b", "cafe")];
        send(&mut child, "a", "deadbeef", HALF, 0);
        next(&mut out, &p).unwrap();
        send(&mut child, "b", "cafe", SIXTEENTH, 1000);
        let mut b = vec![];
        while b.len() < 20 {
            let (work, nonce) = next(&mut out, &p).unwrap();
            match work.as_str() {
                "a" => assert!(b.is_empty(), "a hit for the old work after the new one's"),
                _ => b.push(nonce),
            }
        }
        finish(child, out, &p);
        assert_eq!(b, hits("cafe", SIXTEENTH, 1000, b.last().unwrap() + 1));
    }

    #[test]
    fn a_zero_target_idles_the_work_where_it_is() {
        let (mut child, out) = spawn();
        let p = [("a", "deadbeef")];
        // Lines as they come, so a quiet worker can be told from a busy one.
        let (lines, printed) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut out = out;
            while let Some(hit) = next(&mut out, &p) {
                if lines.send(hit.1).is_err() {
                    break;
                }
            }
        });
        let wait = std::time::Duration::from_millis(300);
        send(&mut child, "a", "deadbeef", HALF, 0);
        let mut seen = vec![printed.recv().unwrap()];
        send(&mut child, "a", "deadbeef", &"0".repeat(64), 0);
        // What was found before the zero target took effect, then nothing.
        while let Ok(nonce) = printed.recv_timeout(wait) {
            seen.push(nonce);
        }
        send(&mut child, "a", "deadbeef", HALF, 0);
        for _ in 0..20 {
            seen.push(printed.recv().unwrap());
        }
        drop(child.stdin.take());
        while let Ok(nonce) = printed.recv() {
            seen.push(nonce);
        }
        reader.join().unwrap();
        assert!(child.wait().unwrap().success());
        // Idle searched nothing: the hits go on from where they stopped.
        assert_eq!(seen, hits("deadbeef", HALF, 0, seen.last().unwrap() + 1));
    }

    #[test]
    fn no_job_then_eof_exits_cleanly_and_a_bad_job_is_refused() {
        let (child, out) = spawn();
        assert!(finish(child, out, &[]).is_empty());
        let (mut child, _out) = spawn();
        writeln!(child.stdin.as_mut().unwrap(), "not json").unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("invalid job line"));
    }
}
