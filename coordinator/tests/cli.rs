use lattice_cashew::{Header, MerkleDictionary, Volume};
use lattice_primitives::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
fn block() -> Block {
    let empty = Volume::new(LatticeState::empty().unwrap()).unwrap();
    Block {
        version: 1,
        parent: None,
        transactions: Header::new(MerkleDictionary::default()).unwrap(),
        target: UInt256([255; 32]),
        next_target: UInt256([255; 32]),
        spec: Volume::new(ChainSpec {
            max_number_of_transactions_per_block: 100,
            max_state_growth: 10000,
            max_block_size: 1000000,
            premine: 0,
            target_block_time: 1000,
            initial_reward: 1,
            halving_interval: 100,
            half_life: 120,
            wasm_policies: vec![],
        })
        .unwrap(),
        parent_state: empty.clone(),
        prev_state: empty.clone(),
        post_state: empty,
        children: Header::new(BTreeMap::new()).unwrap(),
        height: 0,
        timestamp: 0,
        reward_recipient: None,
        nonce: 0,
    }
}
/// A node fixture: every template request is answered with `block` at the
/// easiest target, every submission accepted. It records template request
/// bodies and the heads of every request.
async fn node(
    block: Block,
) -> (
    String,
    Arc<Mutex<Vec<serde_json::Value>>>,
    Arc<Mutex<Vec<String>>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let templates = Arc::new(Mutex::new(vec![]));
    let heads = Arc::new(Mutex::new(vec![]));
    let (t, h) = (templates.clone(), heads.clone());
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (templates, heads, block) = (t.clone(), h.clone(), block.clone());
            tokio::spawn(async move {
                let mut bytes = vec![];
                let mut b = [0];
                while !bytes.ends_with(b"\r\n\r\n") {
                    if socket.read_exact(&mut b).await.is_err() {
                        return;
                    }
                    bytes.push(b[0]);
                }
                let head = String::from_utf8(bytes).unwrap();
                let len = head
                    .lines()
                    .find_map(|l| {
                        l.to_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|n| n.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                let mut body = vec![0; len];
                socket.read_exact(&mut body).await.unwrap();
                heads.lock().unwrap().push(head.clone());
                let (status, reply) = if head.starts_with("POST /mining/templates ") {
                    let n = {
                        let mut t = templates.lock().unwrap();
                        t.push(serde_json::from_slice(&body).unwrap());
                        t.len()
                    };
                    (
                        200,
                        serde_json::json!({"workID":format!("work-{n}"),"block":block,"searchTarget":"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","targets":["0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"],"chainPath":["Nexus"],"chains":[{"chainPath":["Nexus"],"rewardRecipient":"first"},{"chainPath":["Nexus","Kid"]}],"expiresInMilliseconds":30000,"templateDigest":format!("digest-{n}")}),
                    )
                } else if head.starts_with("POST /mining/work ") {
                    (
                        200,
                        serde_json::json!({"accepted":true,"disposition":"accepted","tipCID":"fixture-tip"}),
                    )
                } else if head.starts_with("GET /status ") {
                    let n = templates.lock().unwrap().len();
                    (
                        200,
                        serde_json::json!({"templateDigest":format!("digest-{n}")}),
                    )
                } else {
                    (404, serde_json::json!({"error":{"message":"not found"}}))
                };
                let body = serde_json::to_vec(&reply).unwrap();
                let head = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
            });
        }
    });
    (url, templates, heads)
}

async fn until(what: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[cfg(unix)]
#[tokio::test]
async fn run_rereads_settings_each_round_and_keeps_the_last_good() {
    let (url, templates, heads) = node(block()).await;
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("miner.json");
    let token = dir.path().join("token");
    std::fs::write(&token, "secret").unwrap();
    std::fs::write(
        &config,
        r#"{"worker":"cpu","workers":2,"batchSize":2,"recipients":{"Nexus":"first"}}"#,
    )
    .unwrap();
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_lattice-miner"))
        .args(["run", "--node", &url, "--token-file"])
        .arg(&token)
        .arg("--config")
        .arg(&config)
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let recipient = |body: &serde_json::Value| body["recipients"][0]["address"].clone();
    until("a template for the first recipient", || {
        templates
            .lock()
            .unwrap()
            .iter()
            .any(|b| recipient(b) == "first")
    })
    .await;
    std::fs::write(
        &config,
        r#"{"worker":"cpu","workers":2,"batchSize":2,"recipients":{"Nexus":"second"}}"#,
    )
    .unwrap();
    until("a template for the changed recipient", || {
        templates
            .lock()
            .unwrap()
            .iter()
            .any(|b| recipient(b) == "second")
    })
    .await;
    // A file that no longer checks keeps the settings read before.
    std::fs::write(&config, r#"{"recipents":{"Nexus":"typo"}}"#).unwrap();
    let seen = templates.lock().unwrap().len();
    until("more rounds", || templates.lock().unwrap().len() > seen + 3).await;
    assert!(
        templates.lock().unwrap()[seen..]
            .iter()
            .all(|b| recipient(b) == "second")
    );
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child.id().unwrap() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    let log = String::from_utf8_lossy(&output.stdout);
    assert!(log.contains("accepted tip=fixture-tip"), "{log}");
    assert!(log.contains("recipients: Nexus=first\n"), "{log}");
    assert!(log.contains("recipients: Nexus=second\n"), "{log}");
    assert_eq!(log.matches("recipients: Nexus=second").count(), 1, "{log}");
    // Said once, not every round.
    assert_eq!(
        log.matches("mines Nexus/Kid with no recipient").count(),
        1,
        "{log}"
    );
    assert!(
        log.contains("mining on with the settings read before"),
        "{log}"
    );
    assert!(log.ends_with("mining stopped\n"), "{log}");
    let heads = heads.lock().unwrap();
    assert!(
        heads
            .iter()
            .all(|h| h.contains("authorization: Bearer secret\r\n"))
    );
    assert!(heads.iter().any(|h| h.starts_with("GET /mining/events ")));
    assert!(heads.iter().any(|h| h.starts_with("POST /mining/work ")));
}

#[test]
fn run_refuses_a_bad_first_settings_file_and_needs_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("miner.json");
    std::fs::write(&config, r#"{"recipients":{"Nexus/":"x"}}"#).unwrap();
    let run = |extra: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_lattice-miner"))
            .args(["run", "--node", "http://127.0.0.1:9", "--config"])
            .arg(&config)
            .args(extra)
            .output()
            .unwrap()
    };
    let output = run(&["--cookie-file", "/nonexistent"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("recipients"));
    assert!(!run(&[]).status.success());
}

#[test]
fn check_answers_whether_run_would_accept_the_settings() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("miner.json");
    let check = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_lattice-miner"))
            .args(["check", "--config"])
            .arg(&config)
            .output()
            .unwrap()
    };
    std::fs::write(&config, r#"{"recipients":{"Nexus":"a"}}"#).unwrap();
    assert!(check().status.success());
    std::fs::write(&config, r#"{"recipients":{"Nexus":"a","Nexus":"b"}}"#).unwrap();
    let output = check();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("Nexus is named twice"));
}

/// A worker that will not stop holds up the first SIGTERM's orderly stop;
/// a second stops the miner at once, and the worker with it.
#[cfg(unix)]
#[tokio::test]
async fn a_second_signal_stops_at_once() {
    use std::os::unix::fs::PermissionsExt;
    let (url, templates, _) = node(block()).await;
    let dir = tempfile::tempdir().unwrap();
    let (worker, pidfile) = (dir.path().join("worker"), dir.path().join("pid"));
    std::fs::write(
        &worker,
        format!(
            "#!/bin/sh\necho $$ > {}\nexec sleep 1000\n",
            pidfile.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let config = dir.path().join("miner.json");
    std::fs::write(
        &config,
        serde_json::json!({"worker": worker, "recipients": {"Nexus": "first"}}).to_string(),
    )
    .unwrap();
    let token = dir.path().join("token");
    std::fs::write(&token, "secret").unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_lattice-miner"))
        .args(["run", "--node", &url, "--token-file"])
        .arg(&token)
        .arg("--config")
        .arg(&config)
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    until("a template", || !templates.lock().unwrap().is_empty()).await;
    until("the worker", || {
        std::fs::read_to_string(&pidfile).is_ok_and(|p| p.trim().parse::<i32>().is_ok())
    })
    .await;
    let worker_pid: i32 = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let pid = nix::unistd::Pid::from_raw(child.id().unwrap() as i32);
    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(child.try_wait().unwrap().is_none(), "stopped on the first");
    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .expect("a second SIGTERM stops the miner")
        .unwrap();
    assert!(output.status.success());
    let log = String::from_utf8_lossy(&output.stdout);
    assert!(log.contains("stopped without waiting"), "{log}");
    for _ in 0..200 {
        let ps = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &worker_pid.to_string()])
            .output()
            .unwrap();
        let state = String::from_utf8_lossy(&ps.stdout);
        if state.trim().is_empty() || state.trim().starts_with('Z') {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the worker outlived the miner");
}
