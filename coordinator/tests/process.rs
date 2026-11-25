#![cfg(unix)]
use lattice_miner::*;
use std::{sync::Arc, time::Duration};
fn work() -> Arc<Work> {
    Arc::new(Work {
        work_id: "fixed".into(),
        block_hex: "00".into(),
        prefix: vec![1],
        target: [255; 32],
        targets: vec![[255; 32]],
        stale_token: "stable".into(),
    })
}
/// `--stream`: the job line the worker reads, the hit it prints, and how it
/// ends.
async fn stream(code: &str, job: Job) -> (Vec<StreamHit>, Result<(), Error>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker.py");
    std::fs::write(&path, format!("import sys,json\n{code}\n")).unwrap();
    let mut worker = ProcessWorker::new("python3".into());
    worker.arguments = vec![path.to_string_lossy().into_owned()];
    let (jobs, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (sender, mut hits) = tokio::sync::mpsc::unbounded_channel();
    let run = tokio::spawn(StreamWorker::stream(&worker, receiver, sender));
    jobs.send(job).unwrap();
    let mut seen = vec![];
    if let Ok(Some(hit)) = tokio::time::timeout(Duration::from_secs(10), hits.recv()).await {
        seen.push(hit);
    }
    drop(jobs);
    let ended = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap();
    (seen, ended)
}

#[tokio::test]
async fn a_stream_worker_reads_jobs_prints_hits_and_ends_with_stdin() {
    let job = Job {
        work: work(),
        target: [15; 32],
        start_nonce: 9,
    };
    let echo = r#"
assert sys.argv[1:] == ['--stream'], sys.argv
for line in sys.stdin:
    job = json.loads(line)
    assert job['prefixHex'] == '01', job
    print(json.dumps({'workId': job['workId'], 'nonce': job['startNonce'], 'hash': job['target']}), flush=True)"#;
    let (hits, ended) = stream(echo, job.clone()).await;
    ended.unwrap();
    assert_eq!(
        hits,
        vec![StreamHit {
            work_id: "fixed".into(),
            nonce: 9,
            hash: "0f".repeat(32),
        }]
    );
    let crash = "sys.stdin.readline()\nsys.stderr.write('boom')\nsys.exit(3)";
    let (hits, ended) = stream(crash, job.clone()).await;
    assert!(hits.is_empty());
    assert!(ended.unwrap_err().to_string().contains("boom"));
    // Ending on its own while it still has jobs is a failure too.
    let (_, ended) = stream("sys.stdin.readline()", job).await;
    assert!(ended.is_err());
}

#[tokio::test]
async fn a_stderr_flood_is_drained_and_only_its_tail_kept() {
    let job = Job {
        work: work(),
        target: [15; 32],
        start_nonce: 0,
    };
    let flood = "sys.stdin.readline()\nsys.stderr.write('x'*200000+'TAIL')\nsys.exit(7)";
    let (_, ended) = stream(flood, job).await;
    let error = ended.unwrap_err().to_string();
    assert!(error.contains("TAIL"));
    assert!(error.len() < 4300);
}

#[tokio::test]
async fn a_stopped_stream_takes_the_workers_descendants_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let pidfile = dir.path().join("pid");
    let script = dir.path().join("fork.py");
    std::fs::write(
        &script,
        format!(
            "import os,time\np=os.fork()\nif p==0:\n open({:?},'w').write(str(os.getpid()))\n time.sleep(30)\nelse: time.sleep(30)\n",
            pidfile.to_str().unwrap()
        ),
    )
    .unwrap();
    let mut worker = ProcessWorker::new("python3".into());
    worker.arguments = vec![script.to_str().unwrap().into()];
    let (_jobs, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (sender, _hits) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(StreamWorker::stream(&worker, receiver, sender));
    let pid: i32 = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(pid) = std::fs::read_to_string(&pidfile)
                .ok()
                .and_then(|value| value.parse::<i32>().ok())
            {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker descendant did not become ready");
    task.abort();
    for _ in 0..200 {
        let output = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        let state = String::from_utf8_lossy(&output.stdout);
        if state.trim().is_empty() || state.trim().starts_with('Z') {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("descendant survived the stream");
}
