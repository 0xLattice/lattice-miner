use lattice_miner::*;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
#[tokio::test]
#[ignore = "requires LATTICE_TEST_WORKER pointing to the separately built worker"]
async fn actual_external_cpu_and_automatic_backend_wrapping_contract() {
    let path = std::env::var_os("LATTICE_TEST_WORKER").expect("set LATTICE_TEST_WORKER");
    for backend in ["cpu", "auto"] {
        let mut worker = ProcessWorker::new("external", path.clone().into());
        worker.arguments = vec!["--backend".into(), backend.into()];
        let work = Arc::new(Work {
            work_id: "offline-integration".into(),
            block_hex: "00".into(),
            prefix: b"offline fixture only".to_vec(),
            target: [255; 32],
            targets: vec![[255; 32]],
            stale_token: "fixed".into(),
        });
        let range = NonceRange {
            start_nonce: u64::MAX,
            count: 2,
        };
        let result = worker
            .search(work.clone(), range, CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert!(range.contains(result.nonce));
        assert_eq!(result.work_id, work.work_id);
        assert!(lattice_pow::hash(&work.prefix, result.nonce) <= work.target);
        let mut impossible = (*work).clone();
        impossible.target = [0; 32];
        assert!(
            worker
                .search(Arc::new(impossible), range, CancellationToken::new())
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
#[ignore = "requires LATTICE_TEST_WORKER pointing to the separately built worker"]
async fn actual_external_worker_streams_every_hit() {
    let path = std::env::var_os("LATTICE_TEST_WORKER").expect("set LATTICE_TEST_WORKER");
    let mut worker = ProcessWorker::new("external", path.into());
    worker.arguments = vec!["--backend".into(), "cpu".into()];
    let mut target = [255; 32];
    target[0] = 0x3f;
    let work = Arc::new(Work {
        work_id: "offline-integration".into(),
        block_hex: "00".into(),
        prefix: b"offline fixture only".to_vec(),
        target,
        targets: vec![target],
        stale_token: "fixed".into(),
    });
    let (jobs, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (sender, mut hits) = tokio::sync::mpsc::channel(8);
    let run = tokio::spawn(StreamWorker::stream(&worker, receiver, sender));
    jobs.send(Job {
        work: work.clone(),
        target,
        start_nonce: u64::MAX - 3,
    })
    .unwrap();
    let expected = (0..)
        .map(|i: u64| (u64::MAX - 3).wrapping_add(i))
        .filter(|n| lattice_pow::hash(&work.prefix, *n) <= target)
        .take(10);
    for nonce in expected {
        let hit = hits.recv().await.unwrap();
        assert_eq!(
            (hit.work_id.as_str(), hit.nonce),
            ("offline-integration", nonce)
        );
        assert_eq!(
            hit.hash,
            hex::encode(lattice_pow::hash(&work.prefix, nonce))
        );
    }
    drop(jobs);
    drop(hits);
    run.await.unwrap().unwrap();
}
