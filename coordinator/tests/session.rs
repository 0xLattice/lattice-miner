//! The session `lattice-miner run` mines with, against a fake node and fake
//! stream workers.
use futures_util::{FutureExt, future::BoxFuture};
use lattice_miner::*;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc, watch};

const EASY: Target = {
    let mut t = [0xff; 32];
    t[0] = 0x7f;
    t
};
const HARD: Target = {
    let mut t = [0xff; 32];
    t[0] = 0x0f;
    t
};

fn work(id: &str, digest: &str) -> Work {
    Work {
        work_id: id.into(),
        block_hex: "00".into(),
        prefix: format!("prefix-{id}").into_bytes(),
        target: EASY,
        targets: vec![EASY, HARD],
        stale_token: digest.into(),
    }
}

/// A nonce of `work` whose hash is at most `at_most` but above `above`.
fn nonce(work: &str, above: Target, at_most: Target, skip: usize) -> u64 {
    let prefix = format!("prefix-{work}").into_bytes();
    (0..)
        .filter(|&n| {
            let h = lattice_pow::hash(&prefix, n);
            h > above && h <= at_most
        })
        .nth(skip)
        .unwrap()
}

struct Node {
    works: Mutex<VecDeque<Work>>,
    /// One permit per fetch allowed to return.
    gate: Semaphore,
    fetches: AtomicUsize,
    answers: Mutex<VecDeque<&'static str>>,
    submits: Mutex<Vec<(String, u64)>>,
    in_flight: AtomicUsize,
    most_in_flight: AtomicUsize,
    /// Submissions wait while this is set.
    hold_submits: std::sync::atomic::AtomicBool,
}

impl Node {
    fn new(works: Vec<Work>) -> Arc<Self> {
        Arc::new(Self {
            works: Mutex::new(works.into()),
            gate: Semaphore::new(0),
            fetches: AtomicUsize::new(0),
            answers: Mutex::new(VecDeque::new()),
            submits: Mutex::new(vec![]),
            in_flight: AtomicUsize::new(0),
            most_in_flight: AtomicUsize::new(0),
            hold_submits: Default::default(),
        })
    }
    fn submits(&self) -> Vec<(String, u64)> {
        self.submits.lock().unwrap().clone()
    }
}

impl NodeClient for Node {
    fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>> {
        async {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.gate.acquire().await.unwrap().forget();
            Ok(self.works.lock().unwrap().pop_front())
        }
        .boxed()
    }
    fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>> {
        async { Ok(None) }.boxed()
    }
    fn submit(&self, id: String, nonce: u64) -> BoxFuture<'_, Result<Submission, Error>> {
        async move {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.most_in_flight.fetch_max(now, Ordering::SeqCst);
            self.submits.lock().unwrap().push((id, nonce));
            // Answers go to submissions in the order they are made.
            let answer = self
                .answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or("childOnly");
            tokio::time::sleep(Duration::from_millis(10)).await;
            while self.hold_submits.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(Submission {
                accepted: answer == "accepted",
                disposition: answer.into(),
                tip_cid: None,
            })
        }
        .boxed()
    }
}

/// Records every job each started worker gets, and hands the test each
/// worker's hit channel.
#[derive(Default)]
struct Workers {
    started: AtomicUsize,
    /// (start, work, target, start nonce), in arrival order.
    jobs: Mutex<Vec<(usize, String, Target, u64)>>,
    hits: Mutex<Vec<mpsc::UnboundedSender<StreamHit>>>,
    closed: Mutex<Vec<usize>>,
    /// Starts that fail after their first job.
    failing: Mutex<Vec<usize>>,
    /// Hits a worker sends as it stops, a moment after its jobs close.
    parting: Mutex<Vec<StreamHit>>,
}

struct Fake(Arc<Workers>);

impl StreamWorker for Fake {
    fn stream(
        &self,
        mut jobs: mpsc::UnboundedReceiver<Job>,
        hits: mpsc::UnboundedSender<StreamHit>,
    ) -> BoxFuture<'static, Result<(), Error>> {
        let state = self.0.clone();
        let start = state.started.fetch_add(1, Ordering::SeqCst);
        state.hits.lock().unwrap().push(hits.clone());
        async move {
            while let Some(job) = jobs.recv().await {
                state.jobs.lock().unwrap().push((
                    start,
                    job.work.work_id.clone(),
                    job.target,
                    job.start_nonce,
                ));
                if state.failing.lock().unwrap().contains(&start) {
                    return Err(Error::Worker("fixture failure".into()));
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            for hit in state.parting.lock().unwrap().drain(..) {
                let _ = hits.send(hit);
            }
            state.closed.lock().unwrap().push(start);
            Ok(())
        }
        .boxed()
    }
}

fn session(
    node: &Arc<Node>,
    workers: &Arc<Workers>,
    count: usize,
) -> (Session, watch::Sender<Option<String>>) {
    let (pushes, digests) = watch::channel(Some("d0".to_string()));
    let session = Session::new(
        node.clone(),
        (0..count)
            .map(|_| Arc::new(Fake(workers.clone())) as Arc<dyn StreamWorker>)
            .collect(),
        Some(digests),
        SessionOptions {
            retry_wait: Duration::from_millis(50),
            retry_delay: Duration::ZERO,
            ..Default::default()
        },
    );
    (session, pushes)
}

/// The next event, answering a due fetch the way `run` does.
async fn next(session: &mut Session) -> Event {
    let event = tokio::time::timeout(Duration::from_secs(5), session.next())
        .await
        .expect("an event");
    if matches!(event, Event::FetchDue) {
        session.fetch();
    }
    event
}

/// No event for a while.
async fn quiet(session: &mut Session) {
    if let Ok(event) = tokio::time::timeout(Duration::from_millis(100), session.next()).await {
        panic!("unexpected {event:?}");
    }
}

fn fetched(event: Event) -> String {
    match event {
        Event::Fetched(Ok(Some(work))) => work.work_id.clone(),
        other => panic!("expected a template, got {other:?}"),
    }
}

fn submitted(event: Event) -> (String, u64, String) {
    match event {
        Event::Submitted {
            work_id,
            nonce,
            result,
        } => (work_id, nonce, result.unwrap().disposition),
        other => panic!("expected a submission, got {other:?}"),
    }
}

/// Every job given, each worker's in the order it got them, once the
/// workers have taken what was sent.
async fn jobs(workers: &Workers) -> Vec<(usize, String, Target, u64)> {
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut jobs = workers.jobs.lock().unwrap().clone();
    jobs.sort_by_key(|job| job.0);
    jobs
}

/// Worker `start` finds these, in this order.
fn hits(workers: &Workers, start: usize, found: &[(&str, u64)]) {
    let sender = workers.hits.lock().unwrap()[start].clone();
    let found: Vec<_> = found
        .iter()
        .map(|(work_id, nonce)| StreamHit {
            work_id: work_id.to_string(),
            nonce: *nonce,
            hash: String::new(),
        })
        .collect();
    for hit in found {
        sender.send(hit).unwrap();
    }
}

#[tokio::test]
async fn pushed_digests_lead_to_one_fetch_at_a_time_and_jobs_to_the_same_workers() {
    let node = Node::new(vec![work("w1", "d1"), work("w2", "d2"), work("w3", "d4")]);
    let workers = Arc::new(Workers::default());
    let (mut session, pushes) = session(&node, &workers, 2);
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    node.gate.add_permits(1);
    assert_eq!(fetched(next(&mut session).await), "w1");
    // The digest the template was fetched with says nothing new.
    pushes.send_replace(Some("d1".into()));
    quiet(&mut session).await;
    pushes.send_replace(Some("d2".into()));
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    // While that fetch is in flight, pushes start no other.
    pushes.send_replace(Some("d3".into()));
    quiet(&mut session).await;
    pushes.send_replace(Some("d4".into()));
    quiet(&mut session).await;
    assert_eq!(node.fetches.load(Ordering::SeqCst), 2);
    node.gate.add_permits(1);
    assert_eq!(fetched(next(&mut session).await), "w2");
    // The newest push differs from what was fetched: one more fetch.
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    node.gate.add_permits(1);
    assert_eq!(fetched(next(&mut session).await), "w3");
    quiet(&mut session).await;
    assert_eq!(node.fetches.load(Ordering::SeqCst), 3);
    // Two worker processes for the whole session, each on its own share of
    // the nonces.
    assert_eq!(workers.started.load(Ordering::SeqCst), 2);
    let half = 1 << 63;
    assert_eq!(
        jobs(&workers).await,
        vec![
            (0, "w1".into(), EASY, 0),
            (0, "w2".into(), EASY, 0),
            (0, "w3".into(), EASY, 0),
            (1, "w1".into(), EASY, half),
            (1, "w2".into(), EASY, half),
            (1, "w3".into(), EASY, half),
        ]
    );
}

#[tokio::test]
async fn every_hit_is_submitted_at_once_old_work_included() {
    let node = Node::new(vec![work("w1", "d1"), work("w2", "d2")]);
    let workers = Arc::new(Workers::default());
    let (mut session, pushes) = session(&node, &workers, 1);
    node.gate.add_permits(2);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    pushes.send_replace(Some("d2".into()));
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w2");
    let (a, b) = (nonce("w1", HARD, EASY, 0), nonce("w1", HARD, EASY, 1));
    let c = nonce("w2", HARD, EASY, 0);
    // Hits on the old work, found before the worker took the new job.
    hits(&workers, 0, &[("w1", a), ("w1", b), ("w2", c)]);
    let mut answered = vec![];
    for _ in 0..3 {
        let (work, nonce, _) = submitted(next(&mut session).await);
        answered.push((work, nonce));
    }
    let expected = vec![
        ("w1".to_string(), a),
        ("w1".to_string(), b),
        ("w2".to_string(), c),
    ];
    answered.sort();
    let mut submits = node.submits();
    submits.sort();
    assert_eq!(answered, expected);
    assert_eq!(submits, expected);
    // Side by side, none waiting for another's answer.
    assert_eq!(node.most_in_flight.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn child_only_searches_the_same_work_harder_and_a_parent_block_ends_it() {
    let node = Node::new(vec![work("w1", "d1")]);
    let workers = Arc::new(Workers::default());
    let (mut session, _pushes) = session(&node, &workers, 1);
    node.gate.add_permits(1);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    node.answers
        .lock()
        .unwrap()
        .extend(["childOnly", "accepted"]);
    let child = nonce("w1", HARD, EASY, 0);
    let passed = nonce("w1", HARD, EASY, 1);
    let parent = nonce("w1", [0; 32], HARD, 0);
    hits(&workers, 0, &[("w1", child)]);
    assert_eq!(
        submitted(next(&mut session).await),
        ("w1".into(), child, "childOnly".into())
    );
    // Found at the easier target the work has since passed: a solution,
    // so submitted all the same.
    node.answers.lock().unwrap().push_front("childOnly");
    hits(&workers, 0, &[("w1", passed), ("w1", parent)]);
    assert_eq!(
        submitted(next(&mut session).await),
        ("w1".into(), passed, "childOnly".into())
    );
    assert_eq!(
        submitted(next(&mut session).await),
        ("w1".into(), parent, "accepted".into())
    );
    // The tip moved: a template is due.
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    // A hit found before the search stopped is still a block: submitted.
    let sibling = nonce("w1", [0; 32], HARD, 1);
    hits(&workers, 0, &[("w1", sibling)]);
    assert_eq!(submitted(next(&mut session).await).1, sibling);
    assert_eq!(
        node.submits(),
        vec![
            ("w1".into(), child),
            ("w1".into(), passed),
            ("w1".into(), parent),
            ("w1".into(), sibling)
        ]
    );
    assert_eq!(
        jobs(&workers).await,
        vec![
            (0, "w1".into(), EASY, 0),
            (0, "w1".into(), HARD, 0),
            (0, "w1".into(), [0; 32], 0),
        ]
    );
}

#[tokio::test]
async fn a_failed_worker_is_started_again_with_the_current_job() {
    let node = Node::new(vec![work("w1", "d1")]);
    let workers = Arc::new(Workers::default());
    workers.failing.lock().unwrap().push(0);
    let (mut session, _pushes) = session(&node, &workers, 1);
    node.gate.add_permits(1);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    // What it found before it failed.
    let found = nonce("w1", HARD, EASY, 2);
    node.answers.lock().unwrap().push_back("duplicate");
    hits(&workers, 0, &[("w1", found)]);
    let mut failed = None;
    let _ = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            if let Event::WorkerFailed { worker, error } = session.next().await {
                failed = Some((worker, error.to_string()));
            }
        }
    })
    .await;
    let (worker, error) = failed.expect("a failed worker");
    assert_eq!(worker, 0);
    assert!(error.contains("fixture failure"));
    // After the retry wait, a new worker takes the work searched, on from
    // the last hit rather than over nonces already covered.
    assert_eq!(workers.started.load(Ordering::SeqCst), 2);
    assert_eq!(
        jobs(&workers).await,
        vec![(0, "w1".into(), EASY, 0), (1, "w1".into(), EASY, found + 1)]
    );
}

#[tokio::test]
async fn a_pause_withholds_jobs_until_it_ends() {
    let node = Node::new(vec![work("w1", "d1"), work("w2", "d2")]);
    let workers = Arc::new(Workers::default());
    let (mut session, pushes) = session(&node, &workers, 1);
    node.gate.add_permits(2);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    session.pause(Duration::from_millis(300));
    // A push during the hold fetches nothing.
    pushes.send_replace(Some("d2".into()));
    quiet(&mut session).await;
    assert_eq!(node.fetches.load(Ordering::SeqCst), 1);
    // The same worker process idles on a zero target: nothing restarts.
    assert!(workers.closed.lock().unwrap().is_empty());
    assert_eq!(workers.started.load(Ordering::SeqCst), 1);
    assert_eq!(
        jobs(&workers).await,
        vec![(0, "w1".into(), EASY, 0), (0, "w1".into(), [0; 32], 0)]
    );
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    assert_eq!(fetched(next(&mut session).await), "w2");
    assert_eq!(
        jobs(&workers).await,
        vec![
            (0, "w1".into(), EASY, 0),
            (0, "w1".into(), [0; 32], 0),
            (0, "w2".into(), EASY, 0)
        ]
    );
    assert_eq!(workers.started.load(Ordering::SeqCst), 1);
}

/// A worker process with hits waiting while a submission is in flight still
/// gets the next job.
#[cfg(unix)]
#[tokio::test]
async fn hits_waiting_to_be_submitted_do_not_hold_up_the_next_job() {
    let node = Node::new(vec![work("w1", "d1"), work("w2", "d2")]);
    node.hold_submits.store(true, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let (script, seen) = (dir.path().join("worker.py"), dir.path().join("jobs"));
    // Every job it gets is written down; the first is answered with three hits.
    std::fs::write(
        &script,
        r#"import sys, json
log = open(sys.argv[1], 'a')
hits = [int(n) for n in sys.argv[2].split(',')]
for line in sys.stdin:
    job = json.loads(line)
    log.write(job['workId'] + '\n'); log.flush()
    for n in hits:
        print(json.dumps({'workId': job['workId'], 'nonce': n, 'hash': ''}), flush=True)
    hits = []
"#,
    )
    .unwrap();
    let found: Vec<String> = (0..3)
        .map(|i| nonce("w1", HARD, EASY, i).to_string())
        .collect();
    let mut worker = ProcessWorker::new("python3".into());
    worker.arguments = vec![
        script.to_string_lossy().into_owned(),
        seen.to_string_lossy().into_owned(),
        found.join(","),
    ];
    let (pushes, digests) = watch::channel(Some("d0".to_string()));
    let mut session = Session::new(
        node.clone(),
        vec![Arc::new(worker) as Arc<dyn StreamWorker>],
        Some(digests),
        SessionOptions::default(),
    );
    node.gate.add_permits(2);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    // Every hit is being submitted, none answered yet.
    let _ = tokio::time::timeout(Duration::from_millis(500), session.next()).await;
    assert_eq!(node.submits().len(), 3);
    pushes.send_replace(Some("d2".into()));
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    assert_eq!(fetched(next(&mut session).await), "w2");
    // The submissions are still waiting; w2 reaches the worker.
    let reached = tokio::time::timeout(Duration::from_secs(5), async {
        while !std::fs::read_to_string(&seen)
            .unwrap_or_default()
            .contains("w2")
        {
            tokio::select! {
                _ = session.next() => {}
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
    })
    .await;
    assert!(reached.is_ok(), "the new job never reached the worker");
    assert_eq!(node.submits().len(), 3);
}

#[tokio::test]
async fn a_duplicate_changes_nothing_and_a_refusal_fetches_at_once() {
    let node = Node::new(vec![work("w1", "d1"), work("w1", "d1")]);
    let workers = Arc::new(Workers::default());
    let (mut session, _pushes) = session(&node, &workers, 1);
    node.gate.add_permits(2);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    node.answers
        .lock()
        .unwrap()
        .extend(["duplicate", "invalid"]);
    let (a, b) = (nonce("w1", HARD, EASY, 0), nonce("w1", HARD, EASY, 1));
    hits(&workers, 0, &[("w1", a)]);
    assert_eq!(submitted(next(&mut session).await).2, "duplicate");
    quiet(&mut session).await;
    hits(&workers, 0, &[("w1", b)]);
    assert_eq!(submitted(next(&mut session).await).2, "invalid");
    // Not a transport failure: a template at once, and the search goes on.
    let event = tokio::time::timeout(Duration::from_millis(20), session.next())
        .await
        .expect("a fetch due at once");
    assert!(matches!(event, Event::FetchDue));
    assert_eq!(jobs(&workers).await, vec![(0, "w1".into(), EASY, 0)]);
}

#[tokio::test]
async fn the_same_work_fetched_again_after_it_ended_is_searched_again() {
    let node = Node::new(vec![work("w1", "d1"), work("w1", "d1")]);
    let workers = Arc::new(Workers::default());
    let (mut session, _pushes) = session(&node, &workers, 1);
    node.gate.add_permits(2);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    node.answers.lock().unwrap().push_back("accepted");
    hits(&workers, 0, &[("w1", nonce("w1", [0; 32], HARD, 0))]);
    assert_eq!(submitted(next(&mut session).await).2, "accepted");
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    assert_eq!(fetched(next(&mut session).await), "w1");
    assert_eq!(
        jobs(&workers).await,
        vec![
            (0, "w1".into(), EASY, 0),
            (0, "w1".into(), [0; 32], 0),
            (0, "w1".into(), EASY, 0)
        ]
    );
}

#[tokio::test]
async fn finishing_submits_every_hit_already_found() {
    let node = Node::new(vec![work("w1", "d1")]);
    let workers = Arc::new(Workers::default());
    let (mut session, _pushes) = session(&node, &workers, 1);
    node.gate.add_permits(1);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    node.hold_submits.store(true, Ordering::SeqCst);
    let found: Vec<u64> = (0..3).map(|i| nonce("w1", HARD, EASY, i)).collect();
    hits(
        &workers,
        0,
        &[("w1", found[0]), ("w1", found[1]), ("w1", found[2])],
    );
    let _ = tokio::time::timeout(Duration::from_millis(200), session.next()).await;
    assert_eq!(node.submits().len(), 3);
    // One more, found as the worker stops.
    let last = nonce("w1", HARD, EASY, 3);
    workers.parting.lock().unwrap().push(StreamHit {
        work_id: "w1".into(),
        nonce: last,
        hash: String::new(),
    });
    node.hold_submits.store(false, Ordering::SeqCst);
    let events = tokio::time::timeout(Duration::from_secs(5), session.finish())
        .await
        .expect("finished");
    assert_eq!(events.len(), 4);
    let mut submits = node.submits();
    submits.sort();
    let mut expected: Vec<_> = found
        .iter()
        .chain([&last])
        .map(|n| ("w1".to_string(), *n))
        .collect();
    expected.sort();
    assert_eq!(submits, expected);
}

/// The in-process worker idles on a zero target and goes on from where it
/// was.
#[tokio::test]
async fn the_cpu_worker_idles_on_a_zero_target() {
    let (jobs, receiver) = mpsc::unbounded_channel();
    let (sender, mut hits) = mpsc::unbounded_channel();
    let stream = CpuWorker.stream(receiver, sender);
    let task = tokio::spawn(stream);
    let w1 = Arc::new(work("w1", "d1"));
    let job = |target| Job {
        work: w1.clone(),
        target,
        start_nonce: 0,
    };
    jobs.send(job([0; 32])).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    jobs.send(job(EASY)).unwrap();
    let first = hits.recv().await.unwrap();
    assert_eq!(first.nonce, nonce("w1", [0; 32], EASY, 0));
    drop(jobs);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_pause_is_not_extended_by_another() {
    let node = Node::new(vec![work("w1", "d1"), work("w2", "d2")]);
    let workers = Arc::new(Workers::default());
    let (mut session, _pushes) = session(&node, &workers, 1);
    node.gate.add_permits(2);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    let paused = tokio::time::Instant::now();
    session.pause(Duration::from_millis(300));
    let _ = tokio::time::timeout(Duration::from_millis(150), session.next()).await;
    session.pause(Duration::from_millis(300));
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    let held = paused.elapsed();
    assert!(held < Duration::from_millis(420), "held {held:?}");
}

/// The backlog: a real CPU worker at an easy target finds hits far faster
/// than the node answers them. Hits on the old template are still being
/// answered when the current template's first hit is: it is not queued
/// behind them.
#[tokio::test]
async fn hits_on_an_old_template_do_not_hold_up_the_current_ones() {
    #[derive(Default)]
    struct Slow {
        fetched: AtomicUsize,
        old_in_flight: AtomicUsize,
    }
    impl NodeClient for Slow {
        fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>> {
            async {
                let n = self.fetched.fetch_add(1, Ordering::SeqCst);
                let mut easy = [0xff; 32];
                easy[0] = 0;
                let mut hard = easy;
                hard[1] = 0;
                Ok(Some(Work {
                    work_id: format!("w{n}"),
                    block_hex: "00".into(),
                    prefix: format!("prefix-w{n}").into_bytes(),
                    target: easy,
                    targets: vec![easy, hard],
                    stale_token: format!("d{n}"),
                }))
            }
            .boxed()
        }
        fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>> {
            async { Ok(None) }.boxed()
        }
        fn submit(&self, id: String, _: u64) -> BoxFuture<'_, Result<Submission, Error>> {
            async move {
                let old = id == "w0";
                if old {
                    self.old_in_flight.fetch_add(1, Ordering::SeqCst);
                }
                let rtt = if old { 300 } else { 10 };
                tokio::time::sleep(Duration::from_millis(rtt)).await;
                if old {
                    self.old_in_flight.fetch_sub(1, Ordering::SeqCst);
                }
                Ok(Submission {
                    accepted: false,
                    disposition: if old { "duplicate" } else { "childOnly" }.into(),
                    tip_cid: None,
                })
            }
            .boxed()
        }
    }
    let node = Arc::new(Slow::default());
    let (pushes, digests) = watch::channel(Some("d0".to_string()));
    let mut session = Session::new(
        node.clone(),
        vec![Arc::new(CpuWorker) as Arc<dyn StreamWorker>],
        Some(digests),
        SessionOptions::default(),
    );
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w0");
    // w0's hits pile up.
    let _ = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            session.next().await;
        }
    })
    .await;
    pushes.send_replace(Some("d1".into()));
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    assert_eq!(fetched(next(&mut session).await), "w1");
    let fetched_at = tokio::time::Instant::now();
    let answered = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Event::Submitted { work_id, .. } = session.next().await
                && work_id == "w1"
            {
                return;
            }
        }
    })
    .await;
    assert!(
        answered.is_ok(),
        "no hit on the current template answered within 1 s"
    );
    assert!(fetched_at.elapsed() < Duration::from_secs(1));
    assert!(node.old_in_flight.load(Ordering::SeqCst) > 0);
}

/// Many hits found and not yet submitted when mining stops go out side by
/// side: stopping takes about one answer's time, not one per hit.
#[tokio::test]
async fn finishing_with_many_hits_found_is_prompt() {
    let node = Node::new(vec![work("w1", "d1")]);
    let workers = Arc::new(Workers::default());
    let (mut session, _pushes) = session(&node, &workers, 1);
    node.gate.add_permits(1);
    next(&mut session).await;
    assert_eq!(fetched(next(&mut session).await), "w1");
    let found: Vec<(&str, u64)> = (0..10_000).map(|n| ("w1", n)).collect();
    hits(&workers, 0, &found);
    let started = tokio::time::Instant::now();
    let events = tokio::time::timeout(Duration::from_secs(5), session.finish())
        .await
        .expect("finished");
    // Each answer takes 10 ms; one after another would take 100 s.
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(events.len(), 10_000);
    assert_eq!(node.submits().len(), 10_000);
}

/// Answers that come back out of order: a slow child-only answer at an easier
/// target does not lower the target a faster one raised, and a block
/// accepted on old work does not idle the current one.
mod out_of_order {
    use futures_util::{FutureExt, future::BoxFuture};
    use lattice_miner::*;
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::{mpsc, watch};

    fn t(b0: u8) -> Target {
        let mut t = [0xff; 32];
        t[0] = b0;
        t
    }
    const IDLE: Target = [0; 32];

    struct Node {
        works: Mutex<Vec<Work>>,
        answers: Mutex<HashMap<u64, (&'static str, u64)>>,
        inflight: AtomicUsize,
        most: AtomicUsize,
    }
    impl NodeClient for Node {
        fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>> {
            async {
                let mut w = self.works.lock().unwrap();
                Ok(if w.is_empty() {
                    None
                } else {
                    Some(w.remove(0))
                })
            }
            .boxed()
        }
        fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>> {
            async { Ok(None) }.boxed()
        }
        fn submit(&self, _id: String, nonce: u64) -> BoxFuture<'_, Result<Submission, Error>> {
            async move {
                let n = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
                self.most.fetch_max(n, Ordering::SeqCst);
                let (a, ms) = self
                    .answers
                    .lock()
                    .unwrap()
                    .get(&nonce)
                    .copied()
                    .unwrap_or(("duplicate", 300));
                tokio::time::sleep(Duration::from_millis(ms)).await;
                self.inflight.fetch_sub(1, Ordering::SeqCst);
                Ok(Submission {
                    accepted: a == "accepted",
                    disposition: a.into(),
                    tip_cid: None,
                })
            }
            .boxed()
        }
    }
    #[derive(Default)]
    struct W {
        jobs: Mutex<Vec<(String, Target)>>,
        hits: Mutex<Option<mpsc::UnboundedSender<StreamHit>>>,
    }
    struct Fake(Arc<W>);
    impl StreamWorker for Fake {
        fn stream(
            &self,
            mut jobs: mpsc::UnboundedReceiver<Job>,
            hits: mpsc::UnboundedSender<StreamHit>,
        ) -> BoxFuture<'static, Result<(), Error>> {
            let s = self.0.clone();
            *s.hits.lock().unwrap() = Some(hits);
            async move {
                while let Some(j) = jobs.recv().await {
                    s.jobs
                        .lock()
                        .unwrap()
                        .push((j.work.work_id.clone(), j.target));
                }
                Ok(())
            }
            .boxed()
        }
    }
    fn work(id: &str, d: &str) -> Work {
        Work {
            work_id: id.into(),
            block_hex: "00".into(),
            prefix: format!("p-{id}").into_bytes(),
            target: t(0x7f),
            targets: vec![t(0x7f), t(0x3f), t(0x0f)],
            stale_token: d.into(),
        }
    }
    fn nonce(id: &str, above: Target, at_most: Target) -> u64 {
        let p = format!("p-{id}").into_bytes();
        (0..)
            .find(|&n| {
                let h = lattice_pow::hash(&p, n);
                h > above && h <= at_most
            })
            .unwrap()
    }
    async fn drive(s: &mut Session, ms: u64) {
        let _ = tokio::time::timeout(Duration::from_millis(ms), async {
            loop {
                if let Event::FetchDue = s.next().await {
                    s.fetch();
                }
            }
        })
        .await;
    }
    fn hit(w: &W, id: &str, n: u64) {
        w.hits
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(StreamHit {
                work_id: id.into(),
                nonce: n,
                hash: String::new(),
            })
            .unwrap();
    }

    #[tokio::test]
    async fn stale_child_only_does_not_lower_target_and_old_accept_does_not_idle() {
        let node = Arc::new(Node {
            works: Mutex::new(vec![work("a", "d1"), work("b", "d2")]),
            answers: Default::default(),
            inflight: 0.into(),
            most: 0.into(),
        });
        let w = Arc::new(W::default());
        let (push, rx) = watch::channel(Some("d1".to_string()));
        let mut s = Session::new(
            node.clone(),
            vec![Arc::new(Fake(w.clone())) as Arc<dyn StreamWorker>],
            Some(rx),
            SessionOptions {
                retry_delay: Duration::ZERO,
                ..Default::default()
            },
        );
        drive(&mut s, 50).await;
        let easyhit = nonce("a", t(0x3f), t(0x7f)); // clears 7f only
        let midhit = nonce("a", t(0x0f), t(0x3f)); // clears 3f
        node.answers
            .lock()
            .unwrap()
            .insert(easyhit, ("childOnly", 200));
        node.answers
            .lock()
            .unwrap()
            .insert(midhit, ("childOnly", 10));
        hit(&w, "a", easyhit);
        hit(&w, "a", midhit);
        drive(&mut s, 400).await;
        let jobs = w.jobs.lock().unwrap().clone();
        assert_eq!(jobs.last().unwrap(), &("a".to_string(), t(0x0f)));
        assert!(!jobs.iter().any(|j| j.1 == t(0x3f)));
        // old-work accepted arrives after current moved to b
        let parent = nonce("a", IDLE, t(0x0f));
        node.answers
            .lock()
            .unwrap()
            .insert(parent, ("accepted", 200));
        hit(&w, "a", parent);
        drive(&mut s, 20).await;
        push.send_replace(Some("d2".into()));
        drive(&mut s, 400).await;
        let jobs = w.jobs.lock().unwrap().clone();
        assert_eq!(jobs.last().unwrap(), &("b".to_string(), t(0x7f)));
        // childOnly for current after accepted on current: stays idle
        let bparent = nonce("b", IDLE, t(0x0f));
        let bchild = nonce("b", t(0x3f), t(0x7f));
        node.answers
            .lock()
            .unwrap()
            .insert(bparent, ("accepted", 10));
        node.answers
            .lock()
            .unwrap()
            .insert(bchild, ("childOnly", 200));
        hit(&w, "b", bchild);
        hit(&w, "b", bparent);
        drive(&mut s, 400).await;
        let jobs = w.jobs.lock().unwrap().clone();
        assert_eq!(jobs.last().unwrap(), &("b".to_string(), IDLE));
    }
}
