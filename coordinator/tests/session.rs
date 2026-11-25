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
            tokio::time::sleep(Duration::from_millis(10)).await;
            while self.hold_submits.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            let answer = self
                .answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or("childOnly");
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
        state.hits.lock().unwrap().push(hits);
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
async fn every_hit_is_submitted_in_order_one_at_a_time_old_work_included() {
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
    assert_eq!(answered, expected);
    assert_eq!(node.submits(), expected);
    assert_eq!(node.most_in_flight.load(Ordering::SeqCst), 1);
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
    match next(&mut session).await {
        Event::WorkerFailed { worker: 0, error } => {
            assert!(error.to_string().contains("fixture failure"))
        }
        other => panic!("expected a failed worker, got {other:?}"),
    }
    // After the retry wait, a new worker takes the work searched.
    quiet(&mut session).await;
    assert_eq!(workers.started.load(Ordering::SeqCst), 2);
    assert_eq!(
        jobs(&workers).await,
        vec![(0, "w1".into(), EASY, 0), (1, "w1".into(), EASY, 0)]
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
    // The first hit is being submitted, the others wait.
    let _ = tokio::time::timeout(Duration::from_millis(500), session.next()).await;
    assert_eq!(node.submits().len(), 1);
    pushes.send_replace(Some("d2".into()));
    assert!(matches!(next(&mut session).await, Event::FetchDue));
    assert_eq!(fetched(next(&mut session).await), "w2");
    // The first hit's submission is still waiting; w2 reaches the worker.
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
    assert_eq!(node.submits().len(), 1);
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
    assert_eq!(node.submits().len(), 1);
    node.hold_submits.store(false, Ordering::SeqCst);
    let events = tokio::time::timeout(Duration::from_secs(5), session.finish())
        .await
        .expect("finished");
    assert_eq!(events.len(), 3);
    assert_eq!(
        node.submits(),
        found
            .iter()
            .map(|n| ("w1".to_string(), *n))
            .collect::<Vec<_>>()
    );
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
