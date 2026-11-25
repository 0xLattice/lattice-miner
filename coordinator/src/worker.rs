use crate::*;
use futures_util::{FutureExt, future::BoxFuture};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};
/// The built-in search, on one CPU thread.
pub struct CpuWorker;
#[derive(Clone)]
pub struct ProcessWorker {
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub maximum_stdout: usize,
    pub drain_timeout: Duration,
}
impl ProcessWorker {
    pub fn new(executable: PathBuf) -> Self {
        Self {
            executable,
            arguments: vec![],
            maximum_stdout: 64 * 1024,
            drain_timeout: Duration::from_secs(1),
        }
    }
}
#[cfg(unix)]
struct GroupGuard(i32);
#[cfg(unix)]
impl Drop for GroupGuard {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.0),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}
struct ReaderGuard<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for ReaderGuard<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn output(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    limit: usize,
    tail: bool,
) -> Result<Vec<u8>, Error> {
    let mut out = vec![];
    let mut chunk = [0; 8192];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            return Ok(out);
        }
        if tail {
            out.extend_from_slice(&chunk[..n]);
            if out.len() > limit {
                out.drain(..out.len() - limit);
            }
        } else {
            if n > limit.saturating_sub(out.len()) {
                return Err(Error::Worker("stdout exceeds byte limit".into()));
            }
            out.extend_from_slice(&chunk[..n]);
        }
    }
}

/// Nonces the built-in search covers between looks at its jobs.
const CPU_STREAM_CHUNK: u64 = 1 << 16;

impl StreamWorker for CpuWorker {
    fn stream(
        &self,
        mut jobs: tokio::sync::mpsc::UnboundedReceiver<Job>,
        hits: tokio::sync::mpsc::UnboundedSender<StreamHit>,
    ) -> BoxFuture<'static, Result<(), Error>> {
        async move {
            tokio::task::spawn_blocking(move || {
                use tokio::sync::mpsc::error::TryRecvError;
                // The job searched, its prefix midstate and where it is.
                let mut current: Option<(Job, Sha256, u64)> = None;
                loop {
                    loop {
                        let next = match &current {
                            // Nothing to search, or a zero target: wait.
                            None => jobs.blocking_recv(),
                            Some((job, _, _)) if job.target == [0; 32] => jobs.blocking_recv(),
                            Some(_) => match jobs.try_recv() {
                                Ok(job) => Some(job),
                                Err(TryRecvError::Empty) => break,
                                Err(TryRecvError::Disconnected) => None,
                            },
                        };
                        let Some(next) = next else { return };
                        match current.as_mut() {
                            Some((job, _, _)) if job.work.work_id == next.work.work_id => {
                                job.target = next.target
                            }
                            _ => {
                                let mut base = Sha256::new();
                                base.update(&next.work.prefix);
                                let start = next.start_nonce;
                                current = Some((next, base, start));
                            }
                        }
                    }
                    // Up to the end of a chunk or the next hit, then a look
                    // at the jobs, as the worker executable does.
                    let (job, base, nonce) = current.as_mut().expect("a job to search");
                    for _ in 0..CPU_STREAM_CHUNK {
                        let mut h = base.clone();
                        h.update(nonce.to_be_bytes());
                        let hash: [u8; 32] = h.finalize().into();
                        let found = *nonce;
                        *nonce = nonce.wrapping_add(1);
                        if hash <= job.target {
                            let hit = StreamHit {
                                work_id: job.work.work_id.clone(),
                                nonce: found,
                                hash: hex::encode(hash),
                            };
                            if hits.send(hit).is_err() {
                                return;
                            }
                            break;
                        }
                    }
                }
            })
            .await
            .map_err(|e| Error::Worker(e.to_string()))
        }
        .boxed()
    }
}

impl StreamWorker for ProcessWorker {
    /// The executable with `--stream`: one JSON job per line in, one JSON
    /// hit per line out. Closing its stdin ends it.
    fn stream(
        &self,
        mut jobs: tokio::sync::mpsc::UnboundedReceiver<Job>,
        hits: tokio::sync::mpsc::UnboundedSender<StreamHit>,
    ) -> BoxFuture<'static, Result<(), Error>> {
        let this = self.clone();
        async move {
            use futures_util::StreamExt;
            use tokio::io::AsyncWriteExt;
            let mut command = Command::new(&this.executable);
            command
                .args(&this.arguments)
                .arg("--stream")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                command.as_std_mut().process_group(0);
            }
            let mut child = command.spawn()?;
            #[cfg(unix)]
            let _group = GroupGuard(child.id().expect("spawned PID") as i32);
            let mut stdin = child.stdin.take();
            let mut lines = tokio_util::codec::FramedRead::new(
                child.stdout.take().unwrap(),
                tokio_util::codec::LinesCodec::new_with_max_length(this.maximum_stdout),
            );
            let mut err = ReaderGuard(tokio::spawn(output(
                child.stderr.take().unwrap(),
                4096,
                true,
            )));
            loop {
                tokio::select! {
                    job = jobs.recv(), if stdin.is_some() => match job {
                        Some(job) => {
                            let line = serde_json::to_string(&StreamJob {
                                work_id: job.work.work_id.clone(),
                                prefix_hex: hex::encode(&job.work.prefix),
                                target: hex::encode(job.target),
                                start_nonce: job.start_nonce,
                            })? + "\n";
                            let stdin = stdin.as_mut().expect("open while given jobs");
                            // A worker that has exited is reported below.
                            let _ = stdin.write_all(line.as_bytes()).await;
                        }
                        None => stdin = None,
                    },
                    line = lines.next() => match line {
                        Some(Ok(line)) => {
                            let hit: StreamHit = serde_json::from_str(&line).map_err(|_| {
                                Error::Worker(format!("invalid worker line: {line}"))
                            })?;
                            // Never waits: the session keeps what it has not
                            // submitted yet, so the next job is never held up.
                            if hits.send(hit).is_err() {
                                return Ok(());
                            }
                        }
                        Some(Err(e)) => return Err(Error::Worker(e.to_string())),
                        None => break,
                    },
                }
            }
            let status = child.wait().await?;
            let stderr = tokio::time::timeout(this.drain_timeout, &mut err.0)
                .await
                .map_err(|_| Error::Worker("stderr drain deadline exceeded".into()))?
                .map_err(|e| Error::Worker(e.to_string()))??;
            if !status.success() || stdin.is_some() {
                return Err(Error::Worker(format!(
                    "exit {status}: {}",
                    String::from_utf8_lossy(&stderr)
                )));
            }
            Ok(())
        }
        .boxed()
    }
}
