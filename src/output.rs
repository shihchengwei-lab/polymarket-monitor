use std::{
    io::{self, Write},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, timeout_at};

const FINAL_OUTPUT_TIMEOUT: Duration = Duration::from_secs(5);

/// Format external text within a single terminal field.
pub fn single_line(text: &str) -> impl std::fmt::Display + '_ {
    std::fmt::from_fn(move |f| {
        use std::fmt::Write;
        for ch in text.chars() {
            if ch.is_control()
                || matches!(ch, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{2028}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
            {
                write!(f, "{}", ch.escape_debug())?;
            } else {
                f.write_char(ch)?;
            }
        }
        Ok(())
    })
}

pub(crate) enum Content {
    Text(String),
    Book(Arc<crate::book::view::BookView>, bool),
}
impl From<String> for Content {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}
impl std::fmt::Display for Content {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text(text) => f.write_str(text),
            Self::Book(book, pad) => {
                write!(f, "{book}")?;
                if *pad {
                    writeln!(f)?;
                }
                Ok(())
            }
        }
    }
}

struct Frame {
    payload: Content,
    completed: Option<Receipt>,
}

#[derive(Clone, Debug)]
pub(crate) struct WriteFailure(Arc<io::Error>);

impl std::fmt::Display for WriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("write terminal output")
    }
}

impl std::error::Error for WriteFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl WriteFailure {
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

struct Receipt {
    deadline: Instant,
    sender: oneshot::Sender<Result<Instant>>,
}

#[derive(Default)]
struct State {
    next: Option<Frame>,
    closing: bool,
}

#[derive(Default)]
struct Mailbox {
    state: Mutex<State>,
    changed: Condvar,
}

struct Writer {
    mailbox: Arc<Mailbox>,
    failed: watch::Receiver<Option<WriteFailure>>,
}

impl Writer {
    fn start(mut output: impl Write + Send + 'static) -> Self {
        Self::start_with(move |payload| {
            output.write_all(payload.to_string().as_bytes())?;
            output.flush()
        })
    }

    fn start_with(mut output: impl FnMut(Content) -> io::Result<()> + Send + 'static) -> Self {
        let mailbox = Arc::new(Mailbox::default());
        let worker = Arc::clone(&mailbox);
        let (failure, failed) = watch::channel(None);
        std::thread::spawn(move || {
            loop {
                let frame = {
                    let state = worker.state.lock().expect("output mailbox");
                    let mut state = worker
                        .changed
                        .wait_while(state, |state| state.next.is_none() && !state.closing)
                        .expect("output mailbox");
                    let Some(mut frame) = state.next.take() else {
                        return;
                    };
                    if let Some(receipt) = &frame.completed {
                        if receipt.sender.is_closed() {
                            continue;
                        }
                        if Instant::now() >= receipt.deadline {
                            let receipt = frame.completed.take().expect("terminal receipt");
                            let _ = receipt.sender.send(Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "terminal output timed out",
                            )
                            .into()));
                            continue;
                        }
                    }
                    frame
                };
                // Terminal IO never holds the producer's mailbox lock.
                let result = output(frame.payload).map(|()| Instant::now());
                match result {
                    Ok(at) => {
                        if let Some(receipt) = frame.completed {
                            let _ = receipt.sender.send(Ok(at));
                        }
                    }
                    Err(error) => {
                        let error = WriteFailure(Arc::new(error));
                        let pending = {
                            let mut state = worker.state.lock().expect("output mailbox");
                            failure.send_replace(Some(error.clone()));
                            state.next.take().and_then(|frame| frame.completed)
                        };
                        for receipt in frame.completed.into_iter().chain(pending) {
                            let _ = receipt.sender.send(Err(error.clone().into()));
                        }
                        return;
                    }
                }
            }
        });
        Self { mailbox, failed }
    }

    fn put(&self, frame: Frame) -> Result<()> {
        let mut state = self.mailbox.state.lock().expect("output mailbox");
        if let Some(error) = self.failed.borrow().as_ref() {
            return Err(error.clone().into());
        }
        anyhow::ensure!(!state.closing, "terminal writer has stopped");
        state.next = Some(frame);
        self.mailbox.changed.notify_one();
        Ok(())
    }

    async fn finish(&self, payload: impl Into<Content>, budget: Duration) -> Result<()> {
        let deadline = Instant::now() + budget;
        let (sender, mut receiver) = oneshot::channel();
        self.put(Frame {
            payload: payload.into(),
            completed: Some(Receipt { deadline, sender }),
        })?;
        let result = match timeout_at(deadline, &mut receiver).await {
            Ok(result) => result.context("terminal writer did not confirm output")?,
            Err(_) => match receiver.try_recv() {
                Ok(result) => result,
                Err(_) => {
                    let mut state = self.mailbox.state.lock().expect("output mailbox");
                    if state
                        .next
                        .as_ref()
                        .and_then(|frame| frame.completed.as_ref())
                        .is_some_and(|receipt| receipt.deadline == deadline)
                    {
                        state.next = None;
                    }
                    anyhow::bail!("terminal output timed out");
                }
            },
        };
        let completed_at = result?;
        anyhow::ensure!(completed_at <= deadline, "terminal output timed out");
        Ok(())
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.mailbox.state.lock().expect("output mailbox").closing = true;
        self.mailbox.changed.notify_one();
    }
}

pub struct Output {
    stdout: Writer,
    stderr: Writer,
}

impl Output {
    pub fn terminal() -> Self {
        Self::new(io::stdout(), io::stderr())
    }

    pub fn new(stdout: impl Write + Send + 'static, stderr: impl Write + Send + 'static) -> Self {
        Self {
            stdout: Writer::start(stdout),
            stderr: Writer::start(stderr),
        }
    }

    pub(crate) fn with_book_sink(
        mut sink: impl FnMut(Arc<crate::book::view::BookView>) + Send + 'static,
        stderr: impl Write + Send + 'static,
    ) -> Self {
        Self {
            stdout: Writer::start_with(move |payload| {
                if let Content::Book(book, _) = payload {
                    sink(book);
                }
                Ok(())
            }),
            stderr: Writer::start(stderr),
        }
    }

    pub(crate) fn withdraw_ready<T>(&self, payload: Content, seal: impl FnOnce() -> T) -> T {
        let mut mailbox = self.stdout.mailbox.state.lock().expect("output mailbox");
        let result = seal();
        if !mailbox.closing && self.stdout.failed.borrow().is_none() {
            mailbox.next = Some(Frame {
                payload,
                completed: None,
            });
            self.stdout.mailbox.changed.notify_one();
        }
        result
    }
    pub fn view(&self, text: String) {
        self.publish(text.into());
    }

    pub(crate) fn view_book(&self, book: Arc<crate::book::view::BookView>) {
        self.publish(Content::Book(book, true));
    }

    fn publish(&self, payload: Content) {
        let _ = self.stdout.put(Frame {
            payload,
            completed: None,
        });
    }

    pub fn diagnostic(&self, message: String) {
        let _ = self.stderr.put(Frame {
            payload: format!("{message}\n").into(),
            completed: None,
        });
    }

    pub async fn finish(&self, text: String) -> Result<()> {
        self.stdout.finish(text, FINAL_OUTPUT_TIMEOUT).await
    }

    pub(crate) async fn finish_book(&self, book: crate::book::view::BookView) -> Result<()> {
        self.stdout
            .finish(Content::Book(Arc::new(book), false), FINAL_OUTPUT_TIMEOUT)
            .await
    }

    pub async fn report_failure(&self, text: String) {
        let _ = self.stderr.finish(text, Duration::from_millis(100)).await;
    }

    pub async fn failure(&self) -> anyhow::Error {
        let mut failure = self.stdout.failed.clone();
        loop {
            if let Some(error) = failure.borrow_and_update().as_ref() {
                return error.clone().into();
            }
            if failure.changed().await.is_err() {
                return anyhow::anyhow!("terminal writer stopped");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    struct BlockOnce {
        entered: Option<oneshot::Sender<()>>,
        release: mpsc::Receiver<()>,
        writes: mpsc::Sender<String>,
    }

    impl Write for BlockOnce {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
                let _ = self.release.recv();
            }
            let _ = self.writes.send(String::from_utf8(bytes.to_vec()).unwrap());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn blocked_writer_keeps_only_latest_view_and_final_write_is_acknowledged() {
        let (entered, blocked) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let (writes, read) = mpsc::channel();
        let writer = Writer::start(BlockOnce {
            entered: Some(entered),
            release: wait,
            writes,
        });
        writer
            .put(Frame {
                payload: "in progress".to_owned().into(),
                completed: None,
            })
            .unwrap();
        blocked.await.unwrap();
        for text in ["old", "newer", "last"] {
            writer
                .put(Frame {
                    payload: text.to_owned().into(),
                    completed: None,
                })
                .unwrap();
        }
        assert_eq!(
            writer
                .mailbox
                .state
                .lock()
                .unwrap()
                .next
                .as_ref()
                .unwrap()
                .payload
                .to_string(),
            "last"
        );
        let complete = writer.finish("ready".to_owned(), Duration::from_secs(1));
        tokio::pin!(complete);
        assert!(futures_util::poll!(&mut complete).is_pending());
        release.send(()).unwrap();
        complete.await.unwrap();
        assert_eq!(
            read.try_iter().collect::<Vec<_>>(),
            ["in progress", "ready"]
        );
    }

    #[tokio::test]
    async fn writer_cannot_take_pending_ready_inside_seal() {
        let (entered, blocked) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let (writes, read) = mpsc::channel();
        let output = Output::new(
            BlockOnce {
                entered: Some(entered),
                release: wait,
                writes,
            },
            io::sink(),
        );
        output.view("in progress".into());
        blocked.await.unwrap();
        output.view("Ready".into());
        output.withdraw_ready("NotReady".to_owned().into(), || {
            release.send(()).unwrap();
            assert_eq!(read.recv().unwrap(), "in progress");
            assert!(matches!(
                output.stdout.mailbox.state.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
        });
        assert_eq!(
            read.recv_timeout(Duration::from_secs(2)).unwrap(),
            "NotReady"
        );
    }

    #[tokio::test]
    async fn final_write_timeout_does_not_wait_for_blocked_writer_to_exit() {
        let (entered, _) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let (writes, _read) = mpsc::channel();
        let writer = Writer::start(BlockOnce {
            entered: Some(entered),
            release: wait,
            writes,
        });
        let result = writer
            .finish("ready".to_owned(), Duration::from_millis(25))
            .await;
        assert!(result.unwrap_err().to_string().contains("timed out"));
        drop(writer);
        release.send(()).unwrap();
    }

    struct FlushGate {
        release: mpsc::Receiver<()>,
        finished: Option<oneshot::Sender<()>>,
    }

    #[tokio::test]
    async fn active_queued_and_late_confirmations_share_the_original_fault() {
        struct BrokenPipe {
            gate: Option<(oneshot::Sender<()>, mpsc::Receiver<()>)>,
        }
        impl Write for BrokenPipe {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                let (entered, release) = self.gate.take().unwrap();
                entered.send(()).unwrap();
                release.recv().unwrap();
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (entered, blocked) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let output = Output::new(
            BrokenPipe {
                gate: Some((entered, wait)),
            },
            io::sink(),
        );
        let active = output.finish("active".into());
        let queued = output.finish("queued".into());
        tokio::pin!(active, queued);
        assert!(futures_util::poll!(&mut active).is_pending());
        blocked.await.unwrap();
        assert!(futures_util::poll!(&mut queued).is_pending());
        release.send(()).unwrap();
        let fault = output.failure().await;
        let original = fault.downcast_ref::<WriteFailure>().unwrap();
        for result in [
            active.await,
            queued.await,
            output.finish("late".into()).await,
        ] {
            let error = result.unwrap_err();
            assert!(original.same(error.downcast_ref::<WriteFailure>().unwrap()));
            let cause = error.root_cause().downcast_ref::<io::Error>().unwrap();
            assert_eq!(cause.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(format!("{error:#}"), "write terminal output: pipe closed");
        }
    }

    impl Write for FlushGate {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.release.recv().unwrap();
            self.finished.take().unwrap().send(()).unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn delayed_polling_uses_flush_time_instead_of_whichever_future_is_polled_first() {
        for late in [false, true] {
            let (release, wait) = mpsc::channel();
            let (finished, flushed) = oneshot::channel();
            let writer = Writer::start(FlushGate {
                release: wait,
                finished: Some(finished),
            });
            let budget = Duration::from_millis(250);
            let start = Instant::now();
            let completion = writer.finish("ready".to_owned(), budget);
            tokio::pin!(completion);
            assert!(futures_util::poll!(&mut completion).is_pending());
            while writer.mailbox.state.lock().unwrap().next.is_some() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if late {
                tokio::time::sleep_until(start + budget + Duration::from_millis(30)).await;
            }
            release.send(()).unwrap();
            flushed.await.unwrap();
            tokio::time::sleep_until(
                (start + budget).max(Instant::now()) + Duration::from_millis(20),
            )
            .await;
            assert_eq!(completion.await.is_err(), late);
        }
    }

    #[tokio::test]
    async fn expired_pending_ready_is_removed_while_output_is_blocked() {
        let (entered, blocked) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let (writes, read) = mpsc::channel();
        let writer = Writer::start(BlockOnce {
            entered: Some(entered),
            release: wait,
            writes,
        });
        writer
            .put(Frame {
                payload: "in progress".to_owned().into(),
                completed: None,
            })
            .unwrap();
        blocked.await.unwrap();
        assert!(
            writer
                .finish("ready".to_owned(), Duration::from_millis(25))
                .await
                .is_err()
        );
        assert!(writer.mailbox.state.lock().unwrap().next.is_none());
        release.send(()).unwrap();
        drop(writer);
        let writes = tokio::task::spawn_blocking(move || read.into_iter().collect::<Vec<_>>())
            .await
            .unwrap();
        assert_eq!(writes, ["in progress"]);
    }
}
