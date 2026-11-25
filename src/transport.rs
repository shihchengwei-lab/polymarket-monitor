use std::{
    future::poll_fn,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Poll, ready},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout, timeout_at},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

use crate::book::Subscription;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
use crate::wire::MESSAGE_BYTES;
const INGRESS_BYTES: usize = 16 * 1024 * 1024;
const INGRESS_FRAMES: usize = 64;

#[derive(Clone, Copy)]
pub struct HeartbeatTiming {
    pub interval: Duration,
    pub timeout: Duration,
}

pub const HEARTBEAT_TIMING: HeartbeatTiming = HeartbeatTiming {
    interval: Duration::from_secs(10),
    timeout: Duration::from_secs(20),
};

#[derive(Debug)]
pub(crate) struct SubscriptionStart {
    pub admitted_at: Instant,
    pub flushed_at: Instant,
}

#[derive(Debug)]
pub struct Frame {
    pub received_at: Instant,
    pub payload: Payload,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) enum Payload {
    Raw(String),
    Typed(crate::wire::Event),
}

impl Payload {
    fn charge(&self) -> Result<usize> {
        let bytes = match self {
            Self::Raw(text) => {
                ensure!(
                    text.len() <= MESSAGE_BYTES,
                    "market frame exceeds {MESSAGE_BYTES} bytes"
                );
                text.capacity()
            }
            Self::Typed(event) => {
                event.validate_size()?;
                let bytes = event.retained_bytes();
                ensure!(
                    bytes <= MESSAGE_BYTES,
                    "typed market event exceeds {MESSAGE_BYTES} bytes"
                );
                bytes
            }
        };
        ensure!(bytes <= INGRESS_BYTES, "WebSocket market queue byte limit");
        Ok(bytes)
    }
}

#[derive(Debug)]
pub struct ExpiredSubscription;

impl std::fmt::Display for ExpiredSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Gamma members expired before WebSocket subscription")
    }
}

impl std::error::Error for ExpiredSubscription {}

enum IngressState {
    Accepting { pong_due: Option<Instant> },
    Failed(Instant, anyhow::Error),
    Sealed,
}

impl IngressState {
    fn at(&mut self, now: Instant) -> Result<&mut Option<Instant>> {
        if let Self::Accepting {
            pong_due: Some(due),
        } = self
            && now >= *due
        {
            *self = Self::Failed(*due, anyhow::anyhow!("WebSocket PONG timed out"));
        }
        match self {
            Self::Accepting { pong_due } => Ok(pong_due),
            Self::Failed(..) | Self::Sealed => bail!("WebSocket ingress sealed"),
        }
    }
}

struct Ingress {
    state: Mutex<IngressState>,
    bytes: Arc<Semaphore>,
}

impl Ingress {
    fn lock(&self) -> std::sync::MutexGuard<'_, IngressState> {
        self.state.lock().expect("WebSocket cutoff")
    }

    fn finish(&self, result: Result<()>) {
        let mut state = self.lock();
        let now = Instant::now();
        if state.at(now).is_ok() {
            *state = match result {
                Err(error) => IngressState::Failed(now, error),
                Ok(()) => IngressState::Sealed,
            };
        }
    }

    fn deadline(&self) -> Result<Option<Instant>> {
        let mut state = self.lock();
        Ok(*state.at(Instant::now())?)
    }

    fn arm_ping(&self, timeout: Duration) -> Result<bool> {
        let mut state = self.lock();
        let now = Instant::now();
        let deadline = state.at(now)?;
        let first = deadline.is_none();
        deadline.get_or_insert(now + timeout);
        Ok(first)
    }

    fn pong(&self) -> Result<()> {
        let mut state = self.lock();
        *state.at(Instant::now())? = None;
        Ok(())
    }

    fn write_budget(&self) -> Result<Duration> {
        Ok(self.deadline()?.map_or(SEND_TIMEOUT, |due| {
            SEND_TIMEOUT.min(due.saturating_duration_since(Instant::now()))
        }))
    }

    async fn send_ping<S, E>(&self, socket: &mut S, pong_timeout: Duration) -> Result<()>
    where
        S: Sink<Message, Error = E> + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        timeout(self.write_budget()?, async {
            socket.feed(Message::Text("PING".into())).await?;
            poll_fn(|cx| {
                // While the sole writer is inside poll_flush, a different thread
                // must be able to seal without waiting for the poll to return.
                let first = self.arm_ping(pong_timeout)?;
                let result = Pin::new(&mut *socket).poll_flush(cx);
                if first && result.is_pending() {
                    *self.lock().at(Instant::now())? = None;
                }
                result.map_err(anyhow::Error::from)
            })
            .await
        })
        .await?
    }

    fn admit(&self, frames: &mpsc::Sender<Frame>, text: String) -> bool {
        self.enqueue(frames, Payload::Raw(text))
    }

    fn enqueue(&self, frames: &mpsc::Sender<Frame>, mut payload: Payload) -> bool {
        let mut state = self.lock();
        let received_at = Instant::now();
        if state.at(received_at).is_err() {
            return false;
        }
        if let Payload::Raw(text) = &mut payload
            && text.len() <= MESSAGE_BYTES
            && text.capacity() > text.len()
        {
            text.shrink_to_fit();
        }
        let charge = match payload.charge() {
            Ok(bytes) => bytes,
            Err(err) => {
                *state = IngressState::Failed(received_at, err);
                return false;
            }
        };
        let Ok(permit) = Arc::clone(&self.bytes).try_acquire_many_owned(charge as u32) else {
            *state = IngressState::Failed(
                received_at,
                anyhow::anyhow!("WebSocket market queue byte limit"),
            );
            return false;
        };
        match frames.try_send(Frame {
            received_at,
            payload,
            _bytes: permit,
        }) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                *state = IngressState::Failed(
                    received_at,
                    anyhow::anyhow!("WebSocket market queue full ({INGRESS_FRAMES} messages)"),
                );
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                *state = IngressState::Sealed;
                false
            }
        }
    }
}

/// Only seal can construct this receiver; no later admissions are possible.
pub(crate) struct SealedFrames {
    frames: mpsc::Receiver<Frame>,
    pub(crate) cutoff: Instant,
}

impl Iterator for SealedFrames {
    type Item = Frame;
    fn next(&mut self) -> Option<Frame> {
        self.frames.try_recv().ok()
    }
}

pub struct Transport {
    pub frames: mpsc::Receiver<Frame>,
    ingress: Arc<Ingress>,
    _stop: oneshot::Sender<()>,
}

impl Transport {
    /// Order a pending-view commit against ingress failure and sealing. The
    /// callback may update the mailbox only; rendering and I/O stay outside.
    pub(crate) fn while_accepting(&self, commit: impl FnOnce()) -> bool {
        let mut state = self.ingress.lock();
        if state.at(Instant::now()).is_err() {
            return false;
        }
        commit();
        true
    }

    /// No frame admitted before this instant remains in the queue.
    pub(crate) fn idle_frontier(&self) -> Option<Instant> {
        self.idle_sample().ok().flatten()
    }

    pub(crate) fn idle_sample(&self) -> Result<Option<Instant>> {
        let mut state = self.ingress.lock();
        let now = Instant::now();
        state.at(now)?;
        Ok(self.frames.is_empty().then_some(now))
    }

    pub(crate) fn local() -> (Self, mpsc::Sender<Frame>) {
        let (transport, sender, _) = Self::channel();
        (transport, sender)
    }

    pub(crate) fn enqueue(&self, sender: &mpsc::Sender<Frame>, payload: Payload) -> bool {
        self.ingress.enqueue(sender, payload)
    }

    fn channel() -> (Self, mpsc::Sender<Frame>, oneshot::Receiver<()>) {
        let (frames, receive) = mpsc::channel(INGRESS_FRAMES);
        let ingress = Arc::new(Ingress {
            state: Mutex::new(IngressState::Accepting { pong_due: None }),
            bytes: Arc::new(Semaphore::new(INGRESS_BYTES)),
        });
        let (stop, stopped) = oneshot::channel::<()>();
        let transport = Self {
            frames: receive,
            ingress,
            _stop: stop,
        };
        (transport, frames, stopped)
    }

    /// Return the receiver synchronously. Waiting for the subscription flush
    /// must never give a cancellable future ownership of accepted frames.
    pub fn start(
        endpoint: &str,
        subscription: Subscription,
        timing: HeartbeatTiming,
    ) -> Result<(Self, oneshot::Receiver<SubscriptionStart>)> {
        let endpoint = endpoint.to_owned();
        let (transport, frames, stopped) = Self::channel();
        let worker_ingress = Arc::clone(&transport.ingress);
        let (start, started) = oneshot::channel();
        std::thread::Builder::new().name("polymarket-websocket".to_owned()).spawn(move || {
            let mut start = Some(start);
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();
            let result = match &runtime {
                Ok(runtime) => runtime.block_on(async {
                    tokio::select! {
                        result = receive_market(endpoint, subscription, timing, &mut start, &frames, &worker_ingress) => result,
                        _ = stopped => Ok(()),
                    }
                }),
                Err(err) => Err(anyhow::anyhow!("start WebSocket runtime: {err}")),
            };
            worker_ingress.finish(result);
            drop(frames);
            drop(start);
            if let Ok(runtime) = runtime {
                runtime.shutdown_background();
            }
        }).context("start WebSocket worker")?;
        Ok((transport, started))
    }

    // Closing the receiver preserves all successfully enqueued frames. The
    // lock orders a local cutoff against admission and failure.
    pub fn seal(self) -> (SealedFrames, Result<()>) {
        self.seal_with(None)
    }

    pub(crate) fn seal_with(
        mut self,
        publication: Option<(&crate::output::Output, String)>,
    ) -> (SealedFrames, Result<()>) {
        let (at, termination) = {
            let mut state = self.ingress.lock();
            let mut seal = || {
                let now = Instant::now();
                let _ = state.at(now);
                self.frames.close();
                match std::mem::replace(&mut *state, IngressState::Sealed) {
                    IngressState::Failed(at, err) => (at, Err(err)),
                    _ => (now, Ok(())),
                }
            };
            match publication {
                Some((output, text)) => output.withdraw_ready(text, seal),
                None => seal(),
            }
        };
        (
            SealedFrames {
                frames: self.frames,
                cutoff: at,
            },
            termination,
        )
    }

    #[cfg(test)]
    pub(crate) fn buffered_for_test(
        messages: impl IntoIterator<Item = String>,
    ) -> (Self, mpsc::Sender<Frame>) {
        let (transport, sender, _) = Self::channel();
        for message in messages {
            if !transport.ingress.admit(&sender, message) {
                break;
            }
        }
        (transport, sender)
    }
}

async fn send_subscription<S, E>(
    socket: &mut S,
    subscription: Subscription,
) -> Result<SubscriptionStart>
where
    S: Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let (assets, valid_until) = subscription.into_parts();
    let text = serde_json::json!({
        "type": "market",
        "assets_ids": assets,
        "custom_feature_enabled": true,
    })
    .to_string();
    let send_until = Instant::now() + SEND_TIMEOUT;
    let mut message = Some(Message::Text(text));
    let accepted = timeout_at(
        send_until.min(valid_until),
        poll_fn(|cx| {
            if Instant::now() >= valid_until {
                return Poll::Ready(Err(ExpiredSubscription.into()));
            }
            let mut sink = Pin::new(&mut *socket);
            ready!(sink.as_mut().poll_ready(cx))?;
            let admitted_at = Instant::now();
            if admitted_at >= valid_until {
                return Poll::Ready(Err(ExpiredSubscription.into()));
            }
            sink.start_send(message.take().expect("subscription sent once"))?;
            Poll::Ready(Ok::<_, anyhow::Error>(admitted_at))
        }),
    )
    .await;
    let admitted_at = match accepted {
        Ok(result) => result?,
        Err(_) if valid_until <= send_until => return Err(ExpiredSubscription.into()),
        Err(err) => return Err(err).context("WebSocket subscription send timed out"),
    };
    timeout_at(send_until, socket.flush())
        .await
        .context("WebSocket subscription flush timed out")??;
    Ok(SubscriptionStart {
        admitted_at,
        flushed_at: Instant::now(),
    })
}

async fn receive_market(
    endpoint: String,
    subscription: Subscription,
    timing: HeartbeatTiming,
    started: &mut Option<oneshot::Sender<SubscriptionStart>>,
    frames: &mpsc::Sender<Frame>,
    ingress: &Ingress,
) -> Result<()> {
    let config = WebSocketConfig {
        max_message_size: Some(MESSAGE_BYTES),
        max_frame_size: Some(MESSAGE_BYTES),
        ..Default::default()
    };
    let (mut socket, _) = timeout(
        HANDSHAKE_TIMEOUT,
        connect_async_with_config(endpoint, Some(config), false),
    )
    .await
    .context("WebSocket handshake timed out")??;
    let start = send_subscription(&mut socket, subscription).await?;
    let now = start.flushed_at;
    if started
        .take()
        .expect("subscription acknowledged once")
        .send(start)
        .is_err()
    {
        return Ok(());
    }
    let mut interval = interval_at(now + timing.interval, timing.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let deadline = ingress.deadline()?;
        tokio::select! {
            _ = async {
                match deadline {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => bail!("WebSocket PONG timed out"),
            _ = interval.tick() => {
                ingress.send_ping(&mut socket, timing.timeout).await?;
            }
            incoming = socket.next() => {
                ingress.deadline()?;
                match incoming.context("WebSocket stream ended")?? {
                    Message::Text(text) if text.trim() == "PONG" => ingress.pong()?,
                    Message::Text(text) => if !ingress.admit(frames, text) {
                        return Ok(());
                    },
                    Message::Ping(_) => {
                        let result = timeout(ingress.write_budget()?, socket.flush()).await;
                        ingress.deadline()?;
                        result??;
                    }
                    Message::Close(frame) => bail!("WebSocket closed: {frame:?}"),
                    Message::Binary(_) => bail!("unexpected binary market message"),
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    fn subscription() -> Subscription {
        Subscription::for_test(Instant::now() + Duration::from_secs(10))
    }

    async fn failed_transport(transport: Transport) -> (SealedFrames, anyhow::Error) {
        while !transport.frames.is_closed() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let (frames, termination) = transport.seal();
        (frames, termination.unwrap_err())
    }

    #[tokio::test(start_paused = true)]
    async fn clock_frontier_never_passes_an_admitted_print() {
        let (mut wire, tx) = Transport::local();
        let start = Instant::now();
        tokio::time::advance(Duration::from_millis(99)).await;
        let before = wire.idle_frontier().unwrap();
        assert!(wire.enqueue(&tx, Payload::Raw("print".into())));
        tokio::time::advance(Duration::from_millis(2)).await;
        assert!(wire.idle_frontier().is_none());
        let frame = wire.frames.recv().await.unwrap();
        assert_eq!(frame.received_at, start + Duration::from_millis(99));
        assert!(before <= frame.received_at);
        assert_eq!(
            wire.idle_frontier(),
            Some(start + Duration::from_millis(101))
        );
    }

    #[tokio::test]
    async fn sealing_first_keeps_admitted_fifo_and_rejects_late_frames() {
        let (transport, sender, _) = Transport::channel();
        let ingress = Arc::clone(&transport.ingress);
        assert!(ingress.admit(&sender, "first".to_owned()));
        assert!(ingress.admit(&sender, "second".to_owned()));
        let (mut frames, termination) = transport.seal();
        termination.unwrap();
        assert!(!ingress.admit(&sender, "too late".to_owned()));
        assert!(matches!(frames.next().unwrap().payload, Payload::Raw(s) if s == "first"));
        assert!(matches!(frames.next().unwrap().payload, Payload::Raw(s) if s == "second"));
        assert!(frames.next().is_none());
    }

    #[tokio::test]
    async fn expired_members_are_not_sent_after_a_delayed_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let deadline = Instant::now() + Duration::from_millis(50);
        let subscription = Subscription::for_test(deadline);
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            sleep_until(deadline + Duration::from_millis(10)).await;
            let mut ws = accept_async(socket).await.unwrap();
            assert!(!matches!(ws.next().await, Some(Ok(Message::Text(_)))));
        };
        let client = async {
            let (transport, started) =
                Transport::start(&url, subscription, HEARTBEAT_TIMING).unwrap();
            assert!(started.await.is_err());
            assert!(transport.seal().1.unwrap_err().is::<ExpiredSubscription>());
        };
        timeout(Duration::from_secs(3), async {
            tokio::join!(server, client)
        })
        .await
        .unwrap();
    }

    struct DelayedSink {
        writable: Pin<Box<tokio::time::Sleep>>,
        sent: bool,
        flush: Option<Box<dyn FnMut() -> Poll<std::io::Result<()>>>>,
    }

    impl Sink<Message> for DelayedSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            use std::future::Future;
            self.get_mut().writable.as_mut().poll(cx).map(|()| Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, _: Message) -> std::io::Result<()> {
            self.sent = true;
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.flush.as_mut().map_or(Poll::Ready(Ok(())), |f| f())
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn pending_sink_cannot_accept_subscription_after_lease_expiration() {
        let deadline = Instant::now() + Duration::from_millis(30);
        let subscription = Subscription::for_test(deadline);
        let mut sink = DelayedSink {
            writable: Box::pin(sleep_until(deadline + Duration::from_millis(30))),
            sent: false,
            flush: None,
        };
        let err = send_subscription(&mut sink, subscription)
            .await
            .unwrap_err();
        assert!(err.is::<ExpiredSubscription>());
        assert!(!sink.sent);
        send_subscription(&mut sink, self::subscription())
            .await
            .unwrap();
        assert!(sink.sent);
    }

    #[tokio::test(start_paused = true)]
    async fn pending_ping_write_does_not_hold_the_market_clock() {
        use futures_util::poll;
        let (wire, _tx) = Transport::local();
        let start = Instant::now();
        let mut sink = DelayedSink {
            writable: Box::pin(sleep_until(start + Duration::from_millis(130))),
            sent: false,
            flush: None,
        };
        tokio::time::advance(Duration::from_millis(85)).await;
        let mut sending = Box::pin(wire.ingress.send_ping(&mut sink, Duration::from_secs(20)));
        assert!(poll!(&mut sending).is_pending());
        tokio::time::advance(Duration::from_millis(25)).await;
        assert!(poll!(&mut sending).is_pending());
        assert_eq!(
            wire.idle_frontier(),
            Some(start + Duration::from_millis(110))
        );
        tokio::time::advance(Duration::from_millis(20)).await;
        sending.await.unwrap();
        assert!(sink.sent);
    }

    #[tokio::test(start_paused = true)]
    async fn flush_poll_has_a_visible_cut_even_before_the_writer_returns() {
        use futures_util::{FutureExt, poll};
        let (wire, _) = Transport::local();
        let ingress = wire.ingress.clone();
        let start = Instant::now();
        let budget = Duration::from_millis(50);
        let mut wire = Some(wire);
        let mut sink = DelayedSink {
            writable: Box::pin(sleep_until(start)),
            sent: false,
            flush: Some(Box::new({
                let mut pending = true;
                move || {
                    if std::mem::take(&mut pending) {
                        return Poll::Pending;
                    }
                    let _ = tokio::time::advance(budget).now_or_never();
                    let (frames, result) = wire.take().unwrap().seal();
                    assert_eq!(frames.cutoff, start + budget);
                    assert!(result.unwrap_err().to_string().contains("PONG timed out"));
                    Poll::Ready(Ok(()))
                }
            })),
        };
        let mut send = Box::pin(ingress.send_ping(&mut sink, budget));
        assert!(poll!(&mut send).is_pending());
        assert_eq!(ingress.deadline().unwrap(), None);
        let _ = send.await;
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_cut_is_shared_by_idle_admission_and_seal() {
        for idle in [true, false] {
            let (wire, tx) = Transport::local();
            let timing = HEARTBEAT_TIMING;
            wire.ingress.arm_ping(timing.timeout).unwrap();
            let due = Instant::now() + timing.timeout;
            tokio::time::advance(timing.interval).await;
            wire.ingress.arm_ping(timing.timeout).unwrap();
            tokio::time::advance(timing.timeout).await;
            if idle {
                assert!(wire.idle_sample().is_err());
                assert!(!wire.enqueue(&tx, Payload::Raw("[]".into())));
                assert!(wire.ingress.pong().is_err());
            }
            let (frames, result) = wire.seal();
            assert_eq!(frames.cutoff, due);
            assert!(result.unwrap_err().to_string().contains("PONG timed out"));
        }
    }

    #[tokio::test]
    async fn heartbeat_continues_without_book_consumer_and_closes_on_silence() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let timing = HeartbeatTiming {
            interval: Duration::from_millis(35),
            timeout: Duration::from_millis(160),
        };
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            assert!(ws.next().await.unwrap().unwrap().is_text());
            ws.send(Message::Text("[]".to_owned())).await.unwrap();
            for _ in 0..3 {
                assert_eq!(
                    ws.next().await.unwrap().unwrap(),
                    Message::Text("PING".to_owned())
                );
                ws.send(Message::Text("PONG".to_owned())).await.unwrap();
            }
            while let Some(Ok(message)) = ws.next().await {
                assert_eq!(message, Message::Text("PING".to_owned()));
            }
        };
        let client = async {
            let (transport, started) = Transport::start(&url, subscription(), timing).unwrap();
            started.await.unwrap();
            // Deliberately never poll the book queue.
            assert!(
                failed_transport(transport)
                    .await
                    .1
                    .to_string()
                    .contains("PONG timed out")
            );
        };
        timeout(Duration::from_secs(3), async {
            tokio::join!(server, client);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn market_queue_overflow_ends_subscription_even_without_a_consumer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (opened, established) = oneshot::channel();
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            assert!(ws.next().await.unwrap().unwrap().is_text());
            established.await.unwrap();
            for _ in 0..=INGRESS_FRAMES {
                ws.send(Message::Text("[]".to_owned())).await.unwrap();
            }
        };
        let client = async {
            let (transport, started) =
                Transport::start(&url, subscription(), HEARTBEAT_TIMING).unwrap();
            started.await.unwrap();
            opened.send(()).unwrap();
            let (frames, error) = failed_transport(transport).await;
            assert!(error.to_string().contains("full"));
            assert_eq!(frames.count(), INGRESS_FRAMES);
        };
        timeout(Duration::from_secs(3), async {
            tokio::join!(server, client);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn market_queue_byte_limit_applies_before_message_count_is_full() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (opened, established) = oneshot::channel();
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            established.await.unwrap();
            for _ in 0..INGRESS_BYTES / MESSAGE_BYTES {
                ws.send(Message::Text(" ".repeat(MESSAGE_BYTES)))
                    .await
                    .unwrap();
            }
            ws.send(Message::Text(" ".to_owned())).await.unwrap();
        };
        let client = async {
            let (transport, started) =
                Transport::start(&url, subscription(), HEARTBEAT_TIMING).unwrap();
            started.await.unwrap();
            opened.send(()).unwrap();
            let (frames, error) = failed_transport(transport).await;
            assert!(error.to_string().contains("byte limit"));
            assert_eq!(frames.count(), INGRESS_BYTES / MESSAGE_BYTES);
        };
        timeout(Duration::from_secs(8), async {
            tokio::join!(server, client);
        })
        .await
        .unwrap();
    }
}
