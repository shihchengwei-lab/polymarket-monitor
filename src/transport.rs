use std::{
    future::poll_fn,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll, ready},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
#[cfg(test)]
use futures_util::StreamExt;
use futures_util::{Sink, SinkExt, Stream};
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

#[derive(Default)]
enum Heartbeat {
    #[default]
    Idle,
    Writing,
    Confirming(Instant),
    AwaitingPong(Instant),
}

#[derive(Default)]
struct Control {
    write_due: Option<Instant>,
    heartbeat: Heartbeat,
}

impl Control {
    fn deadline(&self) -> Option<(Instant, &'static str)> {
        let pong = match self.heartbeat {
            Heartbeat::Confirming(due) | Heartbeat::AwaitingPong(due) => {
                Some((due, "WebSocket PONG timed out"))
            }
            Heartbeat::Idle | Heartbeat::Writing => None,
        };
        let write = self
            .write_due
            .map(|due| (due, "WebSocket heartbeat write timed out"));
        pong.into_iter().chain(write).min_by_key(|&(due, _)| due)
    }
}

enum IngressState {
    Accepting(Control),
    Failed(Instant, anyhow::Error),
    Sealed,
}

impl IngressState {
    fn at(&mut self, now: Instant) -> Result<&mut Control> {
        if let Self::Accepting(control) = self
            && let Some((due, reason)) = control.deadline()
            && now >= due
        {
            *self = Self::Failed(due, anyhow::anyhow!(reason));
        }
        match self {
            Self::Accepting(control) => Ok(control),
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
        Ok(state.at(Instant::now())?.deadline().map(|(due, _)| due))
    }

    fn begin_ping(&self) -> Result<bool> {
        let mut state = self.lock();
        let now = Instant::now();
        let control = state.at(now)?;
        if !matches!(control.heartbeat, Heartbeat::Idle) || control.write_due.is_some() {
            return Ok(false);
        }
        control.heartbeat = Heartbeat::Writing;
        control.write_due = Some(now + SEND_TIMEOUT);
        Ok(true)
    }

    fn begin_protocol_write(&self) -> Result<()> {
        let mut state = self.lock();
        let now = Instant::now();
        state.at(now)?.write_due.get_or_insert(now + SEND_TIMEOUT);
        Ok(())
    }

    fn confirming_ping(&self, timeout: Duration) -> Result<()> {
        let mut state = self.lock();
        let now = Instant::now();
        let control = state.at(now)?;
        debug_assert!(matches!(control.heartbeat, Heartbeat::Writing));
        control.heartbeat = Heartbeat::Confirming(now + timeout);
        Ok(())
    }

    fn finish_flush(&self, flushed: bool) -> Result<()> {
        let mut state = self.lock();
        let control = state.at(Instant::now())?;
        if let Heartbeat::Confirming(due) = control.heartbeat {
            control.heartbeat = if flushed {
                Heartbeat::AwaitingPong(due)
            } else {
                Heartbeat::Writing
            };
        }
        if flushed {
            control.write_due = None;
        }
        Ok(())
    }

    fn pong(&self) -> Result<()> {
        let mut state = self.lock();
        let control = state.at(Instant::now())?;
        if matches!(control.heartbeat, Heartbeat::AwaitingPong(_)) {
            control.heartbeat = Heartbeat::Idle;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn send_ping<S, E>(&self, socket: &mut S, pong_timeout: Duration) -> Result<()>
    where
        S: Sink<Message, Error = E> + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        assert!(self.begin_ping()?);
        let mut write = PendingWrite::QueuePing;
        poll_fn(|cx| write.poll(cx, socket, self, pong_timeout)).await
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
            state: Mutex::new(IngressState::Accepting(Control::default())),
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
        publication: Option<(&crate::output::Output, crate::book::view::BookView)>,
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
                Some((output, book)) => {
                    output.withdraw_ready(crate::output::Content::Book(Arc::new(book), false), seal)
                }
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
    pub(crate) fn begin_protocol_write_for_test(&self) {
        self.ingress.begin_protocol_write().unwrap();
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

enum PendingWrite {
    Protocol,
    QueuePing,
    FlushPing,
}

impl PendingWrite {
    fn poll<S, E>(
        &mut self,
        cx: &mut TaskContext<'_>,
        socket: &mut S,
        ingress: &Ingress,
        pong_timeout: Duration,
    ) -> Poll<Result<()>>
    where
        S: Sink<Message, Error = E> + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        ingress.deadline()?;
        let mut socket = Pin::new(socket);
        if matches!(self, Self::QueuePing) {
            let ready_to_send = socket.as_mut().poll_ready(cx);
            ingress.deadline()?;
            ready!(ready_to_send).map_err(anyhow::Error::from)?;
            let queued = socket.as_mut().start_send(Message::Text("PING".into()));
            ingress.deadline()?;
            queued?;
            *self = Self::FlushPing;
        }
        // The deadline must be visible to a concurrent seal even if this poll stalls.
        if matches!(self, Self::FlushPing) {
            ingress.confirming_ping(pong_timeout)?;
        }
        let result = socket.poll_flush(cx);
        ingress.finish_flush(matches!(result, Poll::Ready(Ok(()))))?;
        result.map_err(anyhow::Error::from)
    }
}

enum SocketEvent {
    Flushed,
    Incoming(Message),
}

async fn drive_market<S, E>(
    socket: &mut S,
    now: Instant,
    timing: HeartbeatTiming,
    frames: &mpsc::Sender<Frame>,
    ingress: &Ingress,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut interval = interval_at(now + timing.interval, timing.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut pending: Option<PendingWrite> = None;
    loop {
        let cutoff = ingress.deadline()?;
        tokio::select! {
            biased;
            _ = wait_deadline(cutoff) => { ingress.deadline()?; }
            _ = interval.tick() => {
                if ingress.begin_ping()? {
                    debug_assert!(pending.is_none());
                    pending = Some(PendingWrite::QueuePing);
                }
            }
            event = poll_fn(|cx| {
                if let Some(write) = &mut pending
                    && let Poll::Ready(result) = write.poll(cx, socket, ingress, timing.timeout)
                {
                    return Poll::Ready(result.map(|()| SocketEvent::Flushed));
                }
                // The pinned WebSocket library may flush an automatic Pong on a read.
                // Its WouldBlock path still polls readable data. Only an explicit
                // successful flush confirms a queued text PING.
                let read = Pin::new(&mut *socket).poll_next(cx);
                if matches!(read, Poll::Ready(Some(Ok(Message::Ping(_))))) {
                    ingress.begin_protocol_write()?;
                    if pending.is_none() {
                        pending = Some(PendingWrite::Protocol);
                    }
                } else {
                    ingress.deadline()?;
                }
                read.map(|item| Ok(SocketEvent::Incoming(item.context("WebSocket stream ended")??)))
            }) => {
                match event? {
                    SocketEvent::Flushed => pending = None,
                    SocketEvent::Incoming(Message::Text(text)) if text.trim() == "PONG" => ingress.pong()?,
                    SocketEvent::Incoming(Message::Text(text)) => {
                        if !ingress.admit(frames, text) { return Ok(()); }
                    }
                    SocketEvent::Incoming(Message::Close(frame)) => bail!("WebSocket closed: {frame:?}"),
                    SocketEvent::Incoming(Message::Binary(_)) => bail!("unexpected binary market message"),
                    _ => {}
                }
            }
        }
    }
}

async fn wait_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
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
        // The subscription comes from a smaller, bounded Gamma response.
        // Pending automatic control frames must not grow without bound.
        max_write_buffer_size: MESSAGE_BYTES,
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
    drive_market(&mut socket, now, timing, frames, ingress).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    struct WriteGate {
        io: tokio::io::DuplexStream,
        blocked_from: Instant,
        open: Pin<Box<tokio::time::Sleep>>,
    }

    impl tokio::io::AsyncRead for WriteGate {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.io).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for WriteGate {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if Instant::now() >= self.blocked_from {
                ready!(std::future::Future::poll(self.open.as_mut(), cx));
            }
            Pin::new(&mut self.io).poll_write(cx, bytes)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            if Instant::now() >= self.blocked_from {
                ready!(std::future::Future::poll(self.open.as_mut(), cx));
            }
            Pin::new(&mut self.io).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.io).poll_shutdown(cx)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn market_reads_before_cutoff_with_application_ping_or_protocol_pong_pending() {
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        for protocol in [false, true] {
            let started = Instant::now();
            let at = |ms| started + Duration::from_millis(ms);
            let (client, server) = tokio::io::duplex(4096);
            let mut client = WebSocketStream::from_raw_socket(
                WriteGate {
                    io: client,
                    blocked_from: started,
                    open: Box::pin(sleep_until(at(130))),
                },
                Role::Client,
                None,
            )
            .await;
            let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
            let (mut transport, tx) = Transport::local();
            let ingress = transport.ingress.clone();
            let worker = tokio::spawn(async move {
                let timing = HeartbeatTiming {
                    interval: Duration::from_millis(if protocol { 1000 } else { 85 }),
                    timeout: Duration::from_secs(20),
                };
                let result = drive_market(&mut client, started, timing, &tx, &ingress).await;
                ingress.finish(result);
            });
            tokio::time::advance(Duration::from_millis(85)).await;
            if protocol {
                server.send(Message::Ping(vec![1])).await.unwrap();
            }
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(14)).await;
            server
                .send(Message::Text("trade-before-cut".into()))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            let first = transport
                .frames
                .try_recv()
                .expect("readable trade must be admitted before the write resumes");
            assert_eq!(first.received_at, at(99));
            assert!(matches!(first.payload, Payload::Raw(ref text) if text == "trade-before-cut"));

            tokio::time::advance(Duration::from_millis(1)).await;
            server
                .send(Message::Text("trade-at-cut".into()))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            let second = transport.frames.try_recv().unwrap();
            assert_eq!(second.received_at, at(100));
            assert!(matches!(second.payload, Payload::Raw(ref text) if text == "trade-at-cut"));

            tokio::time::advance(Duration::from_millis(30)).await;
            let reply = server.next().await.unwrap().unwrap();
            assert_eq!(
                reply,
                if protocol {
                    Message::Pong(vec![1])
                } else {
                    Message::Text("PING".into())
                }
            );
            worker.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn silence_after_the_next_ping_times_out_despite_an_earlier_reply() {
        late_heartbeat_reply(21).await;
    }

    #[tokio::test(start_paused = true)]
    async fn an_old_reply_cannot_acknowledge_an_overlapping_ping() {
        late_heartbeat_reply(23).await;
    }

    async fn late_heartbeat_reply(old_reply: u64) {
        use futures_util::FutureExt;
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        let started = Instant::now();
        let at = |secs| started + Duration::from_secs(secs);
        let (client, server) = tokio::io::duplex(4096);
        let mut client = WebSocketStream::from_raw_socket(
            WriteGate {
                io: client,
                blocked_from: at(20),
                open: Box::pin(sleep_until(at(22))),
            },
            Role::Client,
            None,
        )
        .await;
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let (transport, tx) = Transport::local();
        let ingress = transport.ingress.clone();
        tokio::spawn(async move {
            let result = drive_market(&mut client, started, HEARTBEAT_TIMING, &tx, &ingress).await;
            ingress.finish(result);
        });
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            server.next().now_or_never().unwrap().unwrap().unwrap(),
            Message::Text("PING".into())
        );

        let mut second_ping = None;
        for second in 20..=30 {
            tokio::time::advance(at(second).duration_since(Instant::now())).await;
            tokio::task::yield_now().await;
            if let Some(ping) = server.next().now_or_never() {
                assert!(
                    second > old_reply,
                    "extra PING at {second}s before the old reply"
                );
                assert_eq!(ping.unwrap().unwrap(), Message::Text("PING".into()));
                second_ping.get_or_insert(second);
            }
            if second == old_reply {
                server.send(Message::Text("PONG".into())).await.unwrap();
                tokio::task::yield_now().await;
            }
        }

        let due = at(second_ping.expect("the next heartbeat slot must send") + 20);
        tokio::time::advance((due - Duration::from_millis(1)).duration_since(Instant::now())).await;
        assert!(transport.idle_sample().unwrap().is_some());
        tokio::time::advance(Duration::from_millis(1)).await;
        let (frames, result) = transport.seal();
        assert_eq!(frames.cutoff, due);
        assert!(result.unwrap_err().to_string().contains("PONG timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn early_text_and_control_pong_do_not_acknowledge_an_unflushed_ping() {
        use futures_util::FutureExt;
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        let started = Instant::now();
        let at = |ms| started + Duration::from_millis(ms);
        let (client, server) = tokio::io::duplex(4096);
        let mut client = WebSocketStream::from_raw_socket(
            WriteGate {
                io: client,
                blocked_from: at(10_000),
                open: Box::pin(sleep_until(at(12_000))),
            },
            Role::Client,
            None,
        )
        .await;
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let (transport, tx) = Transport::local();
        let ingress = transport.ingress.clone();
        tokio::spawn(async move {
            let result = drive_market(&mut client, started, HEARTBEAT_TIMING, &tx, &ingress).await;
            ingress.finish(result);
        });
        for (ms, message) in [
            (9_000, Some(Message::Text("PONG".into()))),
            (10_000, None),
            (11_000, Some(Message::Text("PONG".into()))),
            (12_000, None),
            (12_500, Some(Message::Pong(vec![1]))),
        ] {
            tokio::time::advance(at(ms).duration_since(Instant::now())).await;
            if let Some(message) = message {
                server.send(message).await.unwrap();
            }
            tokio::task::yield_now().await;
            let ping = server.next().now_or_never();
            if ms == 12_000 {
                assert_eq!(
                    ping.unwrap().unwrap().unwrap(),
                    Message::Text("PING".into())
                );
            } else {
                assert!(ping.is_none());
            }
        }
        tokio::time::advance(at(31_999).duration_since(Instant::now())).await;
        assert!(transport.idle_sample().unwrap().is_some());
        tokio::time::advance(Duration::from_millis(1)).await;
        let (frames, result) = transport.seal();
        assert_eq!(frames.cutoff, at(32_000));
        assert!(result.unwrap_err().to_string().contains("PONG timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_worker_publishes_the_original_protocol_write_cut() {
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        let start = Instant::now();
        let at = |ms| start + Duration::from_millis(ms);
        let (client, server) = tokio::io::duplex(4096);
        let mut client = WebSocketStream::from_raw_socket(
            WriteGate {
                io: client,
                blocked_from: at(1_000),
                open: Box::pin(sleep_until(at(7_000))),
            },
            Role::Client,
            None,
        )
        .await;
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let (transport, tx) = Transport::local();
        let ingress = transport.ingress.clone();
        let worker = tokio::spawn(async move {
            let result = drive_market(&mut client, start, HEARTBEAT_TIMING, &tx, &ingress).await;
            ingress.finish(result);
        });
        tokio::time::advance(Duration::from_secs(1)).await;
        server.send(Message::Ping(vec![1])).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(5_100)).await;
        worker.await.unwrap();
        tokio::time::advance(Duration::from_millis(100)).await;
        let (frames, result) = transport.seal();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("heartbeat write timed out")
        );
        assert_eq!(frames.cutoff, at(6_000));
    }

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
        assert_eq!(ingress.deadline().unwrap(), Some(start + SEND_TIMEOUT));
        let _ = send.await;
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_cut_is_shared_by_idle_admission_and_seal() {
        for idle in [true, false] {
            let (wire, tx) = Transport::local();
            let timing = HEARTBEAT_TIMING;
            assert!(wire.ingress.begin_ping().unwrap());
            wire.ingress.confirming_ping(timing.timeout).unwrap();
            wire.ingress.finish_flush(true).unwrap();
            let due = Instant::now() + timing.timeout;
            tokio::time::advance(timing.interval).await;
            assert!(!wire.ingress.begin_ping().unwrap());
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

    #[tokio::test(start_paused = true)]
    async fn heartbeat_write_cut_is_shared_before_worker_finishes() {
        for (worker_first, sample, before_cut) in [
            (true, false, false),
            (false, true, false),
            (false, false, false),
            (false, false, true),
        ] {
            let (wire, tx) = Transport::local();
            let ingress = wire.ingress.clone();
            ingress.begin_protocol_write().unwrap();
            let due = Instant::now() + SEND_TIMEOUT;
            tokio::time::advance(SEND_TIMEOUT - Duration::from_millis(1)).await;
            assert!(wire.idle_sample().unwrap().is_some());
            if !sample {
                assert!(wire.enqueue(&tx, Payload::Raw("first".into())));
                assert!(wire.enqueue(&tx, Payload::Raw("second".into())));
            }
            if !before_cut {
                tokio::time::advance(Duration::from_millis(1)).await;
                if worker_first {
                    tokio::time::advance(Duration::from_millis(100)).await;
                    ingress.finish(Err(anyhow::anyhow!("worker woke late")));
                    tokio::time::advance(Duration::from_millis(100)).await;
                }
                if sample {
                    assert!(wire.idle_sample().is_err());
                    assert!(!wire.enqueue(&tx, Payload::Raw("late".into())));
                }
            }
            let (mut frames, result) = wire.seal();
            if before_cut {
                assert_eq!(frames.cutoff, due - Duration::from_millis(1));
                result.unwrap();
                tokio::time::advance(Duration::from_millis(101)).await;
                ingress.finish(Err(anyhow::anyhow!("worker woke after stop")));
            } else {
                assert_eq!(frames.cutoff, due);
                assert!(result.unwrap_err().to_string().contains("write timed out"));
            }
            if !sample {
                for expected in ["first", "second"] {
                    let frame = frames.frames.try_recv().unwrap();
                    assert!(matches!(frame.payload, Payload::Raw(text) if text == expected));
                }
            }
            assert!(frames.frames.try_recv().is_err());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn earlier_deadline_wins_even_when_both_are_overdue() {
        for (write_starts, observed, cutoff, reason) in [
            (24, 31, 29, "write timed out"),
            (28, 34, 30, "PONG timed out"),
        ] {
            let (wire, _) = Transport::local();
            let start = Instant::now();
            let at = |secs| start + Duration::from_secs(secs);
            let ingress = &wire.ingress;
            tokio::time::advance(Duration::from_secs(10)).await;
            assert!(ingress.begin_ping().unwrap());
            ingress.confirming_ping(HEARTBEAT_TIMING.timeout).unwrap();
            ingress.finish_flush(true).unwrap();
            tokio::time::advance(at(write_starts).duration_since(Instant::now())).await;
            ingress.begin_protocol_write().unwrap();
            tokio::time::advance(Duration::from_secs(1)).await;
            ingress.begin_protocol_write().unwrap();
            tokio::time::advance(at(observed).duration_since(Instant::now())).await;
            let (frames, result) = wire.seal();
            assert_eq!(frames.cutoff, at(cutoff));
            assert!(result.unwrap_err().to_string().contains(reason));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn late_successful_flush_cannot_clear_a_write_cut() {
        use futures_util::FutureExt;
        let (wire, _) = Transport::local();
        let ingress = &wire.ingress;
        ingress.begin_protocol_write().unwrap();
        let due = Instant::now() + SEND_TIMEOUT;
        tokio::time::advance(SEND_TIMEOUT - Duration::from_millis(1)).await;
        let mut sink = DelayedSink {
            writable: Box::pin(sleep_until(Instant::now())),
            sent: false,
            flush: Some(Box::new(|| {
                let _ = tokio::time::advance(Duration::from_millis(2)).now_or_never();
                Poll::Ready(Ok(()))
            })),
        };
        assert!(
            std::future::poll_fn(|cx| {
                PendingWrite::Protocol.poll(cx, &mut sink, ingress, HEARTBEAT_TIMING.timeout)
            })
            .await
            .is_err()
        );
        assert!(wire.idle_sample().is_err());
        let (frames, result) = wire.seal();
        assert_eq!(frames.cutoff, due);
        assert!(result.unwrap_err().to_string().contains("write timed out"));
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
