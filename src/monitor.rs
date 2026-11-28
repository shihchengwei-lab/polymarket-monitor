use std::{future, ops::ControlFlow, pin::Pin, sync::Arc, time::Duration};

use anyhow::{Result, bail, ensure};
use futures_util::{FutureExt, Stream, StreamExt, future::BoxFuture, stream};
use reqwest::{Client, StatusCode};
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep_until};

use crate::{
    book::{
        Audit, NegRiskEventGroup, SNAPSHOT_TIMEOUT, SubscriptionId,
        input::{FrameFailure, Retirement, apply_frame},
        view::BookView,
    },
    gamma_discovery::{self, Discovery},
    output::{Output, single_line},
    transport::{ExpiredSubscription, Frame, HEARTBEAT_TIMING, HeartbeatTiming, Transport},
    wire::{self, RestBook},
};

const GAMMA_INTERVAL: Duration = Duration::from_secs(30);
const REST_AUDIT_INTERVAL: Duration = Duration::from_secs(30);
const RECOVERY_INITIAL: Duration = Duration::from_secs(15);
const RECOVERY_MAX: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
struct WatchTiming {
    gamma: Duration,
    snapshots: Duration,
    recovery: Duration,
    heartbeat: HeartbeatTiming,
}

const LIVE_TIMING: WatchTiming = WatchTiming {
    gamma: GAMMA_INTERVAL,
    snapshots: SNAPSHOT_TIMEOUT,
    recovery: RECOVERY_INITIAL,
    heartbeat: HEARTBEAT_TIMING,
};

pub struct Endpoints<'a> {
    pub gamma: &'a str,
    pub rest: &'a str,
    pub socket: &'a str,
}

impl Default for Endpoints<'static> {
    fn default() -> Self {
        Self {
            gamma: gamma_discovery::GAMMA,
            rest: "https://clob.polymarket.com",
            socket: "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        }
    }
}

async fn fetch_book(client: &Client, base: &str, asset: &str) -> Result<Option<RestBook>> {
    let response = client
        .get(format!("{base}/book"))
        .query(&[("token_id", asset)])
        .send()
        .await?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let body = wire::success_body(response, 1024 * 1024, "CLOB book body").await?;
    Ok(Some(serde_json::from_slice(&body)?))
}

fn checks<'a>(
    client: &'a Client,
    rest: &'a str,
    audits: Vec<Audit>,
) -> impl Stream<Item = (Audit, Result<Option<RestBook>>)> + 'a {
    stream::iter(audits)
        .map(move |audit| async move {
            let response = fetch_book(client, rest, audit.asset()).await;
            (audit, response)
        })
        .buffer_unordered(8)
}

struct GammaPoll<'a> {
    client: &'a Client,
    base: &'a str,
    interval: Duration,
    next_at: Instant,
}

impl GammaPoll<'_> {
    async fn refresh(&mut self, group: &mut NegRiskEventGroup) -> Result<Discovery> {
        sleep_until(self.next_at).await;
        let result = group
            .receive_discovery(self.client, self.base, self.interval)
            .await;
        self.next_at = Instant::now() + self.interval;
        result
    }
}

struct Recovery {
    initial: Duration,
    delay: Duration,
    state: RecoveryState,
}

enum RecoveryState {
    Idle,
    Backoff(Instant),
    WaitingForGamma,
}

impl Recovery {
    fn observe(&mut self, group: &NegRiskEventGroup) {
        if group.has_quarantined_books() {
            match self.state {
                RecoveryState::Idle => {
                    self.state = RecoveryState::Backoff(Instant::now() + self.delay)
                }
                RecoveryState::Backoff(at) if Instant::now() >= at => {
                    self.state = RecoveryState::WaitingForGamma
                }
                _ => {}
            }
        } else {
            self.state = RecoveryState::Idle;
            if group.has_open_assets() && !group.awaiting_snapshot() {
                self.delay = self.initial;
            }
        }
    }

    fn reconnect(&mut self) {
        self.state = RecoveryState::Idle;
        self.delay = (self.delay * 2).min(RECOVERY_MAX);
    }

    fn deadline(&self) -> Option<Instant> {
        match self.state {
            RecoveryState::Backoff(at) => Some(at),
            _ => None,
        }
    }

    fn waiting(&self) -> bool {
        matches!(self.state, RecoveryState::WaitingForGamma)
    }

    fn due(&self, now: Instant) -> bool {
        self.waiting() || self.deadline().is_some_and(|deadline| now >= deadline)
    }
}

enum SessionEnd {
    Done,
    RecoverIsolated,
    Finished(Result<()>),
}

#[cfg(test)]
pub(crate) fn during_frame_recovery() {
    tests::before_current_frame_recovery();
}

enum Cutoff {
    Stop(Result<()>),
    Once(SubscriptionId),
    RecoverIsolated,
    Gamma(bool),
    Closed(Option<Frame>),
    Failed(anyhow::Error),
    ReconcileDeadline,
    FrameFailed(Box<FrameFailure>),
    NoOpenMarkets,
}

struct WatchControl<'a> {
    once: bool,
    output: &'a Output,
    stop: BoxFuture<'a, Result<()>>,
}

impl WatchControl<'_> {
    fn observed_stop(&mut self) -> Option<Result<()>> {
        self.stop.as_mut().now_or_never()
    }

    async fn finish_retirement(
        &mut self,
        mut retirement: Retirement<'_>,
        mut stopped: Option<Result<()>>,
    ) -> ControlFlow<Result<()>, Result<Option<BookView>>> {
        // Drain reliable input even when stop has won. An accepted Gamma
        // closure owns terminal output through its final write confirmation.
        loop {
            tokio::task::coop::consume_budget().await;
            if !retirement.advance() {
                break;
            }
        }
        if stopped.is_none() && !retirement.closing_event() {
            stopped = self.observed_stop();
        }
        if let Some(stop) = stopped {
            retirement.stop();
            ControlFlow::Break(crate::book::after_cleanup(
                retirement.complete().map(drop),
                stop,
            ))
        } else {
            ControlFlow::Continue(retirement.complete())
        }
    }
    /// Commit point for a completed Gamma request: a stop already observable
    /// here wins. The caller applies the accepted result without yielding.
    fn accept_gamma(
        &mut self,
        update: Result<Discovery>,
    ) -> ControlFlow<Result<()>, (Instant, Result<Discovery>)> {
        #[cfg(test)]
        if matches!(&update, Ok(Discovery::Closed)) {
            tests::before_closed_commit();
        }
        match self.observed_stop() {
            Some(stop) => ControlFlow::Break(stop),
            None => ControlFlow::Continue((Instant::now(), update)),
        }
    }
}

fn publish_view(
    transport: &Transport,
    group: &NegRiskEventGroup,
    last: &mut Option<Arc<BookView>>,
    output: &Output,
) -> bool {
    let view = group.view();
    let changed = last.as_deref() != Some(&view);
    transport.while_accepting(|| {
        if changed {
            let view = Arc::new(view);
            output.view_book(view.clone());
            *last = Some(view);
        }
    })
}

fn publish_current(group: &NegRiskEventGroup, output: &Output) {
    output.view_book(Arc::new(group.view()));
}

async fn finish_subscription(
    transport: Transport,
    cutoff: Cutoff,
    group: &mut NegRiskEventGroup,
    control: &mut WatchControl<'_>,
) -> Result<SessionEnd> {
    let output = control.output;
    #[cfg(test)]
    if matches!(cutoff, Cutoff::Stop(_)) {
        tests::before_stop_seal();
    }
    use crate::book::input::End;
    let (frames, termination) = transport.seal_with(Some((output, group.retiring_view())));
    let mut first = None;
    let mut failure = None;
    let mut transport_result = termination;
    let mut stopped = None;
    let (intent, next) = match cutoff {
        Cutoff::Once(source) => {
            debug_assert_eq!(Some(source), group.source());
            (End::Once, SessionEnd::Done)
        }
        Cutoff::Stop(result) => {
            stopped = Some(result);
            (End::Stop, SessionEnd::Done)
        }
        Cutoff::Closed(popped) => {
            first = popped;
            transport_result =
                transport_result.and_then(|()| anyhow::bail!("WebSocket worker stopped"));
            (End::Reconnect, SessionEnd::Done)
        }
        Cutoff::ReconcileDeadline => (End::Reconnect, SessionEnd::Done),
        Cutoff::Failed(err) => {
            transport_result = crate::book::after_cleanup(transport_result, Err(err));
            (End::Reconnect, SessionEnd::Done)
        }
        Cutoff::FrameFailed(failed) => {
            failure = Some(failed);
            (End::Reconnect, SessionEnd::Done)
        }
        Cutoff::Gamma(true) | Cutoff::RecoverIsolated => {
            (End::Replace, SessionEnd::RecoverIsolated)
        }
        Cutoff::Gamma(false) | Cutoff::NoOpenMarkets => (End::Replace, SessionEnd::Done),
    };
    let mut retirement = Retirement::new(group, first, (frames, transport_result), intent);
    if let Some(failed) = failure {
        retirement.failed(*failed);
    }
    #[cfg(test)]
    tests::before_drain();
    let result = match control.finish_retirement(retirement, stopped).await {
        ControlFlow::Break(Err(error)) if error.is::<crate::book::SettlementCapacity>() => {
            return Err(error);
        }
        ControlFlow::Break(result) => return Ok(SessionEnd::Finished(result)),
        ControlFlow::Continue(result) => result,
    };
    if group.event_closed() {
        let result = result.and_then(|_| {
            anyhow::ensure!(!control.once, "the event is closed");
            Ok(())
        });
        return Ok(SessionEnd::Finished(result));
    }
    if let Ok(Some(view)) = result {
        return Ok(SessionEnd::Finished(tokio::select! {
            biased; stopped=&mut control.stop=>stopped, result=output.finish_book(view)=>result
        }));
    }
    publish_current(group, output);
    result?;
    Ok(if group.has_open_assets() {
        next
    } else {
        SessionEnd::Done
    })
}

fn observe_gamma(
    group: &mut NegRiskEventGroup,
    update: Result<Discovery>,
    output: &Output,
) -> bool {
    report_gamma_error(&update, output);
    group.observe_discovery(update)
}

fn report_gamma_error(result: &Result<Discovery>, output: &Output) {
    if let Err(err) = result {
        output.diagnostic(format!(
            "Gamma refresh: {}",
            single_line(&format!("{err:#}"))
        ));
    }
}

fn market_clock() -> tokio::time::Interval {
    let mut clock = tokio::time::interval(Duration::from_millis(10));
    clock.set_missed_tick_behavior(MissedTickBehavior::Skip);
    clock
}

fn market_tick(group: &mut NegRiskEventGroup, transport: &Transport) -> Option<Cutoff> {
    let commands = group.command_count();
    match transport.idle_sample() {
        Ok(at) if group.deliver_idle_clock(commands, at) => None,
        Ok(_) => Some(Cutoff::ReconcileDeadline),
        Err(_) => Some(Cutoff::Closed(None)),
    }
}

async fn session(
    endpoints: &Endpoints<'_>,
    gamma: &mut GammaPoll<'_>,
    group: &mut NegRiskEventGroup,
    recovery: &mut Recovery,
    timing: &WatchTiming,
    control: &mut WatchControl<'_>,
) -> Result<SessionEnd> {
    let once = control.once;
    let output = control.output;
    let subscription = group.subscription().ok_or(ExpiredSubscription)?;
    let (mut transport, mut started) =
        Transport::start(endpoints.socket, subscription, timing.heartbeat)?;
    let mut paper_clock = market_clock();
    let activation = loop {
        tokio::select! {
        biased;
        stopped = &mut control.stop => break Err(Cutoff::Stop(stopped)),
        result = &mut started => break match result {
            Ok(start) => group.begin_at(start.admitted_at, start.flushed_at + timing.snapshots)
                .map(|source| (source, start.flushed_at))
                .ok_or_else(|| Cutoff::Failed(ExpiredSubscription.into())),
            Err(_) => Err(Cutoff::Closed(None)),
        },
            _=paper_clock.tick(), if group.has_consumer()=>{
                if let Some(cutoff) = market_tick(group, &transport) { break Err(cutoff); }
            }
        }
    };
    let (source, started) = match activation {
        Ok(active) => active,
        Err(cutoff) => return finish_subscription(transport, cutoff, group, control).await,
    };
    recovery.state = RecoveryState::Idle;
    let mut refresh = interval_at(started + Duration::from_secs(1), Duration::from_secs(1));
    refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_view = None;
    let mut ready = false;
    let mut next_check = started;
    type AuditResult = (Audit, Result<Option<RestBook>>);
    let mut pending_checks: Option<Pin<Box<dyn Stream<Item = AuditResult> + '_>>> = None;
    let mut render = true;
    let cutoff = loop {
        if render && !publish_view(&transport, group, &mut last_view, output) {
            break Cutoff::Closed(None);
        }
        group.expire_snapshots();
        if once && let Some(reason) = group.once_failure() {
            break Cutoff::Failed(anyhow::anyhow!("{reason}"));
        }
        if !once {
            recovery.observe(group);
        }
        if !once && recovery.waiting() && group.approval_is_current(Instant::now()) {
            break Cutoff::RecoverIsolated;
        }
        let recovery_at = recovery.deadline();
        let reconcile_wake = group.reconciliation_deadline();
        let snapshot_wake = group.snapshot_deadline();
        render = false;
        tokio::select! {
            stopped = &mut control.stop => break Cutoff::Stop(stopped),
            _ = paper_clock.tick(), if group.has_consumer() => {
                if let Some(cutoff) = market_tick(group, &transport) { break cutoff; }
            }
            _ = sleep_when_set(reconcile_wake) => {},
            _ = sleep_when_set(snapshot_wake), if group.awaiting_snapshot() => render = true,
            _ = sleep_when_set(recovery_at), if !once => {}
            frame = transport.frames.recv() => {
                let Some(frame) = frame else { break Cutoff::Closed(None) };
                if !transport.while_accepting(|| {}) {
                    break Cutoff::Closed(Some(frame));
                }
                if let Err(failed) = apply_frame(source,group, frame) {
                    break Cutoff::FrameFailed(failed);
                }
            }
            update = gamma.refresh(group) => {
                let (_, update) = match control.accept_gamma(update) {
                    ControlFlow::Continue(update) => update, ControlFlow::Break(stop) => break Cutoff::Stop(stop),
                };
                if observe_gamma(group, update, output) {
                    break Cutoff::Gamma(recovery.due(Instant::now()));
                }
            }
            _ = sleep_until(next_check), if pending_checks.is_none() && !group.awaiting_snapshot() => {
                pending_checks = Some(Box::pin(checks(gamma.client, endpoints.rest, group.audits())));
            }
            checked = async {
                match &mut pending_checks {
                    Some(checks) => checks.next().await,
                    None => future::pending().await,
                }
            } => match checked {
                Some((audit, response)) => {
                    if let Err(err) = group.audit(&audit, response) {
                        output.diagnostic(format!("CLOB REST 核對 {}: {}", single_line(audit.asset()), single_line(&format!("{err:#}"))));
                    }
                }
                None => {
                    pending_checks = None;
                    next_check = Instant::now() + REST_AUDIT_INTERVAL;
                }
            },
            _ = refresh.tick() => render = true,
        }
        if group.expired_reconciliation().is_some() {
            break Cutoff::ReconcileDeadline;
        }
        group.expire_snapshots();
        if once && group.quotes_ready() {
            break Cutoff::Once(source);
        }
        let current_ready = group.quotes_ready();
        render |= ready != current_ready;
        ready = current_ready;
        if !group.has_open_assets() {
            break Cutoff::NoOpenMarkets;
        }
    };
    finish_subscription(transport, cutoff, group, control).await
}

async fn sleep_when_set(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => future::pending().await,
    }
}

pub async fn watch(
    client: &Client,
    endpoints: &Endpoints<'_>,
    slug: &str,
    once: bool,
    output: &Output,
) -> Result<()> {
    run_monitor(client, endpoints, slug, once, LIVE_TIMING, output).await
}

/// Retire this subscription using the same stop-and-drain path as CLI shutdown.
pub async fn watch_until(
    client: &Client,
    endpoints: &Endpoints<'_>,
    slug: &str,
    output: &Output,
    stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    watch_group_until(
        client,
        endpoints,
        NegRiskEventGroup::new(slug),
        output,
        stop,
    )
    .await
}

pub async fn watch_group_until(
    client: &Client,
    endpoints: &Endpoints<'_>,
    group: NegRiskEventGroup,
    output: &Output,
    stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    let stop = Box::pin(async {
        tokio::select! {
            _=stop=>Ok(()),
            err=output.failure()=>Err(err)
        }
    });
    watch_group_loop(
        client,
        endpoints,
        group,
        LIVE_TIMING,
        &mut WatchControl {
            once: false,
            output,
            stop,
        },
    )
    .await
}
#[cfg(test)]
async fn watch_with_timing(
    client: &Client,
    endpoints: &Endpoints<'_>,
    slug: &str,
    once: bool,
    timing: WatchTiming,
) -> Result<()> {
    let output = Output::new(std::io::sink(), std::io::sink());
    run_monitor(client, endpoints, slug, once, timing, &output).await
}

async fn run_monitor(
    client: &Client,
    endpoints: &Endpoints<'_>,
    slug: &str,
    once: bool,
    timing: WatchTiming,
    output: &Output,
) -> Result<()> {
    let stop = Box::pin(async {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                ensure!(!once, "monitoring cancelled");
                Ok(())
            }
            error = output.failure() => Err(error),
        }
    });
    let mut control = WatchControl { once, output, stop };
    watch_loop(client, endpoints, slug, timing, &mut control).await
}

async fn watch_loop(
    client: &Client,
    endpoints: &Endpoints<'_>,
    slug: &str,
    timing: WatchTiming,
    control: &mut WatchControl<'_>,
) -> Result<()> {
    watch_group_loop(
        client,
        endpoints,
        NegRiskEventGroup::new(slug),
        timing,
        control,
    )
    .await
}
async fn watch_group_loop(
    client: &Client,
    endpoints: &Endpoints<'_>,
    mut group: NegRiskEventGroup,
    timing: WatchTiming,
    control: &mut WatchControl<'_>,
) -> Result<()> {
    let result = watch_sessions(client, endpoints, &mut group, timing, control).await;
    group.end_consumer(Instant::now());
    let result = group.finish_with(result).await;
    if group.event_closed() && !control.once {
        let output = control.output.finish_book(group.view()).await;
        if let (Err(prior), Err(last)) = (&result, &output)
            && let Some(writer) = last.downcast_ref::<crate::output::WriteFailure>()
            && crate::book::has_failure(prior, &|cause| writer.same(cause))
        {
            return result;
        }
        return crate::book::after_cleanup(result, output);
    }
    result
}

async fn watch_sessions(
    client: &Client,
    endpoints: &Endpoints<'_>,
    group: &mut NegRiskEventGroup,
    timing: WatchTiming,
    control: &mut WatchControl<'_>,
) -> Result<()> {
    if let Some(update) = group.take_terminal_delivery() {
        update?;
    }
    group.check_capacity()?;
    let stopped = control.observed_stop();
    if let Some(retirement) = group.local_retirement(crate::book::input::End::Handoff) {
        match control.finish_retirement(retirement, stopped).await {
            ControlFlow::Break(result) => return result,
            ControlFlow::Continue(result) => {
                result?;
            }
        }
    } else if let Some(result) = stopped {
        group.end_consumer(Instant::now());
        return result;
    }
    let once = control.once;
    let output = control.output;
    let mut gamma = GammaPoll {
        client,
        base: endpoints.gamma,
        interval: timing.gamma,
        next_at: Instant::now(),
    };
    let mut retry = 1;
    let mut recovery = Recovery {
        initial: timing.recovery,
        delay: timing.recovery,
        state: RecoveryState::Idle,
    };
    let mut reconnect_at = None;
    let mut idle_clock = market_clock();
    loop {
        if let Some(at) = reconnect_at.take() {
            loop {
                tokio::select! {
                    _ = sleep_until(at) => break,
                    stopped = &mut control.stop => {
                        group.end_consumer(Instant::now());
                        return stopped;
                    }
                    _=idle_clock.tick(), if group.has_consumer()=>group.clock_at(|| Ok(Some(Instant::now())))?,
                }
            }
        }
        if once && let Some(reason) = group.once_failure() {
            bail!("{reason}");
        }
        if group.event_closed() {
            return Ok(());
        }
        if !group.approval_is_current(Instant::now()) || !group.has_open_assets() {
            let update = tokio::select! {
                update = gamma.refresh(group) => update,
                _ = idle_clock.tick(), if group.has_consumer() => {
                    group.clock_at(|| Ok(Some(Instant::now())))?;
                    continue;
                }
                stopped = &mut control.stop => {
                    group.end_consumer(Instant::now());
                    return stopped;
                }
            };
            let (accepted_at, update) = match control.accept_gamma(update) {
                ControlFlow::Continue(update) => update,
                ControlFlow::Break(stop) => {
                    group.end_consumer(Instant::now());
                    return stop;
                }
            };
            let result = match update {
                Err(err) if once => return Err(err),
                Err(err)
                    if group.awaiting_gamma()
                        && err
                            .downcast_ref::<reqwest::Error>()
                            .and_then(reqwest::Error::status)
                            .is_some_and(|status| {
                                status.is_client_error()
                                    && status != StatusCode::REQUEST_TIMEOUT
                                    && status != StatusCode::TOO_MANY_REQUESTS
                                    && status != StatusCode::MISDIRECTED_REQUEST
                            }) =>
                {
                    return Err(err);
                }
                Ok(Discovery::Unavailable(err)) if group.awaiting_gamma() => return Err(err),
                update => update,
            };
            report_gamma_error(&result, output);
            group.update_discovery_at(accepted_at, result)?;
            if !group.event_closed() {
                publish_current(group, output);
            }
            continue;
        }

        let started = Instant::now();
        let result = session(
            endpoints,
            &mut gamma,
            group,
            &mut recovery,
            &timing,
            control,
        )
        .await;
        match result {
            Ok(SessionEnd::RecoverIsolated) => {
                output.diagnostic("隔離 Token 退避結束，重新訂閱".to_owned());
                group.disconnect();
                recovery.reconnect();
            }
            Ok(SessionEnd::Finished(result)) => return result,
            Ok(SessionEnd::Done) => {
                group.disconnect();
                retry = 1;
            }
            Err(err) if err.is::<crate::book::SettlementCapacity>() => return Err(err),
            Err(err) if err.is::<ExpiredSubscription>() => {}
            Err(err) => {
                group.disconnect();
                if once {
                    return Err(err);
                }
                if started.elapsed() >= Duration::from_secs(30) {
                    retry = 1;
                }
                output.diagnostic(format!(
                    "{}，{retry} 秒後嘗試重新連線",
                    single_line(&format!("{err:#}"))
                ));
                reconnect_at = Some(Instant::now() + Duration::from_secs(retry));
                retry = (retry * 2).min(30);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::book::Subscription;
    use serde_json::json;
    use std::{
        collections::BTreeSet,
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use futures_util::SinkExt;
    use tokio::time::timeout;
    use tokio::{io::AsyncWriteExt, net::TcpListener};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    thread_local! {
        static ADMIT_BEFORE_STOP_SEAL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
        static STOP_BEFORE_CLOSED: std::cell::RefCell<Option<tokio::sync::oneshot::Sender<()>>> =
            const { std::cell::RefCell::new(None) };
        static STOP_AT_DRAIN: std::cell::RefCell<Option<tokio::sync::oneshot::Sender<()>>> =
            const { std::cell::RefCell::new(None) };
        static BEFORE_CURRENT_FRAME_RECOVERY: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
    }

    pub(super) fn before_stop_seal() {
        if let Some(admit) = ADMIT_BEFORE_STOP_SEAL.take() {
            admit();
        }
    }

    pub(super) fn before_closed_commit() {
        if let Some(signal) = STOP_BEFORE_CLOSED.take() {
            signal.send(()).unwrap();
        }
    }

    pub(super) fn before_drain() {
        if let Some(signal) = STOP_AT_DRAIN.take() {
            signal.send(()).unwrap();
        }
    }

    pub(super) fn before_current_frame_recovery() {
        if let Some(check) = BEFORE_CURRENT_FRAME_RECOVERY.take() {
            check();
        }
    }

    use super::*;

    use crate::test_http::{
        CLOSED, GAMMA_FIXTURE, http_request, http_response, http_response_with_status,
    };

    #[tokio::test]
    async fn session_stop_retires_an_open_market_socket() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gamma = format!("http://{}", http.local_addr().unwrap());
        let socket = format!("ws://{}", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &gamma,
            rest: &gamma,
            socket: &socket,
        };
        let client = Client::builder().no_proxy().build().unwrap();
        let output = Output::new(std::io::sink(), std::io::sink());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = async {
            let (req, _) = http_request(&http).await;
            http_response(req, GAMMA_FIXTURE).await;
            let (socket, _) = ws.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            assert!(socket.next().await.unwrap().unwrap().is_text());
            stop.send(()).unwrap();
            while let Some(Ok(msg)) = socket.next().await {
                if msg.is_close() {
                    break;
                }
            }
        };
        timeout(Duration::from_secs(3), async {
            tokio::join!(
                watch_until(&client, &endpoints, "fixture", &output, stopped),
                server
            )
            .0
            .unwrap()
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn market_socket_survives_a_clear_and_reconnects_when_the_clear_is_lost() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &url,
        };
        let client = Client::builder().no_proxy().build().unwrap();
        let output = Output::new(std::io::sink(), std::io::sink());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (tx, mut feed) = tokio::sync::mpsc::unbounded_channel();
        let (prints, mut tape) = tokio::sync::mpsc::unbounded_channel();
        let group = NegRiskEventGroup::with_market_consumer(
            "fixture",
            crate::paper::Quantity::new(25.into()).unwrap(),
            move |_, input| match input {
                crate::book::MarketInput::Depth(depth) => {
                    let _ = tx.send(depth);
                }
                crate::book::MarketInput::Trade(trade) => {
                    let _ = prints.send(trade.clone());
                }
                _ => {}
            },
        );
        let server = async {
            let (req, _) = http_request(&http).await;
            http_response(req, GAMMA_FIXTURE).await;
            let (sock, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(sock).await.unwrap();
            socket.next().await.unwrap().unwrap();
            let initial = books(&[("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")])
                .into_text()
                .unwrap()
                .replace("0.40", "0.003")
                .replace(
                    r#"{"price":"0.60","size":"5"}"#,
                    r#"{"price":"0.004","size":"5"},{"price":"0.005","size":"8"}"#,
                );
            socket.send(Message::Text(initial)).await.unwrap();
            while feed.recv().await.unwrap().is_none() {}
            socket.send(Message::Text(json!({"event_type":"last_trade_price","asset_id":"1","side":"SELL","price":"0.004","size":"7","timestamp":"100"}).to_string())).await.unwrap();
            let trade = tape.recv().await.unwrap();
            assert_eq!(trade.size, rust_decimal::Decimal::from(7));
            let delta = |side, price, size| {
                Message::Text(json!({"event_type":"price_change","market":"a","timestamp":"101","price_changes":[{"asset_id":"1","side":side,"price":price,"size":size,"best_bid":"0.004","best_ask":"0.005"}]}).to_string())
            };
            socket.send(delta("BUY", "0.004", "9")).await.unwrap();
            assert!(feed.recv().await.unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
            socket.send(delta("SELL", "0.004", "0")).await.unwrap();
            // Recovery through this same socket proves no reconnect occurred.
            let ready = feed.recv().await.unwrap().unwrap();
            assert_eq!(ready[0].asks[0].price, rust_decimal::Decimal::new(5, 3));
            socket.send(delta("SELL", "0.004", "7")).await.unwrap();
            assert!(feed.recv().await.unwrap().is_none());
            let (replacement, _) = listener.accept().await.unwrap();
            let mut replacement = accept_async(replacement).await.unwrap();
            assert!(replacement.next().await.unwrap().unwrap().is_text());
            stop.send(()).unwrap();
        };
        timeout(Duration::from_secs(7), async {
            tokio::join!(
                watch_group_until(&client, &endpoints, group, &output, stopped),
                server
            )
            .0
            .unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rest_redirect_is_a_failed_check_and_only_404_means_absent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let body = r#"{"asset_id":"1","market":"a","timestamp":"100","bids":[],"asks":[],"tick_size":"0.01"}"#;
        for status in ["302 Found", "404 Not Found"] {
            let (result, ()) = tokio::join!(fetch_book(&client, &base, "1"), async {
                let (socket, _) = http_request(&listener).await;
                http_response_with_status(socket, status, body).await;
            });
            if status.starts_with("302") {
                assert!(result.unwrap_err().to_string().contains("302"));
            } else {
                assert!(result.unwrap().is_none());
            }
        }
    }

    async fn serve_http(listener: TcpListener) -> BTreeSet<String> {
        let mut books = BTreeSet::new();
        let mut discoveries = 0;
        while discoveries < 3 || books.len() < 3 {
            let (socket, line) = http_request(&listener).await;
            let body = if line == "GET /events/slug/fixture HTTP/1.1" {
                discoveries += 1;
                if discoveries < 3 {
                    GAMMA_FIXTURE.to_owned()
                } else {
                    CLOSED.to_owned()
                }
            } else {
                let asset = line
                    .strip_prefix("GET /book?token_id=")
                    .unwrap()
                    .strip_suffix(" HTTP/1.1")
                    .unwrap();
                assert!(["1", "2", "3", "4"].contains(&asset));
                books.insert(asset.to_owned());
                let market = if asset == "1" || asset == "2" {
                    "a"
                } else {
                    "b"
                };
                json!({"asset_id":asset,"market":market,"timestamp":"100",
                    "bids":[{"price":"0.40","size":"5"}],
                    "asks":[{"price":"0.60","size":"5"}],"tick_size":"0.01"})
                .to_string()
            };
            http_response(socket, &body).await;
        }
        books
    }

    async fn serve_ws(listener: TcpListener) {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let subscription = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&subscription).unwrap(),
            json!({"type":"market","assets_ids":["1","2","3","4"],"custom_feature_enabled":true})
        );
        let early_delta = |timestamp| {
            json!({
                "event_type":"price_change","market":"a","timestamp":timestamp,
                "price_changes":[{"asset_id":"1","price":"0.41","size":"6","side":"BUY"}]
            })
        };
        ws.send(Message::Text(early_delta(100).to_string()))
            .await
            .unwrap();
        let books: Vec<_> = [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")]
            .into_iter()
            .map(|(asset, market)| {
                json!({"event_type":"book","asset_id":asset,"market":market,
                "timestamp":"100","bids":[{"price":"0.40","size":"5"}],
                "asks":[{"price":"0.60","size":"5"}]})
            })
            .collect();
        ws.send(Message::Text(serde_json::to_string(&books).unwrap()))
            .await
            .unwrap();
        let burst: Vec<_> = (101..400).map(early_delta).collect();
        for batch in burst.chunks(75) {
            ws.send(Message::Text(serde_json::to_string(batch).unwrap()))
                .await
                .unwrap();
        }
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Text("PING".into())
        );
        ws.send(Message::Text("PONG".into())).await.unwrap();
        ws.send(Message::Text(
            json!([
                {"event_type":"market_resolved","market":"a"},
                {"event_type":"market_resolved","market":"b"}
            ])
            .to_string(),
        ))
        .await
        .unwrap();
        let _ = ws.next().await;
    }

    #[tokio::test]
    async fn hot_asset_does_not_reconnect_other_books_before_heartbeat_and_resolution() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let socket = format!("ws://{}/ws/market", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &socket,
        };
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        timeout(Duration::from_secs(20), async {
            let (books, _, result) = tokio::join!(
                serve_http(http),
                serve_ws(ws),
                watch_with_timing(
                    &client,
                    &endpoints,
                    "fixture",
                    false,
                    WatchTiming {
                        gamma: Duration::from_secs(6),
                        ..LIVE_TIMING
                    }
                )
            );
            result.unwrap();
            assert_eq!(books, ["2", "3", "4"].map(str::to_owned).into());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn reconnect_checks_gamma_for_resolution_even_without_a_first_book() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let socket = format!("ws://{}/ws/market", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &socket,
        };
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let refreshed = tokio::sync::Notify::new();
        let gamma = async {
            let (socket, request) = http_request(&http).await;
            assert_eq!(request, "GET /events/slug/fixture HTTP/1.1");
            http_response(socket, GAMMA_FIXTURE).await;
            let (socket, request) = http_request(&http).await;
            assert_eq!(request, "GET /events/slug/fixture HTTP/1.1");
            refreshed.notify_one();
            tokio::time::sleep(Duration::from_millis(100)).await;
            http_response(socket, CLOSED).await;
        };
        let socket = async {
            let (socket, _) = ws.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            assert!(socket.next().await.unwrap().unwrap().is_text());
            refreshed.notified().await;
            socket.close(None).await.unwrap();
        };
        let (_, _, result) = timeout(Duration::from_secs(8), async {
            tokio::join!(
                gamma,
                socket,
                watch_with_timing(
                    &client,
                    &endpoints,
                    "fixture",
                    false,
                    WatchTiming {
                        gamma: Duration::from_millis(200),
                        ..LIVE_TIMING
                    }
                )
            )
        })
        .await
        .unwrap();
        result.unwrap();
    }

    #[tokio::test]
    async fn open_event_with_all_members_closed_polls_for_new_members() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let socket = format!("ws://{}/ws/market", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &socket,
        };
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let gamma = async {
            let mut event: serde_json::Value = serde_json::from_str(GAMMA_FIXTURE).unwrap();
            let markets = event["markets"].as_array_mut().unwrap();
            for market in markets {
                market["closed"] = json!(true);
            }
            let (socket, request) = http_request(&http).await;
            assert_eq!(request, "GET /events/slug/fixture HTTP/1.1");
            http_response(socket, &event.to_string()).await;
            event["markets"].as_array_mut().unwrap().push(json!({
                "question":"New member","conditionId":"c","outcomes":["Yes","No"],
                "clobTokenIds":["5","6"],"orderPriceMinTickSize":"0.01","closed":false
            }));
            let (socket, request) = http_request(&http).await;
            assert_eq!(request, "GET /events/slug/fixture HTTP/1.1");
            http_response(socket, &event.to_string()).await;
            let (socket, request) = http_request(&http).await;
            assert_eq!(request, "GET /events/slug/fixture HTTP/1.1");
            http_response(socket, r#"{"slug":"fixture","closed":true}"#).await;
        };
        let websocket = async {
            let (socket, _) = ws.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            let subscription = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let subscription: serde_json::Value = serde_json::from_str(&subscription).unwrap();
            assert_eq!(subscription["assets_ids"], json!(["5", "6"]));
            // Keep the socket alive until the monitor consumes Gamma's terminal response.
            let _ = socket.next().await;
        };
        let (_, _, result) = timeout(Duration::from_secs(8), async {
            tokio::join!(
                gamma,
                websocket,
                watch_with_timing(
                    &client,
                    &endpoints,
                    "fixture",
                    false,
                    WatchTiming {
                        gamma: Duration::from_millis(200),
                        ..LIVE_TIMING
                    }
                )
            )
        })
        .await
        .unwrap();
        result.unwrap();
    }

    #[tokio::test]
    async fn websocket_failure_reconnects_using_fresh_gamma_without_refetching() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let socket = format!("ws://{}/ws/market", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &socket,
        };
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let server = async {
            let (socket, _) = http_request(&http).await;
            http_response(socket, GAMMA_FIXTURE).await;
            let (socket, _) = ws.accept().await.unwrap();
            let mut first = accept_async(socket).await.unwrap();
            let first_subscription = first.next().await.unwrap().unwrap();
            first.close(None).await.unwrap();

            let (socket, _) = tokio::select! {
                result = ws.accept() => result.unwrap(),
                _ = http_request(&http) => panic!("Gamma refreshed before using its cached members"),
            };
            let mut second = accept_async(socket).await.unwrap();
            assert_eq!(second.next().await.unwrap().unwrap(), first_subscription);
            let (socket, _) = http_request(&http).await;
            http_response(socket, CLOSED).await;
            let _ = second.next().await;
        };
        let (_, result) = timeout(Duration::from_secs(6), async {
            tokio::join!(
                server,
                watch_with_timing(
                    &client,
                    &endpoints,
                    "fixture",
                    false,
                    WatchTiming {
                        gamma: Duration::from_secs(3),
                        ..LIVE_TIMING
                    }
                )
            )
        })
        .await
        .unwrap();
        result.unwrap();
    }

    fn books(assets: &[(&str, &str)]) -> Message {
        let books: Vec<_> = assets
            .iter()
            .map(|(asset, market)| {
                json!({
                    "event_type":"book","asset_id":asset,"market":market,"timestamp":"100",
                    "bids":[{"price":"0.40","size":"5"}],"asks":[{"price":"0.60","size":"5"}]
                })
            })
            .collect();
        Message::Text(serde_json::to_string(&books).unwrap())
    }

    async fn check_recovery_with_unchanged_members(early_delta: bool) {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let socket = format!("ws://{}/ws/market", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &socket,
        };
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let gamma_interval = Duration::from_secs(3);
        let snapshots = Duration::from_millis(300);
        let recovery = Duration::from_millis(400);
        let server = async {
            let (socket, _) = http_request(&http).await;
            http_response(socket, GAMMA_FIXTURE).await;
            let (socket, _) = ws.accept().await.unwrap();
            let mut first = accept_async(socket).await.unwrap();
            let subscription = first.next().await.unwrap().unwrap();
            let sent_at = Instant::now();
            if early_delta {
                first.send(Message::Text(json!({
                    "event_type":"price_change","market":"a","timestamp":"99",
                    "price_changes":[{"asset_id":"1","price":"0.41","size":"6","side":"BUY"}]
                }).to_string())).await.unwrap();
            }
            first
                .send(books(&[("2", "a"), ("3", "b"), ("4", "b")]))
                .await
                .unwrap();

            let mut healthy = BTreeSet::new();
            while healthy.len() < 3 {
                let (socket, request) = http_request(&http).await;
                let asset = request
                    .strip_prefix("GET /book?token_id=")
                    .unwrap()
                    .strip_suffix(" HTTP/1.1")
                    .unwrap();
                healthy.insert(asset.to_owned());
                http_response(socket, "{}").await;
            }
            assert_eq!(healthy, ["2", "3", "4"].map(str::to_owned).into());
            let (socket, _) = ws.accept().await.unwrap();
            let expected_delay = if early_delta {
                recovery
            } else {
                snapshots + recovery
            };
            assert!(sent_at.elapsed() >= expected_delay - Duration::from_millis(40));
            let mut second = accept_async(socket).await.unwrap();
            assert_eq!(second.next().await.unwrap().unwrap(), subscription);
            second
                .send(books(&[("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")]))
                .await
                .unwrap();
            let mut synced = BTreeSet::new();
            loop {
                let (socket, request) = http_request(&http).await;
                if request == "GET /events/slug/fixture HTTP/1.1" {
                    http_response(socket, CLOSED).await;
                    break;
                }
                let asset = request
                    .strip_prefix("GET /book?token_id=")
                    .unwrap()
                    .strip_suffix(" HTTP/1.1")
                    .unwrap();
                synced.insert(asset.to_owned());
                http_response(socket, "{}").await;
            }
            assert_eq!(synced, ["1", "2", "3", "4"].map(str::to_owned).into());
            let _ = second.next().await;
        };
        let (_, result) = timeout(Duration::from_secs(6), async {
            tokio::join!(
                server,
                watch_with_timing(
                    &client,
                    &endpoints,
                    "fixture",
                    false,
                    WatchTiming {
                        gamma: gamma_interval,
                        snapshots,
                        recovery,
                        ..LIVE_TIMING
                    }
                )
            )
        })
        .await
        .unwrap();
        result.unwrap();
    }

    #[tokio::test]
    async fn silent_and_early_delta_tokens_recover_without_new_gamma_members() {
        tokio::join!(
            check_recovery_with_unchanged_members(false),
            check_recovery_with_unchanged_members(true)
        );
    }

    #[tokio::test]
    async fn gamma_member_rebind_preserves_the_watch_poll_interval() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let socket = format!("ws://{}/ws/market", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &socket,
        };
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let interval = Duration::from_millis(250);
        let server = async {
            let replacement = GAMMA_FIXTURE
                .replace(r#""conditionId":"b""#, r#""conditionId":"c""#)
                .replace(r#"["3","4"]"#, r#"["5","6"]"#);
            let mut last = None;
            for (body, expected) in [
                (GAMMA_FIXTURE, ["1", "2", "3", "4"]),
                (&replacement, ["1", "2", "5", "6"]),
            ] {
                let (socket, request) = http_request(&http).await;
                assert_eq!(request, "GET /events/slug/fixture HTTP/1.1");
                let now = Instant::now();
                if let Some(previous) = last {
                    assert!(now - previous >= interval - Duration::from_millis(20));
                }
                last = Some(now);
                http_response(socket, body).await;
                let (socket, _) = ws.accept().await.unwrap();
                let mut websocket = accept_async(socket).await.unwrap();
                let subscription = websocket
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_text()
                    .unwrap();
                let subscription: serde_json::Value = serde_json::from_str(&subscription).unwrap();
                assert_eq!(subscription["assets_ids"], json!(expected));
                // Keep the socket alive until the following Gamma response retires it.
                tokio::spawn(async move {
                    let _ = websocket.next().await;
                });
            }
            let (socket, request) = http_request(&http).await;
            assert_eq!(request, "GET /events/slug/fixture HTTP/1.1");
            assert!(last.unwrap().elapsed() >= interval - Duration::from_millis(20));
            http_response(socket, CLOSED).await;
        };
        let (_, result) = timeout(Duration::from_secs(4), async {
            tokio::join!(
                server,
                watch_with_timing(
                    &client,
                    &endpoints,
                    "fixture",
                    false,
                    WatchTiming {
                        gamma: interval,
                        ..LIVE_TIMING
                    }
                )
            )
        })
        .await
        .unwrap();
        result.unwrap();
    }

    #[derive(Clone, Default)]
    struct Recorded(Arc<Mutex<Vec<u8>>>, Arc<tokio::sync::Notify>);

    impl Write for Recorded {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            self.1.notify_waiters();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Recorded {
        fn cursor(&self) -> usize {
            self.0.lock().unwrap().len()
        }

        async fn wait_for_view_after(&self, cursor: usize, matches: impl Fn(&str) -> bool) {
            loop {
                let notified = self.1.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let bytes = self.0.lock().unwrap();
                    let views = std::str::from_utf8(&bytes[cursor..]).unwrap();
                    if views.split("Fixture | ").skip(1).any(&matches) {
                        return;
                    }
                }
                notified.await;
            }
        }

        async fn wait_for(&self, text: &str) {
            loop {
                if String::from_utf8_lossy(&self.0.lock().unwrap()).contains(text) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    #[tokio::test]
    async fn initial_gamma_failure_retries_on_the_poll_schedule_without_subscribing() {
        for (once, status, expected_error) in [
            (false, Some("503 Service Unavailable"), None),
            (false, Some("421 Misdirected Request"), None),
            (false, None, None),
            (true, Some("503 Service Unavailable"), Some("503")),
            (true, Some("421 Misdirected Request"), Some("421")),
            (false, Some("404 Not Found"), Some("404")),
        ] {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", http.local_addr().unwrap());
            let url = format!("ws://{}", ws.local_addr().unwrap());
            let endpoints = Endpoints {
                gamma: &base,
                rest: &base,
                socket: &url,
            };
            let client_timeout = Duration::from_millis(150);
            let client = Client::builder()
                .no_proxy()
                .timeout(client_timeout)
                .build()
                .unwrap();
            let recorded = Recorded::default();
            let output = Output::new(recorded.clone(), io::sink());
            let timing = WatchTiming {
                gamma: Duration::from_millis(220),
                ..LIVE_TIMING
            };
            let server = async {
                let (socket, _) = http_request(&http).await;
                let issued = Instant::now();
                if let Some(status) = status {
                    http_response_with_status(socket, status, "").await;
                } else {
                    tokio::time::sleep(client_timeout).await;
                    drop(socket);
                }
                if expected_error.is_some() {
                    assert!(timeout(timing.gamma * 2, http.accept()).await.is_err());
                    assert!(futures_util::poll!(Box::pin(ws.accept())).is_pending());
                    return;
                }
                recorded.wait_for("Gamma 更新失敗，等待重新查詢").await;
                {
                    let bytes = recorded.0.lock().unwrap();
                    assert!(
                        std::str::from_utf8(&bytes)
                            .unwrap()
                            .contains("| NotReady (")
                    );
                }
                let (second, _) = tokio::select! {
                    socket = ws.accept() => panic!("subscribed before Gamma recovery: {socket:?}"),
                    request = http_request(&http) => request,
                };
                assert!(issued.elapsed() >= timing.gamma);
                http_response(second, GAMMA_FIXTURE).await;
                let (socket, _) = ws.accept().await.unwrap();
                let mut socket = accept_async(socket).await.unwrap();
                let request = socket.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&request).unwrap()["assets_ids"],
                    json!(["1", "2", "3", "4"])
                );
                let (third, _) = http_request(&http).await;
                http_response(third, CLOSED).await;
                let _ = socket.next().await;
            };
            timeout(Duration::from_secs(4), async {
                let (_, result) = tokio::join!(
                    server,
                    run_monitor(&client, &endpoints, "fixture", once, timing, &output)
                );
                if let Some(message) = expected_error {
                    assert!(result.unwrap_err().to_string().contains(message));
                } else {
                    result.unwrap();
                }
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn expired_handshake_waits_for_new_members_without_socket_backoff() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let url = format!("ws://{}", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &url,
        };
        let client = Client::builder().no_proxy().build().unwrap();
        let timing = WatchTiming {
            gamma: Duration::from_millis(220),
            ..LIVE_TIMING
        };
        let server = async {
            let (first, _) = http_request(&http).await;
            http_response(first, GAMMA_FIXTURE).await;
            let (old, _) = ws.accept().await.unwrap();
            tokio::time::sleep(timing.gamma + Duration::from_millis(30)).await;
            let mut old = accept_async(old).await.unwrap();
            assert!(!matches!(old.next().await, Some(Ok(Message::Text(_)))));
            let (refresh, _) = http_request(&http).await;
            let new_members = GAMMA_FIXTURE.replace(r#"["1","2"]"#, r#"["5","6"]"#);
            http_response(refresh, &new_members).await;
            let (new, _) = timeout(Duration::from_millis(750), ws.accept())
                .await
                .unwrap()
                .unwrap();
            let mut new = accept_async(new).await.unwrap();
            let request = new.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&request).unwrap()["assets_ids"],
                json!(["3", "4", "5", "6"])
            );
            let (last, _) = http_request(&http).await;
            http_response(last, CLOSED).await;
            let _ = new.next().await;
        };
        timeout(Duration::from_secs(4), async {
            let (_, result) = tokio::join!(
                server,
                watch_with_timing(&client, &endpoints, "fixture", false, timing)
            );
            result.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn second_disconnect_replaces_ready_with_the_same_stale_view() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let url = format!("ws://{}", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &url,
        };
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let recorded = Recorded::default();
        let output = Output::new(recorded.clone(), io::sink());
        let server = async {
            let (socket, _) = http_request(&http).await;
            http_response(socket, GAMMA_FIXTURE).await;
            for _ in 0..2 {
                let (socket, _) = ws.accept().await.unwrap();
                let mut conn = accept_async(socket).await.unwrap();
                conn.next().await.unwrap().unwrap();
                let before_snapshots = recorded.cursor();
                conn.send(books(&[("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")]))
                    .await
                    .unwrap();
                recorded
                    .wait_for_view_after(before_snapshots, |view| view.starts_with("Ready (2/2"))
                    .await;
                let before_close = recorded.cursor();
                conn.close(None).await.unwrap();
                recorded
                    .wait_for_view_after(before_close, |view| {
                        view.starts_with("NotReady")
                            && view.contains("YES 中間價總和 —")
                            && view.lines().filter(|line| *line == "  YES Stale").count() == 2
                    })
                    .await;
            }
        };
        timeout(Duration::from_secs(6), async {
            tokio::select! {
                () = server => {}
                result = run_monitor(&client, &endpoints, "fixture", false, LIVE_TIMING, &output) =>
                    panic!("monitor exited before the second disconnect: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn close_cursor_cannot_match_two_retirement_views_from_the_previous_close() {
        let mut recorded = Recorded::default();
        let stale = b"Fixture | NotReady (0/2)\n  YES Stale\n  YES Stale\n";
        let ready = b"Fixture | Ready (2/2)\n";
        recorded.write_all(stale).unwrap();
        recorded.write_all(stale).unwrap();
        recorded.write_all(ready).unwrap();

        let cursor = recorded.cursor();
        let mut writer = recorded.clone();
        let next = recorded.wait_for_view_after(cursor, |view| view.starts_with("NotReady"));
        tokio::pin!(next);
        assert!(futures_util::poll!(&mut next).is_pending());
        writer.write_all(stale).unwrap();
        next.await;
    }

    #[tokio::test]
    async fn expired_gamma_during_isolation_recovery_keeps_live_peers_until_replacement_ready() {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let url = format!("ws://{}", ws.local_addr().unwrap());
        let endpoints = Endpoints {
            gamma: &base,
            rest: &base,
            socket: &url,
        };
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let recorded = Recorded::default();
        let output = Output::new(recorded.clone(), io::sink());
        let attempted = tokio::sync::Notify::new();
        let gamma = async {
            let mut discoveries = 0;
            let mut last = None;
            loop {
                let (mut socket, request) = http_request(&http).await;
                if request != "GET /events/slug/fixture HTTP/1.1" {
                    http_response(socket, "{}").await;
                    continue;
                }
                let now = Instant::now();
                if let Some(previous) = last {
                    assert!(now - previous >= Duration::from_millis(230));
                }
                last = Some(now);
                discoveries += 1;
                match discoveries {
                    1 => http_response(socket, GAMMA_FIXTURE).await,
                    2 | 3 => {
                        socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await.unwrap();
                        if discoveries == 3 {
                            attempted.notify_one();
                        }
                    }
                    4 => {
                        recorded.wait_for("bid 0.43").await;
                        http_response(socket, GAMMA_FIXTURE).await;
                    }
                    _ => {
                        recorded.wait_for("| Ready (2/2").await;
                        http_response(socket, CLOSED).await;
                        break;
                    }
                }
            }
        };
        let server = async {
            let (socket, _) = ws.accept().await.unwrap();
            let mut first = accept_async(socket).await.unwrap();
            let subscription = first.next().await.unwrap().unwrap();
            first
                .send(books(&[("2", "a"), ("3", "b"), ("4", "b")]))
                .await
                .unwrap();
            attempted.notified().await;
            first
                .send(Message::Text(
                    json!({"event_type":"price_change","market":"b","timestamp":"101",
                "price_changes":[{"asset_id":"3","price":"0.43","size":"5","side":"BUY"}]})
                    .to_string(),
                ))
                .await
                .unwrap();
            let (socket, _) = ws.accept().await.unwrap();
            let mut second = accept_async(socket).await.unwrap();
            assert_eq!(second.next().await.unwrap().unwrap(), subscription);
            second
                .send(books(&[("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")]))
                .await
                .unwrap();
            let _ = second.next().await;
        };
        let (_, _, result) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                gamma,
                server,
                run_monitor(
                    &client,
                    &endpoints,
                    "fixture",
                    false,
                    WatchTiming {
                        gamma: Duration::from_millis(250),
                        snapshots: Duration::from_millis(100),
                        recovery: Duration::from_millis(200),
                        ..LIVE_TIMING
                    },
                    &output
                )
            )
        })
        .await
        .unwrap();
        result.unwrap();
    }

    fn cutoff_group() -> NegRiskEventGroup {
        prepare_cutoff_group(NegRiskEventGroup::new("fixture"))
    }

    fn prepare_cutoff_group(mut group: NegRiskEventGroup) -> NegRiskEventGroup {
        let outcome = cutoff_update(&["a", "b"]);
        assert!(group.update_discovery(outcome).unwrap());
        group.begin().unwrap();
        let snapshots = books(&[("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")])
            .into_text()
            .unwrap();
        for snapshot in wire::events(&snapshots).unwrap() {
            group
                .apply_received(group.test_source(), snapshot, Instant::now())
                .unwrap();
        }
        group
    }

    fn cutoff_update(conditions: &[&str]) -> Result<Discovery> {
        let members: Vec<_> = conditions
            .iter()
            .map(|&condition| {
                let [yes, no] = match condition {
                    "a" => ["1", "2"],
                    "b" => ["3", "4"],
                    _ => ["5", "6"],
                };
                (condition, yes, no, rust_decimal::Decimal::new(1, 2), false)
            })
            .collect();
        Ok(Discovery::Open(gamma_discovery::fixture_spec(&members)))
    }

    #[tokio::test(start_paused = true)]
    async fn queued_snapshot_and_delayed_delta_keep_their_ingress_time() {
        use crate::paper::{
            AssetId, Cash, ClientOrderId, Price, Quantity, Shares, Side,
            chaos_exchange::ChaosExchange,
            fee_model::{FeeModel, MakerTier},
            order_fsm::{CloseReason, Delivery, OrderMode, OrderRequest, OrderStatus},
        };
        use rust_decimal::Decimal;
        for (queued, age) in [(true, 101), (false, 101), (true, 99)] {
            let mut group = cutoff_group();
            let asset = AssetId::new("1").unwrap();
            let funds = Cash::new(Decimal::TEN).unwrap();
            let mut exchange = ChaosExchange::new(100, funds).unwrap();
            exchange
                .register_asset(asset.clone(), FeeModel::FREE, Shares::ZERO)
                .unwrap();
            let account = exchange.allocate(funds, [], MakerTier::None).unwrap();
            group.attach_paper(exchange).unwrap();
            let old = group
                .paper()
                .unwrap()
                .current_stamp(&asset)
                .unwrap()
                .unwrap();
            let text = if queued {
                books(&[("1", "a")]).into_text().unwrap()
            } else {
                r#"{"event_type":"price_change","market":"a","timestamp":"101","price_changes":[{"asset_id":"1","side":"BUY","price":"0.40","size":"6"}]}"#.to_owned()
            };
            let (mut transport, _sender) = Transport::buffered_for_test([text]);
            let delay = Duration::from_millis(age);
            if queued {
                tokio::time::advance(delay).await;
            }
            let frame = transport.frames.recv().await.unwrap();
            if !queued {
                tokio::time::advance(delay).await;
            }
            apply_frame(group.test_source(), &mut group, frame).unwrap();
            let mut desk = group.paper().unwrap();
            let signal = desk.current_stamp(&asset);
            let size = Quantity::new(Decimal::ONE).unwrap();
            let order = OrderRequest::new(
                ClientOrderId::new(1).unwrap(),
                account,
                asset,
                Side::Buy,
                Price::new(Decimal::new(99, 2)).unwrap(),
                size,
                OrderMode::ImmediateOrCancel,
            );
            if age < 100 {
                let signal = signal.unwrap().unwrap();
                assert_eq!(signal.observed_ms(), old.observed_ms());
                assert_eq!(
                    desk.submit(order, signal, Delivery::Delivered)
                        .unwrap()
                        .status(),
                    OrderStatus::Closed {
                        filled: size.shares(),
                        reason: CloseReason::Filled
                    }
                );
            } else {
                assert_eq!(signal, Ok(None));
                assert_eq!(
                    desk.submit(order, old, Delivery::Delivered).unwrap_err(),
                    crate::paper::Error::StaleSignal
                );
            }
        }
    }

    fn test_control(output: &Output) -> WatchControl<'_> {
        WatchControl {
            once: false,
            output,
            stop: Box::pin(future::pending()),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn gamma_poll_waits_from_completion_without_extending_approval() {
        let start = Instant::now();
        let mut group = NegRiskEventGroup::new("fixture");
        let outcome = cutoff_update(&["a", "b"]);
        let client = Client::builder().no_proxy().build().unwrap();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        let mut poll = GammaPoll {
            client: &client,
            base: &base,
            interval: GAMMA_INTERVAL,
            next_at: start,
        };
        group.pending_gamma_fixture(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            outcome
        });
        let update = poll.refresh(&mut group).await;
        assert!(group.update_discovery(update).unwrap());
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(group.subscription().is_some());
        tokio::time::advance(Duration::from_millis(25_500)).await;
        assert!(group.subscription().is_none());
        assert_eq!(poll.next_at, start + Duration::from_secs(32));
        let mut refresh = Box::pin(poll.refresh(&mut group));
        assert!(futures_util::poll!(&mut refresh).is_pending());
        tokio::time::advance(Duration::from_millis(1500)).await;
        let server = async {
            let (socket, _) = http_request(&http).await;
            http_response(socket, GAMMA_FIXTURE).await;
        };
        let (update, ()) = tokio::join!(refresh, server);
        group.update_discovery(update).unwrap();
        assert!(group.subscription().is_some());
    }

    #[tokio::test]
    async fn ready_publication_is_ordered_against_rejection_with_a_live_sender() {
        use crate::transport::Payload;
        for publish_first in [false, true] {
            let (transport, sender) = Transport::local();
            let mut group = cutoff_group();
            let recorded = Recorded::default();
            let output = Output::new(recorded.clone(), io::sink());
            let mut last = None;
            assert!(transport.while_accepting(|| {}));
            if publish_first {
                assert!(publish_view(&transport, &group, &mut last, &output));
                timeout(Duration::from_secs(2), recorded.wait_for("| Ready ("))
                    .await
                    .unwrap();
            }
            let settled = r#"{"event_type":"market_resolved","market":"a"}"#;
            assert!(transport.enqueue(&sender, Payload::Raw(settled.into())));
            for _ in 1..64 {
                assert!(transport.enqueue(&sender, Payload::Raw("[]".into())));
            }
            assert!(!transport.enqueue(&sender, Payload::Raw("[]".into())));
            assert!(!transport.frames.is_closed());
            assert!(!publish_view(&transport, &group, &mut last, &output));
            assert_eq!(last.is_none(), !publish_first);
            let result = finish_subscription(
                transport,
                Cutoff::Closed(None),
                &mut group,
                &mut test_control(&output),
            )
            .await;
            assert!(result.err().unwrap().to_string().contains("queue full"));
            timeout(Duration::from_secs(2), recorded.wait_for("| NotReady ("))
                .await
                .unwrap();
            assert_eq!(
                String::from_utf8_lossy(&recorded.0.lock().unwrap())
                    .matches("| Ready (")
                    .count(),
                usize::from(publish_first)
            );
            assert_eq!(group.subscription().unwrap().into_parts().0, ["3", "4"]);
        }
    }

    fn group_at_settlement_limit() -> NegRiskEventGroup {
        let mut group = NegRiskEventGroup::new("fixture");
        for n in 0..=crate::book::MAX_SETTLEMENTS {
            let condition = format!("rolling-{n}");
            let tick = rust_decimal::Decimal::new(1, 2);
            let spec = gamma_discovery::fixture_spec(&[
                ("stable", "1", "2", tick, false),
                (&condition, "3", "4", tick, false),
            ]);
            let update = Ok(Discovery::Open(spec));
            assert!(group.update_discovery(update).unwrap());
            if n < crate::book::MAX_SETTLEMENTS {
                let id = group.open_input().unwrap();
                assert!(
                    group
                        .admit_event(id, wire::Event::MarketResolved { market: condition })
                        .unwrap()
                );
                assert!(group.advance_input(id).unwrap());
                assert!(group.close_input(id).unwrap());
            }
        }
        group
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_startup_drains_admitted_settlement_before_exit() {
        for (once, admitted_before_stop) in [(false, true), (false, false), (true, false)] {
            let mut group = group_at_settlement_limit();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let socket = format!("ws://{}", listener.local_addr().unwrap());
            let (allow_flush, handshake) = tokio::sync::oneshot::channel();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (allow_frame, gate) = tokio::sync::oneshot::channel();
            let (ack, admitted) = std::sync::mpsc::channel();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handshake.await.unwrap();
                let mut ws = accept_async(socket).await.unwrap();
                assert!(ws.next().await.unwrap().unwrap().is_text());
                ws.send(Message::Ping(vec![1])).await.unwrap();
                assert_eq!(ws.next().await.unwrap().unwrap(), Message::Pong(vec![1]));
                started_tx.send(()).unwrap();
                gate.await.unwrap();
                ws.send(Message::Text(
                    r#"{"event_type":"market_resolved","market":"rolling-4096"}"#.into(),
                ))
                .await
                .unwrap();
                // The worker only reads this ping after try_send accepted the settlement.
                ws.send(Message::Ping(vec![42])).await.unwrap();
                assert_eq!(ws.next().await.unwrap().unwrap(), Message::Pong(vec![42]));
                ack.send(()).unwrap();
                let _ = ws.next().await;
            });
            let output = Output::new(io::sink(), io::sink());
            let mut admit = Some(move || {
                allow_frame.send(()).unwrap();
                admitted.recv_timeout(Duration::from_secs(4)).unwrap();
            });
            if !admitted_before_stop {
                ADMIT_BEFORE_STOP_SEAL.replace(Some(Box::new(admit.take().unwrap())));
            }
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let mut control = WatchControl {
                once,
                output: &output,
                stop: Box::pin(async move {
                    stopped.await?;
                    ensure!(!once, "monitoring cancelled");
                    Ok(())
                }),
            };
            let client = Client::builder().no_proxy().build().unwrap();
            let mut gamma = GammaPoll {
                client: &client,
                base: "",
                interval: GAMMA_INTERVAL,
                next_at: Instant::now() + GAMMA_INTERVAL,
            };
            let mut recovery = Recovery {
                initial: RECOVERY_INITIAL,
                delay: RECOVERY_INITIAL,
                state: RecoveryState::Idle,
            };
            let endpoints = Endpoints {
                gamma: "",
                rest: "",
                socket: &socket,
            };
            let mut run = Box::pin(session(
                &endpoints,
                &mut gamma,
                &mut group,
                &mut recovery,
                &LIVE_TIMING,
                &mut control,
            ));
            assert!(futures_util::poll!(&mut run).is_pending());
            allow_flush.send(()).unwrap();
            started_rx.recv_timeout(Duration::from_secs(4)).unwrap();
            if let Some(admit) = admit {
                admit();
            }
            stop.send(()).unwrap();
            let err = run.await.err().unwrap();
            assert!(err.is::<crate::book::SettlementCapacity>(), "{err:#}");
            assert!(group.open_input().is_none());
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn stopping_during_handshake_does_not_wait_for_remote_flush() {
        for once in [false, true] {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", http.local_addr().unwrap());
            let socket = format!("ws://{}", ws.local_addr().unwrap());
            let endpoints = Endpoints {
                gamma: &base,
                rest: &base,
                socket: &socket,
            };
            let output = Output::new(io::sink(), io::sink());
            let client = Client::builder().no_proxy().build().unwrap();
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let mut control = WatchControl {
                once,
                output: &output,
                stop: Box::pin(async {
                    stopped.await?;
                    ensure!(!once, "monitoring cancelled");
                    Ok(())
                }),
            };
            let server = async {
                let (request, _) = http_request(&http).await;
                http_response(request, GAMMA_FIXTURE).await;
                let (pending_handshake, _) = ws.accept().await.unwrap();
                stop.send(()).unwrap();
                pending_handshake
            };
            let (_held_socket, result) = timeout(Duration::from_secs(3), async {
                tokio::join!(
                    server,
                    watch_loop(&client, &endpoints, "fixture", LIVE_TIMING, &mut control)
                )
            })
            .await
            .unwrap();
            if once {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("monitoring cancelled")
                );
            } else {
                result.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn selected_gamma_closed_yields_to_stop_before_commit() {
        for active in [false, true] {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", http.local_addr().unwrap());
            let socket = format!("ws://{}", ws.local_addr().unwrap());
            let endpoints = Endpoints {
                gamma: &base,
                rest: &base,
                socket: &socket,
            };
            let client = Client::builder().no_proxy().build().unwrap();
            let recorded = Recorded::default();
            let output = Output::new(recorded.clone(), io::sink());
            let (stop, stopped) = tokio::sync::oneshot::channel();
            STOP_BEFORE_CLOSED.replace(Some(stop));
            let mut control = WatchControl {
                once: false,
                output: &output,
                stop: Box::pin(async {
                    stopped.await?;
                    Ok(())
                }),
            };
            let server = async {
                if active {
                    let (socket, _) = http_request(&http).await;
                    http_response(socket, GAMMA_FIXTURE).await;
                    let (socket, _) = ws.accept().await.unwrap();
                    let mut conn = accept_async(socket).await.unwrap();
                    assert!(conn.next().await.unwrap().unwrap().is_text());
                    let (socket, _) = http_request(&http).await;
                    http_response(socket, CLOSED).await;
                    let _ = conn.next().await;
                } else {
                    let (socket, _) = http_request(&http).await;
                    http_response(socket, CLOSED).await;
                }
            };
            let timing = WatchTiming {
                gamma: Duration::from_millis(200),
                ..LIVE_TIMING
            };
            timeout(Duration::from_secs(4), async {
                let (_, result) = tokio::join!(
                    server,
                    watch_loop(&client, &endpoints, "fixture", timing, &mut control)
                );
                result.unwrap();
            })
            .await
            .unwrap();
            assert!(STOP_BEFORE_CLOSED.take().is_none());
            assert!(!String::from_utf8_lossy(&recorded.0.lock().unwrap()).contains("事件已結束"));
        }
    }

    #[tokio::test]
    async fn retirement_capacity_error_takes_precedence_over_gamma_closed() {
        let names: Vec<_> = (0..=crate::book::MAX_SETTLEMENTS)
            .map(|n| {
                (
                    format!("c{n}"),
                    (2 * n + 1).to_string(),
                    (2 * n + 2).to_string(),
                )
            })
            .collect();
        let rows: Vec<_> = names
            .iter()
            .map(|(id, y, n)| {
                (
                    id.as_str(),
                    y.as_str(),
                    n.as_str(),
                    rust_decimal::Decimal::new(1, 2),
                    false,
                )
            })
            .collect();
        let mut group = NegRiskEventGroup::new("fixture");
        let outcome = Ok(Discovery::Open(gamma_discovery::fixture_spec(&rows)));
        group.update_discovery(outcome).unwrap();
        let source = group.begin().unwrap();
        for (condition, _, _) in &names[..crate::book::MAX_SETTLEMENTS] {
            group
                .apply_received(
                    source,
                    wire::Event::MarketResolved {
                        market: condition.clone(),
                    },
                    Instant::now(),
                )
                .unwrap();
        }
        let closed = Ok(Discovery::Closed);
        assert!(group.observe_discovery(closed));
        let text =
            json!({"event_type":"market_resolved","market":names.last().unwrap().0}).to_string();
        let (transport, _) = Transport::buffered_for_test([text]);
        let output = Output::new(io::sink(), io::sink());
        let mut control = WatchControl {
            once: false,
            output: &output,
            stop: Box::pin(future::ready(Ok(()))),
        };
        let err = finish_subscription(transport, Cutoff::Gamma(false), &mut group, &mut control)
            .await
            .err()
            .unwrap();
        assert!(err.is::<crate::book::SettlementCapacity>());
        assert!(!group.event_closed());
        assert!(group.begin().is_none());
    }

    async fn queued_transport(message: &'static str, disconnect: bool) -> Transport {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::Text(message.to_owned())).await.unwrap();
            if disconnect {
                ws.close(None).await.unwrap();
            } else {
                let _ = ws.next().await;
            }
        });
        let (transport, started) = Transport::start(
            &address,
            Subscription::for_test(Instant::now() + Duration::from_secs(10)),
            HEARTBEAT_TIMING,
        )
        .unwrap();
        started.await.unwrap();
        timeout(Duration::from_secs(3), async {
            while transport.frames.is_empty() || (disconnect && !transport.frames.is_closed()) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        transport
    }

    #[tokio::test]
    async fn admitted_settlement_survives_disconnect_isolation_and_gamma_rebind() {
        for cutoff in [
            Cutoff::Closed(None),
            Cutoff::RecoverIsolated,
            Cutoff::Gamma(false),
        ] {
            let disconnect = matches!(cutoff, Cutoff::Closed(_));
            let message = r#"[{"event_type":"book","bids":"malformed"},
                {"event_type":"market_resolved","market":"a"}]"#;
            let transport = queued_transport(message, disconnect).await;
            let mut group = cutoff_group();
            let output = Output::new(io::sink(), io::sink());
            if matches!(cutoff, Cutoff::Gamma(_)) {
                let outcome = cutoff_update(&["b", "c"]);
                assert!(group.observe_discovery(outcome));
            }
            let result =
                finish_subscription(transport, cutoff, &mut group, &mut test_control(&output))
                    .await;
            assert_eq!(result.is_err(), disconnect);
            let outcome = cutoff_update(&["a", "b", "c"]);
            assert!(group.update_discovery(outcome).unwrap());
            assert_eq!(
                group
                    .subscription()
                    .map(|value| value.into_parts().0)
                    .unwrap_or_default(),
                ["3", "4", "5", "6"]
            );
        }
    }

    #[tokio::test]
    async fn once_cuts_fifo_after_admitted_settlement_or_price_update() {
        for (message, ready) in [
            (r#"{"event_type":"market_resolved","market":"a"}"#, false),
            (
                r#"{"event_type":"price_change","market":"a","timestamp":"101","price_changes":
              [{"asset_id":"1","price":"0.43","size":"5","side":"BUY"}]}"#,
                true,
            ),
        ] {
            let mut group = cutoff_group();
            let transport = queued_transport(message, false).await;
            let recorded = Recorded::default();
            let output = Output::new(recorded.clone(), io::sink());
            let source = group.test_source();
            let result = finish_subscription(
                transport,
                Cutoff::Once(source),
                &mut group,
                &mut test_control(&output),
            )
            .await;
            if ready {
                assert!(
                    matches!(result.unwrap(), SessionEnd::Finished(Ok(())))
                        && String::from_utf8_lossy(&recorded.0.lock().unwrap())
                            .contains("bid 0.43")
                        && !group.ready()
                );
            } else {
                assert!(result.err().unwrap().to_string().contains("closed market"));
            }
        }
    }

    #[tokio::test]
    async fn overflow_before_worker_cleanup_cannot_seal_as_ready() {
        for (count, bytes, cause) in [(64, 2, "queue full"), (2, 8 * 1024 * 1024, "byte limit")] {
            let messages = (0..count)
                .map(|_| {
                    let mut message = " ".repeat(bytes - 2);
                    message.push_str("[]");
                    message
                })
                .chain(std::iter::once("[]".to_owned()));
            let (transport, sender) = Transport::buffered_for_test(messages);
            assert_eq!(transport.frames.len(), count);
            assert!(!transport.frames.is_closed());
            let mut group = cutoff_group();
            assert!(group.ready());
            let output = Output::new(io::sink(), io::sink());
            let source = group.test_source();
            let result = finish_subscription(
                transport,
                Cutoff::Once(source),
                &mut group,
                &mut test_control(&output),
            )
            .await;
            assert!(sender.is_closed());
            assert!(result.err().unwrap().to_string().contains(cause));
            assert!(!group.ready());
        }
    }

    #[tokio::test]
    async fn competing_rebind_and_isolation_preserve_fault_and_settlement() {
        let settled = r#"{"event_type":"market_resolved","market":"a"}"#;
        for (cutoff, expected_assets) in [
            (Cutoff::RecoverIsolated, vec!["3", "4"]),
            (Cutoff::Gamma(false), vec!["3", "4", "5", "6"]),
        ] {
            let (transport, _sender) = Transport::buffered_for_test(
                std::iter::once(settled.to_owned()).chain(std::iter::repeat_n("[]".to_owned(), 64)),
            );
            let mut group = cutoff_group();
            let output = Output::new(io::sink(), io::sink());
            if matches!(cutoff, Cutoff::Gamma(_)) {
                let outcome = cutoff_update(&["b", "c"]);
                assert!(group.observe_discovery(outcome));
            }
            let result =
                finish_subscription(transport, cutoff, &mut group, &mut test_control(&output))
                    .await;
            assert!(result.err().unwrap().to_string().contains("queue full"));
            assert_eq!(
                group
                    .subscription()
                    .map(|value| value.into_parts().0)
                    .unwrap_or_default(),
                expected_assets
            );
            let outcome = cutoff_update(&["a", "b", "c"]);
            assert!(group.update_discovery(outcome).unwrap());
            assert_eq!(
                group
                    .subscription()
                    .map(|value| value.into_parts().0)
                    .unwrap_or_default(),
                ["3", "4", "5", "6"]
            );
        }
    }

    struct BlockedOutput {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }

    struct FrameRecoveryWriter {
        gate: BlockedOutput,
        views: std::sync::mpsc::Sender<String>,
    }

    impl Write for FrameRecoveryWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.gate.write(bytes)?;
            let _ = self.views.send(String::from_utf8(bytes.to_vec()).unwrap());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn gate_frame_recovery_output() -> (Output, tokio::sync::oneshot::Receiver<()>) {
        let (entered, blocked) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (views, written) = std::sync::mpsc::channel::<String>();
        let previous = BEFORE_CURRENT_FRAME_RECOVERY.replace(Some(Box::new(move || {
            release.send(()).unwrap();
            assert_eq!(
                written.recv_timeout(Duration::from_secs(2)).unwrap(),
                "writer occupied"
            );
            let next = written.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(next.contains("| NotReady ("), "{next}");
            assert!(!next.contains("[已關閉]"), "{next}");
        })));
        assert!(previous.is_none());
        let output = Output::new(
            FrameRecoveryWriter {
                gate: BlockedOutput {
                    entered: Some(entered),
                    release: wait,
                },
                views,
            },
            io::sink(),
        );
        output.view("writer occupied".to_owned());
        (output, blocked)
    }

    const CROSSED_FRAME: &str = r#"[
        {"event_type":"price_change","market":"a","timestamp":"101","price_changes":[
            {"asset_id":"1","price":"0.61","size":"3","side":"BUY"}]},
        {"event_type":"market_resolved","market":"a"}
    ]"#;
    const UNDECODABLE_FRAME: &str = r#"[
        {"event_type":"book","asset_id":"1","market":"a","timestamp":"101","bids":"bad","asks":[]},
        {"event_type":"market_resolved","market":"a"}
    ]"#;

    #[tokio::test]
    async fn watch_withdraws_pending_ready_before_recovering_a_failed_batch() {
        for message in [CROSSED_FRAME, UNDECODABLE_FRAME] {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", http.local_addr().unwrap());
            let rest_url = format!("http://{}", rest.local_addr().unwrap());
            let url = format!("ws://{}", ws.local_addr().unwrap());
            let endpoints = Endpoints {
                gamma: &base,
                rest: &rest_url,
                socket: &url,
            };
            let client = Client::builder().no_proxy().build().unwrap();
            let (output, blocked) = gate_frame_recovery_output();
            let server = async {
                blocked.await.unwrap();
                let (socket, _) = http_request(&http).await;
                http_response(socket, GAMMA_FIXTURE).await;
                let (socket, _) = ws.accept().await.unwrap();
                let mut conn = accept_async(socket).await.unwrap();
                conn.next().await.unwrap().unwrap();
                conn.send(books(&[("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")]))
                    .await
                    .unwrap();
                // An audit starts only after the snapshot iteration has offered Ready.
                let (_audit, request) = http_request(&rest).await;
                assert!(request.starts_with("GET /book?"));
                conn.send(Message::Text(message.to_owned())).await.unwrap();
                let (socket, _) = ws.accept().await.unwrap();
                let mut conn = accept_async(socket).await.unwrap();
                let subscription = conn.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&subscription).unwrap()["assets_ids"],
                    json!(["3", "4"])
                );
            };
            timeout(Duration::from_secs(5), async {
                tokio::select! {
                    () = server => {}
                    result = run_monitor(&client, &endpoints, "fixture", false, LIVE_TIMING, &output) => {
                        panic!("monitor exited before reconnect: {result:?}");
                    }
                }
            }).await.unwrap();
            assert!(BEFORE_CURRENT_FRAME_RECOVERY.take().is_none());
        }
    }

    #[tokio::test]
    async fn once_replay_fault_retires_before_recovering_current_and_later_settlements() {
        for (message, error) in [
            (CROSSED_FRAME, "crossed or locked book"),
            (UNDECODABLE_FRAME, "invalid WebSocket message"),
        ] {
            let (transport, _sender) = Transport::buffered_for_test([
                message.to_owned(),
                r#"{"event_type":"market_resolved","market":"b"}"#.to_owned(),
            ]);
            let mut group = cutoff_group();
            let (output, blocked) = gate_frame_recovery_output();
            timeout(Duration::from_secs(3), blocked)
                .await
                .unwrap()
                .unwrap();
            let source = group.test_source();
            let result = finish_subscription(
                transport,
                Cutoff::Once(source),
                &mut group,
                &mut test_control(&output),
            )
            .await;
            assert!(result.err().unwrap().to_string().contains(error));
            assert!(
                group
                    .subscription()
                    .map(|value| value.into_parts().0)
                    .unwrap_or_default()
                    .is_empty()
            );
            assert!(BEFORE_CURRENT_FRAME_RECOVERY.take().is_none());
        }
    }

    impl Write for BlockedOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
                let _ = self.release.recv();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn retire_replaces_pending_ready_before_scanning_a_near_full_tail() {
        const BIG_EVENT: &str = r#"{"event_type":"unhandled"},"#;
        const BAD_AND_SETTLED: &str = r#"{"event_type":"book","bids":"bad"},
            {"event_type":"market_resolved","market":"b"}"#;
        const MESSAGE_BYTES: usize = 8 * 1024 * 1024;
        for worker_failed in [false, true] {
            let messages = ["{}", BAD_AND_SETTLED].map(|tail| {
                let count = (MESSAGE_BYTES - tail.len() - 2) / BIG_EVENT.len();
                format!("[{}{tail}]", BIG_EVENT.repeat(count))
            });
            let (transport, _sender) = Transport::buffered_for_test(
                messages
                    .into_iter()
                    .chain(worker_failed.then(|| "[{}]".repeat(64))),
            );
            assert_eq!(transport.frames.len(), 2);
            let (entered, blocked) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let recorded = Recorded::default();
            let output = Output::new(
                RecordingGate {
                    gate: BlockedOutput {
                        entered: Some(entered),
                        release: wait,
                    },
                    recorded: recorded.clone(),
                },
                io::sink(),
            );
            let cutoff = if worker_failed {
                Cutoff::Closed(None)
            } else {
                Cutoff::Gamma(false)
            };
            timeout(Duration::from_secs(5), tokio::spawn(async move {
                let mut group = prepare_cutoff_group(NegRiskEventGroup::with_market_consumer("fixture", crate::paper::Quantity::new(25.into()).unwrap(), |_,_| {}));
                output.view("writer occupied\n".to_owned());
                blocked.await.unwrap();
                publish_current(&group, &output);

                if matches!(cutoff, Cutoff::Gamma(_)) { let outcome = cutoff_update(&["b", "c"]); assert!(group.observe_discovery(outcome)); }
                let mut control = test_control(&output);
                let retirement = finish_subscription(transport, cutoff, &mut group, &mut control);
                tokio::pin!(retirement);
                // Force the first FIFO read to yield, leaving publication observable.
                while tokio::task::coop::has_budget_remaining() {
                    tokio::task::coop::consume_budget().await;
                }
                let start = Instant::now();
                let suspended = futures_util::poll!(&mut retirement).is_pending();
                let submitted = start.elapsed();
                release.send(()).unwrap();
                assert!(suspended);
                recorded.wait_for("| NotReady (").await;
                let before = recorded.0.lock().unwrap().clone();
                let before = std::str::from_utf8(&before).unwrap();
                assert!(!before.contains("| Ready ("));
                assert!(!before.contains("b [已關閉]"));

                let start = Instant::now();
                assert_eq!(retirement.await.is_err(), worker_failed);
                let drained = start.elapsed();
                recorded.wait_for("b [已關閉]").await;
                eprintln!("tail (worker_failed={worker_failed}): submit NotReady {submitted:?}, drain and submit {drained:?}");
            })).await.unwrap().unwrap();
        }
    }

    struct RecordingGate {
        gate: BlockedOutput,
        recorded: Recorded,
    }

    impl Write for RecordingGate {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.gate.write(bytes)?;
            self.recorded.write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.recorded.flush()
        }
    }

    #[tokio::test]
    async fn blocked_terminal_does_not_stop_watch_heartbeats_or_timeout() {
        for stop_normally in [true, false] {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", http.local_addr().unwrap());
            let url = format!("ws://{}", ws.local_addr().unwrap());
            let endpoints = Endpoints {
                gamma: &base,
                rest: &base,
                socket: &url,
            };
            let client = Client::builder().no_proxy().build().unwrap();
            let (entered, blocked) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let output = Output::new(
                BlockedOutput {
                    entered: Some(entered),
                    release: wait,
                },
                io::sink(),
            );
            let (signal, stopped) = tokio::sync::oneshot::channel();
            let mut control = WatchControl {
                once: !stop_normally,
                output: &output,
                stop: Box::pin(async {
                    if !stop_normally {
                        return future::pending().await;
                    }
                    stopped.await?;
                    Ok(())
                }),
            };
            let server = async {
                let (socket, _) = http_request(&http).await;
                http_response(socket, GAMMA_FIXTURE).await;
                let (socket, _) = ws.accept().await.unwrap();
                let mut websocket = accept_async(socket).await.unwrap();
                assert!(websocket.next().await.unwrap().unwrap().is_text());
                blocked.await.unwrap();
                for _ in 0..3 {
                    assert_eq!(
                        websocket.next().await.unwrap().unwrap(),
                        Message::Text("PING".to_owned())
                    );
                    websocket
                        .send(Message::Text("PONG".to_owned()))
                        .await
                        .unwrap();
                }
                if stop_normally {
                    websocket
                        .send(books(&[("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")]))
                        .await
                        .unwrap();
                    let mut audited = false;
                    let mut refreshed = false;
                    let discovery_and_audit = async {
                        while !audited || !refreshed {
                            let (request, line) = http_request(&http).await;
                            if line.starts_with("GET /events/") {
                                http_response(request, GAMMA_FIXTURE).await;
                                refreshed = true;
                            } else {
                                audited |= line.starts_with("GET /book?token_id=3 ");
                                http_response_with_status(request, "404 Not Found", "").await;
                            }
                        }
                    };
                    tokio::pin!(discovery_and_audit);
                    loop {
                        tokio::select! {
                            () = &mut discovery_and_audit => break,
                            message = websocket.next() => {
                                assert_eq!(message.unwrap().unwrap(), Message::Text("PING".into()));
                                websocket.send(Message::Text("PONG".into())).await.unwrap();
                            }
                            _ = ws.accept() => panic!("healthy subscription was replaced"),
                        }
                    }
                    signal.send(()).unwrap();
                }
                while let Some(Ok(_)) = websocket.next().await {}
            };
            let result = timeout(Duration::from_secs(4), async {
                tokio::join!(
                    server,
                    watch_loop(
                        &client,
                        &endpoints,
                        "fixture",
                        WatchTiming {
                            gamma: Duration::from_millis(600),
                            snapshots: Duration::from_secs(3),
                            heartbeat: HeartbeatTiming {
                                interval: Duration::from_millis(45),
                                timeout: Duration::from_millis(200)
                            },
                            ..LIVE_TIMING
                        },
                        &mut control
                    )
                )
                .1
            })
            .await;
            let _ = release.send(());
            if stop_normally {
                result.unwrap().unwrap();
            } else {
                assert!(
                    result
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("PONG timed out")
                );
            }
        }
    }

    #[test]
    fn isolation_backoff_caps_at_sixty_seconds() {
        let mut recovery = Recovery {
            initial: RECOVERY_INITIAL,
            delay: RECOVERY_INITIAL,
            state: RecoveryState::Idle,
        };
        for seconds in [15, 30, 60, 60] {
            assert_eq!(recovery.delay, Duration::from_secs(seconds));
            recovery.reconnect();
        }
    }

    struct ReadyGate {
        gate: BlockedOutput,
        fail: bool,
    }

    impl Write for ReadyGate {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if String::from_utf8_lossy(bytes).contains("| Ready (") {
                self.gate.write(bytes)?;
                if self.fail {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "test pipe closed",
                    ));
                }
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn once_observes_final_ready_output_success_and_failure() {
        for (fail, cancel) in [(false, false), (true, false), (false, true)] {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", http.local_addr().unwrap());
            let url = format!("ws://{}", ws.local_addr().unwrap());
            let endpoints = Endpoints {
                gamma: &base,
                rest: &base,
                socket: &url,
            };
            let client = Client::builder().no_proxy().build().unwrap();
            let (entered, blocked) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let output = Output::new(
                ReadyGate {
                    gate: BlockedOutput {
                        entered: Some(entered),
                        release: wait,
                    },
                    fail,
                },
                io::sink(),
            );
            let server = async {
                let (socket, _) = http_request(&http).await;
                http_response(socket, GAMMA_FIXTURE).await;
                let (socket, _) = ws.accept().await.unwrap();
                let mut websocket = accept_async(socket).await.unwrap();
                assert!(websocket.next().await.unwrap().unwrap().is_text());
                websocket
                    .send(books(&[("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")]))
                    .await
                    .unwrap();
                let _ = websocket.next().await;
            };
            let check = async {
                let (signal, stopped) = tokio::sync::oneshot::channel();
                let mut control = WatchControl {
                    once: true,
                    output: &output,
                    stop: Box::pin(async {
                        stopped.await?;
                        bail!("monitoring cancelled")
                    }),
                };
                let monitor = watch_loop(&client, &endpoints, "fixture", LIVE_TIMING, &mut control);
                tokio::pin!(monitor);
                tokio::select! {
                    value = blocked => value.unwrap(),
                    result = &mut monitor => panic!("monitor completed before the Ready write: {result:?}"),
                }
                assert!(futures_util::poll!(&mut monitor).is_pending());
                if cancel {
                    signal.send(()).unwrap();
                    assert!(
                        monitor
                            .await
                            .unwrap_err()
                            .to_string()
                            .contains("monitoring cancelled")
                    );
                    release.send(()).unwrap();
                    return;
                }
                release.send(()).unwrap();
                let result = monitor.await;
                assert_eq!(result.is_err(), fail);
            };
            timeout(Duration::from_secs(4), async {
                tokio::join!(server, check);
            })
            .await
            .unwrap();
        }
    }

    struct TerminalFlush {
        gate: BlockedOutput,
        finishing: bool,
    }

    impl Write for TerminalFlush {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.finishing = String::from_utf8_lossy(bytes).contains("事件已結束");
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.finishing {
                self.gate.write(&[])?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn active_closed_finishes_drain_and_flush_across_cancellation() {
        for stop_during_drain in [true, false] {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", http.local_addr().unwrap());
            let socket = format!("ws://{}", ws.local_addr().unwrap());
            let endpoints = Endpoints {
                gamma: &base,
                rest: &base,
                socket: &socket,
            };
            let client = Client::builder().no_proxy().build().unwrap();
            let (entered, blocked) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let output = Output::new(
                TerminalFlush {
                    gate: BlockedOutput {
                        entered: Some(entered),
                        release: wait,
                    },
                    finishing: false,
                },
                io::sink(),
            );
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let mut stop = Some(stop);
            if stop_during_drain {
                STOP_AT_DRAIN.replace(stop.take());
            }
            let mut control = WatchControl {
                once: false,
                output: &output,
                stop: Box::pin(async {
                    stopped.await?;
                    Ok(())
                }),
            };
            let server = async {
                let (request, _) = http_request(&http).await;
                http_response(request, GAMMA_FIXTURE).await;
                let (connection, _) = ws.accept().await.unwrap();
                let mut websocket = accept_async(connection).await.unwrap();
                assert!(websocket.next().await.unwrap().unwrap().is_text());
                let (request, _) = http_request(&http).await;
                http_response(request, CLOSED).await;
                let _ = websocket.next().await;
            };
            let check = async {
                let watch = watch_loop(
                    &client,
                    &endpoints,
                    "fixture",
                    WatchTiming {
                        gamma: Duration::from_millis(200),
                        ..LIVE_TIMING
                    },
                    &mut control,
                );
                tokio::pin!(watch);
                tokio::select! {
                    entered = blocked => entered.unwrap(),
                    result = &mut watch => panic!("returned before terminal flush: {result:?}"),
                }
                if let Some(stop) = stop {
                    stop.send(()).unwrap();
                }
                assert!(futures_util::poll!(&mut watch).is_pending());
                release.send(()).unwrap();
                watch.await.unwrap();
                assert!(STOP_AT_DRAIN.take().is_none());
            };
            timeout(Duration::from_secs(4), async {
                tokio::join!(server, check);
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn closed_event_waits_for_final_flush_and_errors_if_it_cannot_confirm() {
        for expires in [false, true] {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", http.local_addr().unwrap());
            let endpoints = Endpoints {
                gamma: &base,
                rest: &base,
                socket: "",
            };
            let client = Client::builder().no_proxy().build().unwrap();
            let (entered, blocked) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let output = Output::new(
                TerminalFlush {
                    gate: BlockedOutput {
                        entered: Some(entered),
                        release: wait,
                    },
                    finishing: false,
                },
                io::sink(),
            );
            let server = async {
                let (socket, _) = http_request(&http).await;
                http_response(socket, CLOSED).await;
            };
            let check = async {
                let monitor =
                    run_monitor(&client, &endpoints, "fixture", false, LIVE_TIMING, &output);
                tokio::pin!(monitor);
                tokio::select! {
                    value = blocked => value.unwrap(),
                    result = &mut monitor => panic!("monitor exited before terminal flush: {result:?}"),
                }
                assert!(futures_util::poll!(&mut monitor).is_pending());
                if expires {
                    let result = monitor.await;
                    release.send(()).unwrap();
                    assert!(result.unwrap_err().to_string().contains("timed out"));
                } else {
                    release.send(()).unwrap();
                    monitor.await.unwrap();
                }
            };
            timeout(Duration::from_secs(8), async {
                tokio::join!(server, check)
            })
            .await
            .unwrap();
        }
    }
}
