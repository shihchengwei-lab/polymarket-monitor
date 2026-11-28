use super::{Input, NegRiskEventGroup, SNAPSHOT_TIMEOUT, SubscriptionId, after_cleanup};
use crate::{
    gamma_discovery::Discovery,
    transport::{Frame, Payload, SealedFrames, Transport},
    wire::{self, Event},
};
use anyhow::{Context, Result, bail};
use tokio::{sync::mpsc, time::Instant};

pub(super) struct LocalInput {
    transport: Transport,
    sender: mpsc::Sender<Frame>,
}

#[derive(Debug)]
pub(crate) enum FrameFailure {
    Rejected { error: anyhow::Error, frame: Frame },
    Deadline { at: Instant, pending: Option<Frame> },
}

impl Frame {
    fn settlements(self, group: &mut NegRiskEventGroup) -> Result<(), super::SettlementCapacity> {
        match self.payload {
            Payload::Raw(text) => {
                let mut result = Ok(());
                wire::for_each_resolution(&text, |condition| {
                    if result.is_ok() {
                        result = group.resolve(condition);
                    }
                });
                result?;
            }
            Payload::Typed(Event::MarketResolved { market }) => group.resolve(&market)?,
            Payload::Typed(_) => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum End {
    Handoff,
    Reconnect,
    Replace,
    Stop,
    Once,
}

#[derive(Clone, Copy)]
enum Prefix {
    Executable(SubscriptionId),
    Settlements(Instant),
}

pub(crate) struct Retirement<'a> {
    group: &'a mut NegRiskEventGroup,
    first: Option<Frame>,
    frames: SealedFrames,
    prefix: Prefix,
    deadline: Option<Instant>,
    end: End,
    fault: Option<anyhow::Error>,
    transport: Result<()>,
}

impl<'a> Retirement<'a> {
    pub(crate) fn new(
        group: &'a mut NegRiskEventGroup,
        first: Option<Frame>,
        sealed: (SealedFrames, Result<()>),
        end: End,
    ) -> Self {
        let (frames, transport) = sealed;
        let prefix = match group.source() {
            Some(source) => {
                group.deliver(frames.cutoff, super::MarketInput::Sealed);
                Prefix::Executable(source)
            }
            None => Prefix::Settlements(frames.cutoff),
        };
        let deadline = group.expired_reconciliation();
        Self {
            group,
            first,
            frames,
            prefix,
            deadline,
            end,
            fault: None,
            transport,
        }
    }

    pub(crate) fn closing_event(&self) -> bool {
        matches!(self.group.next_membership, Some(Discovery::Closed))
    }

    pub(crate) fn stop(&mut self) {
        self.end = End::Stop;
    }

    fn reject_prefix(&mut self, at: Instant, error: anyhow::Error) {
        self.prefix = Prefix::Settlements(at);
        self.fault = Some(error);
    }
    pub(crate) fn failed(&mut self, failed: FrameFailure) {
        match failed {
            FrameFailure::Rejected { error, frame } => {
                self.reject_prefix(frame.received_at, error);
                self.first = Some(frame);
            }
            FrameFailure::Deadline { at, pending } => {
                if at > self.frames.cutoff {
                    // A subscription already sealed before this timer matured.
                    debug_assert!(pending.is_none());
                    return;
                }
                self.deadline = Some(self.deadline.map_or(at, |d| d.min(at)));
                self.first = pending;
                self.fault = Some(anyhow::anyhow!("crossed book reconciliation timed out"));
            }
        }
        #[cfg(test)]
        crate::monitor::during_frame_recovery();
    }

    pub(crate) fn advance(&mut self) -> bool {
        let Some(tail) = self.first.take().or_else(|| self.frames.next()) else {
            return false;
        };
        match (self.prefix, tail) {
            (Prefix::Executable(source), frame) => {
                if let Some(at) = self.deadline.filter(|at| frame.received_at >= *at) {
                    self.reject_prefix(
                        at,
                        anyhow::anyhow!("crossed book reconciliation timed out"),
                    );
                    self.first = Some(frame);
                } else if let Err(failure) = apply_frame(source, self.group, frame) {
                    self.failed(*failure);
                }
            }
            (Prefix::Settlements(cut), remainder) => {
                let admitted = remainder.received_at;
                if let Err(error) = remainder.settlements(self.group) {
                    self.prefix = Prefix::Settlements(cut.min(admitted));
                    // The scan commits the capacity limit predicted during frame
                    // validation. Only an independent prefix fault is additional.
                    let earlier = self
                        .fault
                        .take()
                        .filter(|e| !e.is::<super::SettlementCapacity>());
                    self.fault =
                        after_cleanup::<()>(Err(error.into()), earlier.map_or(Ok(()), Err)).err();
                    return false;
                }
            }
        }
        true
    }

    pub(crate) fn complete(mut self) -> Result<Option<super::view::BookView>> {
        self.group.expire_snapshots();
        if self.deadline.is_some_and(|d| d <= self.frames.cutoff) && self.fault.is_none() {
            self.fault = Some(anyhow::anyhow!("crossed book reconciliation timed out"));
        }
        let ready = if matches!(self.end, End::Once) && self.fault.is_none() {
            if self.group.quotes_ready() {
                Some(self.group.view())
            } else {
                self.fault = Some(anyhow::anyhow!(
                    "{}",
                    self.group
                        .once_failure()
                        .unwrap_or("market data was not ready at the subscription cutoff")
                ));
                None
            }
        } else {
            None
        };
        let prefix_cut = match self.prefix {
            Prefix::Executable(_) => self.frames.cutoff,
            Prefix::Settlements(at) => at.min(self.frames.cutoff),
        };
        let at = self.deadline.map_or(prefix_cut, |due| due.min(prefix_cut));
        let end_session = matches!(self.end, End::Stop)
            || (matches!(self.end, End::Handoff)
                && (self.fault.is_some() || self.transport.is_err()))
            || self.group.check_capacity().is_err()
            || self.closing_event();
        self.group.retire_at(at, end_session);
        let result = if self.group.check_capacity().is_err() {
            // The scan that latched capacity already carries the typed error.
            Err(self
                .fault
                .unwrap_or_else(|| super::SettlementCapacity.into()))
        } else {
            self.group.install_discovery();
            if matches!(self.end, End::Replace) && !end_session && self.transport.is_ok() {
                Ok(ready)
            } else {
                self.fault.map_or(Ok(ready), Err)
            }
        };
        after_cleanup(result, self.transport)
    }
}

pub(crate) fn apply_frame(
    source: SubscriptionId,
    group: &mut NegRiskEventGroup,
    mut frame: Frame,
) -> Result<(), Box<FrameFailure>> {
    if group.source() != Some(source) {
        return Ok(());
    }
    let at = frame.received_at;
    if let Some(d) = group.reconciliation_deadline().filter(|d| at >= *d) {
        return Err(Box::new(FrameFailure::Deadline {
            at: d,
            pending: Some(frame),
        }));
    }
    let events = match &mut frame.payload {
        Payload::Raw(text) => wire::events(text).context("invalid WebSocket message"),
        Payload::Typed(Event::MarketResolved { market }) => Ok(vec![Event::MarketResolved {
            market: market.clone(),
        }]),
        Payload::Typed(event) => Ok(vec![std::mem::replace(event, Event::Other)]),
    };
    match events.and_then(|events| group.commit_frame(source, events, at)) {
        Ok(None) => Ok(()),
        Ok(Some(d)) => Err(Box::new(FrameFailure::Deadline {
            at: d,
            pending: (at >= d).then_some(frame),
        })),
        Err(error) => Err(Box::new(FrameFailure::Rejected { frame, error })),
    }
}

impl NegRiskEventGroup {
    pub(super) fn command_frontier(&self) -> Option<Instant> {
        let at = match &self.input {
            Input::Local(_, local) => local.transport.idle_frontier()?,
            Input::Subscribed(_) => Instant::now(),
            _ => return None,
        };
        (self.ready() && self.approval_is_current(at)).then_some(at)
    }

    /// Install an approved roster only after accounting for the previous input.
    pub(crate) fn update_discovery(&mut self, outcome: Result<Discovery>) -> Result<bool> {
        self.update_discovery_at(Instant::now(), outcome)
    }

    pub(crate) fn update_discovery_at(
        &mut self,
        accepted_at: Instant,
        outcome: Result<Discovery>,
    ) -> Result<bool> {
        if !self.observe_discovery(outcome) {
            return Ok(false);
        }
        if matches!(self.input, Input::Local(_, _) | Input::Retiring(_)) {
            self.finish_input(None)?;
        } else {
            let closing = matches!(self.next_membership, Some(Discovery::Closed));
            self.retire_at(accepted_at, closing);
            self.install_discovery();
        }
        Ok(true)
    }

    /// Start one bounded local input using the current Gamma approval.
    pub fn open_input(&mut self) -> Option<SubscriptionId> {
        if !matches!(self.input, Input::Idle) {
            return None;
        }
        let now = Instant::now();
        let source = self.begin_at(now, now + SNAPSHOT_TIMEOUT)?;
        let (transport, sender) = Transport::local();
        self.input = Input::Local(source, LocalInput { transport, sender });
        Some(source)
    }

    /// False means the supplied subscription is no longer current.
    pub fn admit_raw(&mut self, source: SubscriptionId, text: String) -> Result<bool> {
        self.admit(source, Payload::Raw(text))
    }

    pub fn admit_event(&mut self, source: SubscriptionId, event: Event) -> Result<bool> {
        self.admit(source, Payload::Typed(event))
    }

    fn admit(&mut self, source: SubscriptionId, payload: Payload) -> Result<bool> {
        if !matches!(self.input,Input::Local(id,_) if id==source) {
            return Ok(false);
        }
        let Input::Local(_, input) = &self.input else {
            return Ok(false);
        };
        if input.transport.enqueue(&input.sender, payload) {
            return Ok(true);
        }
        self.finish_input(None)?;
        bail!("market input closed")
    }

    /// Consume one accepted frame. Its admission time drives quote freshness.
    pub fn advance_input(&mut self, source: SubscriptionId) -> Result<bool> {
        if !matches!(self.input,Input::Local(id,_) if id==source) {
            return Ok(false);
        }
        self.expire_snapshots();
        if self.expired_reconciliation().is_some() {
            return self.finish_input(None).map(|()| false);
        }
        let commands = self.command_count();
        let Input::Local(_, input) = &mut self.input else {
            return Ok(false);
        };
        let Ok(frame) = input.transport.frames.try_recv() else {
            let at = input.transport.idle_frontier();
            if !self.deliver_idle_clock(commands, at) {
                self.finish_input(None)?;
            }
            return Ok(false);
        };
        if let Err(failed) = apply_frame(source, self, frame) {
            return self.finish_input(Some(*failed)).map(|_| false);
        }
        Ok(true)
    }

    pub fn close_input(&mut self, source: SubscriptionId) -> Result<bool> {
        if !matches!(self.input,Input::Local(id,_) if id==source) {
            return Ok(false);
        }
        self.finish_input(None)?;
        Ok(true)
    }

    pub(crate) fn local_retirement(&mut self, end: End) -> Option<Retirement<'_>> {
        if !matches!(self.input, Input::Local(..) | Input::Retiring(_)) {
            return None;
        }
        let input = match std::mem::replace(&mut self.input, Input::Idle) {
            Input::Local(id, input) => {
                self.input = Input::Subscribed(id);
                input
            }
            Input::Retiring(input) => input,
            _ => unreachable!("local input"),
        };
        Some(Retirement::new(self, None, input.transport.seal(), end))
    }

    fn finish_input(&mut self, failure: Option<FrameFailure>) -> Result<()> {
        let mut retirement = self.local_retirement(End::Reconnect).expect("local input");
        if let Some(failed) = failure {
            retirement.failed(failed);
        }
        while retirement.advance() {}
        retirement.complete().map(|_| ())
    }
}
