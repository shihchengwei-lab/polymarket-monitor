//! A live group serializes book commits and paper commands on the same mutable owner.
use super::{Instant, NegRiskEventGroup};
use crate::paper::{
    AccountId, AssetId, Cash, Error, OrderKey, Stamp,
    chaos_exchange::{Balance, ChaosExchange, Receipt},
    hybrid_coordinator::{HybridCoordinator, Progress, Step},
    order_fsm::{Delivery, OrderRequest, OrderStatus},
};

pub(super) struct LivePaper {
    exchange: ChaosExchange,
}
impl LivePaper {
    pub(super) fn new(mut exchange: ChaosExchange, origin: Instant) -> Self {
        for asset in exchange.assets().cloned().collect::<Vec<_>>() {
            exchange.invalidate_depth(&asset).expect("registered asset");
        }
        exchange.bind_live_clock(origin);
        Self { exchange }
    }
    fn clock(&mut self, at: Instant) {
        self.exchange
            .advance_live(at)
            .expect("monotonic feed clock");
    }
    pub(super) fn publish(&mut self, group: &NegRiskEventGroup) {
        self.clock(Instant::now());
        let Some(source) = group.source().filter(|_| {
            !group.quotes_only()
                && !group.event_closed()
                && group.markets().iter().all(|m| !group.market_closed(m))
        }) else {
            self.exchange.retire_live_group();
            return;
        };
        let Some(roster) = group.roster() else { return };
        for asset in self.exchange.assets().cloned().collect::<Vec<_>>() {
            let Some(&(index, outcome)) = roster.by_asset.get(asset.as_str()) else {
                continue;
            };
            let token = roster.markets[index].book(outcome);
            let previous = self.exchange.committed_version(&asset);
            if let Some(book) = token.committed(source, true, previous) {
                let _ = self.exchange.commit_book(&book);
            } else if token.synced().is_none() {
                self.exchange.suspend_asset(&token.asset, source.id);
            }
        }
    }
}

/// Command access to the exchange owned by this live group. Replay input is
/// available only on a standalone exchange.
pub struct PaperDesk<'a> {
    pub(super) group: &'a mut NegRiskEventGroup,
}
impl PaperDesk<'_> {
    fn live(&mut self) -> &mut LivePaper {
        self.group.paper.as_mut().expect("attached paper exchange")
    }
    fn exchange(&self) -> &ChaosExchange {
        &self
            .group
            .paper
            .as_ref()
            .expect("attached paper exchange")
            .exchange
    }
    fn at_command_frontier(&mut self) -> bool {
        let admitted = self.group.command_frontier();
        self.live().clock(admitted.unwrap_or_else(Instant::now));
        admitted.is_some()
    }
    pub fn current_stamp(&mut self, asset: &AssetId) -> Result<Option<Stamp>, Error> {
        if !self.at_command_frontier() {
            return Ok(None);
        }
        self.live().exchange.current_stamp(asset)
    }
    pub fn submit(
        &mut self,
        request: OrderRequest,
        signal: Stamp,
        delivery: Delivery,
    ) -> Result<Receipt, Error> {
        let signal = self.at_command_frontier().then_some(signal);
        self.live().exchange.submit_at(request, signal, delivery)
    }
    pub fn step(
        &mut self,
        strategy: &HybridCoordinator,
        signal: Stamp,
        command: Step,
    ) -> Result<Progress, Error> {
        let signal = self.at_command_frontier().then_some(signal);
        strategy.step_at(&mut self.live().exchange, signal, command)
    }
    pub fn order_status(&self, key: OrderKey) -> Result<OrderStatus, Error> {
        self.exchange().order_status(key)
    }
    pub fn abandon_fallback(&mut self, maker: OrderKey) -> Result<OrderStatus, Error> {
        self.live().exchange.abandon_fallback(maker)
    }
    pub fn cancel(&mut self, key: OrderKey, delivery: Delivery) -> Result<OrderStatus, Error> {
        self.live().exchange.cancel(key, delivery)
    }
    pub fn reconcile(&mut self, key: OrderKey) -> Result<OrderStatus, Error> {
        self.live().exchange.reconcile(key)
    }
    pub fn balance(&self, account: AccountId, asset: &AssetId) -> Result<Balance, Error> {
        self.exchange().balance(account, asset)
    }
    pub fn settle_rebates(&mut self, account: AccountId) -> Result<Cash, Error> {
        self.live().clock(Instant::now());
        self.live().exchange.settle_rebates(account)
    }
}
