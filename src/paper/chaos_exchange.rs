//! Deterministic price-time exchange with a single owner for paper balances.
mod hybrid;

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    AccountId, AssetId, Cash, ClientOrderId, Error, Feed, OrderKey, Price, Quantity, Shares, Side,
    Stamp,
    fee_model::{AccruedRebates, FeeModel, MakerTier},
    order_fsm::{
        CancelReason, CancelRequest, CloseReason, Collateral, Credit, Delivery, Order, OrderMode,
        OrderRequest, OrderStatus,
    },
};

pub const MAX_ASSETS: usize = 64;
pub const MAX_ACCOUNTS: usize = 32;
pub const MAX_ORDERS: usize = 1024;
pub const MAX_DEPTH: usize = 4096;
const DAY_MS: u64 = 86_400_000;
static NEXT_ENGINE: AtomicU64 = AtomicU64::new(1);

/// A validated, full L2 observation. Sequence and age are checked on admission.
pub struct Depth {
    asset: AssetId,
    stamp: Stamp,
    levels: DepthLevels,
}

/// Validated levels without any independent source identity.
pub(crate) struct DepthLevels {
    tick: Price,
    bids: BTreeMap<Price, Quantity>,
    asks: BTreeMap<Price, Quantity>,
}
impl DepthLevels {
    pub(crate) fn new(
        tick: Price,
        bids: impl IntoIterator<Item = (Price, Quantity)>,
        asks: impl IntoIterator<Item = (Price, Quantity)>,
    ) -> Result<Self, Error> {
        let mut depth = Self {
            tick,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        let mut count = 0;
        add_depth(&mut depth.bids, &mut count, bids)?;
        add_depth(&mut depth.asks, &mut count, asks)?;
        if let (Some((bid, _)), Some((ask, _))) =
            (depth.bids.last_key_value(), depth.asks.first_key_value())
            && bid >= ask
        {
            return Err(Error::InvalidDepth);
        }
        if depth
            .bids
            .keys()
            .chain(depth.asks.keys())
            .any(|p| !p.micros().is_multiple_of(tick.micros()))
        {
            return Err(Error::InvalidDepth);
        }
        Ok(depth)
    }
}
impl Depth {
    pub fn new(
        asset: AssetId,
        stamp: Stamp,
        tick: Price,
        bids: impl IntoIterator<Item = (Price, Quantity)>,
        asks: impl IntoIterator<Item = (Price, Quantity)>,
    ) -> Result<Self, Error> {
        Ok(Self {
            asset,
            stamp,
            levels: DepthLevels::new(tick, bids, asks)?,
        })
    }
    pub fn stamp(&self) -> Stamp {
        self.stamp
    }
}

fn add_depth(
    levels: &mut BTreeMap<Price, Quantity>,
    count: &mut usize,
    input: impl IntoIterator<Item = (Price, Quantity)>,
) -> Result<(), Error> {
    for (price, size) in input {
        *count += 1;
        if *count > MAX_DEPTH {
            return Err(Error::Capacity);
        }
        if levels.insert(price, size).is_some() {
            return Err(Error::InvalidDepth);
        }
    }
    Ok(())
}

/// An explicit public trade in tape order. L2 size reductions by themselves
/// never grant a simulated maker fill.
pub struct Trade {
    asset: AssetId,
    stamp: Stamp,
    print: Print,
}
struct Print {
    aggressor: Side,
    price: Price,
    quantity: Quantity,
}
impl Trade {
    pub fn new(
        asset: AssetId,
        stamp: Stamp,
        aggressor: Side,
        price: Price,
        quantity: Quantity,
    ) -> Self {
        Self {
            asset,
            stamp,
            print: Print {
                aggressor,
                price,
                quantity,
            },
        }
    }
}

/// A source-correlated execution. The verified depth carries the shared identity.
/// Replacing half of an already validated pair is forbidden.
/// ```compile_fail
/// use polymarket_monitor::paper::chaos_exchange::{Depth, ExecutionPair};
/// fn replace_depth(pair: &mut ExecutionPair, depth: Depth) { pair.depth = depth; }
/// ```
pub struct ExecutionPair {
    depth: Depth,
    print: Print,
}
impl ExecutionPair {
    pub fn try_new(trade: Trade, depth: Depth) -> Result<Self, Error> {
        if trade.asset != depth.asset || trade.stamp != depth.stamp {
            return Err(Error::InvalidDepth);
        }
        Ok(Self {
            depth,
            print: trade.print,
        })
    }
}

#[derive(Clone, Copy)]
enum Liquidity {
    External { public: u64, available: u64 },
    Paper(ClientOrderId),
}

#[derive(Default)]
struct Level {
    fifo: VecDeque<Liquidity>,
}

impl Level {
    fn public_size(&self) -> u64 {
        self.fifo
            .iter()
            .map(|lot| match lot {
                Liquidity::External { public, .. } => *public,
                Liquidity::Paper(_) => 0,
            })
            .sum()
    }

    fn observe(&mut self, size: u64) {
        let public = self.public_size();
        if size > public {
            let added = size - public;
            match self.fifo.back_mut() {
                Some(Liquidity::External { public, available }) => {
                    *public += added;
                    *available += added;
                }
                _ => self.fifo.push_back(Liquidity::External {
                    public: added,
                    available: added,
                }),
            }
        } else if size < public {
            let mut remaining = public - size;
            for entry in self.fifo.iter_mut().rev() {
                if remaining == 0 {
                    break;
                }
                if let Liquidity::External { public, available } = entry {
                    let cut = remaining.min(*public);
                    *public -= cut;
                    *available = available.saturating_sub(cut);
                    remaining -= cut;
                }
            }
            self.prune_public();
        }
    }

    fn prune_public(&mut self) {
        self.fifo
            .retain(|entry| !matches!(entry, Liquidity::External { public: 0, .. }));
    }

    fn executable(entry: &Liquidity) -> bool {
        !matches!(entry, Liquidity::External { available: 0, .. })
    }

    fn head(&self) -> Option<Liquidity> {
        self.fifo
            .iter()
            .find(|entry| Self::executable(entry))
            .copied()
    }
    fn take_external(&mut self, max: u64) -> u64 {
        let Some(Liquidity::External { available, .. }) =
            self.fifo.iter_mut().find(|entry| Self::executable(entry))
        else {
            return 0;
        };
        let taken = max.min(*available);
        *available -= taken;
        taken
    }

    // A real print depletes public orders even if the simulation assigned its
    // fill to a paper order. A simulated IOC never retires public quantity.
    fn public_print(&mut self, mut size: u64) {
        for entry in &mut self.fifo {
            if size == 0 {
                break;
            }
            if let Liquidity::External { public, available } = entry {
                let cut = size.min(*public);
                *public -= cut;
                *available = (*available).min(*public);
                size -= cut;
            }
        }
        self.prune_public();
    }

    fn remove(&mut self, id: ClientOrderId) {
        self.fifo
            .retain(|entry| !matches!(entry,Liquidity::Paper(found) if *found==id));
    }
}

#[derive(Clone, Copy)]
struct TapeTop {
    bid: Option<(Price, Quantity)>,
    ask: Option<Price>,
}
struct ActiveBook {
    tape: Option<TapeTop>,
    bids: BTreeMap<Price, Level>,
    asks: BTreeMap<Price, Level>,
    tick: Price,
    observed: Stamp,
    latest: Stamp,
}
struct Market {
    last_commit: Option<(u64, u64)>,
    feed: Feed,
    depth: DepthState,
    fee: FeeModel,
    external_shares: i128,
    endowment: u64,
    unallocated: u64,
}
enum DepthState {
    NotReady,
    Rejected(Error),
    Accepted(ActiveBook),
    Suspended(ActiveBook),
}
impl Market {
    fn active(&self) -> Option<&ActiveBook> {
        match &self.depth {
            DepthState::Accepted(book) => Some(book),
            _ => None,
        }
    }
    fn active_mut(&mut self) -> Option<&mut ActiveBook> {
        match &mut self.depth {
            DepthState::Accepted(book) => Some(book),
            _ => None,
        }
    }
    fn valid_next(&self, stamp: Stamp) -> bool {
        stamp.belongs_to(self.feed)
            && self.active().is_none_or(|book| {
                let last = book.latest;
                stamp.sequence() > last.sequence() && stamp.observed >= last.observed
            })
    }
}
impl ActiveBook {
    fn side(&self, side: Side) -> &BTreeMap<Price, Level> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }
    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<Price, Level> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }
    fn best_opposite(&self, side: Side) -> Option<Price> {
        if let Some(top) = self.tape {
            return match side {
                Side::Buy => top.ask,
                Side::Sell => top.bid.map(|(p, _)| p),
            };
        }
        let mut levels = self
            .side(side.opposite())
            .iter()
            .filter_map(|(&price, level)| (!level.fifo.is_empty()).then_some(price));
        match side {
            Side::Buy => levels.next(),
            Side::Sell => levels.next_back(),
        }
    }
    fn executable_prices(&self, side: Side, limit: Price) -> Vec<Price> {
        let opposite = self.side(side.opposite());
        let mut prices: Vec<_> = opposite
            .iter()
            .filter_map(|(&price, level)| {
                (side.crosses(limit, price) && level.head().is_some()).then_some(price)
            })
            .collect();
        if side == Side::Sell {
            prices.reverse();
        }
        prices
    }
    fn prune(&mut self, side: Side, price: Price) {
        let levels = self.side_mut(side);
        if levels
            .get(&price)
            .is_some_and(|level| level.fifo.is_empty())
        {
            levels.remove(&price);
        }
    }
}

struct Account {
    available: u128,
    shares: BTreeMap<AssetId, u64>,
    tier: MakerTier,
    rebates: AccruedRebates,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Balance {
    cash: Cash,
    shares: Shares,
    rebates: Cash,
}
impl Balance {
    pub fn cash(self) -> Cash {
        self.cash
    }
    pub fn shares(self) -> Shares {
        self.shares
    }
    pub fn accrued_rebates(self) -> Cash {
        self.rebates
    }
}

/// Acceptance of this exact request, including orders with a missing simulated acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt {
    key: OrderKey,
    status: OrderStatus,
}

impl Receipt {
    pub fn key(self) -> OrderKey {
        self.key
    }
    pub fn status(self) -> OrderStatus {
        self.status
    }
}

enum FallbackFunds {
    AwaitingCancel { extra: Credit },
    Funded(Collateral),
}

struct Reservation {
    maker: OrderKey,
    request: OrderRequest,
    funds: FallbackFunds,
}
impl Reservation {
    fn held(&self) -> (u128, u64) {
        match &self.funds {
            FallbackFunds::AwaitingCancel { extra } => (extra.cash, extra.shares),
            FallbackFunds::Funded(collateral) => {
                let credit = collateral.held(self.request.quantity());
                (credit.cash, credit.shares)
            }
        }
    }
}

enum OrderSlot {
    ReservedFallback(Reservation),
    Accepted(Order),
}
impl OrderSlot {
    fn order(&self) -> Option<&Order> {
        match self {
            Self::Accepted(order) => Some(order),
            Self::ReservedFallback(_) => None,
        }
    }
    fn order_mut(&mut self) -> Option<&mut Order> {
        match self {
            Self::Accepted(order) => Some(order),
            Self::ReservedFallback(_) => None,
        }
    }
}

use std::time::Duration;
use tokio::time::Instant;

#[derive(Default)]
struct ExchangeClock {
    elapsed: Duration,
    origin: Option<Instant>,
}
impl ExchangeClock {
    fn millis(&self) -> u64 {
        self.elapsed.as_millis() as u64
    }
    fn age_within(&self, observed: Stamp, limit: u64) -> bool {
        self.elapsed
            .checked_sub(observed.observed)
            .is_some_and(|age| age < Duration::from_millis(limit))
    }
    fn offset(&self, at: Instant) -> Duration {
        at.saturating_duration_since(self.origin.expect("live exchange clock"))
    }
}
pub struct ChaosExchange {
    instance: u64,
    clock: ExchangeClock,
    max_age_ms: u64,
    accounts: Vec<Account>,
    markets: BTreeMap<AssetId, Market>,
    orders: BTreeMap<ClientOrderId, OrderSlot>,
    hybrids: BTreeMap<ClientOrderId, super::hybrid_coordinator::HybridPlan>,
    initial_cash: u128,
    unallocated_cash: u128,
    external_cash: i128,
    fee_pool: u128,
    started: bool,
}
impl ChaosExchange {
    pub fn new(max_age_ms: u64, cash: Cash) -> Result<Self, Error> {
        if max_age_ms == 0 {
            return Err(Error::InvalidValue);
        }
        let instance = NEXT_ENGINE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| Error::Capacity)?;
        Ok(Self {
            instance,
            clock: ExchangeClock::default(),
            max_age_ms,
            accounts: Vec::new(),
            markets: BTreeMap::new(),
            orders: BTreeMap::new(),
            hybrids: BTreeMap::new(),
            initial_cash: cash.ticks(),
            unallocated_cash: cash.ticks(),
            external_cash: 0,
            fee_pool: 0,
            started: false,
        })
    }

    pub fn register_asset(
        &mut self,
        asset: AssetId,
        fee: FeeModel,
        shares: Shares,
    ) -> Result<(), Error> {
        if self.started {
            return Err(Error::AlreadyStarted);
        }
        if self.markets.contains_key(&asset) {
            return Err(Error::InvalidValue);
        }
        if self.markets.len() == MAX_ASSETS {
            return Err(Error::Capacity);
        }
        self.markets.insert(
            asset,
            Market {
                last_commit: None,
                feed: Feed {
                    instance: self.instance,
                    slot: self.markets.len(),
                    generation: 0,
                },
                depth: DepthState::NotReady,
                fee,
                external_shares: 0,
                endowment: shares.micros(),
                unallocated: shares.micros(),
            },
        );
        Ok(())
    }

    /// Splits the configured capital pool into disjoint strategy accounts.
    pub fn allocate(
        &mut self,
        cash: Cash,
        shares: impl IntoIterator<Item = (AssetId, Shares)>,
        tier: MakerTier,
    ) -> Result<AccountId, Error> {
        if self.started {
            return Err(Error::AlreadyStarted);
        }
        if self.accounts.len() == MAX_ACCOUNTS {
            return Err(Error::Capacity);
        }
        if cash.ticks() > self.unallocated_cash {
            return Err(Error::InsufficientFunds);
        }
        let mut inventory = BTreeMap::new();
        for (asset, size) in shares {
            let market = self.markets.get(&asset).ok_or(Error::UnknownAsset)?;
            if size.micros() > market.unallocated {
                return Err(Error::InsufficientFunds);
            }
            if inventory.insert(asset, size.micros()).is_some() {
                return Err(Error::InvalidValue);
            }
        }
        for (asset, amount) in &inventory {
            self.markets
                .get_mut(asset)
                .expect("registered asset")
                .unallocated -= amount;
        }
        let slot = self.accounts.len();
        self.unallocated_cash -= cash.ticks();
        self.accounts.push(Account {
            available: cash.ticks(),
            shares: inventory,
            tier,
            rebates: AccruedRebates::default(),
        });
        Ok(AccountId {
            slot,
            instance: self.instance,
        })
    }

    pub fn advance_to(&mut self, now_ms: u64) -> Result<(), Error> {
        if now_ms < self.clock.millis() {
            return Err(Error::StaleSignal);
        }
        // Zero-lag work can be scheduled in the same ledger millisecond as
        // a fractional-time input. It executes no earlier than that input.
        self.clock.elapsed = self.clock.elapsed.max(Duration::from_millis(now_ms));
        Ok(())
    }

    pub(crate) fn bind_live_clock(&mut self, origin: Instant) {
        debug_assert!(self.can_attach() && self.clock.origin.is_none());
        self.clock.origin = Some(origin);
    }

    #[cfg(test)]
    pub(crate) fn live_time_for_test(&self, ms: u64) -> Instant {
        self.clock.origin.unwrap() + Duration::from_millis(ms)
    }

    pub(crate) fn advance_live(&mut self, at: Instant) -> Result<(), Error> {
        let elapsed = self.clock.offset(at);
        if elapsed < self.clock.elapsed {
            return Err(Error::StaleSignal);
        }
        self.clock.elapsed = elapsed;
        Ok(())
    }

    fn fresh(&self, stamp: Stamp) -> bool {
        self.clock.age_within(stamp, self.max_age_ms)
    }

    fn index(&self, account: AccountId) -> Result<usize, Error> {
        if account.instance != self.instance || account.slot >= self.accounts.len() {
            return Err(Error::UnknownAccount);
        }
        Ok(account.slot)
    }

    pub fn balance(&self, account: AccountId, asset: &AssetId) -> Result<Balance, Error> {
        let account = &self.accounts[self.index(account)?];
        if !self.markets.contains_key(asset) {
            return Err(Error::UnknownAsset);
        }
        Ok(Balance {
            cash: Cash::from_ticks(account.available),
            shares: Shares::from_micros(*account.shares.get(asset).unwrap_or(&0)),
            rebates: Cash::from_ticks(account.rebates.total()),
        })
    }

    pub(crate) fn can_attach(&self) -> bool {
        !self.started && self.clock.millis() == 0
    }
    pub fn feed(&self, asset: &AssetId) -> Result<Feed, Error> {
        Ok(self.markets.get(asset).ok_or(Error::UnknownAsset)?.feed)
    }
    pub(crate) fn assets(&self) -> impl Iterator<Item = &AssetId> {
        self.markets.keys()
    }
    pub(crate) fn retire_live_group(&mut self) {
        let assets: Vec<_> = self
            .markets
            .iter()
            .filter(|(_, m)| !matches!(m.depth, DepthState::NotReady))
            .map(|(asset, _)| asset.clone())
            .collect();
        for asset in assets {
            self.invalidate_depth(&asset).expect("registered asset");
        }
    }

    pub(crate) fn committed_version(&self, asset: &AssetId) -> Option<(u64, u64)> {
        self.markets[asset].last_commit
    }

    /// Limit covering the confirmed quantity, or the deepest available partial fill.
    pub(crate) fn hedge_limit(&self, asset: &AssetId, quantity: Shares) -> Option<Price> {
        let book = self
            .markets
            .get(asset)?
            .active()
            .filter(|book| self.fresh(book.observed))?;
        let mut left = quantity.micros();
        let mut limit = None;
        for (&price, level) in &book.asks {
            for lot in &level.fifo {
                if let Liquidity::External { available, .. } = lot
                    && *available > 0
                {
                    limit = Some(price);
                    left = left.saturating_sub(*available);
                    if left == 0 {
                        return limit;
                    }
                }
            }
        }
        limit
    }
    pub fn current_stamp(&self, asset: &AssetId) -> Result<Option<Stamp>, Error> {
        let market = self.markets.get(asset).ok_or(Error::UnknownAsset)?;
        if let DepthState::Rejected(reason) = market.depth {
            return Err(reason);
        }
        Ok(market
            .active()
            .filter(|book| self.fresh(book.observed))
            .map(|book| book.latest))
    }

    pub fn observe(&mut self, depth: Depth) -> Result<(), Error> {
        self.validate_observation(&depth.asset, depth.stamp)?;
        self.apply_depth(depth);
        Ok(())
    }
    pub(crate) fn commit_book(&mut self, book: &crate::book::CommittedBook) -> Result<(), Error> {
        let asset = AssetId::new(book.asset())?;
        let market = self.markets.get_mut(&asset).ok_or(Error::UnknownAsset)?;
        if market
            .last_commit
            .is_some_and(|last| last >= book.version())
        {
            return Err(Error::StaleSignal);
        }
        if market
            .last_commit
            .is_some_and(|(generation, _)| generation != book.version().0)
        {
            market.feed.generation = market
                .feed
                .generation
                .checked_add(1)
                .expect("feed generation exhausted");
            market.depth = DepthState::NotReady;
        }
        market.last_commit = Some(book.version());
        let prior = std::mem::replace(&mut market.depth, DepthState::NotReady);
        let previous = match prior {
            DepthState::Accepted(b) | DepthState::Suspended(b) => Some(b),
            _ => None,
        };

        let observed = market
            .feed
            .stamp_at(book.version().1, self.clock.offset(book.observed_at()));
        let (raw_tick, bid, asks) = book.quote();
        let Ok(tick) = Price::new(raw_tick) else {
            if let Some(b) = previous {
                market.depth = DepthState::Suspended(b);
            }
            self.revalidate_orders(&asset, raw_tick);
            return Err(Error::InvalidValue);
        };
        let mut next = previous.unwrap_or_else(|| ActiveBook {
            tape: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            tick,
            latest: observed,
            observed,
        });
        if let crate::book::BidObservation::Full(bids) = bid {
            let convert = |levels: &[crate::wire::Level]| {
                levels
                    .iter()
                    .map(|l| Ok((Price::new(l.price)?, Quantity::new(l.size)?)))
                    .collect::<Result<Vec<_>, Error>>()
            };
            let Some(bids) = bids else {
                // Retain the full queue and observed liquidity through a crossed revision.
                market.depth = DepthState::Suspended(next);
                self.revalidate_orders(&asset, raw_tick);
                return Ok(());
            };
            let rules_changed = book.tick_revision();
            // Even an expired observation carries the next public quantity.
            // Matching checks its original stamp when consuming liquidity.
            let levels =
                convert(bids).and_then(|bids| DepthLevels::new(tick, bids, convert(asks)?));
            match levels {
                Ok(levels) => {
                    market.depth = DepthState::Accepted(next);
                    self.apply_depth(Depth {
                        asset,
                        stamp: observed,
                        levels,
                    });
                }
                Err(reason) => {
                    market.depth = DepthState::Suspended(next);
                    self.revalidate_orders(&asset, raw_tick);
                    if !rules_changed {
                        self.retire_depth(&asset, DepthState::Rejected(reason))?;
                    }
                    return Err(reason);
                }
            }
            return Ok(());
        }
        let convert = |l: &crate::wire::Level| {
            let p = Price::new(l.price)?;
            if !p.micros().is_multiple_of(tick.micros()) {
                return Err(Error::InvalidDepth);
            }
            Ok((p, Quantity::new(l.size.min(1_000_000.into()))?))
        };
        let asks = asks
            .iter()
            .map(convert)
            .collect::<Result<BTreeMap<_, _>, _>>();
        let top = asks.and_then(|asks| {
            let ask = asks.keys().next().copied();
            Self::observe_levels(&mut next.asks, asks);
            let crate::book::BidObservation::Available(bid) = bid else {
                return Ok(None);
            };
            let bid = bid.as_ref().map(convert).transpose()?;
            if bid.zip(ask).is_some_and(|((bid, _), ask)| bid >= ask) {
                return Err(Error::InvalidDepth);
            }
            Ok(Some(TapeTop { bid, ask }))
        });
        next.tick = tick;
        next.observed = observed;
        next.latest = observed;
        next.tape = top.as_ref().ok().copied().flatten();
        market.depth = if next.tape.is_some() {
            DepthState::Accepted(next)
        } else {
            DepthState::Suspended(next)
        };
        self.started = true;
        self.revalidate_orders(&asset, raw_tick);
        top.map(|_| ())
    }

    pub(crate) fn suspend_asset(&mut self, id: &str, generation: u64) {
        if let Ok(asset) = AssetId::new(id)
            && let Some(market) = self.markets.get_mut(&asset)
            && market.last_commit.is_some_and(|(g, _)| g == generation)
            && matches!(market.depth, DepthState::Accepted(_))
            && let DepthState::Accepted(book) =
                std::mem::replace(&mut market.depth, DepthState::NotReady)
        {
            market.depth = DepthState::Suspended(book);
        }
    }

    pub(crate) fn detach_queues(&mut self) {
        for order in self.orders.values_mut().filter_map(OrderSlot::order_mut) {
            order.detach();
        }
        for market in self.markets.values_mut() {
            market.depth = DepthState::NotReady;
        }
    }

    pub(crate) fn paper_print(
        &mut self,
        asset: AssetId,
        price: Price,
        volume: rust_decimal::Decimal,
    ) -> Result<(), Error> {
        use rust_decimal::{Decimal, prelude::ToPrimitive};
        if volume <= Decimal::ZERO {
            return Err(Error::InvalidValue);
        }
        let market = self.markets.get(&asset).ok_or(Error::UnknownAsset)?;
        let Some(book) = market.active().filter(|b| self.fresh(b.observed)) else {
            return Ok(());
        };
        let Some(level) = book.bids.get(&price) else {
            return Ok(());
        };
        let available: u64 = level
            .fifo
            .iter()
            .map(|lot| match lot {
                Liquidity::External { available, .. } => *available,
                Liquidity::Paper(id) => self
                    .accepted(*id)
                    .working_quantity()
                    .map_or(0, |q| q.shares().micros()),
            })
            .sum();
        let bound = Decimal::from_i128_with_scale(available.into(), 6);
        let micros = (volume.min(bound) * Decimal::from(1_000_000))
            .trunc()
            .to_u64()
            .ok_or(Error::InvalidValue)?;
        self.match_level(&asset, Side::Buy, price, micros);
        if let Some(level) = self.active_book_mut(&asset).bids.get_mut(&price) {
            level.public_print(micros);
        }
        Ok(())
    }

    pub(crate) fn queue_ahead(&self, key: OrderKey) -> Option<Shares> {
        let id = self.owned_id(key).ok()?;
        let request = self.accepted(id).request();
        let depth = &self.markets.get(request.asset())?.depth;
        let book = match depth {
            DepthState::Accepted(b) | DepthState::Suspended(b) => b,
            _ => return None,
        };
        let level = book.side(request.side()).get(&request.limit())?;
        let mut ahead = 0;
        for lot in &level.fifo {
            match lot {
                Liquidity::External { available, .. } => ahead += available,
                Liquidity::Paper(other) if *other == id => return Some(Shares::from_micros(ahead)),
                Liquidity::Paper(other) => {
                    ahead += self.accepted(*other).working_quantity()?.shares().micros()
                }
            }
        }
        None
    }

    fn apply_depth(&mut self, depth: Depth) {
        let market = self
            .markets
            .get_mut(&depth.asset)
            .expect("registered asset");
        if market.active().is_none() {
            market.depth = DepthState::Accepted(ActiveBook {
                tape: None,
                bids: BTreeMap::new(),
                asks: BTreeMap::new(),
                tick: depth.levels.tick,
                observed: depth.stamp,
                latest: depth.stamp,
            });
        }
        let book = market.active_mut().expect("accepted depth");
        for (side, next) in [
            (Side::Buy, depth.levels.bids),
            (Side::Sell, depth.levels.asks),
        ] {
            Self::observe_levels(book.side_mut(side), next);
        }
        book.tape = None;
        book.tick = depth.levels.tick;
        book.observed = depth.stamp;
        book.latest = depth.stamp;
        self.started = true;
        self.revalidate_orders(&depth.asset, depth.levels.tick.decimal());
    }

    fn observe_levels(levels: &mut BTreeMap<Price, Level>, mut next: BTreeMap<Price, Quantity>) {
        levels.retain(|price, level| {
            let size = next
                .remove(price)
                .map_or(0, |quantity| quantity.shares().micros());
            level.observe(size);
            size > 0 || !level.fifo.is_empty()
        });
        for (price, quantity) in next {
            let mut level = Level::default();
            level.observe(quantity.shares().micros());
            levels.insert(price, level);
        }
    }
    fn revalidate_orders(&mut self, asset: &AssetId, tick: rust_decimal::Decimal) {
        let (market, executable) = match &self.markets[asset].depth {
            DepthState::Accepted(b) => (b, true),
            DepthState::Suspended(b) => (b, false),
            _ => return,
        };
        let ask = executable
            .then(|| market.best_opposite(Side::Buy))
            .flatten();
        let bid = executable
            .then(|| market.best_opposite(Side::Sell))
            .flatten();
        let closed: Vec<_> = market
            .bids
            .values()
            .chain(market.asks.values())
            .flat_map(|level| &level.fifo)
            .filter_map(|lot| {
                let Liquidity::Paper(id) = *lot else {
                    return None;
                };
                let request = self.accepted(id).request();
                let opposite = match request.side() {
                    Side::Buy => ask,
                    Side::Sell => bid,
                };
                if !(request.limit().decimal() % tick).is_zero() {
                    Some((id, CloseReason::TickChanged))
                } else if request.crosses(opposite) {
                    Some((id, CloseReason::BookCrossed))
                } else {
                    None
                }
            })
            .collect();
        for (id, reason) in closed {
            self.remove_from_queue(id);
            self.close(id, reason);
        }
    }

    /// A disconnect invalidates all priority assumptions for this asset.
    pub fn invalidate_depth(&mut self, asset: &AssetId) -> Result<(), Error> {
        self.retire_depth(asset, DepthState::NotReady)
    }
    fn retire_depth(&mut self, asset: &AssetId, state: DepthState) -> Result<(), Error> {
        let market = self.markets.get_mut(asset).ok_or(Error::UnknownAsset)?;
        market.feed.generation = market
            .feed
            .generation
            .checked_add(1)
            .expect("feed generation exhausted");
        market.depth = state;
        let working: Vec<_> = self
            .orders
            .iter()
            .filter_map(|(&id, slot)| {
                let order = slot.order()?;
                (order.request().asset() == asset && order.working_quantity().is_some())
                    .then_some(id)
            })
            .collect();
        for id in working {
            self.close(id, CloseReason::LostBook);
        }
        Ok(())
    }

    fn accepted(&self, id: ClientOrderId) -> &Order {
        self.orders[&id].order().expect("accepted order")
    }
    fn owned_id(&self, key: OrderKey) -> Result<ClientOrderId, Error> {
        self.index(key.account)?;
        self.orders
            .get(&key.id)
            .and_then(OrderSlot::order)
            .filter(|o| o.request().account() == key.account)
            .map(|_| key.id)
            .ok_or(Error::UnknownOrder)
    }
    pub(crate) fn order_records(&self) -> impl Iterator<Item = &Order> {
        self.orders.values().filter_map(OrderSlot::order)
    }
    pub(crate) fn has_slots(&self, needed: usize) -> bool {
        needed <= MAX_ORDERS - self.orders.len()
    }
    pub(crate) fn cancel_request(&self, key: OrderKey) -> Option<CancelRequest> {
        self.owned_id(key)
            .ok()
            .and_then(|id| self.accepted(id).cancellation())
    }
    pub(crate) fn schedule_cancel(
        &mut self,
        key: OrderKey,
        reason: CancelReason,
        sent_ms: u64,
        latency: u64,
    ) -> Result<(), Error> {
        let id = self.owned_id(key)?;
        let request = CancelRequest {
            reason,
            sent_ms,
            effective_ms: sent_ms.checked_add(latency).ok_or(Error::InvalidValue)?,
            input_interrupted: self.queue_ahead(key).is_none(),
        };
        self.orders
            .get_mut(&id)
            .and_then(OrderSlot::order_mut)
            .expect("owned order")
            .schedule_cancel(request);
        Ok(())
    }
    /// Retire the order without shortening a cancellation already in flight.
    pub(crate) fn finish_order(&mut self, key: OrderKey, at: u64) -> Result<Option<u64>, Error> {
        let id = self.owned_id(key)?;
        let order = self.accepted(id);
        if order.working_quantity().is_none() {
            return Ok(None);
        }
        let (when, reason) = match order.cancellation() {
            Some(r) if r.effective_ms > at => return Ok(Some(r.effective_ms)),
            Some(r) => (r.effective_ms, CloseReason::Cancelled),
            None => (at, CloseReason::SessionEnded),
        };
        self.advance_to(self.clock.millis().max(when))?;
        self.remove_from_queue(id);
        self.close_at(id, reason, when);
        Ok(None)
    }
    pub fn order_status(&self, key: OrderKey) -> Result<OrderStatus, Error> {
        Ok(self.accepted(self.owned_id(key)?).status())
    }
    fn check_signal(
        &self,
        request: &OrderRequest,
        signal: Option<Stamp>,
    ) -> Result<&Market, Error> {
        let market = self
            .markets
            .get(request.asset())
            .ok_or(Error::UnknownAsset)?;
        if !market
            .active()
            .is_some_and(|book| self.fresh(book.observed) && Some(book.latest) == signal)
        {
            return Err(Error::StaleSignal);
        }
        if !market
            .active()
            .is_some_and(|book| request.limit().micros().is_multiple_of(book.tick.micros()))
        {
            return Err(Error::InvalidValue);
        }
        Ok(market)
    }
    fn collateral(request: &OrderRequest, market: &Market) -> Collateral {
        match request.side() {
            Side::Buy => Collateral::Buy {
                cash_per_micro: market.fee.buy_reserve_per_micro(
                    request.limit(),
                    request.mode() == OrderMode::ImmediateOrCancel,
                ),
            },
            Side::Sell => Collateral::Sell,
        }
    }
    fn reserved_for(&self, fallback: ClientOrderId, maker: OrderKey) -> bool {
        matches!(self.orders.get(&fallback),Some(OrderSlot::ReservedFallback(r)) if r.maker==maker)
    }
    fn vacancy(&self, id: ClientOrderId) -> Result<(), Error> {
        if self.orders.contains_key(&id) || self.hybrids.contains_key(&id) {
            return Err(Error::ConflictingOrder);
        }
        if self.orders.len() >= MAX_ORDERS {
            return Err(Error::Capacity);
        }
        Ok(())
    }
    fn collateral_for(
        &self,
        request: &OrderRequest,
        market: &Market,
        release: (u128, u64),
    ) -> Result<Collateral, Error> {
        let collateral = Self::collateral(request, market);
        let required = collateral.held(request.quantity());
        let wallet = &self.accounts[self.index(request.account())?];
        if required.cash > wallet.available + release.0
            || required.shares
                > wallet.shares.get(request.asset()).copied().unwrap_or(0) + release.1
        {
            return Err(Error::InsufficientFunds);
        }
        Ok(collateral)
    }

    fn cancel_for(
        &mut self,
        key: OrderKey,
        fallback: &OrderRequest,
        signal: Option<Stamp>,
        delivery: Delivery,
    ) -> Result<OrderStatus, Error> {
        let maker = self.accepted(self.owned_id(key)?);
        let market = self.check_signal(fallback, signal)?;
        self.vacancy(fallback.id())?;
        let released = maker.held_totals();
        let collateral = self.collateral_for(fallback, market, released)?;
        let required = collateral.held(fallback.quantity());
        let status = self.cancel(key, delivery)?;
        let wallet = &mut self.accounts[key.account.slot];
        let funds = if status == OrderStatus::Unconfirmed {
            let extra = Credit {
                cash: required.cash.saturating_sub(released.0),
                shares: required.shares.saturating_sub(released.1),
                ..Credit::default()
            };
            Self::debit(wallet, fallback.asset(), &extra);
            FallbackFunds::AwaitingCancel { extra }
        } else {
            Self::debit(wallet, fallback.asset(), &required);
            FallbackFunds::Funded(collateral)
        };
        let reservation = Reservation {
            maker: key,
            request: fallback.clone(),
            funds,
        };
        self.orders
            .insert(fallback.id(), OrderSlot::ReservedFallback(reservation));
        Ok(status)
    }
    fn prior(&self, request: &OrderRequest) -> Result<Option<Receipt>, Error> {
        self.index(request.account())?;
        let Some(order) = self.orders.get(&request.id()).and_then(OrderSlot::order) else {
            if self.orders.contains_key(&request.id()) || self.hybrids.contains_key(&request.id()) {
                return Err(Error::ConflictingOrder);
            }
            return Ok(None);
        };
        if order.request() != request {
            return Err(Error::ConflictingOrder);
        }
        Ok(Some(Receipt {
            key: request.account().order(request.id()),
            status: order.status(),
        }))
    }

    /// Replaying an accepted request reads its receipt even after its signal expires.
    pub fn submit(
        &mut self,
        request: OrderRequest,
        signal: Stamp,
        delivery: Delivery,
    ) -> Result<Receipt, Error> {
        self.submit_at(request, Some(signal), delivery)
    }
    pub(crate) fn submit_at(
        &mut self,
        request: OrderRequest,
        signal: Option<Stamp>,
        delivery: Delivery,
    ) -> Result<Receipt, Error> {
        if let Some(receipt) = self.prior(&request)? {
            return Ok(receipt);
        }
        self.submit_new(request, signal, delivery)
    }

    fn submit_new(
        &mut self,
        request: OrderRequest,
        signal: Option<Stamp>,
        delivery: Delivery,
    ) -> Result<Receipt, Error> {
        let market = self.check_signal(&request, signal)?;
        self.vacancy(request.id())?;
        let collateral = self.collateral_for(&request, market, (0, 0))?;
        let required = collateral.held(request.quantity());
        let account = &mut self.accounts[request.account().slot];
        Self::debit(account, request.asset(), &required);

        Ok(self.accept(request, collateral, delivery))
    }
    fn submit_reserved(
        &mut self,
        id: ClientOrderId,
        signal: Option<Stamp>,
        delivery: Delivery,
    ) -> Result<Receipt, Error> {
        let Some(OrderSlot::ReservedFallback(r)) = self.orders.get(&id) else {
            return Err(Error::ConflictingOrder);
        };
        if !matches!(r.funds, FallbackFunds::Funded(_)) {
            return Err(Error::Unconfirmed);
        }
        self.check_signal(&r.request, signal)?;
        let Some(OrderSlot::ReservedFallback(Reservation {
            request,
            funds: FallbackFunds::Funded(collateral),
            ..
        })) = self.orders.remove(&id)
        else {
            unreachable!("validated funded reservation")
        };
        Ok(self.accept(request, collateral, delivery))
    }
    /// Give up a pending IOC after cancellation is confirmed, even with no fresh market.
    pub fn abandon_fallback(&mut self, maker: OrderKey) -> Result<OrderStatus, Error> {
        let status = self.order_status(maker)?;
        if status == OrderStatus::Unconfirmed {
            return Err(Error::Unconfirmed);
        }
        let fallback = self
            .hybrids
            .get(&maker.id)
            .map(|c| c.fallback_id())
            .ok_or(Error::UnknownOrder)?;
        if !self.reserved_for(fallback, maker) {
            return Err(Error::ConflictingOrder);
        }
        self.release_reservation(fallback);
        self.hybrids.remove(&maker.id);
        Ok(status)
    }

    fn release_reservation(&mut self, id: ClientOrderId) {
        let Some(OrderSlot::ReservedFallback(r)) = self.orders.remove(&id) else {
            unreachable!("owned fallback reservation");
        };
        let (cash, shares) = r.held();
        Self::credit(
            &mut self.accounts[r.maker.account.slot],
            r.request.asset(),
            Credit {
                cash,
                shares,
                ..Credit::default()
            },
        );
    }

    fn accept(
        &mut self,
        request: OrderRequest,
        collateral: Collateral,
        delivery: Delivery,
    ) -> Receipt {
        let key = request.account().order(request.id());
        let id = request.id();
        let market = self.markets[request.asset()]
            .active()
            .expect("admitted book");
        let resting = request.mode() == OrderMode::PostOnly;
        let would_take = resting && request.crosses(market.best_opposite(request.side()));
        let executable = if resting {
            Vec::new()
        } else {
            market.executable_prices(request.side(), request.limit())
        };
        let side = request.side();
        let price = request.limit();
        let asset = request.asset().clone();
        self.orders.insert(
            id,
            OrderSlot::Accepted(Order::new(request, collateral, delivery)),
        );
        if delivery == Delivery::Dropped {
            self.close(id, CloseReason::DroppedBeforeExecution);
        } else if resting {
            if !would_take {
                let book = self.active_book_mut(&asset);
                if let Some(top) = book.tape
                    && side == Side::Buy
                {
                    book.bids
                        .retain(|_, l| l.fifo.iter().any(|v| matches!(v, Liquidity::Paper(_))));
                    if !book.bids.contains_key(&price)
                        && let Some((p, q)) = top.bid
                        && p == price
                    {
                        book.bids
                            .entry(price)
                            .or_default()
                            .observe(q.shares().micros());
                    }
                }
                book.side_mut(side)
                    .entry(price)
                    .or_default()
                    .fifo
                    .push_back(Liquidity::Paper(id));
            } else {
                self.close(id, CloseReason::WouldTake);
            }
        } else {
            self.sweep(id, &asset, side, executable);
        }
        Receipt {
            key,
            status: self.accepted(id).status(),
        }
    }

    fn sweep(&mut self, id: ClientOrderId, asset: &AssetId, side: Side, prices: Vec<Price>) {
        for price in prices {
            let Some(remaining) = self.accepted(id).working_quantity() else {
                return;
            };
            let mut want = remaining.shares().micros();
            let level = self
                .markets
                .get_mut(asset)
                .expect("registered asset")
                .active_mut()
                .expect("active order book")
                .side_mut(side.opposite())
                .get_mut(&price)
                .expect("selected executable level");
            let mut executed = 0;
            while want > 0 {
                let taken = level.take_external(want);
                if taken == 0 {
                    break;
                }
                executed += taken;
                want -= taken;
            }
            let self_trade = want > 0 && matches!(level.head(), Some(Liquidity::Paper(_)));
            if let Some(size) = Quantity::from_micros(executed) {
                self.fill(id, price, size, false);
            }
            if self_trade {
                self.close(id, CloseReason::SelfTradePrevented);
                return;
            }
        }
        self.close(id, CloseReason::UnfilledIoc);
    }

    /// A source-correlated print and its resulting full depth share a single
    /// observation. Both have been decoded before either can change the queue.
    pub fn observe_execution(&mut self, pair: ExecutionPair) -> Result<(), Error> {
        let depth = pair.depth;
        let market = self.validate_observation(&depth.asset, depth.stamp)?;
        if let Some(book) = market.active() {
            Self::validate_print(&pair.print, book.tick)?;
            if self.fresh(book.observed) {
                self.match_print(&depth.asset, &pair.print);
            }
            self.retire_print(&depth.asset, &pair.print);
        } else {
            Self::validate_print(&pair.print, depth.levels.tick)?;
        }
        self.apply_depth(depth);
        Ok(())
    }

    fn validate_observation(&self, asset: &AssetId, stamp: Stamp) -> Result<&Market, Error> {
        let market = self.markets.get(asset).ok_or(Error::UnknownAsset)?;
        if !self.fresh(stamp) || !market.valid_next(stamp) {
            return Err(Error::StaleSignal);
        }
        Ok(market)
    }

    fn validate_print(print: &Print, tick: Price) -> Result<(), Error> {
        if !print.price.micros().is_multiple_of(tick.micros()) {
            return Err(Error::InvalidValue);
        }
        Ok(())
    }

    pub fn trade(&mut self, trade: Trade) -> Result<(), Error> {
        let market = self.validate_observation(&trade.asset, trade.stamp)?;
        let book = market
            .active()
            .filter(|book| self.fresh(book.observed))
            .ok_or(Error::StaleSignal)?;
        Self::validate_print(&trade.print, book.tick)?;
        self.match_print(&trade.asset, &trade.print);
        self.retire_print(&trade.asset, &trade.print);
        self.active_book_mut(&trade.asset).latest = trade.stamp;
        Ok(())
    }

    fn active_book_mut(&mut self, asset: &AssetId) -> &mut ActiveBook {
        self.markets
            .get_mut(asset)
            .and_then(Market::active_mut)
            .expect("accepted depth")
    }

    fn match_print(&mut self, asset: &AssetId, print: &Print) {
        let side = print.aggressor.opposite();
        let mut remaining = print.quantity.shares().micros();
        let prices = self.markets[asset]
            .active()
            .unwrap()
            .executable_prices(print.aggressor, print.price);
        for price in prices {
            if remaining == 0 {
                break;
            }
            remaining = self.match_level(asset, side, price, remaining);
        }
    }
    fn match_level(
        &mut self,
        asset: &AssetId,
        side: Side,
        price: Price,
        mut remaining: u64,
    ) -> u64 {
        while remaining > 0 {
            let market = self.active_book_mut(asset);
            let Some(level) = market.side_mut(side).get_mut(&price) else {
                break;
            };
            match level.head() {
                None => break,
                Some(Liquidity::External { .. }) => remaining -= level.take_external(remaining),
                Some(Liquidity::Paper(id)) => {
                    let size = remaining.min(
                        self.accepted(id)
                            .working_quantity()
                            .expect("queued order")
                            .shares()
                            .micros(),
                    );
                    self.fill(
                        id,
                        price,
                        Quantity::from_micros(size).expect("positive fill"),
                        true,
                    );
                    remaining -= size;
                    if self.accepted(id).working_quantity().is_none() {
                        self.remove_from_queue(id);
                    }
                }
            }
        }
        remaining
    }
    fn retire_print(&mut self, asset: &AssetId, print: &Print) {
        let side = print.aggressor.opposite();
        let market = self.active_book_mut(asset);
        if let Some(level) = market.side_mut(side).get_mut(&print.price) {
            level.public_print(print.quantity.shares().micros());
        }
        market.prune(side, print.price);
    }

    pub fn cancel(&mut self, key: OrderKey, delivery: Delivery) -> Result<OrderStatus, Error> {
        let id = self.owned_id(key)?;
        let order = self
            .orders
            .get_mut(&id)
            .and_then(OrderSlot::order_mut)
            .expect("owned order");
        if order.is_unconfirmed() {
            return Err(Error::Unconfirmed);
        }
        if order.working_quantity().is_some() {
            if delivery != Delivery::Delivered {
                order.suppress_ack();
            }
            if delivery != Delivery::Dropped {
                self.remove_from_queue(id);
                self.close(id, CloseReason::Cancelled);
            }
        }
        self.order_status(key)
    }

    fn remove_from_queue(&mut self, id: ClientOrderId) {
        let request = self.orders[&id].order().expect("queued order").request();
        let market = match &mut self
            .markets
            .get_mut(request.asset())
            .expect("registered asset")
            .depth
        {
            DepthState::Accepted(b) | DepthState::Suspended(b) => b,
            _ => return,
        };
        if let Some(level) = market.side_mut(request.side()).get_mut(&request.limit()) {
            level.remove(id);
        }
        market.prune(request.side(), request.limit());
    }

    pub fn reconcile(&mut self, key: OrderKey) -> Result<OrderStatus, Error> {
        let id = self.owned_id(key)?;
        let order = self
            .orders
            .get_mut(&id)
            .and_then(OrderSlot::order_mut)
            .expect("owned order");
        let mut credit = order.confirm();
        let asset = order.request().asset().clone();
        let status = order.status();
        if let Some(fallback) = self.hybrids.get(&id).map(|c| c.fallback_id())
            && let Some(OrderSlot::ReservedFallback(r)) = self.orders.get_mut(&fallback)
            && r.maker == key
            && let FallbackFunds::AwaitingCancel { extra } = &r.funds
        {
            if matches!(
                status,
                OrderStatus::Closed {
                    reason: CloseReason::Cancelled,
                    ..
                }
            ) {
                let collateral = Self::collateral(&r.request, &self.markets[&asset]);
                let required = collateral.held(r.request.quantity());
                // Move the maker's released collateral directly into the IOC.
                credit.cash -= required.cash - extra.cash;
                credit.shares -= required.shares - extra.shares;
                r.funds = FallbackFunds::Funded(collateral);
            } else {
                self.release_reservation(fallback);
                self.hybrids.remove(&id);
            }
        }
        Self::credit(&mut self.accounts[key.account.slot], &asset, credit);
        Ok(status)
    }

    /// Delivers confirmed accrued rebates from earlier simulated days, once
    /// the accumulated payable amount reaches one pUSD.
    pub fn settle_rebates(&mut self, account: AccountId) -> Result<Cash, Error> {
        let index = self.index(account)?;
        let wallet = &mut self.accounts[index];
        let payout = wallet.rebates.payout(self.clock.millis() / DAY_MS);
        wallet.available += payout;
        Ok(Cash::from_ticks(payout))
    }

    fn debit(wallet: &mut Account, asset: &AssetId, required: &Credit) {
        wallet.available -= required.cash;
        *wallet.shares.entry(asset.clone()).or_default() -= required.shares;
    }

    fn credit(wallet: &mut Account, asset: &AssetId, credit: Credit) {
        wallet.available += credit.cash;
        if credit.shares > 0 {
            *wallet.shares.entry(asset.clone()).or_default() += credit.shares;
        }
        wallet.rebates.merge(credit.rebates);
    }

    fn close(&mut self, id: ClientOrderId, reason: CloseReason) {
        self.close_at(id, reason, self.clock.millis());
    }

    fn close_at(&mut self, id: ClientOrderId, reason: CloseReason, at: u64) {
        let order = self
            .orders
            .get_mut(&id)
            .and_then(OrderSlot::order_mut)
            .expect("accepted order");
        let credit = order.close(reason, at);
        let request = order.request();
        Self::credit(
            &mut self.accounts[request.account().slot],
            request.asset(),
            credit,
        );
    }

    fn fill(&mut self, id: ClientOrderId, price: Price, quantity: Quantity, maker: bool) {
        let order = self
            .orders
            .get_mut(&id)
            .and_then(OrderSlot::order_mut)
            .expect("accepted order");
        let request = order.request();
        let wallet = &mut self.accounts[request.account().slot];
        let market = self
            .markets
            .get_mut(request.asset())
            .expect("registered asset");
        let gross = price.value(quantity.shares());
        let fee = market.fee.taker_fee(price, quantity.shares());
        let rebate = if maker {
            market
                .fee
                .maker_rebate(wallet.tier, price, quantity.shares())
        } else {
            Cash::ZERO
        };
        let direction = if request.side() == Side::Buy { 1 } else { -1 };
        self.external_cash += direction * gross.ticks() as i128;
        market.external_shares -= direction * i128::from(quantity.shares().micros());
        if maker {
            self.external_cash -= fee.ticks() as i128;
        }
        self.fee_pool += fee.ticks() - rebate.ticks();
        let charged = if maker { Cash::ZERO } else { fee };
        let credit = order.fill(
            quantity,
            gross,
            charged,
            AccruedRebates::earned(self.clock.millis() / DAY_MS, rebate),
            self.clock.millis(),
        );
        let asset = order.request().asset();
        Self::credit(wallet, asset, credit);
    }

    /// Checks cash and per-asset shares across strategy wallets, live
    /// reservations, unknown receipts, accrued rebates and external flows.
    pub fn is_conserved(&self) -> bool {
        let mut cash = self.external_cash + (self.fee_pool + self.unallocated_cash) as i128;
        let mut shares: BTreeMap<_, _> = self
            .markets
            .iter()
            .map(|(asset, market)| {
                (
                    asset,
                    market.external_shares + i128::from(market.unallocated),
                )
            })
            .collect();
        for account in &self.accounts {
            cash += (account.available + account.rebates.total()) as i128;
            for (asset, amount) in &account.shares {
                *shares.get_mut(asset).expect("registered asset") += i128::from(*amount);
            }
        }
        for slot in self.orders.values() {
            if let OrderSlot::ReservedFallback(r) = slot {
                let (reserved_cash, reserved_shares) = r.held();
                cash += reserved_cash as i128;
                *shares.get_mut(r.request.asset()).expect("registered asset") +=
                    i128::from(reserved_shares);
            }
        }
        for order in self.orders.values().filter_map(OrderSlot::order) {
            let (held_cash, held_shares) = order.held_totals();
            cash += held_cash as i128;
            *shares
                .get_mut(order.request().asset())
                .expect("registered asset") += i128::from(held_shares);
        }
        cash == self.initial_cash as i128
            && shares
                .into_iter()
                .all(|(asset, shares)| shares == i128::from(self.markets[asset].endowment))
    }
}
