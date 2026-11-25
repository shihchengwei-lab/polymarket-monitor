use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, OnceLock};

use anyhow::{Result, bail, ensure};
use num_bigint::{BigInt, Sign};
use rust_decimal::Decimal;
use tokio::time::Instant;

use self::live_paper::LivePaper;
pub use self::live_paper::PaperDesk;
pub(crate) mod frame;
pub(crate) mod input;
mod live_paper;
use crate::paper::{Error as PaperError, chaos_exchange::ChaosExchange};

use crate::gamma_discovery::{self, Discovery, GroupSpec};
use crate::wire::{Change, Event, Level, RestBook, Side, Snapshot, validate_tick};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Levels {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

const LEVEL_LIMIT: usize = 4_096;

#[derive(Debug)]
struct DepthLimit;

impl fmt::Display for DepthLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "book exceeds {LEVEL_LIMIT} active price levels")
    }
}

impl std::error::Error for DepthLimit {}

impl Levels {
    fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        let levels = Self::default().apply(
            snapshot
                .bids
                .iter()
                .copied()
                .map(|level| (Side::Buy, level))
                .chain(
                    snapshot
                        .asks
                        .iter()
                        .copied()
                        .map(|level| (Side::Sell, level)),
                ),
        )?;
        levels.validate()?;
        Ok(levels)
    }

    fn apply(mut self, changes: impl IntoIterator<Item = (Side, Level)>) -> Result<Self> {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        for (side, level) in changes {
            ensure!(
                level.price > Decimal::ZERO && level.price < Decimal::ONE,
                "price outside (0, 1): {}",
                level.price
            );
            ensure!(level.size >= Decimal::ZERO, "negative size: {}", level.size);
            match side {
                Side::Buy => &mut bids,
                Side::Sell => &mut asks,
            }
            .insert(level.price, level.size);
        }
        let mut depth = self.bids.len() + self.asks.len();
        for (levels, changes) in [(&self.bids, &bids), (&self.asks, &asks)] {
            for (price, size) in changes {
                match (levels.contains_key(price), size.is_zero()) {
                    (true, true) => depth -= 1,
                    (false, false) => depth += 1,
                    _ => {}
                }
            }
        }
        if depth > LEVEL_LIMIT {
            return Err(DepthLimit.into());
        }
        // Apply the final value per level, deleting before adding so a full
        // book can replace a level within the same exchange batch.
        for (levels, changes) in [(&mut self.bids, &bids), (&mut self.asks, &asks)] {
            for (price, size) in changes {
                if size.is_zero() {
                    levels.remove(price);
                }
            }
        }
        self.bids
            .extend(bids.into_iter().filter(|(_, size)| !size.is_zero()));
        self.asks
            .extend(asks.into_iter().filter(|(_, size)| !size.is_zero()));
        Ok(self)
    }

    fn validate(&self) -> Result<()> {
        if let (Some((bid, _)), Some((ask, _))) =
            (self.bids.last_key_value(), self.asks.first_key_value())
        {
            ensure!(bid < ask, "crossed or locked book: bid {bid}, ask {ask}");
        }
        Ok(())
    }

    fn quotes(&self) -> Quotes {
        let level = |(&price, &size)| Level { price, size };
        Quotes {
            bid: self.bids.last_key_value().map(level),
            ask: self.asks.first_key_value().map(level),
        }
    }
}

/// Immutable executable buy-side projection, shared across successive basket updates.
pub struct BuyDepth {
    pub asset: String,
    pub observed_at: Instant,
    pub tick: Decimal,
    pub bid: Option<Level>,
    pub asks: Vec<Level>,
}
/// Immutable paper-market projection from one committed subscription revision.
/// External consumers cannot replay one side under a different signal.
///
/// ```compile_fail,E0624
/// use polymarket_monitor::{book::CommittedBook, paper::chaos_exchange::ChaosExchange};
/// fn overwrite(book: &CommittedBook, engine: &mut ChaosExchange) {
///     let _ = engine.commit_book(book);
/// }
/// ```
pub struct CommittedBook {
    observed_at: Instant,
    subscription: SubscriptionId,
    revision: BookRevision,
    asset: String,
    tick: Decimal,
    bid: BidObservation,
    asks: Vec<Level>,
}
pub(crate) enum BidObservation {
    Available(Option<Level>),
    Full(Option<Vec<Level>>),
    Suspended,
}
impl CommittedBook {
    pub fn asset(&self) -> &str {
        &self.asset
    }
    pub(crate) fn quote(&self) -> (Decimal, &BidObservation, &[Level]) {
        (self.tick, &self.bid, &self.asks)
    }
    pub(crate) fn observed_at(&self) -> Instant {
        self.observed_at
    }
    pub(crate) fn tick_revision(&self) -> bool {
        matches!(self.revision, BookRevision::Tick(_))
    }
    pub(crate) fn version(&self) -> (u64, u64) {
        (self.subscription.id, self.revision.number())
    }
}
#[derive(Debug)]
struct AdditionalFailure(anyhow::Error);

impl fmt::Display for AdditionalFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "additional failure: {:#}", self.0)
    }
}

pub(crate) fn has_failure<T: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static>(
    error: &anyhow::Error,
    matches: &impl Fn(&T) -> bool,
) -> bool {
    error.downcast_ref::<T>().is_some_and(matches)
        || error
            .downcast_ref::<AdditionalFailure>()
            .is_some_and(|extra| has_failure(&extra.0, matches))
}

pub(crate) fn after_cleanup<T>(result: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (result, cleanup) {
        (Err(primary), Err(other)) => Err(primary.context(AdditionalFailure(other))),
        (outcome, Ok(())) => outcome,
        (_, Err(error)) => Err(error),
    }
}
pub type BasketUpdate = Option<Arc<[Arc<BuyDepth>]>>;
/// Ordered local observations. A clock frontier cannot overtake admitted market input.
pub enum MarketInput<'a> {
    Depth(BasketUpdate),
    Offers(Arc<CommittedBook>),
    AssetUnavailable(&'a str, u64),
    Sealed,
    Ending(&'a mut Result<Option<Instant>>),
    CancelMatured(&'a mut Result<()>),
    Trade(&'a crate::wire::PublicTrade),
    CommandCount(&'a mut usize),
    Clock(usize),
    Frontier,
    Interrupted,
}
type MarketConsumer = Box<dyn FnMut(Instant, MarketInput<'_>) + Send + Sync>;

#[derive(Debug)]
struct ReceiptFailure(Arc<anyhow::Error>);
impl fmt::Display for ReceiptFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ReceiptFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.as_ref().source()
    }
}

enum Consumer {
    Absent,
    Active(Decimal, MarketConsumer),
    PendingReceipt {
        due: Instant,
        receive: MarketConsumer,
    },
    Failed(Arc<anyhow::Error>),
}

#[derive(Clone, Copy)]
enum BookRevision {
    Depth(u64),
    Tick(u64),
}
impl BookRevision {
    fn number(self) -> u64 {
        match self {
            Self::Depth(n) | Self::Tick(n) => n,
        }
    }
}

#[derive(Clone)]
struct BookData {
    projection: OnceLock<Arc<BuyDepth>>,
    observed_at: Instant,
    levels: Levels,
    timestamp: u64,
    revision: BookRevision,
    tick: Decimal,
}

#[derive(Clone)]
enum DisconnectReason {
    Clean,
    AmbiguousSnapshot,
    SnapshotTimeout,
    Capacity,
}

impl DisconnectReason {
    fn bootstrap_failure(&self) -> Option<&'static str> {
        match self {
            Self::Clean => None,
            Self::AmbiguousSnapshot => {
                Some("initial WebSocket snapshot order could not be established")
            }
            Self::SnapshotTimeout => Some("timed out waiting for initial WebSocket book"),
            Self::Capacity => Some("WebSocket book exceeded the supported depth"),
        }
    }
}

#[derive(Clone)]
enum BookState {
    Disconnected(DisconnectReason),
    Syncing { timestamp: u64, tick: Decimal },
    Synced(BookData),
    Reconciling { book: BookData, deadline: Instant },
    Stale,
    StaleOverCapacity,
}

#[derive(Clone)]
struct TokenBook {
    published: Option<(u64, u64)>,
    asset: String,
    state: BookState,
}

impl TokenBook {
    fn committed(
        &self,
        source: SubscriptionId,
        full: bool,
        previous: Option<(u64, u64)>,
    ) -> Option<CommittedBook> {
        let (book, available) = match &self.state {
            BookState::Synced(book) => (book, true),
            BookState::Reconciling { book, .. } => (book, false),
            _ => return None,
        };
        let version = (source.id, book.revision.number());
        if previous == Some(version) {
            return None;
        }
        let levels = |map: &BTreeMap<Decimal, Decimal>| {
            map.iter()
                .map(|(&price, &size)| Level { price, size })
                .collect()
        };
        let bid = if full {
            BidObservation::Full(available.then(|| levels(&book.levels.bids)))
        } else if !available {
            BidObservation::Suspended
        } else {
            BidObservation::Available(book.levels.quotes().bid)
        };
        Some(CommittedBook {
            observed_at: book.observed_at,
            subscription: source,
            revision: book.revision,
            asset: self.asset.clone(),
            tick: book.tick,
            bid,
            asks: levels(&book.levels.asks),
        })
    }

    fn new(asset: String) -> Self {
        Self {
            asset,
            published: None,
            state: BookState::Disconnected(DisconnectReason::Clean),
        }
    }

    fn begin(&mut self, tick: Decimal) {
        self.disconnect();
        self.state = BookState::Syncing { timestamp: 0, tick };
    }

    fn invalidate(&mut self) {
        self.state = match self.state {
            BookState::Syncing { .. } => BookState::Disconnected(DisconnectReason::Clean),
            BookState::Synced(_) | BookState::Reconciling { .. } => BookState::Stale,
            BookState::Disconnected(_) | BookState::Stale | BookState::StaleOverCapacity => return,
        };
    }

    fn disconnect(&mut self) {
        self.invalidate();
        self.state = BookState::Disconnected(DisconnectReason::Clean);
    }

    fn quarantined(&self) -> bool {
        self.bootstrap_failure().is_some()
    }

    fn bootstrap_failure(&self) -> Option<&'static str> {
        match &self.state {
            BookState::Disconnected(reason) => reason.bootstrap_failure(),
            BookState::StaleOverCapacity => DisconnectReason::Capacity.bootstrap_failure(),
            _ => None,
        }
    }

    fn expire_snapshot(&mut self) {
        if matches!(self.state, BookState::Syncing { .. }) {
            self.state = BookState::Disconnected(DisconnectReason::SnapshotTimeout);
        }
    }

    fn snapshot(
        &mut self,
        snapshot: &Snapshot,
        received_at: Instant,
        deadline: Instant,
        now: impl FnOnce() -> Instant,
    ) -> Result<()> {
        let (initial, previous, revision, tick) = match &self.state {
            BookState::Syncing {
                timestamp, tick, ..
            } => (true, *timestamp, 0, *tick),
            BookState::Synced(book) | BookState::Reconciling { book, .. } => {
                (false, book.timestamp, book.revision.number() + 1, book.tick)
            }
            _ => return Ok(()),
        };
        let levels = Levels::from_snapshot(snapshot);
        let committed_at = now();
        if initial && committed_at >= deadline {
            self.expire_snapshot();
            return Ok(());
        }
        ensure!(
            snapshot.timestamp >= previous,
            "WebSocket snapshot went backwards"
        );
        let book = BookData {
            projection: OnceLock::new(),
            observed_at: received_at,
            levels: levels?,
            timestamp: snapshot.timestamp,
            revision: BookRevision::Depth(revision),
            tick,
        };
        let pending = match self.state {
            BookState::Reconciling { deadline, .. } => Some(deadline),
            _ => None,
        };
        self.commit_levels(book, pending, false, committed_at);
        Ok(())
    }

    fn commit_levels(
        &mut self,
        book: BookData,
        pending: Option<Instant>,
        crossed: bool,
        now: Instant,
    ) {
        self.state = match pending {
            Some(deadline)
                if now >= deadline
                    || crossed
                    || book.levels.bids.is_empty()
                    || book.levels.asks.is_empty() =>
            {
                BookState::Reconciling { book, deadline }
            }
            _ => BookState::Synced(book),
        };
    }

    fn changes(
        &mut self,
        timestamp: u64,
        changes: &[Change],
        received_at: Instant,
        now: impl FnOnce() -> Instant,
    ) -> Result<()> {
        match std::mem::replace(&mut self.state, BookState::Stale) {
            // Without a sequence, a future first snapshot cannot prove it covers this delta.
            BookState::Syncing { .. } => {
                self.state = BookState::Disconnected(DisconnectReason::AmbiguousSnapshot);
            }
            state @ (BookState::Synced(_) | BookState::Reconciling { .. }) => {
                let (mut book, old_deadline) = match state {
                    BookState::Synced(b) => (b, None),
                    BookState::Reconciling { book, deadline } => (book, Some(deadline)),
                    _ => unreachable!(),
                };
                ensure!(
                    timestamp >= book.timestamp,
                    "WebSocket delta went backwards"
                );
                let official_open = changes
                    .last()
                    .and_then(|c| c.best_bid.zip(c.best_ask))
                    .is_some_and(|(b, a)| Decimal::ZERO < b && b < a && a < Decimal::ONE);
                book.levels = book
                    .levels
                    .apply(changes.iter().map(|c| (c.side, c.level)))?;
                book.projection.take();
                book.observed_at = received_at;
                book.timestamp = timestamp;
                book.revision = BookRevision::Depth(book.revision.number() + 1);
                let validation = book.levels.validate();
                let crossed = validation.is_err();
                if !official_open {
                    validation?;
                }
                let deadline = old_deadline.or_else(|| {
                    crossed.then(|| received_at + std::time::Duration::from_millis(500))
                });
                self.commit_levels(book, deadline, crossed, now());
            }
            other => self.state = other,
        }
        Ok(())
    }

    fn update(&mut self, apply: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        if self.quarantined() {
            return Ok(());
        }
        let was_synced = matches!(
            self.state,
            BookState::Synced(_) | BookState::Reconciling { .. }
        );
        match apply(self) {
            Err(err) if err.is::<DepthLimit>() => {
                self.state = if was_synced {
                    BookState::StaleOverCapacity
                } else {
                    BookState::Disconnected(DisconnectReason::Capacity)
                };
                Ok(())
            }
            Err(err) => {
                self.invalidate();
                bail!("token {}: {err}", self.asset);
            }
            Ok(()) => Ok(()),
        }
    }

    fn tick(&mut self, timestamp: u64, tick: Decimal) -> Result<()> {
        validate_tick(tick)?;
        match &mut self.state {
            BookState::Syncing {
                timestamp: previous,
                tick: current,
                ..
            } => {
                ensure!(
                    timestamp >= *previous,
                    "WebSocket tick change went backwards"
                );
                *previous = timestamp;
                *current = tick;
            }
            BookState::Synced(book) | BookState::Reconciling { book, .. } => {
                ensure!(
                    timestamp >= book.timestamp,
                    "WebSocket tick change went backwards"
                );
                if book.tick != tick {
                    book.projection.take();
                    book.tick = tick;
                    book.revision = BookRevision::Tick(book.revision.number() + 1);
                }
                book.timestamp = timestamp;
            }
            _ => {}
        }
        Ok(())
    }

    fn label(&self) -> &'static str {
        match self.state {
            BookState::Disconnected(DisconnectReason::Clean) => "Disconnected",
            BookState::Syncing { .. } => "Syncing",
            BookState::Synced(_) => "Synced",
            BookState::Reconciling { .. } => "Reconciling",
            BookState::Stale => "Stale",
            BookState::StaleOverCapacity => "Stale [價位數超限，本次連線隔離]",
            BookState::Disconnected(DisconnectReason::AmbiguousSnapshot) => {
                "Disconnected [首張快照順序不明，本次連線隔離]"
            }
            BookState::Disconnected(DisconnectReason::SnapshotTimeout) => {
                "Disconnected [首張快照逾時，本次連線隔離]"
            }
            BookState::Disconnected(DisconnectReason::Capacity) => {
                "Disconnected [價位數超限，本次連線隔離]"
            }
        }
    }

    fn synced(&self) -> Option<&BookData> {
        match &self.state {
            BookState::Synced(book) => Some(book),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct Quotes {
    bid: Option<Level>,
    ask: Option<Level>,
}

impl Quotes {
    fn midpoint(self) -> Option<BigInt> {
        let numerator = |price: Decimal| price.mantissa() * 10_i128.pow(28 - price.scale());
        Some(BigInt::from(
            numerator(self.bid?.price) + numerator(self.ask?.price),
        ))
    }
}

// Midpoint numerators use a denominator of 2 × 10^28. Multiplying by 5
// converts that representation into exact units of 10^-29 for display.
const PROBABILITY_ONE: i128 = 2 * 10_i128.pow(28);

fn format_probability(numerator: BigInt) -> String {
    let scaled = numerator * 5_u8;
    let sign = if scaled.sign() == Sign::Minus {
        "-"
    } else {
        ""
    };
    let digits = scaled.magnitude().to_str_radix(10);
    let padded = format!("{digits:0>30}");
    let (whole, fraction) = padded.split_at(padded.len() - 29);
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}

impl fmt::Display for Quotes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let display = |level: Option<Level>| {
            level.map_or_else(
                || "—".to_owned(),
                |level| format!("{} × {}", level.price, level.size),
            )
        };
        write!(f, "bid {} / ask {}", display(self.bid), display(self.ask))
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    Yes,
    No,
}

struct Market {
    condition: String,
    name: String,
    initial_tick: Decimal,
    closed: bool,
    yes: TokenBook,
    no: TokenBook,
}

impl Market {
    fn books(&self) -> [&TokenBook; 2] {
        [&self.yes, &self.no]
    }

    fn book(&self, outcome: Outcome) -> &TokenBook {
        match outcome {
            Outcome::Yes => &self.yes,
            Outcome::No => &self.no,
        }
    }
}

#[derive(Default)]
struct Roster {
    markets: Vec<Market>,
    by_asset: BTreeMap<String, (usize, Outcome)>,
}

impl Roster {
    fn new(specs: Vec<crate::gamma_discovery::MarketSpec>) -> Self {
        let markets: Vec<_> = specs
            .into_iter()
            .map(|spec| {
                let (condition, name, [yes, no], initial_tick, closed) = spec.into_parts();
                Market {
                    condition,
                    name,
                    initial_tick,
                    closed,
                    yes: TokenBook::new(yes),
                    no: TokenBook::new(no),
                }
            })
            .collect();
        let by_asset = markets
            .iter()
            .enumerate()
            .flat_map(|(index, market)| {
                [
                    (market.yes.asset.clone(), (index, Outcome::Yes)),
                    (market.no.asset.clone(), (index, Outcome::No)),
                ]
            })
            .collect();
        Self { markets, by_asset }
    }

    fn market_index(&self, condition: &str) -> Option<usize> {
        self.markets
            .binary_search_by(|market| market.condition.as_str().cmp(condition))
            .ok()
    }

    fn matches(&self, spec: &GroupSpec, resolved: &HashSet<String>) -> bool {
        spec.markets().len() == self.markets.len()
            && spec
                .markets()
                .iter()
                .zip(&self.markets)
                .all(|(spec, market)| {
                    let (condition, [yes, no], closed) = spec.identity();
                    market.condition == condition
                        && market.yes.asset == *yes
                        && market.no.asset == *no
                        && (market.closed == closed || resolved.contains(condition))
                })
    }

    fn refresh(&mut self, specs: Vec<crate::gamma_discovery::MarketSpec>) {
        for (market, spec) in self.markets.iter_mut().zip(specs) {
            let (_, name, _, initial_tick, closed) = spec.into_parts();
            market.name = name;
            market.initial_tick = initial_tick;
            market.closed = closed;
        }
    }

    fn books_by_asset(&self) -> impl Iterator<Item = (&Market, &TokenBook)> {
        self.by_asset.values().map(|&(index, outcome)| {
            let market = &self.markets[index];
            (market, market.book(outcome))
        })
    }
}

enum Membership {
    Pending,
    SettlementCapacity(Option<Result<bool>>),
    Open {
        roster: Roster,
        valid_until: Instant,
    },
    Unavailable {
        roster: Roster,
        reason: String,
    },
    Closed(Roster, Option<Result<bool>>),
}

/// Failed attachment returns the exchange with all orders and reservations intact.
pub struct AttachError {
    pub reason: PaperError,
    exchange: Box<ChaosExchange>,
}
impl AttachError {
    pub fn into_exchange(self) -> ChaosExchange {
        *self.exchange
    }
}
impl fmt::Debug for AttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}
impl fmt::Display for AttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}
impl std::error::Error for AttachError {}
pub(crate) const SNAPSHOT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

// Only an installed Gamma approval can produce this request.
pub struct Subscription {
    assets: Vec<String>,
    valid_until: Instant,
}

impl Subscription {
    pub(crate) fn into_parts(self) -> (Vec<String>, Instant) {
        (self.assets, self.valid_until)
    }

    #[cfg(test)]
    pub(crate) fn for_test(valid_until: Instant) -> Self {
        Self {
            assets: vec!["1".to_owned()],
            valid_until,
        }
    }
}

/// Identifies the connection that produced a market-data frame.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SubscriptionId {
    id: u64,
    deadline: Instant,
}
pub const MAX_SETTLEMENTS: usize = 4096;
#[derive(Debug)]
pub struct SettlementCapacity;
impl fmt::Display for SettlementCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "group settlement history exceeds {MAX_SETTLEMENTS} markets"
        )
    }
}
impl std::error::Error for SettlementCapacity {}

type PendingGamma = std::pin::Pin<Box<dyn Future<Output = Result<Discovery>> + Send + Sync>>;

enum Input {
    Idle,
    Subscribed(SubscriptionId),
    Local(SubscriptionId, input::LocalInput),
    Retiring(input::LocalInput),
}

pub struct NegRiskEventGroup {
    slug: std::sync::Arc<str>,
    pending_gamma: Option<PendingGamma>,
    gamma_failed: bool,
    clock_origin: Instant,
    consumer: Consumer,
    paper: Option<LivePaper>,
    title: String,
    membership: Membership,
    next_membership: Option<Discovery>,
    resolved: HashSet<String>,
    input: Input,
}

#[derive(Debug)]
pub struct Audit {
    asset: String,
    revision: u64,
    subscription: Option<SubscriptionId>,
}

impl Audit {
    pub fn asset(&self) -> &str {
        &self.asset
    }
}

impl NegRiskEventGroup {
    pub fn new(slug: &str) -> Self {
        Self {
            slug: slug.into(),
            pending_gamma: None,
            gamma_failed: false,
            clock_origin: Instant::now(),
            consumer: Consumer::Absent,
            paper: None,
            title: slug.to_owned(),
            membership: Membership::Pending,
            next_membership: None,
            resolved: HashSet::new(),
            input: Input::Idle,
        }
    }

    pub fn with_market_consumer(
        slug: &str,
        quantity: crate::paper::Quantity,
        consumer: impl FnMut(Instant, MarketInput<'_>) + Send + Sync + 'static,
    ) -> Self {
        let mut group = Self::new(slug);
        group.consumer = Consumer::Active(quantity.shares().decimal(), Box::new(consumer));
        group
    }

    pub(crate) fn source(&self) -> Option<SubscriptionId> {
        match self.input {
            Input::Local(id, _) | Input::Subscribed(id) => Some(id),
            _ => None,
        }
    }
    pub(crate) fn has_consumer(&self) -> bool {
        matches!(self.consumer, Consumer::Active(..))
    }
    pub(crate) fn command_count(&mut self) -> usize {
        let mut count = 0;
        self.deliver(Instant::now(), MarketInput::CommandCount(&mut count));
        count
    }
    pub(crate) fn clock_at(
        &mut self,
        frontier: impl FnOnce() -> Result<Option<Instant>>,
    ) -> Result<()> {
        // Only commands visible before the ingress cut can belong to this batch.
        let count = self.command_count();
        ensure!(
            self.deliver_idle_clock(count, frontier()?),
            "crossed book reconciliation timed out"
        );
        Ok(())
    }

    pub(crate) fn deliver_idle_clock(&mut self, count: usize, at: Option<Instant>) -> bool {
        if let Some(at) = at {
            if self.reconciliation_deadline().is_some_and(|due| at >= due) {
                return false;
            }
            self.deliver(at, MarketInput::Clock(count));
        }
        true
    }

    pub(crate) async fn finish_with<T>(&mut self, result: Result<T>) -> Result<T> {
        after_cleanup(result, self.wait_for_receipt().await)
    }
    pub(crate) fn deliver(&mut self, at: Instant, input: MarketInput<'_>) {
        if let Consumer::Active(_, consumer) = &mut self.consumer {
            consumer(at, input);
        }
    }
    pub(crate) fn end_consumer(&mut self, at: Instant) {
        if !matches!(self.consumer, Consumer::Active(..)) {
            return;
        }
        let Consumer::Active(_, mut receive) =
            std::mem::replace(&mut self.consumer, Consumer::Absent)
        else {
            unreachable!()
        };
        let mut receipt = Ok(None);
        receive(at, MarketInput::Ending(&mut receipt));
        self.consumer = match receipt {
            Ok(None) => Consumer::Absent,
            Ok(Some(due))
                if due
                    <= at
                        + std::time::Duration::from_millis(crate::paper::MAX_PAPER_LATENCY_MS) =>
            {
                Consumer::PendingReceipt { due, receive }
            }
            Ok(Some(_)) => Consumer::Failed(Arc::new(anyhow::anyhow!(
                "paper receipt exceeds latency limit"
            ))),
            Err(e) => Consumer::Failed(Arc::new(e)),
        };
        self.complete_due_receipt();
    }

    /// Original deadline for a terminal cancellation still owned by this group.
    pub fn receipt_deadline(&self) -> Option<Instant> {
        match self.consumer {
            Consumer::PendingReceipt { due, .. } => Some(due),
            _ => None,
        }
    }

    fn complete_due_receipt(&mut self) {
        if self
            .receipt_deadline()
            .is_none_or(|due| Instant::now() < due)
        {
            return;
        }
        if let Consumer::PendingReceipt { due, mut receive } =
            std::mem::replace(&mut self.consumer, Consumer::Absent)
        {
            let mut result = Ok(());
            receive(due, MarketInput::CancelMatured(&mut result));
            if let Err(error) = result {
                self.consumer = Consumer::Failed(Arc::new(error));
            }
        }
    }

    /// Complete a due receipt once, without waiting. Before the deadline returns false.
    /// Synchronous callers needing final publication must advance before dropping the group.
    pub fn advance_receipt(&mut self) -> Result<bool> {
        self.complete_due_receipt();
        self.receipt_complete()
    }

    async fn wait_for_receipt(&mut self) -> Result<()> {
        if let Some(due) = self.receipt_deadline()
            && Instant::now() < due
        {
            tokio::time::sleep_until(due).await;
        }
        self.advance_receipt().map(|_| ())
    }

    /// Read the published result without advancing. A completed failure is repeatable.
    pub fn receipt_complete(&self) -> Result<bool> {
        match &self.consumer {
            Consumer::PendingReceipt { .. } => Ok(false),
            Consumer::Failed(error) => Err(anyhow::Error::new(ReceiptFailure(error.clone()))),
            _ => Ok(true),
        }
    }
    fn publish_offers(&mut self, at: Instant, source: SubscriptionId) {
        let Consumer::Active(_, consumer) = &mut self.consumer else {
            return;
        };
        let Membership::Open { roster, .. } = &mut self.membership else {
            return;
        };
        if roster.markets.len() > crate::paper::chaos_exchange::MAX_ASSETS {
            return;
        }
        for market in &mut roster.markets {
            let token = &mut market.yes;
            if let Some(book) = token.committed(source, false, token.published) {
                token.published = Some(book.version());
                consumer(at, MarketInput::Offers(Arc::new(book)));
            } else if !matches!(
                token.state,
                BookState::Synced(_) | BookState::Reconciling { .. }
            ) && let Some((generation, _)) = token.published.take()
            {
                consumer(at, MarketInput::AssetUnavailable(&token.asset, generation));
            }
        }
    }
    fn buy_depth(&self, quantity: Decimal) -> BasketUpdate {
        if self.markets().len() > crate::paper::chaos_exchange::MAX_ASSETS || !self.ready() {
            return None;
        }
        self.markets()
            .iter()
            .map(|market| {
                let book = market.yes.synced()?;
                Some(
                    book.projection
                        .get_or_init(|| {
                            let mut left = quantity;
                            let asks = book
                                .levels
                                .asks
                                .iter()
                                .map_while(|(&price, &size)| {
                                    if left <= Decimal::ZERO {
                                        return None;
                                    }
                                    left -= size.min(left);
                                    Some(Level { price, size })
                                })
                                .collect();
                            Arc::new(BuyDepth {
                                asset: market.yes.asset.clone(),
                                observed_at: book.observed_at,
                                tick: book.tick,
                                bid: book.levels.quotes().bid,
                                asks,
                            })
                        })
                        .clone(),
                )
            })
            .collect()
    }

    fn roster(&self) -> Option<&Roster> {
        match &self.membership {
            Membership::SettlementCapacity(_) | Membership::Pending => None,
            Membership::Open { roster, .. }
            | Membership::Unavailable { roster, .. }
            | Membership::Closed(roster, _) => Some(roster),
        }
    }

    fn roster_mut(&mut self) -> Option<&mut Roster> {
        match &mut self.membership {
            Membership::SettlementCapacity(_) | Membership::Pending => None,
            Membership::Open { roster, .. }
            | Membership::Unavailable { roster, .. }
            | Membership::Closed(roster, _) => Some(roster),
        }
    }

    fn markets(&self) -> &[Market] {
        self.roster().map_or(&[], |roster| &roster.markets)
    }

    fn market_closed(&self, market: &Market) -> bool {
        market.closed || self.resolved.contains(&market.condition)
    }

    fn open_books(&self) -> impl Iterator<Item = &TokenBook> {
        self.roster()
            .into_iter()
            .flat_map(Roster::books_by_asset)
            .filter_map(|(market, book)| {
                (matches!(self.membership, Membership::Open { .. }) && !self.market_closed(market))
                    .then_some(book)
            })
    }

    fn assets(&self) -> Vec<String> {
        self.open_books().map(|book| book.asset.clone()).collect()
    }

    pub fn approval_is_current(&self, now: Instant) -> bool {
        self.next_membership.is_none()
            && matches!(self.membership, Membership::Open { valid_until, .. } if now < valid_until)
    }

    pub fn subscription(&self) -> Option<Subscription> {
        let Membership::Open { valid_until, .. } = self.membership else {
            return None;
        };
        if !self.approval_is_current(Instant::now()) {
            return None;
        }
        let assets = self.assets();
        (!assets.is_empty()).then_some(Subscription {
            assets,
            valid_until,
        })
    }

    pub fn has_open_assets(&self) -> bool {
        self.open_books().next().is_some()
    }

    #[cfg(test)]
    pub(crate) fn test_source(&self) -> SubscriptionId {
        self.source().unwrap()
    }
    #[cfg(test)]
    pub(crate) fn begin(&mut self) -> Option<SubscriptionId> {
        let now = Instant::now();
        self.begin_at(now, now + SNAPSHOT_TIMEOUT)
    }

    pub(crate) fn begin_at(
        &mut self,
        admitted_at: Instant,
        deadline: Instant,
    ) -> Option<SubscriptionId> {
        if !matches!(self.input, Input::Idle)
            || !self.approval_is_current(admitted_at)
            || !self.has_open_assets()
        {
            return None;
        }
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = SubscriptionId {
            deadline,
            id: NEXT
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .expect("subscription identity exhausted"),
        };
        self.input = Input::Subscribed(id);
        if let Membership::Open { roster, .. } = &mut self.membership {
            for market in &mut roster.markets {
                if !market.closed && !self.resolved.contains(&market.condition) {
                    market.yes.begin(market.initial_tick);
                    market.no.begin(market.initial_tick);
                }
            }
        }
        self.publish_paper();
        Some(id)
    }

    fn books_mut(&mut self) -> impl Iterator<Item = &mut TokenBook> {
        self.roster_mut()
            .into_iter()
            .flat_map(|roster| &mut roster.markets)
            .flat_map(|market| [&mut market.yes, &mut market.no])
    }

    pub(crate) fn invalidate(&mut self) {
        self.retire_at(Instant::now(), false);
    }
    fn retire_at(&mut self, at: Instant, end_session: bool) {
        let active = self.source().is_some();
        self.input = match std::mem::replace(&mut self.input, Input::Idle) {
            Input::Local(_, input) | Input::Retiring(input) => Input::Retiring(input),
            _ => Input::Idle,
        };
        if end_session {
            self.end_consumer(at);
        } else if active {
            self.deliver(at, MarketInput::Interrupted);
        }
        self.books_mut().for_each(TokenBook::invalidate);
        self.publish_paper();
    }

    pub(crate) fn disconnect(&mut self) {
        self.invalidate();
        self.books_mut().for_each(TokenBook::disconnect);
        self.publish_paper();
    }

    fn market_synced(&self, market: &Market) -> bool {
        !self.market_closed(market)
            && market
                .books()
                .into_iter()
                .all(|book| book.synced().is_some())
    }

    pub fn ready(&self) -> bool {
        matches!(&self.membership, Membership::Open { roster, .. }
            if roster.markets.iter().all(|market| self.market_synced(market)))
    }

    /// Transfers exclusive ownership of an unused paper exchange to this feed.
    pub fn attach_paper(
        &mut self,
        exchange: ChaosExchange,
    ) -> std::result::Result<(), AttachError> {
        if self.paper.is_some() || !exchange.can_attach() {
            return Err(AttachError {
                reason: PaperError::AlreadyStarted,
                exchange: Box::new(exchange),
            });
        }
        self.paper = Some(LivePaper::new(exchange, self.clock_origin));
        self.publish_paper();
        Ok(())
    }
    pub fn paper(&mut self) -> Option<PaperDesk<'_>> {
        self.paper.as_ref()?;
        Some(PaperDesk { group: self })
    }

    fn publish_paper(&mut self) {
        if let Some(mut live) = self.paper.take() {
            live.publish(self);
            self.paper = Some(live);
        }
    }

    pub fn awaiting_snapshot(&self) -> bool {
        self.open_books()
            .any(|book| matches!(book.state, BookState::Syncing { .. }))
    }

    pub fn snapshot_deadline(&self) -> Option<Instant> {
        self.source()
            .filter(|_| self.awaiting_snapshot())
            .map(|s| s.deadline)
    }
    pub fn reconciliation_deadline(&self) -> Option<Instant> {
        self.open_books()
            .filter_map(|b| match b.state {
                BookState::Reconciling { deadline, .. } => Some(deadline),
                _ => None,
            })
            .min()
    }
    pub(crate) fn expired_reconciliation(&self) -> Option<Instant> {
        self.reconciliation_deadline()
            .filter(|deadline| Instant::now() >= *deadline)
    }
    pub(crate) fn expire_snapshots(&mut self) {
        if self
            .snapshot_deadline()
            .is_none_or(|deadline| Instant::now() < deadline)
        {
            return;
        }
        self.books_mut().for_each(TokenBook::expire_snapshot);
        self.publish_paper();
    }

    pub fn has_quarantined_books(&self) -> bool {
        self.open_books().any(TokenBook::quarantined)
    }

    pub fn event_closed(&self) -> bool {
        matches!(self.membership, Membership::Closed(..))
    }

    pub fn awaiting_gamma(&self) -> bool {
        matches!(self.membership, Membership::Pending)
    }

    pub fn once_failure(&self) -> Option<&str> {
        match &self.membership {
            Membership::SettlementCapacity(_) => Some("settlement history capacity exceeded"),
            Membership::Pending => None,
            Membership::Closed(..) => Some("the event is closed"),
            Membership::Unavailable { reason, .. } => Some(reason),
            Membership::Open { roster, .. }
                if roster
                    .markets
                    .iter()
                    .any(|market| self.market_closed(market)) =>
            {
                Some("the event contains a closed market")
            }
            Membership::Open { .. } => self.open_books().find_map(TokenBook::bootstrap_failure),
        }
    }

    pub(crate) fn check_capacity(&self) -> std::result::Result<(), SettlementCapacity> {
        if matches!(self.membership, Membership::SettlementCapacity(_)) {
            Err(SettlementCapacity)
        } else {
            Ok(())
        }
    }

    fn resolve(&mut self, condition: &str) -> std::result::Result<(), SettlementCapacity> {
        self.check_capacity()?;
        if self.resolved.contains(condition) {
            return Ok(());
        }
        let Some(index) = self.roster().and_then(|r| r.market_index(condition)) else {
            return Ok(());
        };
        if self.resolved.len() == MAX_SETTLEMENTS {
            self.cancel_gamma_refresh();
            self.membership = Membership::SettlementCapacity(None);
            self.next_membership = None;
            return Err(SettlementCapacity);
        }
        let market = &mut self.roster_mut().expect("known condition").markets[index];
        market.yes.invalidate();
        market.no.invalidate();
        self.resolved.insert(condition.to_owned());
        self.publish_paper();
        Ok(())
    }

    /// Fetch and commit one Gamma decision. When another select branch wins,
    /// the group keeps the unfinished HTTP future. The next call resumes it
    /// with the original client, endpoint and TTL.
    /// Returns true when membership changes. An HTTP failure leaves the
    /// previous approval in place and returns its error.
    ///
    /// ```compile_fail,E0499
    /// use polymarket_monitor::book::NegRiskEventGroup;
    /// async fn overlapping(group: &mut NegRiskEventGroup, client: &reqwest::Client) {
    ///     let ttl = std::time::Duration::from_secs(30);
    ///     let first = group.refresh_discovery(client, "http://localhost", ttl);
    ///     let second = group.refresh_discovery(client, "http://localhost", ttl);
    ///     tokio::join!(first, second);
    /// }
    /// ```
    pub async fn refresh_discovery(
        &mut self,
        client: &reqwest::Client,
        base: &str,
        ttl: std::time::Duration,
    ) -> Result<bool> {
        if self.terminal_delivery().is_some() {
            return self.finish_discovery().await;
        }
        let result = self.receive_discovery(client, base, ttl).await;
        if let Err(err) = result {
            self.gamma_failed = true;
            return Err(err);
        }
        let changed = self.update_discovery(result);
        if let Some(delivery) = self.terminal_delivery() {
            *delivery = Some(changed);
            return self.finish_discovery().await;
        }
        changed
    }

    // Only a terminal membership can own a committed Gamma result.
    fn terminal_delivery(&mut self) -> Option<&mut Option<Result<bool>>> {
        match &mut self.membership {
            Membership::Closed(_, delivery) | Membership::SettlementCapacity(delivery) => {
                Some(delivery)
            }
            _ => None,
        }
    }

    pub(crate) fn take_terminal_delivery(&mut self) -> Option<Result<bool>> {
        self.terminal_delivery()?.take()
    }

    async fn finish_discovery(&mut self) -> Result<bool> {
        let receipt = self.wait_for_receipt().await;
        // Taking the result after the last await makes delivery cancel-safe.
        let update = self
            .take_terminal_delivery()
            .unwrap_or_else(|| self.check_capacity().map(|()| false).map_err(Into::into));
        after_cleanup(update, receipt)
    }

    #[cfg(test)]
    pub(crate) fn pending_gamma_fixture(
        &mut self,
        future: impl std::future::Future<Output = Result<Discovery>> + Send + Sync + 'static,
    ) {
        self.pending_gamma = Some(Box::pin(future));
    }

    /// Discard an unfinished refresh without changing the installed roster.
    pub fn cancel_gamma_refresh(&mut self) {
        self.pending_gamma = None;
    }

    pub(crate) async fn receive_discovery(
        &mut self,
        client: &reqwest::Client,
        base: &str,
        ttl: std::time::Duration,
    ) -> Result<Discovery> {
        let pending = self.pending_gamma.get_or_insert_with(|| {
            let (client, base, slug) = (client.clone(), base.to_owned(), self.slug.clone());
            Box::pin(async move { gamma_discovery::discover(&client, &base, &slug, ttl).await })
        });
        let result = pending.await;
        self.pending_gamma = None;
        result
    }

    pub(crate) fn observe_discovery(&mut self, result: Result<Discovery>) -> bool {
        if self.check_capacity().is_err()
            || self.event_closed()
            || matches!(self.next_membership, Some(Discovery::Closed))
        {
            return false;
        }
        self.gamma_failed = result.is_err();
        let Ok(decision) = result else { return false };
        if matches!(&decision, Discovery::Open(spec) if Instant::now() >= spec.valid_until()) {
            return false;
        }
        let change = match decision {
            Discovery::Open(spec) => {
                if let Membership::Open {
                    roster,
                    valid_until,
                } = &mut self.membership
                    && roster.matches(&spec, &self.resolved)
                {
                    let (title, specs, deadline) = spec.into_parts();
                    roster.refresh(specs);
                    self.title = title;
                    *valid_until = deadline;
                    None
                } else {
                    Some(Discovery::Open(spec))
                }
            }
            other => Some(other),
        };
        if let Some(update) = change {
            self.next_membership = Some(update);
            true
        } else {
            self.publish_paper();
            false
        }
    }

    fn install_discovery(&mut self) {
        let Some(next) = self.next_membership.take() else {
            return;
        };
        let roster = self.take_roster();
        self.membership = match next {
            Discovery::Open(spec) => {
                let (title, specs, valid_until) = spec.into_parts();
                self.title = title;
                Membership::Open {
                    roster: Roster::new(specs),
                    valid_until,
                }
            }
            Discovery::Unavailable(reason) => Membership::Unavailable {
                roster,
                reason: format!("{reason:#}"),
            },
            Discovery::Closed => Membership::Closed(roster, None),
        };
        self.publish_paper();
    }

    fn take_roster(&mut self) -> Roster {
        match std::mem::replace(&mut self.membership, Membership::Pending) {
            Membership::SettlementCapacity(_) | Membership::Pending => Roster::default(),
            Membership::Open { roster, .. }
            | Membership::Unavailable { roster, .. }
            | Membership::Closed(roster, _) => roster,
        }
    }

    #[cfg(test)]
    fn update(
        &mut self,
        asset: &str,
        condition: &str,
        apply: impl FnOnce(&mut TokenBook) -> Result<()>,
    ) -> Result<()> {
        let Membership::Open { roster, .. } = &mut self.membership else {
            return Ok(());
        };
        let Some(&(index, outcome)) = roster.by_asset.get(asset) else {
            return Ok(());
        };
        let market = &mut roster.markets[index];
        if market.closed || self.resolved.contains(&market.condition) {
            return Ok(());
        }
        let expected_condition = &market.condition;
        let book = match outcome {
            Outcome::Yes => &mut market.yes,
            Outcome::No => &mut market.no,
        };
        book.update(|book| {
            ensure!(
                expected_condition == condition,
                "unexpected condition ID {condition}"
            );
            apply(book)
        })
    }

    #[cfg(test)]
    pub(crate) fn apply_received(
        &mut self,
        source: SubscriptionId,
        event: Event,
        at: Instant,
    ) -> Result<()> {
        if self.source() == Some(source) {
            self.commit_frame(source, vec![event], at)?;
        }
        Ok(())
    }

    pub fn audits(&self) -> Vec<Audit> {
        self.open_books()
            .filter_map(|book| {
                Some(Audit {
                    asset: book.asset.clone(),
                    revision: book.synced()?.revision.number(),
                    subscription: self.source(),
                })
            })
            .collect()
    }

    pub fn audit(&self, request: &Audit, response: Result<Option<RestBook>>) -> Result<()> {
        let Membership::Open { roster, .. } = &self.membership else {
            return Ok(());
        };
        let Some(&(index, outcome)) = roster.by_asset.get(request.asset()) else {
            return Ok(());
        };
        let market = &roster.markets[index];
        let Some(book) = market.book(outcome).synced() else {
            return Ok(());
        };
        if request.subscription != self.source() || request.revision != book.revision.number() {
            return Ok(());
        }
        let response =
            response?.ok_or_else(|| anyhow::anyhow!("no REST book for subscribed asset"))?;
        let snapshot = response.snapshot;
        snapshot.validate_size()?;
        ensure!(
            snapshot.asset_id == request.asset && snapshot.market == market.condition,
            "REST snapshot identity does not match request"
        );
        validate_tick(response.tick_size)?;
        // REST and WS expose timestamps, but their snapshot boundaries are not known to coincide.
        ensure!(
            Levels::from_snapshot(&snapshot)? == book.levels,
            "REST and WebSocket levels differ"
        );
        ensure!(
            response.tick_size == book.tick,
            "REST tick {} differs from WebSocket tick {}",
            response.tick_size,
            book.tick
        );
        Ok(())
    }

    fn midpoint(book: &TokenBook) -> Option<BigInt> {
        book.synced()?.levels.quotes().midpoint()
    }

    fn yes_sum(&self, ready: bool) -> Option<BigInt> {
        if !ready {
            return None;
        }
        self.markets()
            .iter()
            .map(|market| Self::midpoint(&market.yes))
            .sum()
    }

    pub(crate) fn retiring_view(&self) -> String {
        format!(
            "{} | NotReady (0/{} 子市場)\n",
            crate::output::single_line(&self.title),
            self.markets().len()
        )
    }

    pub fn render(&self) -> String {
        use crate::output::single_line;
        use std::fmt::Write;

        let markets = self.markets();
        let synced = markets
            .iter()
            .filter(|market| self.market_synced(market))
            .count();
        let ready = matches!(self.membership, Membership::Open { .. }) && synced == markets.len();
        let status = if ready { "Ready" } else { "NotReady" };
        let mut output = format!(
            "{} | {status} ({synced}/{} 子市場)\n",
            single_line(&self.title),
            markets.len()
        );
        match &self.membership {
            Membership::Unavailable { reason, .. } => {
                let _ = writeln!(output, "Gamma 成員不可用：{}", single_line(reason));
            }
            Membership::Closed(..) => {
                let _ = writeln!(output, "事件已結束");
            }
            Membership::SettlementCapacity(_) => {
                let _ = writeln!(output, "{SettlementCapacity}");
            }
            Membership::Pending => {
                let _ = writeln!(output, "尚未取得 Gamma 成員資料");
            }
            Membership::Open { .. } => {}
        }
        if self.gamma_failed {
            let status = if self.awaiting_gamma() {
                "等待重新查詢"
            } else {
                "使用上次確認的資料"
            };
            let _ = writeln!(output, "Gamma 更新失敗，{status}");
        }
        let sum = self.yes_sum(ready).map_or_else(
            || "—".to_owned(),
            |sum| {
                let difference = &sum - PROBABILITY_ONE;
                format!(
                    "{} (與 1 的差 {})",
                    format_probability(sum),
                    format_probability(difference)
                )
            },
        );
        let _ = writeln!(output, "YES 中間價總和 {sum}");
        for market in markets {
            let _ = writeln!(
                output,
                "{}{}",
                single_line(&market.name),
                if self.event_closed() || self.market_closed(market) {
                    " [已關閉]"
                } else {
                    ""
                }
            );
            for (outcome, book) in ["YES", "NO"].into_iter().zip(market.books()) {
                let _ = write!(output, "  {outcome} {}", book.label());
                if let Some(synced) = book.synced() {
                    let _ = write!(
                        output,
                        " | {} | tick {}",
                        synced.levels.quotes(),
                        synced.tick
                    );
                }
                let _ = writeln!(output);
            }
            if ready {
                let gap = Self::midpoint(&market.yes)
                    .zip(Self::midpoint(&market.no))
                    .map_or_else(
                        || "—".to_owned(),
                        |(yes, no)| {
                            let difference = yes + no - PROBABILITY_ONE;
                            if difference.sign() == Sign::NoSign {
                                "符合".to_owned()
                            } else {
                                format!("偏差 {}", format_probability(difference))
                            }
                        },
                    );
                let _ = writeln!(output, "  YES + NO 補數檢核 {gap}");
            }
        }
        output
    }
}

#[cfg(test)]
mod tests {
    mod gamma_order;
    use super::*;
    use crate::gamma_discovery::fixture_spec;
    use crate::wire::events;

    fn spec() -> GroupSpec {
        fixture_spec(&[
            ("a", "1", "2", Decimal::new(1, 2), false),
            ("b", "3", "4", Decimal::new(1, 2), false),
        ])
    }

    impl NegRiskEventGroup {
        fn fixture_discovery(&mut self, result: Result<Discovery>) -> bool {
            self.update_discovery(result).unwrap()
        }
        fn apply(&mut self, source: SubscriptionId, event: Event) -> Result<()> {
            self.apply_received(source, event, Instant::now())
        }
        fn test_book(&self, asset: &str) -> &TokenBook {
            let roster = self.roster().unwrap();
            let (index, outcome) = roster.by_asset[asset];
            roster.markets[index].book(outcome)
        }

        fn test_books(&self) -> impl Iterator<Item = &TokenBook> {
            self.markets().iter().flat_map(Market::books)
        }
    }

    fn open_group() -> NegRiskEventGroup {
        let mut group = NegRiskEventGroup::new("fixture");
        group.fixture_discovery(Ok(Discovery::Open(spec())));
        group
    }

    fn apply(group: &mut NegRiskEventGroup, frame: &str) -> Result<()> {
        for event in events(frame)? {
            group.apply(group.test_source(), event)?;
        }
        Ok(())
    }

    fn snapshot(asset: &str, market: &str, timestamp: u64) -> String {
        format!(
            r#"{{
            "event_type":"book","market":"{market}","asset_id":"{asset}","timestamp":"{timestamp}",
            "bids":[{{"price":"0.40","size":"5"}}],"asks":[{{"price":"0.60","size":"7"}}]
        }}"#
        )
    }

    fn delta(timestamp: u64, price: &str, size: &str, side: &str) -> String {
        format!(
            r#"{{
            "event_type":"price_change","market":"a","timestamp":"{timestamp}","price_changes":[
                {{"asset_id":"1","price":"{price}","size":"{size}","side":"{side}"}}
            ]
        }}"#
        )
    }

    fn ready_group() -> NegRiskEventGroup {
        let mut group = open_group();
        group.begin().unwrap();
        for (asset, market) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
            assert!(!group.ready());
            apply(&mut group, &snapshot(asset, market, 100)).unwrap();
        }
        assert!(group.ready());
        assert_eq!(
            group.yes_sum(group.ready()),
            Some(BigInt::from(PROBABILITY_ONE))
        );
        assert!(group.render().contains("補數檢核 符合"));
        group
    }

    #[test]
    fn snapshot_interleaving_keeps_socket_order_and_rejects_late_rest_overwrite() {
        let mut group = open_group();
        group.begin().unwrap();
        apply(&mut group, &format!("[{}]", snapshot("1", "a", 100))).unwrap();
        let audit = group.audits().pop().unwrap();
        apply(&mut group, &delta(100, "0.42", "11", "BUY")).unwrap();
        let rest: RestBook = serde_json::from_str(
            r#"{
            "market":"a","asset_id":"1","timestamp":"100","tick_size":"0.05",
            "bids":[{"price":"0.40","size":"5"}],"asks":[{"price":"0.60","size":"7"}]
        }"#,
        )
        .unwrap();
        group.audit(&audit, Ok(Some(rest))).unwrap();
        let book = group.test_book("1").synced().unwrap();
        assert_eq!(book.levels.bids.len(), 2);
        assert_eq!(book.levels.quotes().bid.unwrap().price, Decimal::new(42, 2));
        assert_eq!(book.tick, Decimal::new(1, 2));
    }

    #[test]
    fn ambiguous_first_book_isolates_only_the_hot_token_until_a_fresh_subscription() {
        for timestamp in [99, 100, 101] {
            let mut group = open_group();
            group.begin().unwrap();
            apply(&mut group, &delta(100, "0.41", "9", "BUY")).unwrap();
            apply(&mut group, &snapshot("1", "a", timestamp)).unwrap();
            for n in 101..=200 {
                apply(&mut group, &delta(n, "0.42", "11", "BUY")).unwrap();
            }
            for (asset, market) in [("2", "a"), ("3", "b"), ("4", "b")] {
                apply(&mut group, &snapshot(asset, market, 100)).unwrap();
                assert!(group.test_book(asset).synced().is_some());
            }
            assert!(group.test_book("1").quarantined());
            assert!(!group.awaiting_snapshot());
            assert!(!group.ready());
            group.disconnect();
            group.begin().unwrap();
            apply(&mut group, &snapshot("1", "a", 100)).unwrap();
            assert!(group.test_book("1").synced().is_some());
        }
    }

    #[test]
    fn zero_size_removes_last_bid_and_ask_without_inventing_a_midpoint() {
        let mut group = ready_group();
        apply(&mut group, &delta(101, "0.40", "0.000", "BUY")).unwrap();
        apply(&mut group, &delta(102, "0.60", "0", "SELL")).unwrap();
        let quotes = group.test_book("1").synced().unwrap().levels.quotes();
        assert!(quotes.bid.is_none() && quotes.ask.is_none());
        assert!(group.ready());
        assert_eq!(group.yes_sum(group.ready()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn locked_delta_hides_quotes_until_clear_or_original_deadline() {
        use tokio::time::{Duration, advance};
        for clears in [true, false] {
            let mut group = ready_group();
            let source = group.test_source();
            let initial = snapshot("1", "a", 101).replace("0.40", "0.003").replace(
                r#"{"price":"0.60","size":"7"}"#,
                r#"{"price":"0.004","size":"7"},{"price":"0.005","size":"8"}"#,
            );
            apply(&mut group, &initial).unwrap();
            let lock = delta(102, "0.004", "9", "BUY").replace(
                r#""side":"BUY""#,
                r#""side":"BUY","best_bid":"0.004","best_ask":"0.005""#,
            );
            apply(&mut group, &lock).unwrap();
            assert!(!group.ready());
            assert!(group.test_book("1").synced().is_none());
            assert!(group.buy_depth(25.into()).is_none());
            assert!(group.audits().iter().all(|a| a.asset() != "1"));
            let deadline = group.reconciliation_deadline().unwrap();
            if clears {
                advance(Duration::from_millis(20)).await;
                apply(&mut group, &delta(103, "0.004", "0", "SELL")).unwrap();
                assert!(group.ready());
                assert_eq!(group.source(), Some(source));
                assert!(group.reconciliation_deadline().is_none());
            } else {
                advance(Duration::from_millis(499)).await;
                apply(&mut group, &lock).unwrap();
                assert_eq!(group.reconciliation_deadline(), Some(deadline));
                advance(Duration::from_millis(1)).await;
                assert_eq!(group.expired_reconciliation(), Some(deadline));
                assert!(!group.ready());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_and_delta_use_the_commit_clock_to_resume() {
        use std::time::Duration;
        for snapshot_repair in [true, false] {
            for millis in [499, 500, 501] {
                let mut group = ready_group();
                let lock = delta(101, "0.60", "9", "BUY").replace(
                    r#""side":"BUY""#,
                    r#""side":"BUY","best_bid":"0.60","best_ask":"0.61""#,
                );
                apply(&mut group, &lock).unwrap();
                let start = Instant::now();
                let commit = || start + Duration::from_millis(millis);
                let result = group.update("1", "a", |book| {
                    if snapshot_repair {
                        let Event::Book(s) = events(&snapshot("1", "a", 102))?.pop().unwrap()
                        else {
                            unreachable!()
                        };
                        book.snapshot(&s, start, start + SNAPSHOT_TIMEOUT, commit)
                    } else {
                        let Event::PriceChange { price_changes, .. } =
                            events(&delta(102, "0.60", "0", "BUY"))?.pop().unwrap()
                        else {
                            unreachable!()
                        };
                        book.changes(102, &price_changes, start, commit)
                    }
                });
                result.unwrap();
                assert_eq!(group.ready(), millis < 500);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_expiry_releases_public_input_and_drains_every_accepted_settlement() {
        for settlements in [true, false] {
            let mut group = open_group();
            let source = group.open_input().unwrap();
            for (asset, market) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
                group
                    .admit_raw(source, snapshot(asset, market, 100))
                    .unwrap();
                group.advance_input(source).unwrap();
            }
            let lock = delta(101, "0.60", "9", "BUY").replace(
                r#""side":"BUY""#,
                r#""side":"BUY","best_bid":"0.60","best_ask":"0.61""#,
            );
            group.admit_raw(source, lock).unwrap();
            group.advance_input(source).unwrap();
            if settlements {
                group
                    .admit_event(source, Event::MarketResolved { market: "a".into() })
                    .unwrap();
            }
            tokio::time::advance(std::time::Duration::from_millis(500)).await;
            assert!(group.advance_input(source).is_err());
            assert!(!group.admit_event(source, Event::Other).unwrap());
            assert_eq!(group.resolved.contains("a"), settlements);
            assert!(group.open_input().is_some());
        }
    }
    #[tokio::test(start_paused = true)]
    async fn first_popped_settlement_and_queued_settlement_survive_timeout() {
        let mut group = open_group();
        let spec = fixture_spec(&[
            ("a", "1", "2", Decimal::new(1, 2), false),
            ("b", "3", "4", Decimal::new(1, 2), false),
            ("c", "5", "6", Decimal::new(1, 2), false),
        ]);
        group.fixture_discovery(Ok(Discovery::Open(spec)));
        let source = group.begin().unwrap();
        apply(&mut group, &snapshot("1", "a", 100)).unwrap();
        let lock = delta(101, "0.60", "9", "BUY").replace(
            r#""side":"BUY""#,
            r#""side":"BUY","best_bid":"0.60","best_ask":"0.61""#,
        );
        apply(&mut group, &lock).unwrap();
        let (mut wire, _tx) = crate::transport::Transport::buffered_for_test([
            r#"{"event_type":"market_resolved","market":"a"}"#.into(),
            r#"{"event_type":"market_resolved","market":"b"}"#.into(),
        ]);
        let first = wire.frames.recv().await.unwrap();
        tokio::time::advance(std::time::Duration::from_millis(500)).await;
        assert_eq!(group.source(), Some(source));
        let (tail, _) = wire.seal();
        let mut retired = input::Retirement::new(
            &mut group,
            Some(first),
            (tail, Ok(())),
            input::End::Reconnect,
        );
        while retired.advance() {}
        assert!(
            retired
                .complete()
                .unwrap_err()
                .to_string()
                .contains("reconciliation timed out")
        );
        assert_eq!(group.resolved, HashSet::from(["a".into(), "b".into()]));
        assert!(group.open_input().is_some());
        assert_eq!(group.assets(), ["5", "6"]);
    }

    #[tokio::test(start_paused = true)]
    async fn sealed_local_fifo_preserves_the_prefix_at_gamma_or_close_and_reopens_empty() {
        use std::sync::Mutex;
        for rebind in [true, false] {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&seen);
            let mut group = NegRiskEventGroup::with_market_consumer(
                "fixture",
                crate::paper::Quantity::new(25.into()).unwrap(),
                move |at, input| {
                    let label = match input {
                        MarketInput::Sealed => "seal",
                        MarketInput::Trade(_) => "fill",
                        MarketInput::Interrupted => "gap",
                        MarketInput::Frontier => "frontier",
                        _ => return,
                    };
                    observed.lock().unwrap().push((label, at));
                },
            );
            group.fixture_discovery(Ok(Discovery::Open(spec())));
            let source = group.open_input().unwrap();
            let t = Instant::now();
            group.admit_raw(source,r#"{"event_type":"last_trade_price","asset_id":"1","price":".40","size":"7","side":"SELL","timestamp":"1"}"#.into()).unwrap();
            if rebind {
                group.fixture_discovery(Ok(Discovery::Open(fixture_spec(&[
                    ("a", "1", "2", Decimal::new(1, 2), false),
                    ("c", "5", "6", Decimal::new(1, 2), false),
                ]))));
            } else {
                assert!(group.close_input(source).unwrap());
            }
            assert_eq!(
                *seen.lock().unwrap(),
                [("seal", t), ("fill", t), ("gap", t)]
            );
            assert!(!group.admit_event(source, Event::Other).unwrap());
            let next = group.open_input().unwrap();
            assert!(!group.ready());
            let assets = group.assets();
            for (index, asset) in assets.iter().enumerate() {
                let condition = match asset.as_str() {
                    "1" | "2" => "a",
                    "3" | "4" => "b",
                    _ => "c",
                };
                group
                    .admit_raw(next, snapshot(asset, condition, 100))
                    .unwrap();
                group.advance_input(next).unwrap();
                assert_eq!(group.ready(), index + 1 == assets.len());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn empty_and_unknown_frames_advance_time_and_bad_frame_uses_ingress_instant() {
        use std::sync::Mutex;
        let origin = Instant::now();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let capture = Arc::clone(&seen);
        let mut group = NegRiskEventGroup::with_market_consumer(
            "fixture",
            crate::paper::Quantity::new(25.into()).unwrap(),
            move |at, event| {
                let kind = match event {
                    MarketInput::Frontier => "time",
                    MarketInput::Interrupted => "gap",
                    _ => return,
                };
                capture
                    .lock()
                    .unwrap()
                    .push((kind, (at - origin).as_millis()));
            },
        );
        group.fixture_discovery(Ok(Discovery::Open(spec())));
        let source = group.open_input().unwrap();
        for (t, raw) in [
            (80, "[]"),
            (90, r#"{"event_type":"other"}"#),
            (95, r#"{"event_type":"book","bids":"bad"}"#),
            (100, "[]"),
        ] {
            tokio::time::sleep_until(origin + std::time::Duration::from_millis(t)).await;
            group.admit_raw(source, raw.into()).unwrap();
        }
        tokio::time::sleep_until(origin + std::time::Duration::from_millis(110)).await;
        assert!(group.advance_input(source).unwrap());
        assert!(group.advance_input(source).unwrap());
        assert!(group.advance_input(source).is_err());
        assert_eq!(
            *seen.lock().unwrap(),
            [("time", 80), ("time", 90), ("gap", 95)]
        );
    }

    #[test]
    fn crossed_single_leg_invalidates_group_and_hides_bad_levels() {
        let mut group = ready_group();
        let Event::PriceChange { price_changes, .. } =
            events(&delta(101, "0.61", "3", "BUY")).unwrap().remove(0)
        else {
            unreachable!()
        };
        assert!(
            group
                .update("1", "a", |b| b.changes(
                    101,
                    &price_changes,
                    Instant::now(),
                    Instant::now
                ))
                .is_err()
        );
        assert_eq!(group.test_book("1").label(), "Stale");
        assert_eq!(group.test_book("2").label(), "Synced");
        assert_eq!(group.yes_sum(group.ready()), None);
        assert!(!group.ready());
    }

    #[test]
    fn ready_depth_allows_ioc() {
        use crate::paper::order_fsm::{CloseReason, Delivery, OrderStatus};
        let (mut group, account, asset) = funded_group();
        let order = buy_one(account, asset.clone());
        let mut desk = group.paper().unwrap();
        let stamp = desk.current_stamp(&asset).unwrap().unwrap();
        let receipt = desk
            .submit(order.clone(), stamp, Delivery::Delivered)
            .unwrap();
        assert_eq!(
            receipt.status(),
            OrderStatus::Closed {
                filled: order.quantity().shares(),
                reason: CloseReason::Filled
            }
        );
        let balance = desk.balance(account, &asset).unwrap();
        assert_eq!(balance.cash().decimal(), Decimal::new(94, 1));
        assert_eq!(balance.shares(), order.quantity().shares());
    }

    #[tokio::test(start_paused = true)]
    async fn only_current_approval_can_start_a_subscription() {
        let mut group = open_group();
        let expires = spec().valid_until();
        tokio::time::sleep_until(expires).await;
        assert!(group.begin().is_none());
        group.fixture_discovery(Ok(Discovery::Open(spec())));
        assert!(group.approval_is_current(Instant::now()));
        let closed = Ok(Discovery::Closed);
        assert!(group.observe_discovery(closed));
        assert!(group.begin().is_none());
        group.install_discovery();
        assert!(!group.fixture_discovery(Ok(Discovery::Open(spec()))));
        assert!(group.event_closed());
        assert!(group.begin().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn first_snapshot_expires_at_the_subscriptions_own_deadline() {
        let mut group = open_group();
        let source = group.begin().unwrap();
        for (asset, market) in [("2", "a"), ("3", "b"), ("4", "b")] {
            apply(&mut group, &snapshot(asset, market, 100)).unwrap();
        }
        let received = Instant::now();
        tokio::time::advance(SNAPSHOT_TIMEOUT).await;
        group
            .apply_received(
                source,
                events(&snapshot("1", "a", 100)).unwrap().remove(0),
                received,
            )
            .unwrap();
        assert!(group.test_book("1").quarantined());
        assert!(group.test_book("2").synced().is_some());
    }

    #[test]
    fn oversized_removal_and_raw_fault_revoke_ioc_and_preserve_accepted_settlements() {
        use crate::paper::{Shares, order_fsm::Delivery};
        for typed in [true, false] {
            let (mut group, account, asset) = fund(open_group());
            let source = group.open_input().unwrap();
            for (token, condition) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
                assert!(
                    group
                        .admit_raw(source, snapshot(token, condition, 100))
                        .unwrap()
                );
                assert!(group.advance_input(source).unwrap());
            }
            let order = buy_one(account, asset.clone());
            let signal = group
                .paper()
                .unwrap()
                .current_stamp(&asset)
                .unwrap()
                .unwrap();
            if typed {
                let repeated = Event::PriceChange {
                    market: "a".into(),
                    timestamp: 101,
                    price_changes: (0..=crate::wire::MAX_ROWS)
                        .map(|_| Change {
                            best_bid: None,
                            best_ask: None,
                            asset_id: "1".into(),
                            side: Side::Sell,
                            level: Level {
                                price: Decimal::new(6, 1),
                                size: Decimal::ZERO,
                            },
                        })
                        .collect(),
                };
                assert!(
                    group
                        .admit_event(source, repeated)
                        .unwrap_err()
                        .to_string()
                        .contains("exceeds 16384 rows")
                );
            } else {
                let bad = r#"[{"event_type":"book","bids":"bad"},{"event_type":"market_resolved","market":"a"}]"#;
                assert!(group.admit_raw(source, bad.to_owned()).unwrap());
                assert!(
                    group
                        .admit_event(source, Event::MarketResolved { market: "b".into() })
                        .unwrap()
                );
                assert!(
                    group
                        .advance_input(source)
                        .unwrap_err()
                        .to_string()
                        .contains("invalid WebSocket message")
                );
                assert!(group.assets().is_empty());
            }
            assert!(!group.ready());
            assert!(!group.admit_raw(source, snapshot("1", "a", 200)).unwrap());
            let mut desk = group.paper().unwrap();
            assert_eq!(
                desk.submit(order, signal, Delivery::Delivered),
                Err(PaperError::StaleSignal)
            );
            let balance = desk.balance(account, &asset).unwrap();
            assert_eq!(balance.cash().decimal(), Decimal::TEN);
            assert_eq!(balance.shares(), Shares::ZERO);
        }
    }

    #[test]
    fn roster_replacement_drains_only_the_retiring_subscription() {
        let mut group = open_group();
        let old = group.open_input().unwrap();
        assert!(group.open_input().is_none());
        let settlement = |id: &str| Event::MarketResolved { market: id.into() };
        assert!(group.admit_event(old, settlement("a")).unwrap());
        let bc = fixture_spec(&[
            ("b", "3", "4", Decimal::new(1, 2), false),
            ("c", "5", "6", Decimal::new(1, 2), false),
        ]);
        let outcome = Ok(Discovery::Open(bc));
        assert!(group.update_discovery(outcome).unwrap());
        let outcome = Ok(Discovery::Open(spec()));
        assert!(group.update_discovery(outcome).unwrap());
        let current = group.open_input().unwrap();
        assert!(!group.admit_event(old, settlement("b")).unwrap());
        assert!(!group.admit_raw(old, "{".into()).unwrap());
        for token in ["3", "4"] {
            assert!(group.admit_raw(current, snapshot(token, "b", 100)).unwrap());
            assert!(group.advance_input(current).unwrap());
        }
        assert_eq!(group.assets(), ["3", "4"]);
        assert_eq!(group.audits().len(), 2);
        assert!(!group.ready());
    }

    #[test]
    fn unavailable_revokes_paper_until_new_approval_and_all_fresh_snapshots() {
        let (mut group, _, asset) = funded_group();
        let seed = group
            .paper()
            .unwrap()
            .current_stamp(&asset)
            .unwrap()
            .unwrap();
        assert!(group.fixture_discovery(Ok(Discovery::Unavailable(anyhow::anyhow!("ineligible")))));
        assert!(!group.ready() && group.subscription().is_none());
        assert!(group.fixture_discovery(Ok(Discovery::Open(spec()))));
        assert!(!group.ready());
        let source = group.open_input().unwrap();
        for (token, market) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
            assert!(
                group
                    .admit_raw(source, snapshot(token, market, 101))
                    .unwrap()
            );
            assert!(group.advance_input(source).unwrap());
        }
        let fresh = group
            .paper()
            .unwrap()
            .current_stamp(&asset)
            .unwrap()
            .unwrap();
        assert!(group.ready() && fresh != seed);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stale_timer_cannot_expire_the_next_subscription() {
        let mut group = open_group();
        group.begin().unwrap();
        let old_deadline = group.snapshot_deadline().unwrap();
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        group.disconnect();
        group.begin().unwrap();
        for (asset, market) in [("2", "a"), ("3", "b"), ("4", "b")] {
            apply(&mut group, &snapshot(asset, market, 100)).unwrap();
        }
        tokio::time::sleep_until(old_deadline).await;
        group.expire_snapshots();
        assert!(group.awaiting_snapshot());
        assert!(!group.has_quarantined_books());
        tokio::time::sleep_until(group.snapshot_deadline().unwrap()).await;
        group.expire_snapshots();
        assert!(group.has_quarantined_books());
        assert!(!group.awaiting_snapshot());
        assert!(group.once_failure().is_some());
        assert_eq!(group.audits().len(), 3);
    }

    #[test]
    fn settlement_capacity_is_terminal_and_keeps_unconfirmed_orders_reconcilable() {
        use crate::paper::order_fsm::{CloseReason, Delivery, OrderStatus};
        let (mut group, account, asset) = funded_group();
        let mut desk = group.paper().unwrap();
        let signal = desk.current_stamp(&asset).unwrap().unwrap();
        let req = buy_one(account, asset.clone());
        let key = account.order(req.id());
        desk.submit(req, signal, Delivery::Lost).unwrap();
        for n in 0..=MAX_SETTLEMENTS {
            let condition = format!("rolling-{n}");
            let roster = fixture_spec(&[
                ("stable", "1", "2", Decimal::new(1, 2), false),
                (&condition, "3", "4", Decimal::new(1, 2), false),
            ]);
            assert!(group.fixture_discovery(Ok(Discovery::Open(roster))));
            let source = group.open_input().unwrap();
            let resolved = || Event::MarketResolved {
                market: condition.clone(),
            };
            assert!(group.admit_event(source, resolved()).unwrap());
            if n < MAX_SETTLEMENTS {
                assert!(group.advance_input(source).unwrap());
                assert!(group.admit_event(source, resolved()).unwrap());
                assert!(group.advance_input(source).unwrap());
                assert!(group.close_input(source).unwrap());
            } else {
                let closed = Ok(Discovery::Closed);
                let err = group.update_discovery(closed).unwrap_err();
                assert!(err.is::<SettlementCapacity>());
                assert_eq!(err.to_string(), SettlementCapacity.to_string());
                assert!(!group.event_closed());
            }
        }
        assert_eq!(group.resolved.len(), MAX_SETTLEMENTS);
        assert!(group.resolved.contains("rolling-0"));
        assert!(!group.ready());
        assert!(group.begin().is_none());
        assert!(!group.fixture_discovery(Ok(Discovery::Open(spec()))));
        assert!(!group.fixture_discovery(Ok(Discovery::Closed)));
        assert!(group.check_capacity().is_err());
        let mut desk = group.paper().unwrap();
        let new = buy_one_with_id(account, asset.clone(), 2);
        let new_key = account.order(new.id());
        assert_eq!(
            desk.submit(new, signal, Delivery::Delivered),
            Err(PaperError::StaleSignal)
        );
        assert_eq!(desk.order_status(new_key), Err(PaperError::UnknownOrder));
        for _ in 0..2 {
            assert!(
                matches!(desk.reconcile(key),Ok(OrderStatus::Closed {reason:CloseReason::Filled,filled}) if filled.decimal()==Decimal::ONE)
            );
            let balance = desk.balance(account, &asset).unwrap();
            assert_eq!(balance.cash().decimal(), Decimal::new(94, 1));
            assert_eq!(balance.shares().decimal(), Decimal::ONE);
        }
    }

    #[test]
    fn rest_audit_counts_both_sides_before_merging_and_ignores_obsolete_requests() {
        let mut group = ready_group();
        let audit = group
            .audits()
            .into_iter()
            .find(|a| a.asset() == "1")
            .unwrap();
        let json = r#"{"market":"a","asset_id":"1","timestamp":"100","tick_size":"0.01","bids":[{"price":"0.40","size":"5"}],"asks":[{"price":"0.60","size":"7"}]}"#;
        let repeated = |count| {
            let mut book: RestBook = serde_json::from_str(json).unwrap();
            book.snapshot.bids.resize(count, book.snapshot.bids[0]);
            book
        };
        let view = group.render();
        group
            .audit(&audit, Ok(Some(repeated(crate::wire::MAX_ROWS - 1))))
            .unwrap();
        assert!(
            group
                .audit(&audit, Ok(Some(repeated(crate::wire::MAX_ROWS))))
                .is_err()
        );
        assert_eq!(group.render(), view);
        apply(&mut group, &snapshot("1", "a", 101)).unwrap();
        group
            .audit(&audit, Ok(Some(repeated(crate::wire::MAX_ROWS))))
            .unwrap();
    }

    #[test]
    fn delayed_snapshot_cannot_cross_subscriptions() {
        let (mut group, account, asset) = funded_group();
        let old = group.test_source();
        group.disconnect();
        let current = group.begin().unwrap();
        for (token, market) in [("2", "a"), ("3", "b"), ("4", "b")] {
            apply(&mut group, &snapshot(token, market, 101)).unwrap();
        }
        group
            .apply(old, events(&snapshot("1", "a", 999)).unwrap().remove(0))
            .unwrap();
        assert!(!group.ready());
        assert_eq!(group.paper().unwrap().current_stamp(&asset), Ok(None));
        group
            .apply(current, events(&snapshot("1", "a", 1)).unwrap().remove(0))
            .unwrap();
        assert!(group.ready());
        let mut desk = group.paper().unwrap();
        let signal = desk.current_stamp(&asset).unwrap().unwrap();
        assert!(
            desk.submit(
                buy_one(account, asset),
                signal,
                crate::paper::order_fsm::Delivery::Delivered
            )
            .is_ok()
        );
    }
    #[tokio::test(start_paused = true)]
    async fn tick_revises_orders_independently_of_depth_age() {
        use crate::paper::{
            ClientOrderId, Price, Quantity, Side,
            order_fsm::{CloseReason, Delivery, OrderMode, OrderRequest, OrderStatus},
        };
        for expired in [false, true] {
            let (mut group, account, asset) = funded_group();
            let mut desk = group.paper().unwrap();
            let old = desk.current_stamp(&asset).unwrap().unwrap();
            for (n, limit) in [(2, 41), (3, 40)] {
                let maker = OrderRequest::new(
                    ClientOrderId::new(n).unwrap(),
                    account,
                    asset.clone(),
                    Side::Buy,
                    Price::new(Decimal::new(limit, 2)).unwrap(),
                    Quantity::new(Decimal::ONE).unwrap(),
                    OrderMode::PostOnly,
                );
                desk.submit(maker, old, Delivery::Delivered).unwrap();
            }
            tokio::time::advance(std::time::Duration::from_millis(if expired {
                100
            } else {
                90
            }))
            .await;
            apply(&mut group,r#"{"event_type":"tick_size_change","market":"a","asset_id":"1","timestamp":"101","new_tick_size":"0.05"}"#).unwrap();
            let mut desk = group.paper().unwrap();
            assert!(matches!(
                desk.order_status(account.order(ClientOrderId::new(2).unwrap())),
                Ok(OrderStatus::Closed {
                    reason: CloseReason::TickChanged,
                    ..
                })
            ));
            assert!(matches!(
                desk.order_status(account.order(ClientOrderId::new(3).unwrap())),
                Ok(OrderStatus::Working { .. })
            ));
            assert_eq!(
                desk.balance(account, &asset).unwrap().cash().decimal(),
                Decimal::new(96, 1)
            );
            let request = buy_one(account, asset.clone());
            assert_eq!(
                desk.submit(request.clone(), old, Delivery::Delivered),
                Err(PaperError::StaleSignal)
            );
            let signal = desk.current_stamp(&asset).unwrap();
            if expired {
                assert_eq!(signal, None);
                apply(&mut group, &snapshot("1", "a", 102)).unwrap();
                let mut desk = group.paper().unwrap();
                let fresh = desk.current_stamp(&asset).unwrap().unwrap();
                assert_eq!(
                    desk.submit(request, fresh, Delivery::Delivered)
                        .unwrap()
                        .status(),
                    OrderStatus::Closed {
                        filled: crate::paper::Shares::new(Decimal::ONE).unwrap(),
                        reason: CloseReason::Filled
                    }
                );
            } else {
                let updated = signal.unwrap();
                assert_eq!(updated.observed_ms(), old.observed_ms());
                assert!(desk.submit(request, updated, Delivery::Delivered).is_ok());
            }
        }
    }

    #[test]
    fn attached_fine_tick_closes_only_off_grid_orders() {
        use crate::paper::{
            ClientOrderId, Price, Quantity, Side,
            order_fsm::{CloseReason, Delivery, OrderMode, OrderRequest, OrderStatus},
        };
        let (mut group, account, asset) = funded_group();
        let mut desk = group.paper().unwrap();
        let signal = desk.current_stamp(&asset).unwrap().unwrap();
        let [aligned, off_grid] = [39, 40].map(|cents| {
            let request = OrderRequest::new(
                ClientOrderId::new(cents).unwrap(),
                account,
                asset.clone(),
                Side::Buy,
                Price::new(Decimal::new(cents as i64, 2)).unwrap(),
                Quantity::new(Decimal::ONE).unwrap(),
                OrderMode::PostOnly,
            );
            desk.submit(request, signal, Delivery::Delivered)
                .unwrap()
                .key()
        });
        let change = |timestamp, tick| {
            format!(
                r#"{{"event_type":"tick_size_change","market":"a","asset_id":"1","timestamp":"{timestamp}","new_tick_size":"{tick}"}}"#
            )
        };
        apply(&mut group, &change(101, ".0000003")).unwrap();
        assert_eq!(group.paper().unwrap().current_stamp(&asset), Ok(None));
        for check_after_recovery in [false, true] {
            if check_after_recovery {
                apply(&mut group, &change(102, ".01")).unwrap();
            }
            let desk = group.paper().unwrap();
            assert!(matches!(
                desk.order_status(aligned),
                Ok(OrderStatus::Working { .. })
            ));
            assert!(matches!(
                desk.order_status(off_grid),
                Ok(OrderStatus::Closed {
                    reason: CloseReason::TickChanged,
                    ..
                })
            ));
        }
    }

    fn funded_group() -> (
        NegRiskEventGroup,
        crate::paper::AccountId,
        crate::paper::AssetId,
    ) {
        fund(ready_group())
    }

    fn fund(
        mut group: NegRiskEventGroup,
    ) -> (
        NegRiskEventGroup,
        crate::paper::AccountId,
        crate::paper::AssetId,
    ) {
        use crate::paper::{
            AssetId, Cash, Shares,
            fee_model::{FeeModel, MakerTier},
        };
        let asset = AssetId::new("1").unwrap();
        let cash = Cash::new(Decimal::TEN).unwrap();
        let mut exchange = ChaosExchange::new(100, cash).unwrap();
        exchange
            .register_asset(asset.clone(), FeeModel::FREE, Shares::ZERO)
            .unwrap();
        exchange
            .register_asset(AssetId::new("2").unwrap(), FeeModel::FREE, Shares::ZERO)
            .unwrap();
        let account = exchange.allocate(cash, [], MakerTier::None).unwrap();
        group.attach_paper(exchange).unwrap();
        (group, account, asset)
    }

    fn buy_one(
        account: crate::paper::AccountId,
        asset: crate::paper::AssetId,
    ) -> crate::paper::order_fsm::OrderRequest {
        buy_one_with_id(account, asset, 1)
    }

    fn buy_one_with_id(
        account: crate::paper::AccountId,
        asset: crate::paper::AssetId,
        id: u64,
    ) -> crate::paper::order_fsm::OrderRequest {
        use crate::paper::{
            ClientOrderId, Price, Quantity, Side,
            order_fsm::{OrderMode, OrderRequest},
        };
        OrderRequest::new(
            ClientOrderId::new(id).unwrap(),
            account,
            asset,
            Side::Buy,
            Price::new(Decimal::new(60, 2)).unwrap(),
            Quantity::new(Decimal::ONE).unwrap(),
            OrderMode::ImmediateOrCancel,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn attached_depth_tracks_real_changes_while_peer_reconciles() {
        use crate::paper::{
            AssetId, ClientOrderId, Price, Quantity, Side,
            order_fsm::{Delivery, OrderMode, OrderRequest, OrderStatus},
        };
        for replenished in [false, true] {
            let (mut group, account, _) = funded_group();
            let asset = AssetId::new("2").unwrap();
            let set_ask = |time, size| {
                delta(time, "0.60", size, "SELL").replace(r#""asset_id":"1""#, r#""asset_id":"2""#)
            };
            apply(&mut group, &set_ask(101, "5")).unwrap();
            let request = |id, mode, limit| {
                OrderRequest::new(
                    ClientOrderId::new(id).unwrap(),
                    account,
                    asset.clone(),
                    Side::Buy,
                    Price::new(limit).unwrap(),
                    Quantity::new(5.into()).unwrap(),
                    mode,
                )
            };
            let mut desk = group.paper().unwrap();
            let initial = desk.current_stamp(&asset).unwrap().unwrap();
            let maker = desk
                .submit(
                    request(3, OrderMode::PostOnly, Decimal::new(4, 1)),
                    initial,
                    Delivery::Delivered,
                )
                .unwrap();

            let buy = |group: &mut NegRiskEventGroup, id| {
                let mut desk = group.paper().unwrap();
                let signal = desk.current_stamp(&asset).unwrap().unwrap();
                let status = desk
                    .submit(
                        request(id, OrderMode::ImmediateOrCancel, Decimal::new(6, 1)),
                        signal,
                        Delivery::Delivered,
                    )
                    .unwrap()
                    .status();
                let OrderStatus::Closed { filled, .. } = status else {
                    panic!("expected IOC receipt")
                };
                filled.decimal()
            };
            assert_eq!(buy(&mut group, 1), Decimal::from(5));
            let lock = delta(102, "0.60", "9", "BUY").replace(
                r#""side":"BUY""#,
                r#""side":"BUY","best_bid":"0.60","best_ask":"0.61""#,
            );
            apply(&mut group, &lock).unwrap();
            assert_eq!(group.paper().unwrap().current_stamp(&asset), Ok(None));
            assert_eq!(
                group.paper().unwrap().submit(
                    request(2, OrderMode::ImmediateOrCancel, Decimal::new(6, 1)),
                    initial,
                    Delivery::Delivered
                ),
                Err(PaperError::StaleSignal)
            );
            let received = Instant::now();
            let queued = set_ask(103, if replenished { "0" } else { "5" });
            let event = events(&queued).unwrap().remove(0);
            tokio::time::advance(std::time::Duration::from_millis(100)).await;
            group
                .apply_received(group.test_source(), event, received)
                .unwrap();
            apply(&mut group, &set_ask(104, "5")).unwrap();
            apply(&mut group, &delta(105, "0.60", "0", "BUY")).unwrap();
            assert_eq!(
                buy(&mut group, 2),
                Decimal::from(5 * u32::from(replenished))
            );
            assert!(matches!(
                group.paper().unwrap().order_status(maker.key()),
                Ok(OrderStatus::Working { .. })
            ));
            let balance = group.paper().unwrap().balance(account, &asset).unwrap();
            assert_eq!(
                balance.shares().decimal(),
                Decimal::from(if replenished { 10 } else { 5 })
            );
        }
    }

    #[test]
    fn settlement_blocks_old_ioc_across_all_group_legs() {
        use crate::paper::{Shares, order_fsm::Delivery};
        let (mut group, account, asset) = funded_group();
        let order = buy_one(account, asset.clone());
        let signal = group
            .paper()
            .unwrap()
            .current_stamp(&asset)
            .unwrap()
            .unwrap();
        apply(
            &mut group,
            r#"{"event_type":"market_resolved","market":"b"}"#,
        )
        .unwrap();
        let mut desk = group.paper().unwrap();
        assert_eq!(desk.current_stamp(&asset), Ok(None));
        assert_eq!(
            desk.submit(order.clone(), signal, Delivery::Delivered),
            Err(PaperError::StaleSignal)
        );
        assert_eq!(
            desk.order_status(account.order(order.id())),
            Err(PaperError::UnknownOrder)
        );
        let balance = desk.balance(account, &asset).unwrap();
        assert_eq!(balance.cash().decimal(), Decimal::TEN);
        assert_eq!(balance.shares(), Shares::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn attached_submit_uses_exact_time_at_both_ends_of_quote_age() {
        use crate::paper::order_fsm::{CloseReason, Delivery, OrderStatus};
        use std::time::Duration;
        for (age, valid) in [(99_999, true), (100_000, false)] {
            let (mut group, account, asset) = fund(open_group());
            let source = group.open_input().unwrap();
            tokio::time::advance(Duration::from_micros(500)).await;
            for (id, market) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
                group.admit_raw(source, snapshot(id, market, 100)).unwrap();
                assert!(group.advance_input(source).unwrap());
            }
            let signal = group
                .paper()
                .unwrap()
                .current_stamp(&asset)
                .unwrap()
                .unwrap();
            tokio::time::advance(Duration::from_micros(age)).await;
            let mut desk = group.paper().unwrap();
            let result = desk.submit(buy_one(account, asset.clone()), signal, Delivery::Delivered);
            if valid {
                assert!(matches!(
                    result.unwrap().status(),
                    OrderStatus::Closed {
                        reason: CloseReason::Filled,
                        ..
                    }
                ));
            } else {
                assert_eq!(result, Err(PaperError::StaleSignal));
            }
            assert_eq!(
                desk.balance(account, &asset).unwrap().shares().decimal(),
                Decimal::from(u32::from(valid))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reread_cannot_refresh_age() {
        let (mut group, _, asset) = funded_group();
        let signal = group
            .paper()
            .unwrap()
            .current_stamp(&asset)
            .unwrap()
            .unwrap();
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        apply(&mut group, &snapshot("2", "a", 101)).unwrap();
        assert_eq!(group.paper().unwrap().current_stamp(&asset), Ok(None));
        apply(&mut group, &snapshot("1", "a", 101)).unwrap();
        let renewed = group
            .paper()
            .unwrap()
            .current_stamp(&asset)
            .unwrap()
            .unwrap();
        assert_ne!(renewed, signal);
        assert_eq!(renewed.observed_ms() - signal.observed_ms(), 100);
    }

    #[test]
    fn new_subscription_requires_new_books_and_preserves_unconfirmed_reservations() {
        use crate::paper::{
            ClientOrderId, Price, Quantity, Side,
            order_fsm::{CloseReason, Delivery, OrderMode, OrderRequest, OrderStatus},
        };
        let (mut group, account, asset) = fund(open_group());
        let first = group.open_input().unwrap();
        for (token, market) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
            assert!(
                group
                    .admit_raw(first, snapshot(token, market, 100))
                    .unwrap()
            );
            assert!(group.advance_input(first).unwrap());
        }
        let mut desk = group.paper().unwrap();
        let old = desk.current_stamp(&asset).unwrap().unwrap();
        let maker = OrderRequest::new(
            ClientOrderId::new(7).unwrap(),
            account,
            asset.clone(),
            Side::Buy,
            Price::new(Decimal::new(4, 1)).unwrap(),
            Quantity::new(Decimal::ONE).unwrap(),
            OrderMode::PostOnly,
        );
        let receipt = desk.submit(maker, old, Delivery::Lost).unwrap();
        assert!(group.close_input(first).unwrap());
        let second = group.open_input().unwrap();
        let mut desk = group.paper().unwrap();
        assert_eq!(desk.current_stamp(&asset), Ok(None));
        assert_eq!(
            desk.order_status(receipt.key()),
            Ok(OrderStatus::Unconfirmed)
        );
        assert_eq!(
            desk.balance(account, &asset).unwrap().cash().decimal(),
            Decimal::new(96, 1)
        );
        assert!(matches!(
            desk.reconcile(receipt.key()),
            Ok(OrderStatus::Closed {
                reason: CloseReason::LostBook,
                ..
            })
        ));
        assert_eq!(
            desk.balance(account, &asset).unwrap().cash().decimal(),
            Decimal::TEN
        );
        for (token, market) in [("2", "a"), ("3", "b"), ("4", "b"), ("1", "a")] {
            assert!(
                !group
                    .admit_raw(first, snapshot(token, market, 999))
                    .unwrap()
            );
            assert!(!group.ready());
            assert!(
                group
                    .admit_raw(second, snapshot(token, market, 99))
                    .unwrap()
            );
            assert!(group.advance_input(second).unwrap());
        }
        let mut desk = group.paper().unwrap();
        let new = desk.current_stamp(&asset).unwrap().unwrap();
        assert_ne!(new, old);
        assert_eq!(
            desk.submit(buy_one(account, asset.clone()), old, Delivery::Delivered),
            Err(PaperError::StaleSignal)
        );
        assert!(matches!(
            desk.submit(buy_one(account, asset), new, Delivery::Delivered)
                .unwrap()
                .status(),
            OrderStatus::Closed {
                reason: CloseReason::Filled,
                ..
            }
        ));
    }

    #[test]
    fn paper_rejection_retires_only_the_failed_asset_and_preserves_unknown_receipts() {
        use crate::paper::{
            AssetId, ClientOrderId, Price, Quantity, Side,
            order_fsm::{CloseReason, Delivery, OrderMode, OrderRequest, OrderStatus},
        };
        let (mut group, account, asset) = funded_group();
        let other = AssetId::new("2").unwrap();
        let mut desk = group.paper().unwrap();
        let stamp = desk.current_stamp(&asset).unwrap().unwrap();
        let peer = desk.current_stamp(&other).unwrap().unwrap();
        let maker = OrderRequest::new(
            ClientOrderId::new(9).unwrap(),
            account,
            asset.clone(),
            Side::Buy,
            Price::new(Decimal::new(4, 1)).unwrap(),
            Quantity::new(Decimal::ONE).unwrap(),
            OrderMode::PostOnly,
        );
        let receipt = desk.submit(maker.clone(), stamp, Delivery::Lost).unwrap();
        apply(&mut group, &delta(101, "0.4000001", "1", "BUY")).unwrap();
        let mut desk = group.paper().unwrap();
        assert_eq!(desk.current_stamp(&asset), Err(PaperError::InvalidValue));
        assert_eq!(desk.current_stamp(&other), Ok(Some(peer)));
        assert_eq!(
            desk.submit(maker, stamp, Delivery::Delivered).unwrap(),
            receipt
        );
        assert_eq!(
            desk.balance(account, &asset).unwrap().cash().decimal(),
            Decimal::new(96, 1)
        );
        assert!(matches!(
            desk.submit(buy_one(account, other), peer, Delivery::Delivered)
                .unwrap()
                .status(),
            OrderStatus::Closed {
                reason: CloseReason::Filled,
                ..
            }
        ));
        assert!(matches!(
            desk.reconcile(receipt.key()),
            Ok(OrderStatus::Closed {
                reason: CloseReason::LostBook,
                ..
            })
        ));
        assert_eq!(
            desk.balance(account, &asset).unwrap().cash().decimal(),
            Decimal::new(94, 1)
        );
    }

    #[test]
    fn conversion_failure_retires_previous_depth() {
        use crate::paper::order_fsm::Delivery;
        let (mut group, account, asset) = funded_group();
        let signal = group
            .paper()
            .unwrap()
            .current_stamp(&asset)
            .unwrap()
            .unwrap();
        apply(&mut group, &delta(101, "0.4000001", "1", "BUY")).unwrap();
        assert!(group.ready());
        let mut desk = group.paper().unwrap();
        assert!(desk.current_stamp(&asset).is_err());
        let order = buy_one(account, asset.clone());
        assert_eq!(
            desk.submit(order.clone(), signal, Delivery::Delivered),
            Err(PaperError::StaleSignal)
        );
        assert_eq!(
            desk.order_status(account.order(order.id())),
            Err(PaperError::UnknownOrder)
        );
        apply(&mut group, &snapshot("1", "a", 102)).unwrap();
        let mut desk = group.paper().unwrap();
        let renewed = desk.current_stamp(&asset).unwrap().unwrap();
        assert_ne!(renewed, signal);
        assert!(desk.submit(order, renewed, Delivery::Delivered).is_ok());
    }

    #[test]
    fn price_change_batch_is_checked_after_all_updates() {
        let mut group = ready_group();
        apply(
            &mut group,
            r#"{"event_type":"price_change","market":"a","timestamp":101,
            "price_changes":[
                {"asset_id":"1","price":"0.61","size":"4","side":"BUY"},
                {"asset_id":"1","price":"0.60","size":"0","side":"SELL"},
                {"asset_id":"1","price":"0.65","size":"9","side":"SELL"}
            ]}"#,
        )
        .unwrap();
        assert!(group.ready());
        assert_eq!(
            group
                .test_book("1")
                .synced()
                .unwrap()
                .levels
                .quotes()
                .ask
                .unwrap()
                .price,
            Decimal::new(65, 2)
        );
    }

    #[test]
    fn disconnect_clears_levels_and_requires_new_snapshots_for_all_tokens() {
        let mut group = ready_group();
        group.invalidate();
        assert!(group.test_books().all(|book| book.label() == "Stale"));
        group.disconnect();
        assert!(
            group
                .test_books()
                .all(|book| book.label() == "Disconnected")
        );
        assert_eq!(group.yes_sum(group.ready()), None);
        group.begin().unwrap();
        apply(&mut group, &snapshot("1", "a", 1)).unwrap();
        assert_eq!(group.test_book("1").label(), "Synced");
        assert_eq!(group.test_book("2").label(), "Syncing");
        assert!(!group.ready());
    }

    #[test]
    fn resolution_closes_both_legs_and_survives_reconnect_and_late_snapshot() {
        let mut group = ready_group();
        apply(
            &mut group,
            r#"{"event_type":"market_resolved","market":"a"}"#,
        )
        .unwrap();
        assert_eq!(group.test_book("1").label(), "Stale");
        assert_eq!(group.test_book("2").label(), "Stale");
        assert!(!group.ready());
        group.disconnect();
        group.begin().unwrap();
        apply(&mut group, &snapshot("1", "a", 200)).unwrap();
        assert_eq!(group.assets(), ["3", "4"]);
        assert_eq!(group.test_book("1").label(), "Disconnected");
        assert_eq!(group.test_book("3").label(), "Syncing");
    }

    #[test]
    fn rest_disagreement_is_diagnostic_and_cannot_rewrite_tick_or_ordering() {
        let mut group = ready_group();
        let audit = group.audits().remove(0);
        let response = || {
            serde_json::from_str(
                r#"{
            "market":"a","asset_id":"1","timestamp":"200","tick_size":"0.02",
            "bids":[{"price":"0.40","size":"5"}],"asks":[{"price":"0.60","size":"7"}]
        }"#,
            )
            .unwrap()
        };
        assert!(group.audit(&audit, Ok(Some(response()))).is_err());
        assert!(group.audit(&audit, Ok(Some(response()))).is_err());
        assert!(group.ready());
        assert_eq!(
            group.test_book("1").synced().unwrap().tick,
            Decimal::new(1, 2)
        );
        apply(
            &mut group,
            r#"{"event_type":"tick_size_change","asset_id":"1","market":"a",
            "timestamp":"150","new_tick_size":"0.05"}"#,
        )
        .unwrap();
        assert_eq!(
            group.test_book("1").synced().unwrap().tick,
            Decimal::new(5, 2)
        );
        assert!(group.ready());
    }

    #[test]
    fn gamma_reseeds_same_members_and_old_audit_cannot_cross_subscriptions() {
        let mut group = ready_group();
        let previous = group.audits().remove(0);
        let updated = fixture_spec(&[
            ("a", "1", "2", Decimal::new(2, 2), false),
            ("b", "3", "4", Decimal::new(1, 2), false),
        ]);
        assert!(!group.fixture_discovery(Ok(Discovery::Open(updated))));
        assert_eq!(
            group.test_book("1").synced().unwrap().tick,
            Decimal::new(1, 2)
        );
        group.disconnect();
        group.begin().unwrap();
        apply(&mut group, &snapshot("1", "a", 100)).unwrap();
        assert_eq!(
            group.test_book("1").synced().unwrap().tick,
            Decimal::new(2, 2)
        );
        group.audit(&previous, Ok(None)).unwrap();
    }

    #[test]
    fn initial_snapshot_uses_commit_deadline_without_expiring_healthy_peers() {
        for adjustment in [None, Some(0), Some(1)] {
            let mut group = open_group();
            group.begin().unwrap();
            for (asset, market) in [("2", "a"), ("3", "b"), ("4", "b")] {
                apply(&mut group, &snapshot(asset, market, 100)).unwrap();
            }
            let deadline = Instant::now() + std::time::Duration::from_secs(15);
            let Event::Book(snapshot) = events(&snapshot("1", "a", 100)).unwrap().remove(0) else {
                unreachable!()
            };
            let after_decode = || match adjustment {
                Some(nanos) => deadline + std::time::Duration::from_nanos(nanos),
                None => deadline - std::time::Duration::from_nanos(1),
            };
            group
                .update("1", "a", |book| {
                    book.snapshot(&snapshot, Instant::now(), deadline, after_decode)
                })
                .unwrap();
            assert_eq!(group.ready(), adjustment.is_none());
            assert_eq!(group.once_failure().is_some(), adjustment.is_some());
            group.expire_snapshots();
            assert!(group.test_book("3").synced().is_some());
            assert_eq!(
                group.test_book("1").synced().is_some(),
                adjustment.is_none()
            );
        }
    }

    #[test]
    fn aggregate_renders_the_full_precision_of_accepted_prices() {
        let mut group = ready_group();
        let set_book = |group: &mut NegRiskEventGroup, asset, condition, bid, ask| {
            apply(
                group,
                &format!(
                    r#"{{"event_type":"book","asset_id":"{asset}","market":"{condition}",
                "timestamp":"101","bids":[{{"price":"{bid}","size":"5"}}],
                "asks":[{{"price":"{ask}","size":"5"}}]}}"#
                ),
            )
            .unwrap();
        };
        for (asset, condition) in [("1", "a"), ("3", "b")] {
            set_book(
                &mut group,
                asset,
                condition,
                "0.0000000000000000000000000001",
                "0.0000000000000000000000000002",
            );
        }
        assert_eq!(
            format_probability(group.yes_sum(group.ready()).unwrap()),
            "0.0000000000000000000000000003"
        );
        set_book(
            &mut group,
            "1",
            "a",
            "0.50",
            "0.5000000000000000000000000001",
        );
        set_book(&mut group, "2", "a", "0.49", "0.51");
        set_book(&mut group, "3", "b", "0.24", "0.26");
        let output = group.render();
        assert!(output.contains("YES 中間價總和 0.75000000000000000000000000005 (與 1 的差 -0.24999999999999999999999999995)"));
        assert!(output.contains("補數檢核 偏差 0.00000000000000000000000000005"));
    }

    #[test]
    fn rest_results_and_errors_follow_tick_revision_even_if_tick_changes_back() {
        let mut group = ready_group();
        let audit = group.audits().remove(0);
        let tick = |value| {
            format!(
                r#"{{"event_type":"tick_size_change","asset_id":"1","market":"a",
            "timestamp":"150","new_tick_size":"{value}"}}"#
            )
        };
        apply(&mut group, &tick("0.01")).unwrap();
        assert!(
            group
                .audit(&audit, Err(anyhow::anyhow!("current HTTP failure")))
                .is_err()
        );
        apply(&mut group, &tick("0.05")).unwrap();
        apply(&mut group, &tick("0.01")).unwrap();
        group
            .audit(&audit, Err(anyhow::anyhow!("obsolete HTTP failure")))
            .unwrap();
        group.audit(&audit, Ok(None)).unwrap();
        let fresh = group.audits().remove(0);
        assert!(group.audit(&fresh, Ok(None)).is_err());
    }

    #[test]
    fn confirmed_resolution_outlives_membership_but_gamma_only_closure_can_reopen() {
        let mut group = ready_group();
        apply(
            &mut group,
            r#"{"event_type":"market_resolved","market":"a"}"#,
        )
        .unwrap();
        let updated = fixture_spec(&[
            ("b", "3", "4", Decimal::new(1, 2), true),
            ("c", "5", "6", Decimal::new(1, 2), false),
        ]);
        assert!(group.fixture_discovery(Ok(Discovery::Open(updated))));
        assert_eq!(group.assets(), ["5", "6"]);
        let updated = fixture_spec(&[
            ("a", "1", "2", Decimal::new(1, 2), false),
            ("b", "3", "4", Decimal::new(1, 2), false),
            ("c", "5", "6", Decimal::new(1, 2), false),
        ]);
        assert!(group.fixture_discovery(Ok(Discovery::Open(updated))));
        assert_eq!(group.assets(), ["3", "4", "5", "6"]);
    }

    #[test]
    fn confirmed_ineligibility_revokes_readiness_but_transport_failure_keeps_current_session() {
        let mut group = ready_group();
        assert!(!group.fixture_discovery(Err(anyhow::anyhow!("timeout"))));
        assert!(group.ready());
        assert!(group.render().contains("Gamma 更新失敗"));
        assert!(group.fixture_discovery(Ok(Discovery::Unavailable(anyhow::anyhow!("one market")))));
        assert!(!group.has_open_assets());
        assert!(!group.fixture_discovery(Err(anyhow::anyhow!("timeout again"))));
        assert_eq!(group.yes_sum(group.ready()), None);
        assert!(group.fixture_discovery(Ok(Discovery::Open(spec()))));
        assert!(!group.ready());
        group.begin().unwrap();
        for (asset, market) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
            apply(&mut group, &snapshot(asset, market, 100)).unwrap();
        }
        assert!(group.ready());
    }
    #[test]
    fn depth_limit_applies_across_frames_and_allows_a_full_book_to_replace_a_level() {
        let mut group = ready_group();
        let price = |index: usize| Decimal::new(index as i64, 5);
        let change = |asset: &str, index, size| Change {
            best_bid: None,
            best_ask: None,
            asset_id: asset.to_owned(),
            side: Side::Buy,
            level: Level {
                price: price(index),
                size,
            },
        };
        group
            .apply(
                group.test_source(),
                Event::Book(Snapshot {
                    asset_id: "1".to_owned(),
                    market: "a".to_owned(),
                    timestamp: 101,
                    bids: (1..(LEVEL_LIMIT - 1))
                        .map(|i| Level {
                            price: price(i),
                            size: Decimal::ONE,
                        })
                        .collect(),
                    asks: vec![Level {
                        price: Decimal::new(6, 1),
                        size: Decimal::ONE,
                    }],
                }),
            )
            .unwrap();
        let update = |changes| Event::PriceChange {
            market: "a".to_owned(),
            timestamp: 102,
            price_changes: changes,
        };
        group
            .apply(
                group.test_source(),
                update(vec![change("1", LEVEL_LIMIT - 1, Decimal::ONE)]),
            )
            .unwrap();
        group
            .apply(
                group.test_source(),
                update(vec![
                    change("1", LEVEL_LIMIT, Decimal::ONE),
                    change("1", 1, Decimal::ZERO),
                ]),
            )
            .unwrap();
        assert_eq!(
            group.test_book("1").synced().unwrap().levels.bids.len(),
            LEVEL_LIMIT - 1
        );
        assert!(group.ready());
        group
            .apply(
                group.test_source(),
                update(vec![
                    change("1", LEVEL_LIMIT + 1, Decimal::ONE),
                    change("2", 42000, Decimal::ONE),
                ]),
            )
            .unwrap();
        assert!(group.test_book("1").quarantined());
        assert!(group.test_book("1").synced().is_none());
        assert_eq!(
            group
                .test_book("2")
                .synced()
                .unwrap()
                .levels
                .quotes()
                .bid
                .unwrap()
                .price,
            price(42000)
        );
        assert!(!group.ready());
    }
    #[tokio::test(start_paused = true)]
    async fn terminal_receipt_is_published_once_on_explicit_progress() {
        let (finished, result) = std::sync::mpsc::channel();
        let mut group = NegRiskEventGroup::with_market_consumer(
            "fixture",
            crate::paper::Quantity::new(1.into()).unwrap(),
            move |at, input| match input {
                MarketInput::Ending(wait) => {
                    *wait = Ok(Some(at + std::time::Duration::from_millis(15)))
                }
                MarketInput::CancelMatured(_) => finished.send(at).unwrap(),
                _ => {}
            },
        );
        let at = Instant::now();
        let due = at + std::time::Duration::from_millis(15);
        group.end_consumer(at);
        assert_eq!(group.receipt_deadline(), Some(due));
        assert!(!group.advance_receipt().unwrap());
        tokio::time::advance(std::time::Duration::from_millis(65)).await;
        assert!(!group.receipt_complete().unwrap());
        assert!(result.try_recv().is_err());
        assert!(group.advance_receipt().unwrap());
        assert!(group.advance_receipt().unwrap());
        assert_eq!(group.receipt_deadline(), None);
        assert_eq!(result.try_iter().collect::<Vec<_>>(), [due]);
    }
    #[tokio::test(start_paused = true)]
    async fn capacity_before_subscription_activation_ends_at_admission() {
        let mut group = open_group();
        group
            .resolved
            .extend((0..MAX_SETTLEMENTS).map(|n| format!("old-{n}")));
        let (send, ended) = std::sync::mpsc::channel();
        group.consumer = Consumer::Active(
            25.into(),
            Box::new(move |at, input| {
                if let MarketInput::Ending(_) = input {
                    send.send(at).unwrap();
                }
            }),
        );
        let admitted = Instant::now();
        let (wire, _sender) = crate::transport::Transport::buffered_for_test([
            r#"{"event_type":"market_resolved","market":"a"}"#.to_owned(),
        ]);
        tokio::time::advance(std::time::Duration::from_millis(40)).await;
        assert!(group.source().is_none());
        let mut retirement =
            input::Retirement::new(&mut group, None, wire.seal(), input::End::Stop);
        while retirement.advance() {}
        assert!(
            retirement
                .complete()
                .unwrap_err()
                .is::<SettlementCapacity>()
        );
        assert_eq!(ended.try_iter().collect::<Vec<_>>(), [admitted]);
    }

    #[test]
    fn capacity_in_a_rejected_or_queued_frame_ends_the_consumer_once() {
        let settlement = r#"{"event_type":"market_resolved","market":"a"}"#;
        let invalid = r#"{"event_type":"book","bids":"bad"}"#;
        for frames in [
            vec![format!("[{invalid},{settlement}]")],
            vec![invalid.into(), settlement.into()],
        ] {
            let mut group = open_group();
            group
                .resolved
                .extend((0..MAX_SETTLEMENTS).map(|n| format!("old-{n}")));
            let (send, received) = std::sync::mpsc::channel();
            group.consumer = Consumer::Active(
                25.into(),
                Box::new(move |_, input| match input {
                    MarketInput::Ending(_) => send.send("end").unwrap(),
                    MarketInput::Interrupted => send.send("interrupted").unwrap(),
                    _ => {}
                }),
            );
            let source = group.open_input().unwrap();
            for frame in frames {
                group.admit_raw(source, frame).unwrap();
            }
            let error = group.advance_input(source).unwrap_err();
            assert!(error.is::<SettlementCapacity>());
            let prefix = error.downcast_ref::<AdditionalFailure>().unwrap();
            assert_eq!(prefix.0.to_string(), "invalid WebSocket message");
            assert_eq!(received.try_iter().collect::<Vec<_>>(), ["end"]);
            assert!(group.open_input().is_none());
        }
    }
}
