//! Prepare one wire frame before publishing any of its market or paper effects.
use super::*;
use crate::wire::PublicTrade;

#[cfg(test)]
pub(crate) mod tests {
    thread_local! {
        static BEFORE_COMMIT: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    }
    pub(crate) fn before_next_commit(f: impl FnOnce() + 'static) {
        BEFORE_COMMIT.set(Some(Box::new(f)));
    }
    pub(super) fn after_prepare() {
        if let Some(f) = BEFORE_COMMIT.take() {
            f();
        }
    }
}

enum Edit {
    Snapshot(Snapshot),
    Changes(u64, Vec<Change>),
    Tick(u64, Decimal),
}
impl Edit {
    fn apply(
        &self,
        book: &mut TokenBook,
        received: Instant,
        source: SubscriptionId,
        commit: Instant,
    ) -> Result<()> {
        match self {
            Self::Snapshot(s) => book.snapshot(s, received, source.deadline, || commit),
            Self::Changes(timestamp, changes) => {
                book.changes(*timestamp, changes, received, || commit)
            }
            Self::Tick(timestamp, tick) => book.tick(*timestamp, *tick),
        }
    }
}

enum Item {
    Books(String, Vec<(String, Edit)>),
    Resolve(String),
    Trade(PublicTrade),
    Frontier,
}
impl From<Event> for Item {
    fn from(event: Event) -> Self {
        match event {
            Event::Book(s) => Self::Books(
                s.market.clone(),
                vec![(s.asset_id.clone(), Edit::Snapshot(s))],
            ),
            Event::PriceChange {
                market,
                timestamp,
                price_changes,
            } => {
                let mut grouped = BTreeMap::<String, Vec<Change>>::new();
                for c in price_changes {
                    grouped.entry(c.asset_id.clone()).or_default().push(c);
                }
                Self::Books(
                    market,
                    grouped
                        .into_iter()
                        .map(|(id, batch)| (id, Edit::Changes(timestamp, batch)))
                        .collect(),
                )
            }
            Event::TickSizeChange {
                market,
                asset_id,
                timestamp,
                new_tick_size,
            } => Self::Books(
                market,
                vec![(asset_id, Edit::Tick(timestamp, new_tick_size))],
            ),
            Event::MarketResolved { market } => Self::Resolve(market),
            Event::LastTradePrice(t) => Self::Trade(t),
            Event::Other => Self::Frontier,
        }
    }
}

struct StagedBook {
    book: TokenBook,
    last: usize,
    index: usize,
    outcome: Outcome,
}
#[derive(Default)]
struct Preparation {
    books: HashMap<String, StagedBook>,
    first_deadline: Option<Instant>,
    next_boundary: Option<Instant>,
}
fn earliest(slot: &mut Option<Instant>, at: Instant) {
    *slot = Some(slot.map_or(at, |prev| prev.min(at)));
}
impl Preparation {
    fn deadline(&mut self, due: Instant, now: Instant) {
        earliest(&mut self.first_deadline, due);
        if due > now {
            earliest(&mut self.next_boundary, due);
        }
    }
}

impl NegRiskEventGroup {
    fn prepare_frame(
        &self,
        items: &[Item],
        source: SubscriptionId,
        received: Instant,
        now: Instant,
    ) -> Result<Preparation> {
        self.check_capacity()?;
        let mut prepared = Preparation::default();
        if let Some(due) = self.reconciliation_deadline() {
            prepared.deadline(due, now);
        }
        if self.awaiting_snapshot() && source.deadline > now {
            earliest(&mut prepared.next_boundary, source.deadline);
        }
        let Some(roster) = self.roster() else {
            return Ok(prepared);
        };
        let mut resolutions = HashSet::new();
        for (i, item) in items.iter().enumerate() {
            match item {
                Item::Resolve(id)
                    if !self.resolved.contains(id) && roster.market_index(id).is_some() =>
                {
                    resolutions.insert(id);
                    if self.resolved.len() + resolutions.len() > MAX_SETTLEMENTS {
                        return Err(SettlementCapacity.into());
                    }
                }
                Item::Books(condition, edits) => {
                    for (asset, edit) in edits {
                        let Some(&(index, outcome)) = roster.by_asset.get(asset) else {
                            continue;
                        };
                        let market = &roster.markets[index];
                        if self.market_closed(market) || resolutions.contains(&market.condition) {
                            continue;
                        }
                        let staged =
                            prepared
                                .books
                                .entry(asset.clone())
                                .or_insert_with(|| StagedBook {
                                    book: market.book(outcome).clone(),
                                    last: i,
                                    index,
                                    outcome,
                                });
                        if now >= source.deadline {
                            staged.book.expire_snapshot();
                        }
                        staged.last = i;
                        staged.book.update(|b| {
                            ensure!(
                                market.condition == *condition,
                                "unexpected condition ID {condition}"
                            );
                            edit.apply(b, received, source, now)
                        })?;
                        if let BookState::Reconciling { deadline, .. } = staged.book.state {
                            prepared.deadline(deadline, now);
                        }
                    }
                }
                Item::Trade(trade) => ensure!(
                    Decimal::ZERO < trade.price
                        && trade.price < Decimal::ONE
                        && trade.size > Decimal::ZERO,
                    "invalid public trade price or size"
                ),
                _ => {}
            }
        }
        Ok(prepared)
    }

    pub(super) fn commit_frame(
        &mut self,
        source: SubscriptionId,
        events: Vec<Event>,
        at: Instant,
    ) -> Result<Option<Instant>> {
        let mut sample = Instant::now();
        ensure!(at <= sample, "future reception time");
        let mut items = Vec::with_capacity(events.len());
        for event in events {
            event.validate_size()?;
            items.push(Item::from(event));
        }
        let (mut prepared, commit) = loop {
            let staged = self.prepare_frame(&items, source, at, sample);
            #[cfg(test)]
            tests::after_prepare();
            let commit = Instant::now();
            // A first snapshot that times out during validation is isolated locally.
            if staged.is_err()
                && sample < source.deadline
                && source.deadline <= commit
                && self.awaiting_snapshot()
            {
                sample = commit;
                continue;
            }
            let staged = staged?;
            if staged.next_boundary.is_some_and(|d| d <= commit) {
                sample = commit;
                continue;
            }
            break (staged, commit);
        };
        let reached = prepared.first_deadline.filter(|d| *d <= commit);
        if reached.is_some_and(|d| at >= d) {
            return Ok(reached);
        }
        if commit >= source.deadline {
            self.books_mut().for_each(TokenBook::expire_snapshot);
        }
        if items.is_empty() {
            self.deliver(at, MarketInput::Frontier);
        }
        for (i, item) in items.into_iter().enumerate() {
            match item {
                Item::Books(_, edits) => {
                    for (asset, edit) in edits {
                        let Some(staged) = prepared.books.get(&asset) else {
                            continue;
                        };
                        let market = &mut self.roster_mut().unwrap().markets[staged.index];
                        let book = match staged.outcome {
                            Outcome::Yes => &mut market.yes,
                            Outcome::No => &mut market.no,
                        };
                        if staged.last == i {
                            book.state = prepared.books.remove(&asset).unwrap().book.state;
                        } else {
                            // All transitions were checked in FIFO order at this commit instant.
                            book.update(|b| edit.apply(b, at, source, commit))
                                .expect("prepared book transition");
                        }
                    }
                }
                Item::Resolve(id) => self.resolve(&id).expect("prepared settlement capacity"),
                Item::Trade(t) => {
                    if self.roster().is_some_and(|r| {
                        r.by_asset
                            .get(&t.asset_id)
                            .is_some_and(|(i, _)| !self.market_closed(&r.markets[*i]))
                    }) {
                        self.deliver(at, MarketInput::Trade(&t));
                    } else {
                        self.deliver(at, MarketInput::Frontier);
                    }
                    continue;
                }
                Item::Frontier => {
                    self.deliver(at, MarketInput::Frontier);
                    continue;
                }
            }
            self.publish_offers(at, source);
            if let Consumer::Active(n, _) = self.consumer {
                self.deliver(at, MarketInput::Depth(self.buy_depth(n)));
            }
            self.publish_paper();
        }
        Ok(reached)
    }
}
