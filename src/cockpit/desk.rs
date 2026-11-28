use super::Screen;
use crate::wire::{PublicTrade, Side as TradeSide};
use crate::{
    book::{BasketUpdate, CommittedBook, MarketInput},
    gamma_discovery::{self, Discovery},
    paper::{
        AccountId, AssetId, Cash, ClientOrderId, OrderKey, Price, Quantity, Shares, Side,
        chaos_exchange::ChaosExchange,
        fee_model::{FeeModel, MakerTier},
        order_fsm::{CancelReason, CloseReason, Delivery, OrderMode, OrderRequest, OrderStatus},
    },
};
use anyhow::{Context, Result, ensure};
use reqwest::Client;
use rust_decimal::Decimal as D;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot, watch};

pub(super) const SIZE: u32 = 25;
const CAPITAL: u32 = 1000;
const AGE: u64 = 60_000;
fn dec(v: &Value) -> Result<D> {
    Ok(D::from_str(
        v.as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| v.to_string())
            .as_str(),
    )?)
}
fn wall() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone, PartialEq)]
pub(super) struct Leg {
    condition: String,
    name: String,
    asset: AssetId,
    no: AssetId,
    fee: FeeModel,
    rate: D,
    minimum: D,
    delay: u64,
}
impl Leg {
    fn same_rules(&self, other: &Self) -> bool {
        self.condition == other.condition
            && self.asset == other.asset
            && self.no == other.no
            && self.fee == other.fee
            && self.minimum == other.minimum
            && self.delay == other.delay
    }

    fn identity(&self) -> String {
        crate::book::view::market_identity(&self.condition, self.asset.as_str(), self.no.as_str())
    }
}

async fn roster(client: &Client, slug: &str, gamma: &str, clob: &str) -> Result<Vec<Leg>> {
    let Discovery::Open(group) =
        gamma_discovery::discover(client, gamma, slug, Duration::from_secs(60)).await?
    else {
        anyhow::bail!("事件未開放")
    };
    ensure!(
        !group.quotes_only(),
        "此事件僅提供具名選項報價，紙上交易已停用"
    );
    let specs = group.into_parts().1;
    ensure!(specs.len() <= 64, "紙上引擎最多支援 64 腿");
    let mut legs = Vec::new();
    for s in specs {
        let (condition, name, ids, _, closed) = s.into_parts();
        ensure!(!closed, "市場已結束");
        let response = client
            .get(format!("{clob}/clob-markets/{condition}"))
            .send()
            .await?;
        let info: Value = serde_json::from_slice(
            &crate::wire::success_body(response, 128 * 1024, "CLOB market rules").await?,
        )?;
        ensure!(
            info["c"] == condition
                && info["ao"] == true
                && info["t"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|t| t["o"] == "Yes" && t["t"] == ids[0])),
            "CLOB 名冊不符"
        );
        let rate = if info["fd"].is_null() {
            let response = client
                .get(format!("{clob}/fee-rate"))
                .query(&[("token_id", &ids[0])])
                .send()
                .await?;
            let fee: Value = serde_json::from_slice(
                &crate::wire::success_body(response, 4096, "CLOB fee rate").await?,
            )?;
            ensure!(dec(&fee["base_fee"])? == D::ZERO, "無法核對曲線費率");
            D::ZERO
        } else {
            dec(&info["fd"]["r"])?
        };
        ensure!(
            rate == D::ZERO || (info["fd"]["e"] == 1 && info["fd"]["to"] == true),
            "未知費率模型"
        );
        legs.push(Leg {
            condition,
            name,
            asset: AssetId::new(&ids[0])?,
            no: AssetId::new(&ids[1])?,
            fee: FeeModel::new(rate, false)?,
            rate,
            minimum: dec(&info["mos"])?,
            delay: info["sd"].as_u64().unwrap_or(0) * 1000,
        });
    }
    Ok(legs)
}

#[derive(Clone, PartialEq)]
struct Quote {
    bid: Price,
    ahead: D,
}

fn quotes(update: BasketUpdate, legs: &[Leg], now: Instant) -> Option<(Vec<Quote>, Instant)> {
    let books = update?;
    if books.len() != legs.len() {
        return None;
    }
    let expiry = books.iter().map(|b| b.observed_at).min()? + Duration::from_millis(AGE);
    if now >= expiry {
        return None;
    }
    let qs = legs
        .iter()
        .map(|l| {
            let b = books.iter().find(|b| b.asset == l.asset.as_str())?;
            let bid = b.bid?;
            Price::new(b.tick).ok()?;
            if b.asks.is_empty()
                || b.asks.iter().any(|v| {
                    Price::new(v.price).is_err()
                        || Quantity::new(v.size.min(D::from(1_000_000))).is_err()
                })
            {
                return None;
            }
            Some(Quote {
                bid: Price::new(bid.price).ok()?,
                ahead: bid.size,
            })
        })
        .collect::<Option<_>>()?;
    Some((qs, expiry))
}

pub enum Command {
    Lag(u64),
    Auto(bool),
    Fire,
}
pub type Request = (Command, oneshot::Sender<Result<(), String>>);
impl CancelReason {
    fn label(self) -> &'static str {
        match self {
            Self::EdgeLost => "[DYNAMIC-CANCEL: 價差消失]",
            Self::Timeout => "[TIMEOUT-CANCEL: 20s 逾時]",
            Self::QuoteUnavailable => "[DYNAMIC-CANCEL: 報價失效]",
            Self::Disabled => "策略已關閉",
        }
    }
}
struct Hedge {
    leg: usize,
    limit: Price,
    due: u64,
}
enum Round {
    Sending {
        leg: usize,
        quote: Quote,
        due: u64,
        force: bool,
    },
    Resting {
        leg: usize,
        key: OrderKey,
        price: Price,
        initial: D,
        placed: u64,
        forced_entry_cost: Option<D>,
    },
    AwaitingFillReport {
        leg: usize,
        filled: D,
        due: u64,
    },
    Hedging {
        filled: D,
        orders: Vec<Hedge>,
    },
}
struct Desk {
    fee_block: Option<&'static str>,
    legs: Vec<Leg>,
    ex: ChaosExchange,

    account: AccountId,
    quotes: Option<Vec<Quote>>,
    round: Option<Round>,
    lag: u64,
    auto: bool,
    next_id: u64,

    retry: u64,
    log: VecDeque<String>,
    markers: VecDeque<Value>,
    tape: VecDeque<Value>,
    visual_id: u64,
}
impl Desk {
    fn new(legs: Vec<Leg>, origin: Instant) -> Result<Self> {
        let cash = Cash::new(CAPITAL.into())?;
        let mut ex = ChaosExchange::new(AGE, cash)?;
        ex.bind_live_clock(origin);
        for l in &legs {
            ex.register_asset(l.asset.clone(), l.fee, Shares::ZERO)?;
        }
        let account = ex.allocate(cash, [], MakerTier::None)?;
        Ok(Self {
            fee_block: None,
            legs,
            ex,

            account,
            quotes: None,
            round: None,
            lag: 150,
            auto: false,
            next_id: 1,

            retry: 0,
            log: VecDeque::new(),
            markers: VecDeque::new(),
            tape: VecDeque::new(),
            visual_id: 0,
        })
    }
    fn mark(&mut self, leg: usize, kind: &str, detail: String) {
        self.visual_id += 1;
        if self.markers.len() == 128 {
            self.markers.pop_front();
        }
        self.markers.push_back(
            json!({"id":self.visual_id,"at":wall(),"leg":leg,"kind":kind,"detail":detail}),
        );
    }
    fn public_print(&mut self, msg: &PublicTrade) {
        let token = msg.asset_id.as_str();
        let Some((leg, l)) = self
            .legs
            .iter()
            .enumerate()
            .find(|(_, l)| l.asset.as_str() == token || l.no.as_str() == token)
        else {
            return;
        };
        let (price, size) = (msg.price, msg.size);
        let side = match msg.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        };
        self.visual_id += 1;
        if self.tape.len() == 64 {
            self.tape.pop_front();
        }
        self.tape.push_back(json!({"id":self.visual_id,"at":wall(),"leg":leg,"token":if l.asset.as_str()==token{"YES"}else{"NO"},"side":side,"price":price.to_string(),"size":size.to_string()}));
    }
    fn note(&mut self, now: u64, msg: impl std::fmt::Display) {
        if self.log.len() == 24 {
            self.log.pop_front();
        }
        self.log
            .push_back(format!("{}.{:03}s · {msg}", now / 1000, now % 1000));
    }
    fn reject_hedge(&mut self, leg: usize, filled: D, reason: &str, at: u64) {
        let detail = format!("{reason}，保留未補 {filled} 股");
        self.note(at, format!("{} · {detail}", self.legs[leg].name));
        self.mark(leg, "補腿", detail);
    }

    fn verify_rules(&mut self, result: &Result<Vec<Leg>, String>, at: u64) {
        self.fee_block = match result {
            Ok(legs)
                if legs.len() == self.legs.len()
                    && self
                        .legs
                        .iter()
                        .zip(legs)
                        .all(|(current, fresh)| current.same_rules(fresh)) =>
            {
                for (current, fresh) in self.legs.iter_mut().zip(legs) {
                    current.name.clone_from(&fresh.name);
                }
                None
            }
            Ok(_) => Some("場次規則已變更"),
            Err(_) => Some("費率核對失敗"),
        };
        if let Some(reason) = self.fee_block {
            self.auto = false;
            if matches!(self.round, Some(Round::Hedging { .. }))
                && let Some(Round::Hedging { filled, orders }) = self.round.take()
            {
                for order in orders {
                    self.reject_hedge(order.leg, filled, reason, at);
                }
            }
        }
    }
    fn cost(&self, i: usize, resting_price: Price) -> Result<D> {
        self.quotes.as_ref().context("行情尚未就緒")?;
        let n = D::from(SIZE);
        let shares = Shares::new(n)?;
        // This paper account uses MakerTier::None, so its expected rebate is zero.
        let rebate = self.legs[i]
            .fee
            .maker_rebate(MakerTier::None, resting_price, shares);
        let mut cost = n * resting_price.decimal() - rebate.decimal();
        for (j, leg) in self.legs.iter().enumerate() {
            ensure!(n >= leg.minimum, "未達最小股數");
            if i != j {
                let estimate = self
                    .ex
                    .estimate_buy(&leg.asset, shares)
                    .filter(|estimate| estimate.shares == shares)
                    .context("可見補腿深度不足")?;
                cost += estimate.cost;
            }
        }
        Ok(cost)
    }

    fn entry_cost(&self, leg: usize, bid: Price, force: bool) -> Result<D> {
        let cost = self.cost(leg, bid)?;
        ensure!(force || cost < D::from(SIZE), "含費成本未低於 25");
        ensure!(self.balance(leg)?.cash().decimal() >= cost, "可用現金不足");
        Ok(cost)
    }

    fn check_cancel(&mut self, t: u64) -> Result<()> {
        let Some(Round::Resting {
            leg,
            price,
            key,
            placed,
            forced_entry_cost,
            ..
        }) = self.round
        else {
            return Ok(());
        };
        if self.ex.cancel_request(key).is_some()
            || !matches!(self.ex.order_status(key)?, OrderStatus::Working { .. })
        {
            return Ok(());
        }
        let n = D::from(SIZE);
        let reason = if t >= placed + 20_000 {
            CancelReason::Timeout
        } else if forced_entry_cost.is_none() && self.quotes.is_none() {
            CancelReason::QuoteUnavailable
        } else if forced_entry_cost.is_none() && !self.auto {
            CancelReason::Disabled
        } else {
            match self.cost(leg, price) {
                Err(_) if forced_entry_cost.is_some() => return Ok(()),
                Err(_) => CancelReason::QuoteUnavailable,
                Ok(cost)
                    if forced_entry_cost
                        .map_or(cost >= n, |entry| cost > entry + n * D::new(1, 2)) =>
                {
                    CancelReason::EdgeLost
                }
                Ok(_) => return Ok(()),
            }
        };
        self.ex.schedule_cancel(key, reason, t, self.lag)?;
        self.note(t, format!("{} 送出撤單，等待確認", reason.label()));
        Ok(())
    }

    fn start(&mut self, t: u64, force: bool) -> Result<()> {
        ensure!(self.ex.has_slots(self.legs.len()), "紙上訂單容量不足");
        ensure!(self.round.is_none(), "已有一輪進行中");
        let qs = self.quotes.as_ref().context("等待已同步行情與成交帶")?;
        let (i, q) = qs
            .iter()
            .enumerate()
            .min_by_key(|(_, q)| q.ahead)
            .context("無有效市場")?;
        ensure!(q.ahead <= D::from(1_000_000), "前方隊列超出紙上引擎容量");
        let cost = self.entry_cost(i, q.bid, force)?;
        self.round = Some(Round::Sending {
            leg: i,
            quote: q.clone(),
            due: t + self.lag,
            force,
        });
        self.note(
            t,
            format!(
                "{} · {} · 25 股送出，估計整筆成本 {cost}",
                if force {
                    "強制試射"
                } else {
                    "閘門通過"
                },
                self.legs[i].name
            ),
        );
        Ok(())
    }
    fn balance(&self, i: usize) -> Result<crate::paper::chaos_exchange::Balance> {
        Ok(self.ex.balance(self.account, &self.legs[i].asset)?)
    }
    fn commit_offers(&mut self, book: std::sync::Arc<CommittedBook>) -> Result<()> {
        if self
            .legs
            .iter()
            .any(|leg| leg.asset.as_str() == book.asset())
        {
            self.ex.commit_book(&book)?;
        }
        Ok(())
    }
    fn submit(
        &mut self,
        i: usize,
        p: Price,
        n: D,
        mode: OrderMode,
    ) -> Result<(OrderKey, OrderStatus)> {
        let a = self.legs[i].asset.clone();
        let stamp = self.ex.current_stamp(&a)?.context("報價過期")?;
        let id = ClientOrderId::new(self.next_id)?;
        self.next_id += 1;
        let r = self.ex.submit(
            OrderRequest::new(id, self.account, a, Side::Buy, p, Quantity::new(n)?, mode),
            stamp,
            Delivery::Delivered,
        )?;
        Ok((r.key(), r.status()))
    }
    fn step(&mut self, t: u64) -> Result<()> {
        self.ex.advance_to(t)?;
        self.check_cancel(t)?;
        let Some(r) = self.round.take() else {
            return Ok(());
        };
        self.round = match r {
            Round::Sending {
                leg,
                quote,
                due,
                force,
            } if t >= due => {
                let q = self.quotes.as_ref().context("掛單送達時盤面失效")?[leg].clone();
                ensure!(q.bid == quote.bid, "送達時掛價失效");
                let cost = self.entry_cost(leg, quote.bid, force)?;
                ensure!(q.ahead <= D::from(1_000_000), "前方隊列超出紙上引擎容量");
                let (key, status) = self.submit(leg, q.bid, SIZE.into(), OrderMode::PostOnly)?;
                ensure!(
                    matches!(status, OrderStatus::Working { .. }),
                    "掛單未進入隊列：{status:?}"
                );
                self.mark(leg, "掛單", format!("25 股 @ {}", q.bid.decimal()));
                self.note(t, format!("Post-Only 已排隊 · 前方 {} 股", q.ahead));
                Some(Round::Resting {
                    leg,
                    key,
                    price: q.bid,
                    initial: self.ex.queue_ahead(key).unwrap_or(Shares::ZERO).decimal(),
                    placed: t,
                    forced_entry_cost: force.then_some(cost),
                })
            }
            Round::Resting {
                leg,
                key,
                price,
                initial,
                placed,
                forced_entry_cost,
            } => {
                let cancel = self.ex.cancel_request(key);
                let status = self.ex.order_status(key)?;
                let filled = match status {
                    OrderStatus::Working { filled, .. } | OrderStatus::Closed { filled, .. } => {
                        filled.decimal()
                    }
                    _ => D::ZERO,
                };
                if matches!(status, OrderStatus::Closed { .. })
                    || cancel
                        .as_ref()
                        .is_some_and(|record| t >= record.effective_ms)
                {
                    if matches!(status, OrderStatus::Working { .. }) {
                        self.ex.cancel(key, Delivery::Delivered)?;
                    }
                    if let Some(record) = cancel {
                        let detail = format!(
                            "#{} {}，累計成交 {filled} 股",
                            key.client_order_id().get(),
                            record.reason.label()
                        );
                        self.mark(leg, "撤單", detail);
                    }
                    let reason = match status {
                        OrderStatus::Closed { reason, .. } => format!("{reason:?}"),
                        _ => "撤單確認".into(),
                    };
                    self.note(t, format!("掛單結束 · {reason} · 確認成交 {filled} 股"));
                    self.retry = t + 1000;
                    if filled > D::ZERO {
                        Some(Round::AwaitingFillReport {
                            leg,
                            filled,
                            due: t + self.lag,
                        })
                    } else {
                        None
                    }
                } else {
                    Some(Round::Resting {
                        leg,
                        key,
                        price,
                        initial,
                        placed,
                        forced_entry_cost,
                    })
                }
            }
            Round::AwaitingFillReport { leg, filled, due } if t >= due => {
                let mut orders = Vec::new();
                let confirmed = Shares::new(filled)?;
                for i in 0..self.legs.len() {
                    if i == leg {
                        continue;
                    }
                    if let Some(reason) = self.fee_block {
                        self.reject_hedge(i, filled, reason, t);
                    } else if let Some(estimate) =
                        self.ex.estimate_buy(&self.legs[i].asset, confirmed)
                    {
                        orders.push(Hedge {
                            leg: i,
                            limit: estimate.limit,
                            due: t + self.lag + self.legs[i].delay,
                        });
                    } else {
                        self.auto = false;
                        self.mark(i, "補腿", "缺少有效賣盤，保留未補持倉".into());
                    }
                }
                if orders.is_empty() {
                    return Ok(());
                }
                self.note(t, format!("確認 {filled} 股，送出限價 IOC 補腿"));
                Some(Round::Hedging { filled, orders })
            }
            Round::Hedging { filled, orders } => {
                let mut pending = Vec::new();
                for h in orders {
                    if t < h.due {
                        pending.push(h);
                        continue;
                    }
                    let i = h.leg;
                    let result = (|| -> Result<D> {
                        ensure!(filled >= self.legs[i].minimum, "不足最小股數");
                        match self
                            .submit(i, h.limit, filled, OrderMode::ImmediateOrCancel)?
                            .1
                        {
                            OrderStatus::Closed { filled, .. } => Ok(filled.decimal()),
                            s => anyhow::bail!("IOC 狀態 {s:?}"),
                        }
                    })();
                    if result.as_ref().ok() != Some(&filled) {
                        self.auto = false;
                    }
                    let status = result
                        .map(|n| format!("成交 {n}/{filled} 股"))
                        .unwrap_or_else(|e| e.to_string());
                    self.mark(i, "補腿", status.clone());
                    self.note(
                        t,
                        format!(
                            "IOC {} · 限價 {} · {status}",
                            self.legs[i].name,
                            h.limit.decimal()
                        ),
                    );
                }
                if pending.is_empty() {
                    None
                } else {
                    Some(Round::Hedging {
                        filled,
                        orders: pending,
                    })
                }
            }
            r => Some(r),
        };
        Ok(())
    }
    fn next_due(&self) -> Option<u64> {
        match &self.round {
            Some(Round::Sending { due, .. } | Round::AwaitingFillReport { due, .. }) => Some(*due),
            Some(Round::Resting { placed, key, .. }) => Some(
                self.ex
                    .cancel_request(*key)
                    .as_ref()
                    .map_or(placed + 20_000, |record| record.effective_ms),
            ),
            Some(Round::Hedging { orders, .. }) => orders.iter().map(|h| h.due).min(),
            None => None,
        }
    }
    fn run_due(&mut self, t: u64) -> Result<()> {
        if let Err(error) = self.step(t) {
            // Rejected arrivals can end a round. They must not consume the
            // next admitted market event that drove the logical clock.
            self.note(t, error);
            self.retry = t + 1000;
            if self.round.is_some() {
                anyhow::bail!("無法推進紙上訂單期限");
            }
        }
        Ok(())
    }
    fn print(&mut self, t: u64, msg: &PublicTrade) -> Result<()> {
        let Some(Round::Resting {
            leg, key, price, ..
        }) = self.round
        else {
            return Ok(());
        };
        let l = &self.legs[leg];
        if msg.asset_id != l.asset.as_str()
            || !matches!(msg.side, TradeSide::Sell)
            || msg.price != price.decimal()
        {
            return Ok(());
        }

        self.ex.advance_to(t)?;
        let before = self.balance(leg)?.shares().decimal();
        self.ex.paper_print(l.asset.clone(), price, msg.size)?;
        let change = self.balance(leg)?.shares().decimal() - before;
        if change > D::ZERO {
            let kind = if matches!(self.ex.order_status(key)?, OrderStatus::Working { .. }) {
                "部分成交"
            } else {
                "成交"
            };
            self.mark(leg, kind, format!("Maker 成交 {change} 股"));
        }
        self.note(t, format!("同價賣出成交回報 {} 股", msg.size));
        Ok(())
    }
    fn finish_round(&mut self, t: u64) -> Result<Option<u64>> {
        if let Some(Round::Resting { key, .. }) = &self.round
            && let Some(due) = self.ex.finish_order(*key, t)?
        {
            return Ok(Some(due));
        }
        self.round = None;
        Ok(None)
    }

    fn screen(&self) -> Value {
        let cash = self.balance(0).unwrap().cash().decimal();
        let mut frozen = D::ZERO;
        let state = match self.round {
            None => "待命".to_owned(),
            Some(Round::Sending { .. }) => "掛單傳輸中".into(),
            Some(Round::AwaitingFillReport { filled, .. }) => format!("等待成交回報 · {filled} 股"),
            Some(Round::Hedging { filled, .. }) => format!("等待 IOC 補腿 · {filled} 股"),
            Some(Round::Resting { key, price, .. }) => {
                let ahead = self.ex.queue_ahead(key).unwrap_or(Shares::ZERO).decimal();
                let filled = match self.ex.order_status(key) {
                    Ok(OrderStatus::Working { filled, remaining }) => {
                        frozen = remaining.shares().decimal() * price.decimal();
                        filled.decimal()
                    }
                    Ok(OrderStatus::Closed { filled, .. }) => filled.decimal(),
                    _ => D::ZERO,
                };
                format!(
                    "{} · 前方 {ahead} 股 · 已成交 {filled} / 25",
                    if self.ex.cancel_request(key).is_some() {
                        "撤單傳輸中"
                    } else {
                        "排隊中"
                    }
                )
            }
        };
        let positions: Vec<_> = self
            .legs
            .iter()
            .enumerate()
            .map(|(i, l)| (l.name.as_str(), self.balance(i).unwrap().shares().decimal()))
            .collect();
        let marked = self.quotes.as_ref().map(|qs| {
            positions
                .iter()
                .zip(qs)
                .fold(cash + frozen - D::from(CAPITAL), |v, ((_, n), q)| {
                    v + *n * q.bid.decimal()
                })
        });
        let queue = match &self.round {
            Some(Round::Resting {
                leg, key, initial, ..
            }) => Some(
                json!({"leg":leg,"initial":initial.to_string(),"remaining":self.ex.queue_ahead(*key).unwrap_or(Shares::ZERO).decimal().to_string()}),
            ),
            _ => None,
        };
        let mut orders = Vec::new();
        let mut cancellations = Vec::new();
        for order in self.ex.order_records() {
            let id = order.request().id().get();
            let request = order.cancellation();
            let closed = match order.status() {
                OrderStatus::Closed { reason, filled } => {
                    Some((reason, filled.decimal().to_string()))
                }
                _ => None,
            };
            if let Some((reason, filled)) = &closed {
                let cause = match (reason, request) {
                    (CloseReason::Cancelled, Some(r)) => json!(r.reason),
                    _ => json!(reason),
                };
                orders.push(json!({"order_id":id,"reason":cause,"filled":filled,"effective_ms":order.closed_at()}));
            }
            if let Some(r) = request {
                let mut entry = json!(r);
                entry["order_id"] = json!(id);
                entry["label"] = json!(r.reason.label());
                entry["outcome"] = json!(match &closed {
                    None => "pending",
                    Some((CloseReason::Filled, _)) => "filled_before_cancel",
                    Some((CloseReason::Cancelled, _)) => "cancelled",
                    _ => "closed_by_market",
                });
                if let Some((reason, filled)) = closed {
                    entry["filled"] = json!(filled);
                    entry["close_reason"] = json!(reason);
                }
                cancellations.push(entry);
            }
        }
        json!({"orders":orders,"cancellations":cancellations,"queue":queue,"slots":positions.iter().zip(&self.legs).map(|((name,n),leg)|json!({"identity":leg.identity(),"name":name,"shares":n.to_string()})).collect::<Vec<_>>(),"markers":self.markers,"tape":self.tape,"state":state,"busy":self.round.is_some(),"ready":self.quotes.is_some(),"lag":self.lag,"auto":self.auto,"cash":cash.to_string(),"frozen":frozen.to_string(),"pnl":marked.map(|v|v.to_string()),"conserved":self.ex.is_conserved(),"log":self.log,"positions":positions.iter().filter(|(_,n)|*n>D::ZERO).map(|(s,n)|format!("{s} × {n}")).collect::<Vec<_>>(),"error":self.fee_block,"rates":self.fee_block.is_none().then(||self.legs.iter().map(|l|(l.identity(),Value::String(l.rate.to_string()))).collect::<serde_json::Map<_,_>>())})
    }
}
use tokio::time::{Instant, sleep};
fn publish(tx: &watch::Sender<Screen>, value: Value) {
    tx.send_if_modified(|s| {
        if *s.desk == value {
            return false;
        }
        s.desk = std::sync::Arc::new(value);
        true
    });
}
/// Fee discovery runs off the market input path. It never owns an order or a clock.
pub(super) type FeeUpdate = (Instant, Result<Vec<Leg>, String>);

pub(super) async fn metadata(client: Client, slug: String, tx: mpsc::Sender<FeeUpdate>) {
    loop {
        let result = roster(
            &client,
            &slug,
            gamma_discovery::GAMMA,
            "https://clob.polymarket.com",
        )
        .await
        .map_err(|e| e.to_string());
        let delay = if result.is_ok() { 60 } else { 5 };
        let Ok(slot) = tx.reserve().await else { return };
        slot.send((Instant::now(), result));
        sleep(Duration::from_secs(delay)).await;
    }
}

enum DeskInput<'a> {
    Market(MarketInput<'a>),
    Fees(Result<Vec<Leg>, String>),
}
#[derive(Clone, Copy)]
enum Frontier {
    Earlier,
    Market,
    Idle,
}

#[derive(Clone, Copy)]
enum Due {
    Fee,
    QuoteExpiry,
    Order(u64),
}

pub(super) struct Controller {
    desk: Option<Desk>,
    next_fee: Option<FeeUpdate>,
    meta: mpsc::Receiver<FeeUpdate>,
    commands: mpsc::Receiver<Request>,
    tx: watch::Sender<Screen>,
    origin: Instant,
    expiry: Option<Instant>,
    latest: BasketUpdate,
    startup_books: BTreeMap<String, std::sync::Arc<CommittedBook>>,
    sealed: bool,
    next_screen: u64,
}
impl Controller {
    pub(super) fn new(
        meta: mpsc::Receiver<FeeUpdate>,
        commands: mpsc::Receiver<Request>,
        tx: watch::Sender<Screen>,
    ) -> Self {
        Self {
            desk: None,
            next_fee: None,
            meta,
            commands,
            tx,
            origin: Instant::now(),
            expiry: None,
            latest: None,
            startup_books: BTreeMap::new(),
            sealed: false,
            next_screen: 0,
        }
    }
    fn advance(&mut self, at: Instant, frontier: Frontier) -> Result<bool> {
        let mut changed = false;
        loop {
            if self.next_fee.is_none() {
                self.next_fee = self.meta.try_recv().ok();
            }
            let fee = self.next_fee.as_ref().map(|(when, _)| *when);
            let order = self.desk.as_ref().and_then(Desk::next_due);
            // On an exact tie, the first eligible candidate wins. Re-read the
            // round after each action: verification or expiry can retire an order.
            let candidates = [
                fee.map(|when| (when, Due::Fee)),
                self.expiry.map(|when| (when, Due::QuoteExpiry)),
                order.map(|ms| (self.origin + Duration::from_millis(ms), Due::Order(ms))),
            ];
            let next = candidates
                .into_iter()
                .flatten()
                .filter(|(when, due)| {
                    *when < at
                        || (*when == at
                            && match frontier {
                                Frontier::Earlier => false,
                                Frontier::Market => !matches!(due, Due::Fee),
                                Frontier::Idle => true,
                            })
                })
                .min_by_key(|(when, _)| *when);
            match next {
                Some((when, Due::Fee)) => {
                    let (_, result) = self.next_fee.take().unwrap();
                    self.process(when, DeskInput::Fees(result));
                    changed = true;
                }
                Some((when, Due::QuoteExpiry)) => {
                    self.expiry = None;
                    let d = self
                        .desk
                        .as_mut()
                        .expect("quotes belong to an initialized desk");
                    d.ex.advance_live(when)?;
                    d.quotes = None;
                    d.check_cancel(when.saturating_duration_since(self.origin).as_millis() as u64)?;
                }
                Some((_, Due::Order(ms))) => {
                    self.desk.as_mut().unwrap().run_due(ms)?;
                    changed = true;
                }
                None => return Ok(changed),
            }
        }
    }

    fn finish(&mut self, t: u64) -> Result<Option<Instant>> {
        let Some(d) = &mut self.desk else {
            return Ok(None);
        };
        d.auto = false;
        d.quotes = None;
        self.expiry = None;
        let wait = d
            .finish_round(t)?
            .map(|due| self.origin + Duration::from_millis(due));
        publish(&self.tx, d.screen());
        Ok(wait)
    }

    pub(super) fn receive(&mut self, at: Instant, input: MarketInput<'_>) {
        let frontier = match &input {
            MarketInput::CommandCount(_) | MarketInput::Sealed | MarketInput::CancelMatured(_) => {
                self.process(at, DeskInput::Market(input));
                return;
            }
            MarketInput::Ending(_) | MarketInput::Interrupted | MarketInput::Frontier => {
                Frontier::Earlier
            }
            MarketInput::Clock(_) => Frontier::Idle,
            MarketInput::Offers(_)
            | MarketInput::Depth(_)
            | MarketInput::AssetUnavailable(..)
            | MarketInput::Trade(_) => Frontier::Market,
        };
        let t = at.saturating_duration_since(self.origin).as_millis() as u64;
        let mut changed = match self.advance(at, frontier) {
            Ok(changed) => changed,
            Err(error) => {
                if let MarketInput::Ending(wait) = input {
                    *wait = Err(error);
                } else if let Some(d) = &mut self.desk {
                    d.note(t, error);
                    d.retry = t + 1000;
                }
                return;
            }
        };
        let interrupted = matches!(input, MarketInput::Interrupted);
        let ending = matches!(input, MarketInput::Ending(_));
        let offers = matches!(input, MarketInput::Offers(_));
        self.process(at, DeskInput::Market(input));
        if ending {
            return;
        }
        // An interruption first removes unsafe work. Only its surviving local
        // receipts may mature at the cut. An empty frame never opens that cut.
        let after = if interrupted {
            Frontier::Market
        } else {
            frontier
        };
        if !matches!(after, Frontier::Earlier) {
            match self.advance(at, after) {
                Ok(progress) => changed |= progress,
                Err(error) => {
                    if let Some(d) = &mut self.desk {
                        d.note(t, error);
                        d.retry = t + 1000;
                    }
                }
            }
        }
        if !self.sealed
            && (changed || interrupted || (!offers && t >= self.next_screen))
            && let Some(d) = &self.desk
        {
            publish(&self.tx, d.screen());
            self.next_screen = t + 100;
        }
    }
    fn process(&mut self, at: Instant, input: DeskInput<'_>) {
        use DeskInput::{Fees, Market};
        let t = at.saturating_duration_since(self.origin).as_millis() as u64;
        if let Market(MarketInput::CommandCount(count)) = input {
            *count = self.commands.len();
            return;
        }
        if let Market(MarketInput::Ending(wait)) = input {
            *wait = self.finish(t);
            return;
        }
        if let Market(MarketInput::CancelMatured(result)) = input {
            *result = (|| {
                let d = self.desk.as_mut().context("missing pending cancellation")?;
                ensure!(
                    d.finish_round(t)?.is_none(),
                    "cancel receipt has not matured"
                );
                publish(&self.tx, d.screen());
                Ok(())
            })();
            return;
        }
        if matches!(input, Market(MarketInput::Sealed)) {
            self.sealed = true;
            self.tx.send_modify(|screen| {
                std::sync::Arc::make_mut(&mut screen.desk)["ready"] = json!(false);
            });
            return;
        }
        match &input {
            Fees(result) if self.desk.is_none() => match result {
                Ok(legs) => self.desk = Desk::new(legs.clone(), self.origin).ok(),
                Err(error) => publish(&self.tx, json!({"error":error})),
            },
            Market(MarketInput::Depth(depth)) => {
                self.latest = depth.clone();
            }
            Market(MarketInput::Interrupted) => {
                self.latest = None;
                self.startup_books.clear();
                self.sealed = false;
            }
            _ => {}
        }
        let Some(d) = &mut self.desk else {
            match input {
                Market(MarketInput::Offers(book)) => {
                    self.startup_books.insert(book.asset().into(), book);
                }
                Market(MarketInput::AssetUnavailable(asset, _)) => {
                    self.startup_books.remove(asset);
                }
                _ => {}
            }
            return;
        };
        let result = (|| -> Result<()> {
            d.ex.advance_live(at)?;
            match &input {
                Fees(result) => d.verify_rules(result, t),
                Market(MarketInput::Frontier) => return Ok(()),
                Market(MarketInput::Interrupted) => {
                    self.expiry = None;
                    d.quotes = None;
                }
                _ => {}
            }
            for (_, book) in std::mem::take(&mut self.startup_books) {
                if let Err(error) = d.commit_offers(book) {
                    d.note(t, error);
                }
            }
            if let Market(MarketInput::AssetUnavailable(asset, generation)) = &input {
                d.ex.suspend_asset(asset, *generation);
                return Ok(());
            }
            if let Market(MarketInput::Offers(book)) = &input {
                d.commit_offers(book.clone())?;
                return Ok(());
            }
            if matches!(
                input,
                Fees(_) | Market(MarketInput::Depth(_) | MarketInput::Interrupted)
            ) {
                let (qs, expiry) = if d.fee_block.is_none()
                    && d.legs
                        .iter()
                        .all(|l| d.ex.current_stamp(&l.asset).ok().flatten().is_some())
                {
                    quotes(self.latest.clone(), &d.legs, at).unzip()
                } else {
                    (None, None)
                };
                d.quotes = qs;
                self.expiry = expiry;
            }
            match input {
                Market(MarketInput::Trade(trade)) => {
                    d.public_print(trade);
                    d.print(t, trade)?;
                }
                Market(MarketInput::Interrupted) => {
                    d.ex.detach_queues();
                    if matches!(d.round, Some(Round::Resting { .. })) {
                        d.note(t, "輸入中斷，缺口期間成交未確認；舊隊列停止撮合");
                    }
                    if matches!(d.round, Some(Round::Sending { .. } | Round::Hedging { .. })) {
                        d.round = None;
                        d.note(t, "輸入中斷，停止未完成委託，保留已確認持倉");
                    }
                    d.auto = false;
                }
                Market(MarketInput::Clock(count)) => {
                    for _ in 0..count {
                        let Ok((cmd, reply)) = self.commands.try_recv() else {
                            break;
                        };
                        if reply.is_closed() {
                            continue;
                        }
                        let answer = match cmd {
                            Command::Lag(n) => {
                                d.lag = n;
                                Ok(())
                            }
                            Command::Auto(true) if d.fee_block.is_some() => {
                                Err(anyhow::anyhow!("場次規則尚未核准"))
                            }
                            Command::Auto(v) => {
                                d.auto = v;
                                if !v
                                    && matches!(d.round, Some(Round::Sending { force: false, .. }))
                                {
                                    d.round = None;
                                }
                                Ok(())
                            }
                            Command::Fire => d.start(t, true),
                        };
                        let _ = reply.send(answer.map_err(|e| e.to_string()));
                        self.next_screen = 0;
                    }
                    if d.auto && d.round.is_none() && t >= d.retry {
                        d.retry = t + 500;
                        let _ = d.start(t, false);
                    }
                }
                Fees(_) | Market(MarketInput::Depth(_) | MarketInput::Frontier) => {}
                Market(MarketInput::CommandCount(_))
                | Market(MarketInput::Ending(_))
                | Market(MarketInput::CancelMatured(_))
                | Market(MarketInput::Sealed)
                | Market(MarketInput::AssetUnavailable(..))
                | Market(MarketInput::Offers(_)) => {
                    unreachable!()
                }
            }
            // Apply the input before admitting any immediate work it creates.
            d.check_cancel(t)?;
            if let Some(Round::Resting { key, .. }) = d.round
                && matches!(d.ex.order_status(key)?, OrderStatus::Closed { .. })
            {
                d.step(t)?;
            }
            Ok(())
        })();
        if let Err(e) = result {
            d.note(t, e);
            d.retry = t + 1000;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn price(s: &str) -> Price {
        Price::new(s.parse().unwrap()).unwrap()
    }
    fn desk() -> Desk {
        desk_with_fee(D::new(5, 2))
    }
    fn desk_with_fee(rate: D) -> Desk {
        let legs = ["1", "2"].map(|id| Leg {
            condition: if id == "1" { "a" } else { "b" }.into(),
            name: id.into(),
            asset: AssetId::new(id).unwrap(),
            no: AssetId::new(&format!("9{id}")).unwrap(),
            fee: FeeModel::new(rate, false).unwrap(),
            rate,
            minimum: 5.into(),
            delay: 0,
        });
        let mut d = Desk::new(legs.into(), Instant::now()).unwrap();
        d.quotes = Some(vec![
            Quote {
                bid: price(".40"),
                ahead: 2.into(),
            },
            Quote {
                bid: price(".58"),
                ahead: 100.into(),
            },
        ]);
        commit_fixture(&mut d, 0, 0, ".01", &[(".41", 100)]);
        commit_fixture(&mut d, 0, 1, ".01", &[(".59", 100)]);
        d
    }
    fn commit_fixture(d: &mut Desk, t: u64, leg: usize, tick: &str, asks: &[(&str, u32)]) {
        use crate::wire::Level;
        let q = &d.quotes.as_ref().unwrap()[leg];
        let book = crate::book::BuyDepth {
            asset: d.legs[leg].asset.as_str().into(),
            observed_at: d.ex.live_time_for_test(t),
            tick: tick.parse().unwrap(),
            bid: Some(Level {
                price: q.bid.decimal(),
                size: q.ahead,
            }),
            asks: asks
                .iter()
                .map(|(p, n)| Level {
                    price: p.parse().unwrap(),
                    size: (*n).into(),
                })
                .collect(),
        };
        d.ex.advance_to(t).unwrap();
        d.commit_offers(super::timing_tests::committed(&book))
            .unwrap();
    }
    fn sell(d: &mut Desk, t: u64, size: &str) {
        d.print(
            t,
            &serde_json::from_value(
                json!({"asset_id":"1","price":"0.40","size":size,"side":"SELL","timestamp":t}),
            )
            .unwrap(),
        )
        .unwrap();
    }
    #[test]
    fn forced_round_skips_only_edge_and_hedges_all_cancel_race_fills() {
        let mut d = desk();
        d.lag = 100;
        assert!(d.start(0, false).is_err()); // 0.99 before fees, over 1 after fees
        commit_fixture(&mut d, 0, 1, ".01", &[(".59", 24)]);
        assert!(d.start(0, true).is_err());
        commit_fixture(&mut d, 0, 1, ".01", &[(".59", 100)]);
        d.start(0, true).unwrap();
        assert!(d.start(1, true).is_err());
        d.step(99).unwrap();
        assert_eq!(d.screen()["cash"], "1000");
        d.step(100).unwrap();
        assert_eq!(d.balance(0).unwrap().cash().decimal(), D::from(990));
        sell(&mut d, 110, "2"); // consumes external queue only
        assert_eq!(d.balance(0).unwrap().shares(), Shares::ZERO);
        let snap = d.screen();
        assert_eq!(snap["queue"]["initial"], "2");
        assert_eq!(snap["queue"]["remaining"], "0");
        assert_eq!(snap["markers"].as_array().unwrap().len(), 1);
        sell(&mut d, 115, "5");
        let snap = d.screen();
        assert_eq!(snap["markers"][1]["kind"], "部分成交");
        assert_eq!(snap["slots"][0]["shares"], "5");
        assert_eq!(snap["slots"][1]["shares"], "0");
        commit_fixture(&mut d, 115, 1, ".01", &[(".61", 100)]);
        d.step(115).unwrap();
        sell(&mut d, 180, "3");
        d.step(215).unwrap(); // cancel acknowledges 8
        d.step(315).unwrap();
        d.step(414).unwrap();
        assert_eq!(d.balance(1).unwrap().shares(), Shares::ZERO);
        d.step(415).unwrap();
        for i in 0..2 {
            assert_eq!(d.balance(i).unwrap().shares().decimal(), D::from(8));
        }
        assert!(d.round.is_none());
        let screen = d.screen();
        assert!(screen["queue"].is_null());
        let kinds: Vec<_> = screen["markers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["掛單", "部分成交", "部分成交", "撤單", "補腿"]);
        assert!(d.ex.is_conserved());
        assert_eq!(d.screen()["frozen"], "0");
    }
    #[test]
    fn edge_loss_at_80ms_cancels_once_and_hedges_only_five_confirmed_shares() {
        let mut d = desk_with_fee(D::ZERO);
        d.auto = true;
        d.lag = 20;
        commit_fixture(&mut d, 0, 1, ".001", &[(".585", 25)]);
        let n = D::from(SIZE);
        assert_eq!(d.cost(0, price(".40")).unwrap() / n, D::new(985, 3));
        d.start(0, false).unwrap();
        d.step(20).unwrap();
        // Five shares remain cheap. Buying the deeper twenty removes the edge.
        d.quotes.as_mut().unwrap()[1].bid = price(".57");
        commit_fixture(&mut d, 80, 1, ".0001", &[(".58", 5), (".6075", 20)]);
        assert_eq!(d.cost(0, price(".40")).unwrap() / n, D::new(1002, 3));
        d.check_cancel(80).unwrap();
        assert_eq!(d.screen()["cancellations"][0]["outcome"], "pending");
        d.check_cancel(81).unwrap();
        sell(&mut d, 90, "7");
        d.step(100).unwrap();
        d.step(120).unwrap();
        d.step(140).unwrap();
        assert_eq!(
            d.log
                .iter()
                .filter(|l| l.contains("[DYNAMIC-CANCEL: 價差消失]"))
                .count(),
            1
        );
        for i in 0..2 {
            assert_eq!(d.balance(i).unwrap().shares().decimal(), D::from(5));
        }
        assert!(d.ex.is_conserved());
    }

    #[test]
    fn unavailable_quotes_cancel_the_strategy_but_force_keeps_its_deadline() {
        for forced in [true, false] {
            let mut d = desk_with_fee(D::ZERO);
            d.lag = 20;
            d.auto = !forced;
            d.start(0, forced).unwrap();
            d.step(20).unwrap();
            d.quotes = None;
            d.check_cancel(80).unwrap();
            if forced {
                assert!(d.screen()["cancellations"].as_array().unwrap().is_empty());
                d.check_cancel(20_020).unwrap();
                assert!(d.log.back().unwrap().contains("[TIMEOUT-CANCEL: 20s 逾時]"));
            } else {
                assert_eq!(d.screen()["cancellations"][0]["outcome"], "pending");
                assert!(d.log.back().unwrap().contains("[DYNAMIC-CANCEL: 報價失效]"));
            }
        }
    }

    #[test]
    fn forced_entry_tolerates_one_cent_and_times_out_at_twenty_seconds() {
        for timeout in [true, false] {
            let mut d = desk_with_fee(D::ZERO);
            d.lag = 10;
            commit_fixture(&mut d, 0, 1, ".01", &[(".62", 100)]);
            d.start(0, true).unwrap();
            d.step(10).unwrap();
            commit_fixture(&mut d, 11, 1, ".01", &[(".63", 100)]);
            d.check_cancel(20_009).unwrap();
            assert!(d.screen()["cancellations"].as_array().unwrap().is_empty());
            if timeout {
                d.check_cancel(20_010).unwrap();
            } else {
                commit_fixture(&mut d, 20_009, 1, ".001", &[(".631", 100)]);
                d.check_cancel(20_009).unwrap();
            }
            assert!(d.log.back().unwrap().contains(if timeout {
                "[TIMEOUT-CANCEL: 20s 逾時]"
            } else {
                "[DYNAMIC-CANCEL: 價差消失]"
            }));
        }
    }

    #[test]
    fn idle_public_no_print_is_visible_without_a_paper_fill() {
        let mut d = desk();
        let msg = serde_json::from_value(
            json!({"asset_id":"92","price":"0.3","size":"8","side":"BUY","timestamp":0}),
        )
        .unwrap();
        d.public_print(&msg);
        let view = d.screen();
        assert_eq!(view["tape"][0]["leg"], 1);
        assert_eq!(view["tape"][0]["token"], "NO");
        assert_eq!(view["slots"][1]["shares"], "0");
        assert!(view["markers"].as_array().unwrap().is_empty());
    }
    #[test]
    fn hedge_limits_survive_latency_and_leave_exposure_visible() {
        let mut d = desk();
        d.lag = 10;
        d.start(0, true).unwrap();
        d.step(10).unwrap();
        sell(&mut d, 11, "27");
        d.step(11).unwrap();
        d.step(21).unwrap();
        commit_fixture(&mut d, 30, 1, ".01", &[(".60", 100)]);
        d.step(31).unwrap();
        assert_eq!(d.balance(0).unwrap().shares().decimal(), D::from(25));
        assert_eq!(d.balance(1).unwrap().shares(), Shares::ZERO);
        assert!(d.ex.is_conserved());
        assert!(!d.auto);
    }
}

#[cfg(test)]
mod timing_tests;
