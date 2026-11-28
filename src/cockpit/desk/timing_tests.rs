use super::*;
use crate::{
    book::BuyDepth,
    wire::{Level, PublicTrade},
};
use std::sync::Arc;

use crate::book::{NegRiskEventGroup, SubscriptionId};
use std::sync::mpsc as fixture_queue;

struct BookFixture {
    group: NegRiskEventGroup,
    source: SubscriptionId,
    observed: fixture_queue::Receiver<Arc<CommittedBook>>,
}
async fn approve(group: &mut NegRiskEventGroup, members: [(&str, &str, &str); 2]) {
    approve_named(
        group,
        members.map(|(condition, yes, no)| (condition, yes, no, condition)),
    )
    .await;
}

async fn approve_named(group: &mut NegRiskEventGroup, members: [(&str, &str, &str, &str); 2]) {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", server.local_addr().unwrap());
    let client = Client::builder().no_proxy().build().unwrap();
    let serve = async {
        let (mut socket, _) = server.accept().await.unwrap();
        let mut request = [0; 2048];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        let markets = members.map(|(c, y, n, name)| {
            json!({
                "question":name, "conditionId":c, "outcomes":["Yes","No"],
                "clobTokenIds":[y,n],"orderPriceMinTickSize":"0.0001","closed":false
            })
        });
        let body = json!({"slug":"fixture","title":"Fixture","closed":false,"negRisk":true,"markets":markets}).to_string();
        let reply = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(reply.as_bytes()).await.unwrap();
    };
    tokio::join!(
        group.refresh_discovery(&client, &base, Duration::from_secs(3600)),
        serve
    )
    .0
    .unwrap();
}
impl BookFixture {
    async fn new() -> Self {
        let (tx, observed) = fixture_queue::channel();
        let mut group = NegRiskEventGroup::with_market_consumer(
            "fixture",
            Quantity::new(25.into()).unwrap(),
            move |_, input| {
                if let MarketInput::Offers(book) = input {
                    tx.send(book).unwrap()
                }
            },
        );
        approve(&mut group, [("a", "1", "91"), ("b", "2", "92")]).await;
        let source = group.open_input().unwrap();
        Self {
            group,
            source,
            observed,
        }
    }

    fn snapshot(&mut self, book: &BuyDepth) -> Arc<CommittedBook> {
        let level = |l: &Level| json!({"price":l.price.to_string(),"size":l.size.to_string()});
        let msg = json!({"event_type":"book","market":if book.asset=="1" {"a"} else {"b"},
            "asset_id":book.asset,"timestamp":"1","bids":book.bid.iter().map(level).collect::<Vec<_>>(),
            "asks":book.asks.iter().map(level).collect::<Vec<_>>()});
        let event = crate::wire::events(&msg.to_string()).unwrap().remove(0);
        self.group
            .apply_received(self.source, event, book.observed_at)
            .unwrap();
        self.observed.try_recv().unwrap()
    }
}

pub(super) fn committed(book: &BuyDepth) -> Arc<CommittedBook> {
    thread_local! {
        static SOURCE: std::cell::RefCell<BookFixture> = std::cell::RefCell::new(std::thread::spawn(|| {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(BookFixture::new())
        }).join().unwrap());
    }
    SOURCE.with_borrow_mut(|source| {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .start_paused(true)
                        .build()
                        .unwrap()
                        .block_on(async {
                            tokio::time::advance(
                                book.observed_at.saturating_duration_since(Instant::now()),
                            )
                            .await;
                            source.snapshot(book)
                        })
                })
                .join()
                .unwrap()
        })
    })
}

struct Replay {
    driver: Controller,
    commands: mpsc::Sender<Request>,
    fees: mpsc::Sender<FeeUpdate>,
}
impl Replay {
    fn new(force: bool) -> Self {
        let mut replay = Self::idle(0);
        replay.depth(0, ".585");
        replay.command(Command::Lag(20));
        replay.command(if force {
            Command::Fire
        } else {
            Command::Auto(true)
        });
        replay.clock(20);
        replay
    }
    fn idle(hedge_delay: u64) -> Self {
        let legs = ["1", "2"].map(|id| Leg {
            condition: if id == "1" { "a" } else { "b" }.into(),
            name: id.into(),
            asset: AssetId::new(id).unwrap(),
            no: AssetId::new(&format!("9{id}")).unwrap(),
            fee: FeeModel::new(D::ZERO, false).unwrap(),
            rate: D::ZERO,
            minimum: 5.into(),
            delay: if id == "2" { hedge_delay } else { 0 },
        });
        let (fees, meta) = mpsc::channel(1);
        let (commands, rx) = mpsc::channel(8);
        let (tx, _) = watch::channel(Screen::default());
        let driver = Controller::new(meta, rx, tx);
        fees.try_send((driver.origin - Duration::from_nanos(1), Ok(legs.into())))
            .unwrap();
        Self {
            driver,
            commands,
            fees,
        }
    }
    fn input(&mut self, ms: u64, input: MarketInput<'_>) {
        self.driver
            .receive(self.driver.origin + Duration::from_millis(ms), input);
    }
    fn command(&mut self, cmd: Command) {
        self.command_at(0, cmd);
    }
    fn command_at(&mut self, t: u64, cmd: Command) {
        self.request_at(t, cmd).unwrap();
    }
    fn request_at(&mut self, t: u64, cmd: Command) -> Result<(), String> {
        let (tx, mut reply) = oneshot::channel();
        self.commands.try_send((cmd, tx)).unwrap();
        self.clock(t);
        reply.try_recv().unwrap()
    }
    fn clock(&mut self, t: u64) {
        self.input(t, MarketInput::Clock(self.driver.commands.len()));
    }
    fn depth(&mut self, t: u64, ask: &str) {
        let level = |p: &str, size: u32| Level {
            price: p.parse().unwrap(),
            size: size.into(),
        };
        let legs =
            [("1", ".40", ".41", 2), ("2", ".57", ask, 100)].map(|(asset, bid, ask, ahead)| {
                Arc::new(BuyDepth {
                    asset: asset.into(),
                    observed_at: self.driver.origin + Duration::from_millis(t),
                    tick: D::new(1, 4),
                    bid: Some(level(bid, ahead)),
                    asks: vec![level(ask, 100)],
                })
            });
        for book in &legs {
            self.input(t, MarketInput::Offers(committed(book)));
        }
        self.input(t, MarketInput::Depth(Some(Arc::from(legs))));
    }
    fn sell(&mut self, t: u64, qty: &str) {
        let trade = PublicTrade {
            asset_id: "1".into(),
            price: D::new(4, 1),
            size: qty.parse().unwrap(),
            side: TradeSide::Sell,
            timestamp: 1,
            transaction_hash: format!("print-{t}"),
        };
        self.input(t, MarketInput::Trade(&trade));
    }
    fn screen(&self) -> Value {
        self.driver.desk.as_ref().unwrap().screen()
    }
}
#[test]
fn admitted_prints_straddle_cancel_even_when_consumer_misses_ticks() {
    for tick in [false, true] {
        let mut r = Replay::new(false);
        r.depth(80, ".602");
        if tick {
            r.clock(98);
        }
        r.sell(99, "7");
        if tick {
            r.clock(100);
        }
        r.sell(100, "8");
        r.sell(101, "9");
        // A queued depth at 119 precedes the report due at 120.
        r.depth(119, ".58");
        r.clock(160);
        let view = r.screen();
        assert_eq!(view["slots"][0]["shares"], "5");
        assert_eq!(view["slots"][1]["shares"], "5");
        assert_eq!(dec(&view["cash"]).unwrap(), D::new(9951, 1));
        assert_eq!(view["conserved"], true);
        let c = &view["cancellations"][0];
        assert_eq!(c["effective_ms"], 100);
        assert_eq!(c["filled"], "5");
    }
}
#[test]
fn delayed_consumer_orders_fee_invalidation_between_admitted_prints() {
    for (fee_ms, expected) in [(110, "5"), (80, "0")] {
        let mut r = Replay::new(false);
        r.fees
            .try_send((
                r.driver.origin + Duration::from_millis(fee_ms),
                Err("unverified".into()),
            ))
            .unwrap();
        r.input(80, MarketInput::Frontier);
        r.sell(105, "7");
        r.sell(135, "9");
        r.clock(180);
        let view = r.screen();
        let cancels = view["cancellations"].as_array().unwrap();
        assert_eq!(cancels.len(), 1);
        assert_eq!(cancels[0]["sent_ms"], fee_ms);
        assert_eq!(cancels[0]["effective_ms"], fee_ms + 20);
        assert_eq!(cancels[0]["filled"], expected);
        assert_eq!(view["slots"][0]["shares"], expected);
        assert_eq!(view["conserved"], true);
    }
}

fn remove_hedge_bid(r: &mut Replay) {
    let mut legs = r.driver.latest.clone().unwrap().to_vec();
    let old = &legs[1];
    legs[1] = Arc::new(BuyDepth {
        asset: old.asset.clone(),
        tick: old.tick,
        observed_at: r.driver.origin + Duration::from_millis(80),
        bid: None,
        asks: old.asks.clone(),
    });
    r.input(80, MarketInput::Offers(committed(&legs[1])));
    r.input(80, MarketInput::Depth(Some(legs.into())));
}
fn fee_at(r: &mut Replay, micros: u64, result: Result<Vec<Leg>, String>) {
    r.fees
        .try_send((r.driver.origin + Duration::from_micros(micros), result))
        .unwrap();
}
#[test]
fn rename_keeps_in_flight_hedges_and_updates_labels_but_fee_changes_stop_them() {
    for fee_changed in [false, true] {
        let mut r = Replay::new(false);
        r.sell(21, "27");
        r.clock(41);
        let mut rules = r.driver.desk.as_ref().unwrap().legs.clone();
        rules[1].name = "New name".into();
        if fee_changed {
            rules[1].rate = D::new(1, 1);
            rules[1].fee = FeeModel::new(rules[1].rate, false).unwrap();
        }
        fee_at(&mut r, 50_000, Ok(rules));
        r.clock(62);
        let snapshot = r.screen();
        assert_eq!(snapshot["slots"][0]["shares"], "25");
        assert_eq!(
            snapshot["slots"][1]["shares"],
            if fee_changed { "0" } else { "25" }
        );
        assert_eq!(snapshot["auto"], !fee_changed);
        assert_eq!(snapshot["frozen"], "0");
        assert_eq!(snapshot["conserved"], true);
        if !fee_changed {
            assert_eq!(snapshot["slots"][1]["name"], "New name");
        }
    }
}

#[test]
fn published_rates_follow_verification_even_between_normal_screen_ticks() {
    let mut r = Replay::new(false);
    let snapshots = r.driver.tx.subscribe();
    let legs = r.driver.desk.as_ref().unwrap().legs.clone();
    for (micros, valid) in [(21_000, false), (23_000, true)] {
        fee_at(
            &mut r,
            micros,
            if valid {
                Ok(legs.clone())
            } else {
                Err("unverified".into())
            },
        );
        r.input(micros / 1000 + 1, MarketInput::Frontier);
        let frame = snapshots.borrow();
        assert_eq!(
            frame.desk["rates"][crate::book::view::market_identity("a", "1", "91")].as_str(),
            valid.then_some("0")
        );
        assert_eq!(frame.desk["error"].is_null(), valid);
    }
}

#[test]
fn changed_rules_leave_confirmed_fill_unhedged() {
    let mut r = Replay::new(false);
    let mut rules = r.driver.desk.as_ref().unwrap().legs.clone();
    rules[1].rate = D::new(1, 1);
    rules[1].fee = FeeModel::new(rules[1].rate, false).unwrap();
    fee_at(&mut r, 80_000, Ok(rules));
    r.sell(99, "7");
    r.clock(260);
    let v = r.screen();
    assert_eq!(v["cancellations"][0]["filled"], "5");
    assert_eq!(v["cancellations"][0]["effective_ms"], 100);
    assert_eq!(v["slots"][0]["shares"], "5");
    assert_eq!(v["slots"][1]["shares"], "0");
    assert_eq!(v["cash"], "998");
    assert_eq!(v["frozen"], "0");
    assert_eq!(v["conserved"], true);
    assert!(
        v["markers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["detail"] == "場次規則已變更，保留未補 5 股")
    );
}

#[test]
fn fee_failure_retires_only_hedges_still_in_flight() {
    for (micros, filled) in [
        (130_000, false),
        (139_999, false),
        (140_001, true),
        (141_000, true),
    ] {
        let mut r = Replay::new(false);
        let rules = r.driver.desk.as_ref().unwrap().legs.clone();
        remove_hedge_bid(&mut r);
        r.sell(99, "7");
        fee_at(&mut r, micros, Err("unverified".into()));
        // Queue the recovery before advancing to either observation.
        r.input(120, MarketInput::Frontier);
        fee_at(&mut r, micros + 5000, Ok(rules));
        r.clock(260);
        let v = r.screen();
        assert_eq!(v["slots"][0]["shares"], "5");
        assert_eq!(v["slots"][1]["shares"], if filled { "5" } else { "0" });
        assert_eq!(v["cash"], if filled { "995.075" } else { "998" });
        assert_eq!(v["frozen"], "0");
        assert_eq!(v["conserved"], true);
        assert_eq!(
            v["markers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["detail"] == "費率核對失敗，保留未補 5 股"),
            !filled
        );
    }
}

#[test]
fn recovered_rules_allow_unsent_hedge_and_new_round() {
    let mut r = Replay::new(false);
    let rules = r.driver.desk.as_ref().unwrap().legs.clone();
    fee_at(&mut r, 110_000, Err("unverified".into()));
    r.input(80, MarketInput::Frontier);
    r.sell(105, "7");
    fee_at(&mut r, 135_000, Ok(rules));
    r.clock(260);
    let v = r.screen();
    assert_eq!(v["cancellations"][0]["effective_ms"], 130);
    assert_eq!(v["cancellations"][0]["filled"], "5");
    assert_eq!(v["slots"][0]["shares"], "5");
    assert_eq!(v["slots"][1]["shares"], "5");
    assert_eq!(v["cash"], "995.075");
    assert_eq!(v["auto"], false);
    r.command_at(1200, Command::Fire);
    r.clock(1240);
    assert!(matches!(
        r.driver.desk.as_ref().unwrap().round,
        Some(Round::Resting { .. })
    ));
}

#[test]
fn hedge_buys_five_with_no_bid_through_ioc_arrival() {
    let mut r = Replay::new(false);
    remove_hedge_bid(&mut r);
    r.sell(99, "7");
    r.clock(160);
    let view = r.screen();
    assert_eq!(view["slots"][0]["shares"], "5");
    assert_eq!(view["slots"][1]["shares"], "5");
    assert_eq!(view["conserved"], true);
}

#[test]
fn forced_spike_survives_burst_and_old_generation_cannot_fill() {
    for interrupt in [false, true] {
        let mut r = Replay::new(true);
        r.depth(80, ".602");
        if interrupt {
            r.input(85, MarketInput::Interrupted);
            r.depth(87, ".585");
            r.sell(90, "20");
        } else {
            for _ in 0..64 {
                r.depth(90, ".585");
            }
        }
        r.clock(150);
        let view = r.screen();
        let c = &view["cancellations"][0];
        assert_eq!(view["cancellations"].as_array().unwrap().len(), 1);
        assert_eq!(c["reason"], "edge_lost");
        assert_eq!(
            (c["sent_ms"].as_u64(), c["effective_ms"].as_u64()),
            (Some(80), Some(100))
        );
        assert_eq!(c["input_interrupted"], interrupt);
        assert_eq!(c["filled"], "0");
        assert_eq!(view["frozen"], "0");
    }
}

#[test]
fn missing_quotes_and_exact_penny_preserve_forced_patience_deadline() {
    let mut forced = Replay::new(true);
    forced.input(80, MarketInput::Depth(None));
    forced.depth(120, ".595");
    forced.clock(20_019);
    assert_eq!(dec(&forced.screen()["frozen"]).unwrap(), D::TEN);
    forced.clock(20_050);
    let view = forced.screen();
    assert_eq!(view["cancellations"][0]["reason"], "timeout");
    assert_eq!(view["cancellations"][0]["sent_ms"], 20_020);
    assert_eq!(view["frozen"], "0");
    let mut auto = Replay::new(false);
    auto.input(80, MarketInput::Depth(None));
    auto.clock(110);
    let view = auto.screen();
    assert_eq!(view["cancellations"][0]["reason"], "quote_unavailable");
    assert_eq!(view["frozen"], "0");
}

async fn connected_replay(
    force: bool,
) -> (
    Arc<std::sync::Mutex<Replay>>,
    NegRiskEventGroup,
    SubscriptionId,
    tokio::task::JoinHandle<()>,
) {
    let replay = Arc::new(std::sync::Mutex::new(Replay::new(force)));
    let consumer = replay.clone();
    let mut group = NegRiskEventGroup::with_market_consumer(
        "fixture",
        Quantity::new(25.into()).unwrap(),
        move |at, input| consumer.lock().unwrap().driver.receive(at, input),
    );
    // Keep mock HTTP I/O from auto-advancing Tokio's paused clock.
    let clock_guard = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    approve(&mut group, [("a", "1", "91"), ("b", "2", "92")]).await;
    let source = group.open_input().unwrap();
    (replay, group, source, clock_guard)
}

#[tokio::test(start_paused = true)]
async fn gamma_rebind_drains_admitted_trade_into_the_same_ledger() {
    let (replay, mut group, source, clock_guard) = connected_replay(false).await;
    tokio::time::advance(Duration::from_millis(80)).await;
    replay.lock().unwrap().depth(80, ".602");
    tokio::time::advance(Duration::from_millis(19)).await;
    group.admit_raw(source,r#"{"event_type":"last_trade_price","asset_id":"1","price":"0.40","size":"7","side":"SELL","timestamp":"1"}"#.into()).unwrap();
    approve(&mut group, [("a", "1", "91"), ("c", "3", "93")]).await;
    clock_guard.abort();
    let mut replay = replay.lock().unwrap();
    replay.clock(200);
    let v = replay.screen();
    assert_eq!(v["cancellations"][0]["filled"], "5");
    assert_eq!(v["cancellations"][0]["effective_ms"], 100);
    assert_eq!(v["slots"][0]["shares"], "5");
    assert_eq!(v["slots"][1]["shares"], "0");
    assert_eq!(dec(&v["cash"]).unwrap(), D::from(998));
    assert_eq!(v["frozen"], "0");
    assert_eq!(v["conserved"], true);
}

#[tokio::test(start_paused = true)]
async fn reconciliation_timeout_preserves_only_the_executable_fifo_prefix() {
    for (bad_frame, expected) in [(false, "5"), (true, "0")] {
        let (replay, mut group, source, clock_guard) = connected_replay(false).await;
        tokio::time::advance(Duration::from_millis(80)).await;
        for message in [
            r#"{"event_type":"book","asset_id":"91","market":"a","timestamp":"1","bids":[{"price":".40","size":"10"}],"asks":[{"price":".41","size":"10"}]}"#,
            r#"{"event_type":"price_change","market":"a","timestamp":"2","price_changes":[{"asset_id":"91","side":"BUY","price":".41","size":"1","best_bid":".40","best_ask":".41"}]}"#,
        ] {
            group.admit_raw(source, message.into()).unwrap();
            assert!(group.advance_input(source).unwrap());
        }
        assert!(group.reconciliation_deadline().is_some());
        tokio::time::advance(Duration::from_millis(15)).await;
        if bad_frame {
            group.admit_raw(source, "{".into()).unwrap();
        }
        tokio::time::advance(Duration::from_millis(4)).await;
        group.admit_raw(source,r#"{"event_type":"last_trade_price","asset_id":"1","price":"0.40","size":"7","side":"SELL","timestamp":"1"}"#.into()).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        group.admit_raw(source,r#"{"event_type":"last_trade_price","asset_id":"1","price":"0.40","size":"8","side":"SELL","timestamp":"1"}"#.into()).unwrap();
        tokio::time::advance(Duration::from_millis(480)).await;
        assert!(group.advance_input(source).is_err());
        assert!(!group.ready());
        let mut replay = replay.lock().unwrap();
        replay.clock(580);
        let view = replay.screen();
        assert_eq!(view["slots"][0]["shares"], expected);
        assert_eq!(view["cancellations"][0]["filled"], expected);
        assert_eq!(view["cancellations"][0]["effective_ms"], 100);
        assert_eq!(view["frozen"], "0");
        assert_eq!(view["conserved"], true);
        clock_guard.abort();
    }
}

#[test]
fn terminal_stop_releases_working_order_without_sending_dynamic_cancel() {
    let mut r = Replay::new(false);
    r.sell(40, "7");
    let mut wait = Ok(None);
    r.input(85, MarketInput::Ending(&mut wait));
    assert_eq!(wait.unwrap(), None);
    let view = r.screen();
    assert_eq!(view["slots"][0]["shares"], "5");
    assert!(view["cancellations"].as_array().unwrap().is_empty());
    assert_eq!(
        view["orders"][0],
        json!({"order_id":1,"reason":"session_ended","effective_ms":85,"filled":"5"})
    );
    assert_eq!(view["frozen"], "0");
    assert_eq!(view["conserved"], true);
}

#[test]
fn same_millisecond_zero_latency_cancel_follows_the_trigger() {
    for (trade_first, expected) in [(true, "5"), (false, "0")] {
        let mut r = Replay::new(false);
        r.command_at(30, Command::Lag(0));
        if trade_first {
            r.sell(100, "7");
        }
        r.depth(100, ".602");
        if !trade_first {
            r.sell(100, "7");
        }
        let v = r.screen();
        assert_eq!(v["slots"][0]["shares"], expected);
        assert_eq!(v["cancellations"][0]["filled"], expected);
        assert_eq!(v["cancellations"][0]["effective_ms"], 100);
        assert_eq!(v["conserved"], true);
    }
}

#[tokio::test(start_paused = true)]
async fn bad_raw_frame_cuts_hedges_at_ingress_even_when_decoded_at_110ms() {
    for (gap, qty, cash) in [(95, "0", "990"), (100, "0", "990"), (105, "25", "975.375")] {
        let (replay, mut group, source, clock_guard) = connected_replay(true).await;
        replay.lock().unwrap().sell(60, "27");
        tokio::time::advance(Duration::from_millis(gap)).await;
        group
            .admit_raw(source, r#"{"event_type":"book","bids":"bad"}"#.into())
            .unwrap();
        tokio::time::advance(Duration::from_millis(110 - gap)).await;
        assert!(group.advance_input(source).is_err());
        clock_guard.abort();
        let mut r = replay.lock().unwrap();
        r.clock(110);
        let v = r.screen();
        assert_eq!(v["slots"][0]["shares"], "25");
        assert_eq!(v["slots"][1]["shares"], qty);
        assert_eq!(dec(&v["cash"]).unwrap(), cash.parse::<D>().unwrap());
        assert_eq!(v["conserved"], true);
    }
}

#[tokio::test(start_paused = true)]
async fn cutoff_uses_ingress_precision_and_empty_frames_cannot_change_fills() {
    for micros in [139_999, 140_000, 140_001] {
        for noop in [false, true] {
            for ending in [false, true] {
                let (replay, mut group, source, guard) = connected_replay(false).await;
                {
                    let mut r = replay.lock().unwrap();
                    remove_hedge_bid(&mut r);
                    r.sell(99, "7");
                    r.clock(120);
                }
                tokio::time::advance(Duration::from_micros(micros)).await;
                if noop {
                    group.admit_raw(source, "[]".into()).unwrap();
                    assert!(group.advance_input(source).unwrap());
                }
                if ending {
                    let mut receipt = Ok(None);
                    replay
                        .lock()
                        .unwrap()
                        .driver
                        .receive(Instant::now(), MarketInput::Ending(&mut receipt));
                    assert!(receipt.unwrap().is_none());
                } else {
                    group.admit_raw(source, "{".into()).unwrap();
                    assert!(group.advance_input(source).is_err());
                }
                let view = replay.lock().unwrap().screen();
                assert_eq!(
                    view["slots"][1]["shares"],
                    if micros > 140_000 { "5" } else { "0" },
                    "{micros} {noop} {ending}"
                );
                assert_eq!(view["slots"][0]["shares"], "5");
                assert_eq!(
                    view["cash"],
                    if micros > 140_000 { "995.075" } else { "998" }
                );
                assert_eq!(view["conserved"], true);
                guard.abort();
            }
        }
    }
}

#[tokio::test]
async fn both_http_fee_sources_require_success_before_authorizing_hedges() {
    use crate::test_http::{GAMMA_FIXTURE, http_request, http_response_with_status};
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    for (primary, fallback) in [(200, 200), (302, 200), (200, 302)] {
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", server.local_addr().unwrap());
        let gamma = GAMMA_FIXTURE
            .replace("First", "1")
            .replace("Second", "2")
            .replace(r#"["1","2"]"#, r#"["1","91"]"#)
            .replace(r#"["3","4"]"#, r#"["2","92"]"#);
        let serve = async {
            loop {
                let (socket, path) = http_request(&server).await;
                let (code, body) = if path.contains("/events/") {
                    (200, gamma.clone())
                } else if path.contains("/fee-rate?") {
                    (fallback, json!({"base_fee":0}).to_string())
                } else {
                    let (condition, id) = if path.contains("/clob-markets/a ") {
                        ("a", "1")
                    } else {
                        assert!(path.contains("/clob-markets/b "));
                        ("b", "2")
                    };
                    (
                        primary,
                        json!({"c":condition,"ao":true,"mos":5,"t":[{"o":"Yes","t":id}]})
                            .to_string(),
                    )
                };
                http_response_with_status(
                    socket,
                    if code == 200 { "200 OK" } else { "302 Found" },
                    &body,
                )
                .await;
            }
        };
        let result = tokio::select! {result=roster(&client,"fixture",&base,&base)=>result,_=serve=>unreachable!()};
        let valid = primary == 200 && fallback == 200;
        assert_eq!(result.is_ok(), valid);
        let mut r = Replay::new(false);
        remove_hedge_bid(&mut r);
        r.sell(99, "7");
        fee_at(&mut r, 130_000, result.map_err(|e| e.to_string()));
        r.clock(160);
        let v = r.screen();
        assert_eq!(v["slots"][1]["shares"], if valid { "5" } else { "0" });
        assert_eq!(
            v["rates"][crate::book::view::market_identity("a", "1", "91")].as_str(),
            valid.then_some("0")
        );
        assert_eq!(v["error"].is_null(), valid);
    }
}

#[tokio::test(start_paused = true)]
async fn cancel_receipt_advances_with_another_noop_still_queued() {
    let (replay, mut group, source, clock_guard) = connected_replay(false).await;
    replay.lock().unwrap().depth(80, ".602");
    tokio::time::advance(Duration::from_micros(100_001)).await;
    group.admit_raw(source, "[]".into()).unwrap();
    group
        .admit_raw(source, r#"{"event_type":"other"}"#.into())
        .unwrap();
    assert!(group.advance_input(source).unwrap());
    let v = replay.lock().unwrap().screen();
    assert_eq!(v["cancellations"][0]["outcome"], "cancelled");
    assert_eq!(v["cancellations"][0]["effective_ms"], 100);
    assert_eq!(v["frozen"], "0");
    assert_eq!(v["conserved"], true);
    assert!(group.advance_input(source).unwrap());
    clock_guard.abort();
}

#[tokio::test(start_paused = true)]
async fn terminal_close_completes_local_receipt_without_market_clock() {
    let mut r = Replay::new(false);
    r.depth(80, ".602");
    let origin = r.driver.origin;
    let screen = r.driver.tx.subscribe();
    let mut due = Ok(None);
    r.input(85, MarketInput::Ending(&mut due));
    assert_eq!(due.unwrap(), Some(origin + Duration::from_millis(100)));
    assert!(dec(&screen.borrow().desk["frozen"]).unwrap() > D::ZERO);
    let mut receipt = Ok(());
    r.input(100, MarketInput::CancelMatured(&mut receipt));
    receipt.unwrap();
    let v = &screen.borrow().desk;
    assert_eq!(v["cancellations"][0]["input_interrupted"], false);
    assert_eq!(v["cash"], "1000");
    assert_eq!(v["frozen"], "0");
    assert_eq!(v["cancellations"][0]["outcome"], "cancelled");
    assert_eq!(v["cancellations"][0]["effective_ms"], 100);
    assert_eq!(v["conserved"], true);
}

#[tokio::test]
async fn committed_tick_rechecks_price_without_rebuilding_maker_priority() {
    for (bid, isolated, tick, off_grid) in [
        (".41", true, ".10", true),
        (".40", true, ".10", false),
        (".41", false, ".10", true),
        (".40", false, ".10", false),
        (".39", true, ".0000003", false),
        (".40", false, ".0000003", true),
    ] {
        let mut source = BookFixture::new().await;
        let mut replay = Replay::new(true);
        let desk = replay.driver.desk.as_mut().unwrap();
        let asset = desk.legs[0].asset.clone();
        let mut book = BuyDepth {
            asset: "1".into(),
            observed_at: Instant::now(),
            tick: D::new(1, 4),
            bid: Some(Level {
                price: bid.parse().unwrap(),
                size: 2.into(),
            }),
            asks: vec![Level {
                price: D::new(6, 1),
                size: 100.into(),
            }],
        };
        // Start a separate account so only this fixture's source can write its book.
        let origin = Instant::now();
        let mut desk = Desk::new(desk.legs.clone(), origin).unwrap();
        let first = source.snapshot(&book);
        desk.commit_offers(first.clone()).unwrap();
        let limit = Price::new(bid.parse().unwrap()).unwrap();
        let (key, _) = desk
            .submit(0, limit, 25.into(), OrderMode::PostOnly)
            .unwrap();
        book.bid = Some(Level {
            price: D::new(4, 1),
            size: 1.into(),
        });
        let second = source.snapshot(&book);
        desk.ex.advance_to(40).unwrap();
        desk.commit_offers(second).unwrap();
        let latest = desk.ex.current_stamp(&asset).unwrap();
        assert!(desk.commit_offers(first).is_err());
        assert_eq!(desk.ex.current_stamp(&asset).unwrap(), latest);
        let apply_change = |source: &mut BookFixture, desk: &mut Desk, ms, change: Value| {
            source
                .group
                .admit_raw(source.source, change.to_string())
                .unwrap();
            source.group.advance_input(source.source).unwrap();
            desk.ex.advance_to(ms).unwrap();
            let result = desk.commit_offers(source.observed.try_recv().unwrap());
            if ms == 50 && tick == ".0000003" {
                assert!(result.is_err());
            } else {
                result.unwrap();
            }
        };
        if isolated {
            let lock = json!({"event_type":"price_change","market":"a","timestamp":"45","price_changes":[{"asset_id":"1","side":"SELL","price":".40","size":"10","best_bid":".40","best_ask":".60"}]});
            apply_change(&mut source, &mut desk, 45, lock);
        }
        for (ms, tick) in [(50, tick), (60, ".01")] {
            let change = json!({"event_type":"tick_size_change","market":"a","asset_id":"1","timestamp":ms.to_string(),"new_tick_size":tick});
            apply_change(&mut source, &mut desk, ms, change);
            if ms == 50 && isolated {
                desk.ex.paper_print(asset.clone(), limit, 7.into()).unwrap();
                assert_eq!(desk.balance(0).unwrap().shares().decimal(), D::ZERO);
                if off_grid {
                    assert_eq!(
                        desk.screen()["orders"][0],
                        json!({"order_id":1,"reason":"tick_changed","effective_ms":50,"filled":"0"})
                    );
                }
            }
        }
        if isolated {
            let clear = json!({"event_type":"price_change","market":"a","timestamp":"65","price_changes":[{"asset_id":"1","side":"SELL","price":".40","size":"0"}]});
            apply_change(&mut source, &mut desk, 65, clear);
        }
        desk.ex.advance_to(70).unwrap();
        if off_grid {
            assert!(matches!(
                desk.ex.order_status(key).unwrap(),
                OrderStatus::Closed {
                    reason: crate::paper::order_fsm::CloseReason::TickChanged,
                    ..
                }
            ));
            assert_eq!(desk.balance(0).unwrap().cash().decimal(), D::from(1000));
        } else {
            assert_eq!(desk.ex.queue_ahead(key).unwrap().decimal(), D::TWO);
        }
        desk.ex.paper_print(asset, limit, 7.into()).unwrap();
        assert_eq!(
            desk.balance(0).unwrap().shares().decimal(),
            if off_grid { D::ZERO } else { D::from(5) }
        );
        assert!(desk.ex.is_conserved());
    }
}

fn hedge_offers(r: &mut Replay, t: u64, bid: &str, levels: &[(&str, u32)]) {
    let level = |(price, size): (&str, u32)| Level {
        price: price.parse().unwrap(),
        size: size.into(),
    };
    let b = BuyDepth {
        asset: "2".into(),
        observed_at: r.driver.origin + Duration::from_millis(t),
        tick: D::new(1, 4),
        bid: Some(level((bid, 100))),
        asks: levels.iter().copied().map(level).collect(),
    };
    r.input(t, MarketInput::Offers(committed(&b)));
    // Price discovery only exposes the first 25 shares. The exchange saw the full side above.
    let mut legs = r.driver.latest.clone().unwrap().to_vec();
    let mut b = b;
    b.asks.truncate(1);
    legs[1] = Arc::new(b);
    r.input(t, MarketInput::Depth(Some(legs.into())));
}

fn completed_first_round() -> Replay {
    let mut r = Replay::new(true);
    hedge_offers(&mut r, 20, ".57", &[(".585", 25)]);
    r.sell(21, "27");
    r.clock(61);
    assert_eq!(r.screen()["slots"][0]["shares"], "25");
    assert_eq!(r.screen()["slots"][1]["shares"], "25");
    r
}

#[test]
fn second_round_uses_only_remaining_and_replenished_liquidity() {
    for (intermediate, final_asks, refill) in [
        (vec![], vec![(".585", 25)], true),
        (vec![(".585", 25)], vec![(".585", 25)], false),
        (vec![(".58", 25), (".585", 25)], vec![(".585", 25)], false),
        (vec![(".585", 25)], vec![(".585", 50)], true),
        (vec![(".585", 25)], vec![(".585", 25), (".59", 25)], true),
    ] {
        let mut r = completed_first_round();
        hedge_offers(&mut r, 200, ".57", &intermediate);
        hedge_offers(&mut r, 201, ".57", &final_asks);
        assert_eq!(r.request_at(1200, Command::Fire).is_ok(), refill);
        r.clock(1220);
        r.sell(1221, "27");
        r.clock(1261);
        let v = r.screen();
        let expected = if refill { "50" } else { "25" };
        assert_eq!(v["slots"][0]["shares"], expected);
        assert_eq!(v["slots"][1]["shares"], expected);
        assert_eq!(v["frozen"], "0");
        assert_eq!(v["conserved"], true);
    }
}

#[test]
fn hedge_preserves_partial_fill_within_the_original_limit() {
    let mut r = Replay::new(true);
    hedge_offers(&mut r, 20, ".57", &[(".585", 25)]);
    r.sell(21, "27");
    r.clock(41);
    hedge_offers(&mut r, 42, ".57", &[(".585", 10), (".590", 15)]);
    r.clock(62);
    let v = r.screen();
    assert_eq!(v["slots"][0]["shares"], "25");
    assert_eq!(v["slots"][1]["shares"], "10");
    assert_eq!(v["frozen"], "0");
    assert_eq!(v["conserved"], true);
}

#[tokio::test]
async fn cockpit_command_market_wire_and_snapshot_share_one_order_timeline() {
    run_market_wire(None).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_retirement_accounts_for_prints_received_while_consumer_was_stalled() {
    for malformed in [false, true] {
        run_market_wire(Some(malformed)).await;
    }
}

async fn run_market_wire(timeout_gap: Option<bool>) {
    use crate::{book::NegRiskEventGroup, monitor, output::Output};
    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gamma = format!("http://{}", http.local_addr().unwrap());
    let socket = format!("ws://{}", ws.local_addr().unwrap());
    let endpoints = monitor::Endpoints {
        gamma: &gamma,
        rest: &gamma,
        socket: &socket,
    };
    let client = Client::builder().no_proxy().build().unwrap();
    let output = Output::new(std::io::sink(), std::io::sink());
    let (tx, mut screen) = watch::channel(Screen::default());
    let (cmd, inbox) = mpsc::channel(8);
    let (fees, metadata) = mpsc::channel(1);
    let legs = ["1", "2"].map(|id| Leg {
        condition: if id == "1" { "a" } else { "b" }.into(),
        name: id.into(),
        asset: AssetId::new(id).unwrap(),
        no: AssetId::new(&format!("9{id}")).unwrap(),
        fee: FeeModel::new(D::ZERO, false).unwrap(),
        rate: D::ZERO,
        minimum: 5.into(),
        delay: 0,
    });
    fees.try_send((Instant::now(), Ok(legs.into()))).unwrap();
    let mut desk = Controller::new(metadata, inbox, tx);
    let mut saw_ready = false;
    let group = NegRiskEventGroup::with_market_consumer(
        "fixture",
        Quantity::new(25.into()).unwrap(),
        move |at, e| {
            let stall = timeout_gap.is_some() && saw_ready && matches!(e, MarketInput::Depth(None));
            saw_ready |= matches!(e, MarketInput::Depth(Some(_)));
            desk.receive(at, e);
            if stall {
                saw_ready = false;
                // The WebSocket worker continues admitting frames on its own thread.
                std::thread::sleep(Duration::from_millis(620));
            }
        },
    );
    let (stop, stopped) = oneshot::channel();
    let exchange = async {
        let (mut req, _) = http.accept().await.unwrap();
        let mut buf = [0; 2048];
        assert!(req.read(&mut buf).await.unwrap() > 0);
        let markets=[("a","1","91"),("b","2","92")].map(|(cond,yes,no)|json!({"question":cond,"conditionId":cond,"outcomes":["Yes","No"],"clobTokenIds":[yes,no],"orderPriceMinTickSize":"0.001","closed":false}));
        let body=json!({"slug":"fixture","title":"Fixture","closed":false,"negRisk":true,"markets":markets}).to_string();
        req.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let (sock, _) = ws.accept().await.unwrap();
        let mut market = accept_async(sock).await.unwrap();
        assert!(market.next().await.unwrap().unwrap().is_text());
        let books=[("a","1",".40",".41","2"),("a","91",".50",".51","100"),("b","2",".57",".585","100"),("b","92",".40",".41","100")].map(|(m,id,bid,ask,q)|json!({"event_type":"book","market":m,"asset_id":id,"timestamp":"100","bids":[{"price":bid,"size":q}],"asks":[{"price":ask,"size":"100"}]}));
        market
            .send(Message::Text(json!(books).to_string()))
            .await
            .unwrap();
        screen.wait_for(|s| s.desk["ready"] == true).await.unwrap();
        let route = super::super::session::Route {
            id: 0,
            screen: screen.clone(),
            commands: cmd,
        };
        let (switches, _) = mpsc::channel(1);
        let fire = hyper::Request::post("/desk/fire")
            .header("Origin", "http://127.0.0.1:8080")
            .header("X-Paper-Control", "1")
            .header("X-Cockpit-Session", "0")
            .body(hyper::Body::empty())
            .unwrap();
        assert_eq!(
            super::super::command(fire, route, &switches).await.status(),
            hyper::StatusCode::OK
        );
        screen
            .wait_for(|s| s.desk["queue"].is_object())
            .await
            .unwrap();
        if let Some(bad) = timeout_gap {
            let lock = json!({"event_type":"price_change","market":"b","timestamp":"101","price_changes":[{"asset_id":"2","side":"BUY","price":".61","size":"9","best_bid":".57","best_ask":".61"}]});
            // Queue separate wire frames in a single flush so the good print arrives before cancel maturity.
            let edge = json!({"event_type":"price_change","market":"b","timestamp":"101","price_changes":[{"asset_id":"2","side":"SELL","price":".585","size":"0"},{"asset_id":"2","side":"SELL","price":".61","size":"100"}]});
            market.feed(Message::Text(edge.to_string())).await.unwrap();
            market.feed(Message::Text(lock.to_string())).await.unwrap();
            if bad {
                market.feed(Message::Text("{".into())).await.unwrap();
            }
            market.feed(Message::Text(json!({"event_type":"last_trade_price","asset_id":"1","price":".40","size":"7","side":"SELL","timestamp":"1"}).to_string())).await.unwrap();
            market.flush().await.unwrap();
            screen
                .wait_for(|s| s.desk["orders"].as_array().is_some_and(|v| !v.is_empty()))
                .await
                .unwrap();
            let view = &screen.borrow().desk;
            assert_eq!(view["orders"][0]["reason"], "edge_lost");
            assert_eq!(view["slots"][0]["shares"], if bad { "0" } else { "5" });
            assert_eq!(
                view["cancellations"][0]["filled"],
                if bad { "0" } else { "5" }
            );
            assert_eq!(view["frozen"], "0");
            assert_eq!(view["conserved"], true);
        } else {
            market.send(Message::Text(json!([
            {"event_type":"price_change","market":"b","timestamp":"101","price_changes":[{"asset_id":"2","side":"SELL","price":".585","size":"0"},{"asset_id":"2","side":"SELL","price":".61","size":"100"}]},
            {"event_type":"last_trade_price","asset_id":"1","price":".40","size":"7","side":"SELL","timestamp":(wall()+1).to_string()}
        ]).to_string())).await.unwrap();
            screen
                .wait_for(|s| s.desk["slots"][1]["shares"] == "5")
                .await
                .unwrap();
            let d = published_desk(&screen).await;
            assert_eq!(d["slots"][0]["shares"], "5");
            assert_eq!(d["cancellations"][0]["reason"], "edge_lost");
            assert_eq!(d["cancellations"][0]["filled"], "5");
            assert_eq!(dec(&d["cash"]).unwrap(), D::new(99495, 2));
            assert_eq!(d["conserved"], true);
        }
        stop.send(()).unwrap();
        // Keep the peer connected until the monitor retires the socket.
        while let Some(Ok(message)) = market.next().await {
            if message.is_close() {
                break;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(
            monitor::watch_group_until(&client, &endpoints, group, &output, stopped),
            exchange
        )
        .0
        .unwrap()
    })
    .await
    .unwrap();
}

#[test]
fn silent_wire_then_disconnect_preserves_effective_cancellation() {
    let mut replay = Replay::new(false);
    replay.depth(80, ".602");
    replay.clock(110);
    let view = replay.screen();
    assert_eq!(view["frozen"], "0");
    let receipt = &view["cancellations"][0];
    assert_eq!(receipt["sent_ms"], 80);
    assert_eq!(receipt["effective_ms"], 100);
    assert_eq!(receipt["outcome"], "cancelled");
    replay.input(130, MarketInput::Interrupted);
    replay.clock(150);
    assert_eq!(replay.screen()["cancellations"][0], *receipt);
}

#[tokio::test(start_paused = true)]
async fn every_admitted_print_consumes_its_own_volume() {
    for (count, volume, filled) in [(2, "4", "6"), (1, "1000001", "25")] {
        let (r, mut group, source, guard) = connected_replay(true).await;
        tokio::time::advance(Duration::from_millis(40)).await;
        let trade = json!({"event_type":"last_trade_price","asset_id":"1","price":".40","size":volume,"side":"SELL","timestamp":"1"});
        group
            .admit_raw(source, Value::Array(vec![trade; count]).to_string())
            .unwrap();
        group.advance_input(source).unwrap();
        let view = r.lock().unwrap().screen();
        assert_eq!(view["slots"][0]["shares"], filled);
        assert_eq!(view["conserved"], true);
        guard.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn stop_before_local_handoff_closes_at_the_cutoff() {
    let (replay, group, _, guard) = connected_replay(false).await;
    tokio::time::advance(Duration::from_millis(85)).await;
    let (stop, stopped) = oneshot::channel();
    stop.send(()).unwrap();
    crate::monitor::watch_group_until(
        &Client::new(),
        &crate::monitor::Endpoints {
            gamma: "http://127.0.0.1:1",
            rest: "http://127.0.0.1:1",
            socket: "ws://127.0.0.1:1",
        },
        group,
        &crate::output::Output::new(std::io::sink(), std::io::sink()),
        stopped,
    )
    .await
    .unwrap();
    guard.abort();
    let view = replay.lock().unwrap().screen();
    assert_eq!(view["orders"][0]["reason"], "session_ended");
    assert_eq!(view["orders"][0]["effective_ms"], 85);
    assert_eq!(view["cancellations"], json!([]));
    assert_eq!(dec(&view["frozen"]).unwrap(), D::ZERO);
}

#[tokio::test(start_paused = true)]
async fn broken_local_tail_finishes_original_receipt_before_returning_error() {
    for (gamma, cancel_wait) in [(true, false), (true, true), (false, false)] {
        let (replay, mut group, source, guard) = connected_replay(false).await;
        replay.lock().unwrap().depth(80, ".602");
        tokio::time::advance(Duration::from_millis(83)).await;
        group
            .admit_raw(
                source,
                json!({
                    "event_type": "last_trade_price", "asset_id": "1", "price": ".40",
                    "size": "7", "side": "SELL", "timestamp": "1"
                })
                .to_string(),
            )
            .unwrap();
        group.advance_input(source).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        group.admit_raw(source, "{".into()).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        guard.abort();
        let client = Client::new();
        let err = if gamma {
            group.pending_gamma_fixture(async { Ok(Discovery::Closed) });
            if cancel_wait {
                let wait = group.refresh_discovery(&client, "unused", Duration::ZERO);
                tokio::pin!(wait);
                assert!(futures_util::poll!(&mut wait).is_pending());
            }
            let error = group
                .refresh_discovery(&client, "unused", Duration::ZERO)
                .await
                .unwrap_err();
            assert!(
                !group
                    .refresh_discovery(&client, "unused", Duration::ZERO)
                    .await
                    .unwrap()
            );
            error
        } else {
            let (_stop, stopped) = oneshot::channel();
            crate::monitor::watch_group_until(
                &client,
                &crate::monitor::Endpoints::default(),
                group,
                &crate::output::Output::new(std::io::sink(), std::io::sink()),
                stopped,
            )
            .await
            .unwrap_err()
        };
        assert!(format!("{err:#}").contains("invalid WebSocket message"));
        let v = replay.lock().unwrap().screen();
        assert_eq!(v["frozen"], "0");
        assert_eq!(dec(&v["cash"]).unwrap(), D::from(998));
        assert_eq!(v["slots"][0]["shares"], "5");
        assert_eq!(v["slots"][1]["shares"], "0");
        assert_eq!(v["conserved"], true);
        assert_eq!(
            v["orders"][0],
            json!({"order_id":1,"reason":"edge_lost","effective_ms":100,"filled":"5"})
        );
    }
}

#[test]
fn newly_crossing_book_cannot_release_detached_order() {
    let mut r = Replay::new(false);
    r.depth(80, ".602");
    r.input(85, MarketInput::Interrupted);
    let b = BuyDepth {
        asset: "1".into(),
        observed_at: r.driver.origin + Duration::from_millis(90),
        tick: D::new(1, 4),
        bid: Some(Level {
            price: D::new(38, 2),
            size: 10.into(),
        }),
        asks: vec![Level {
            price: D::new(39, 2),
            size: 10.into(),
        }],
    };
    r.input(90, MarketInput::Offers(committed(&b)));
    r.clock(90);
    let v = r.screen();
    assert_eq!(dec(&v["frozen"]).unwrap(), D::TEN);
    assert_eq!(v["cancellations"][0]["outcome"], "pending");
    r.clock(100);
    let v = r.screen();
    assert_eq!(v["frozen"], "0");
    assert_eq!(v["orders"][0]["reason"], "edge_lost");
    assert_eq!(v["orders"][0]["effective_ms"], 100);
}

#[tokio::test(start_paused = true)]
async fn later_http_command_cannot_enter_an_older_clock_batch() {
    let (r, mut group, source, guard) = connected_replay(false).await;
    tokio::time::advance(Duration::from_millis(30)).await;
    let route = {
        let mut r = r.lock().unwrap();
        let mut wait = Ok(None);
        r.input(30, MarketInput::Ending(&mut wait));
        r.depth(30, ".585");
        r.command_at(30, Command::Lag(0));
        super::super::session::Route {
            id: 0,
            screen: r.driver.tx.subscribe(),
            commands: r.commands.clone(),
        }
    };
    let fire = hyper::Request::post("/desk/fire")
        .header("Origin", "http://127.0.0.1:8080")
        .header("X-Paper-Control", "1")
        .header("X-Cockpit-Session", "0")
        .body(hyper::Body::empty())
        .unwrap();
    let (switches, _) = mpsc::channel(1);
    let command = super::super::command(fire, route, &switches);
    tokio::pin!(command);
    let (mut wire, tx) = crate::transport::Transport::local();
    group
        .clock_at(|| {
            let at = wire.idle_frontier().unwrap();
            assert!(wire.enqueue(
                &tx,
                crate::transport::Payload::Raw(
                    r#"{"event_type":"market_resolved","market":"b"}"#.into()
                )
            ));
            assert!(futures_util::FutureExt::now_or_never(command.as_mut()).is_none());
            Ok(Some(at))
        })
        .unwrap();
    assert!(futures_util::poll!(&mut command).is_pending());
    crate::book::input::apply_frame(source, &mut group, wire.frames.try_recv().unwrap()).unwrap();
    group.clock_at(|| wire.idle_sample()).unwrap();
    assert_eq!(command.await.status(), hyper::StatusCode::CONFLICT);
    let view = r.lock().unwrap().screen();
    assert_eq!(view["cash"], "1000");
    assert_eq!(view["frozen"], "0");
    guard.abort();
}

async fn published_desk(screen: &watch::Receiver<Screen>) -> Value {
    let get = hyper::Request::get("/snapshot")
        .body(hyper::Body::empty())
        .unwrap();
    let reply = super::super::response(get, screen);
    let body = hyper::body::to_bytes(reply.into_body()).await.unwrap();
    let mut snapshot: Value = serde_json::from_slice(&body).unwrap();
    snapshot["desk"].take()
}

#[tokio::test(start_paused = true)]
async fn admitted_fee_and_ioc_at_idle_clock_use_one_cut() {
    for (wake, command, admitted_before) in [
        (140_000, false, true),
        (140_000, true, true),
        (140_001, false, true),
        (140_000, false, false),
    ] {
        let (replay, mut group, source, guard) = connected_replay(false).await;
        let route = {
            let mut r = replay.lock().unwrap();
            remove_hedge_bid(&mut r);
            r.sell(99, "7");
            r.clock(120);
            if admitted_before {
                fee_at(&mut r, 140_000, Err("unverified".into()));
            }
            super::super::session::Route {
                id: 0,
                screen: r.driver.tx.subscribe(),
                commands: r.commands.clone(),
            }
        };
        let screen = route.screen.clone();
        let lag = hyper::Request::post("/desk/lag/21")
            .header("Origin", "http://127.0.0.1:8080")
            .header("X-Paper-Control", "1")
            .header("X-Cockpit-Session", "0")
            .body(hyper::Body::empty())
            .unwrap();
        let (switches, _) = mpsc::channel(1);
        let request = super::super::command(lag, route, &switches);
        tokio::pin!(request);
        if command {
            assert!(futures_util::poll!(&mut request).is_pending());
        }
        tokio::time::advance(Duration::from_micros(wake)).await;
        assert!(!group.advance_input(source).unwrap());
        if command {
            assert_eq!(request.await.status(), hyper::StatusCode::OK);
        }
        if !admitted_before {
            fee_at(
                &mut replay.lock().unwrap(),
                140_000,
                Err("unverified".into()),
            );
            assert!(!group.advance_input(source).unwrap());
        }
        let view = published_desk(&screen).await;
        assert_eq!(view["lag"], if command { 21 } else { 20 });
        assert_eq!(view["slots"][0]["shares"], "5");
        assert_eq!(
            view["slots"][1]["shares"],
            if admitted_before { "0" } else { "5" }
        );
        assert_eq!(
            view["cash"],
            if admitted_before { "998" } else { "995.075" }
        );
        assert_eq!(view["frozen"], "0");
        assert_eq!(view["conserved"], true);
        guard.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn equal_market_print_precedes_zero_lag_fee_cancel() {
    let (replay, mut group, source, guard) = connected_replay(false).await;
    {
        let mut r = replay.lock().unwrap();
        r.command_at(30, Command::Lag(0));
        fee_at(&mut r, 80_000, Err("unverified".into()));
    }
    tokio::time::advance(Duration::from_millis(80)).await;
    group.admit_raw(source,r#"{"event_type":"last_trade_price","asset_id":"1","price":"0.40","size":"7","side":"SELL","timestamp":"1"}"#.into()).unwrap();
    assert!(group.advance_input(source).unwrap());
    assert!(!group.advance_input(source).unwrap());
    let screen = replay.lock().unwrap().driver.tx.subscribe();
    let view = published_desk(&screen).await;
    assert_eq!(view["slots"][0]["shares"], "5");
    assert_eq!(view["slots"][1]["shares"], "0");
    assert_eq!(view["cash"], "998");
    assert_eq!(view["frozen"], "0");
    assert_eq!(view["conserved"], true);
    let cancels = view["cancellations"].as_array().unwrap();
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0]["effective_ms"], 80);
    assert_eq!(cancels[0]["filled"], "5");
    guard.abort();
}

// Run both the accepted frames and the sampled idle clock through the group.
// The one-shot pause is inside CommandCount, between the two real clock reads.
struct DeadlineMarket {
    group: NegRiskEventGroup,
    source: SubscriptionId,
    replay: Arc<std::sync::Mutex<Replay>>,
    pause: Arc<std::sync::atomic::AtomicBool>,
    clocks: Arc<std::sync::Mutex<Vec<Instant>>>,
    interrupts: Arc<std::sync::Mutex<Vec<Instant>>>,
    guard: tokio::task::JoinHandle<()>,
    origin: Instant,
}

impl Drop for DeadlineMarket {
    fn drop(&mut self) {
        self.guard.abort();
    }
}
impl DeadlineMarket {
    async fn new(delay: u64, a_us: u64, b_us: u64) -> Self {
        use futures_util::FutureExt;
        use std::sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        };
        let replay = Arc::new(Mutex::new(Replay::idle(delay)));
        let origin = replay.lock().unwrap().driver.origin;
        let pause = Arc::new(AtomicBool::new(false));
        let clocks = Arc::new(Mutex::new(Vec::new()));
        let interrupts = Arc::new(Mutex::new(Vec::new()));
        let (r, p, c, i) = (
            replay.clone(),
            pause.clone(),
            clocks.clone(),
            interrupts.clone(),
        );
        let mut group = NegRiskEventGroup::with_market_consumer(
            "fixture",
            Quantity::new(25.into()).unwrap(),
            move |at, input| {
                match &input {
                    MarketInput::Clock(_) => c.lock().unwrap().push(at),
                    MarketInput::Interrupted => i.lock().unwrap().push(at),
                    MarketInput::CommandCount(_) if p.swap(false, Ordering::SeqCst) => {
                        let _ = tokio::time::advance(Duration::from_micros(1)).now_or_never();
                        assert_eq!(Instant::now(), at + Duration::from_micros(1));
                    }
                    _ => {}
                }
                r.lock().unwrap().driver.receive(at, input);
            },
        );
        let guard = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        approve(&mut group, [("a", "1", "91"), ("b", "2", "92")]).await;
        let source = group.open_input().unwrap();
        let mut m = Self {
            group,
            source,
            replay,
            pause,
            clocks,
            interrupts,
            guard,
            origin,
        };
        for (id, market, bid, ask, size, us) in [
            ("1", "a", ".40", ".41", "2", a_us),
            ("91", "a", ".40", ".41", "10", a_us),
            ("92", "b", ".40", ".41", "10", a_us),
            ("2", "b", ".57", ".585", "100", b_us),
        ] {
            m.at(us).await;
            m.frame(json!({"event_type":"book","market":market,"asset_id":id,"timestamp":"1","bids":[{"price":bid,"size":size}],"asks":[{"price":ask,"size":"100"}]}));
        }
        m
    }
    async fn at(&self, us: u64) {
        let target = self.origin + Duration::from_micros(us);
        tokio::time::advance(target.duration_since(Instant::now())).await;
        assert_eq!(Instant::now(), target);
    }
    fn frame(&mut self, msg: Value) {
        assert!(self.group.admit_raw(self.source, msg.to_string()).unwrap());
        assert!(self.group.advance_input(self.source).unwrap());
    }
    fn idle(&mut self) {
        assert!(!self.group.advance_input(self.source).unwrap());
    }
    async fn command(&mut self, path: &str) {
        assert_eq!(self.request(path).await, hyper::StatusCode::OK);
    }
    async fn request(&mut self, path: &str) -> hyper::StatusCode {
        let route = {
            let r = self.replay.lock().unwrap();
            super::super::session::Route {
                id: 0,
                screen: r.driver.tx.subscribe(),
                commands: r.commands.clone(),
            }
        };
        let request = hyper::Request::post(path)
            .header("Origin", "http://127.0.0.1:8080")
            .header("X-Paper-Control", "1")
            .header("X-Cockpit-Session", "0")
            .body(hyper::Body::empty())
            .unwrap();
        let (switches, _) = mpsc::channel(1);
        let command = super::super::command(request, route, &switches);
        tokio::pin!(command);
        assert!(futures_util::poll!(&mut command).is_pending());
        self.idle();
        command.await.status()
    }
    fn sell(&mut self) {
        self.frame(json!({"event_type":"last_trade_price","asset_id":"1","price":".40","size":"7","side":"SELL","timestamp":"1"}));
    }
    async fn view(&self) -> Value {
        let screen = self.replay.lock().unwrap().driver.tx.subscribe();
        published_desk(&screen).await
    }
    async fn send_hedge(&mut self, start_us: u64, cancel_us: u64) {
        self.at(start_us).await;
        self.command("/desk/lag/20").await;
        self.command("/desk/auto/on").await;
        self.at(start_us + 20_000).await;
        self.idle();
        self.at(cancel_us).await;
        self.command("/desk/auto/off").await;
        self.at(cancel_us + 19_000).await;
        self.sell();
        self.at(cancel_us + 40_000).await;
        self.idle();
    }
}

#[tokio::test(start_paused = true)]
async fn snapshot_associates_quotes_and_fees_by_identity_across_renames() {
    let mut m = DeadlineMarket::new(0, 0, 0).await;
    let selector = |name| [("a", "1", "91", "Same"), ("b", "2", "92", name)];
    let mut rules = m
        .replay
        .lock()
        .unwrap()
        .driver
        .desk
        .as_ref()
        .unwrap()
        .legs
        .clone();
    rules[1].rate = D::new(1, 1);
    rules[1].fee = FeeModel::new(rules[1].rate, false).unwrap();
    // Seed the fee display from an independently verified paper roster.
    let desk = Desk::new(rules, m.origin).unwrap().screen();
    let (channel, screen) = watch::channel(Screen::default());
    channel.send_modify(|s| s.desk = Arc::new(desk));
    let mut ids = None;
    for name in ["  Second", "Same"] {
        approve_named(&mut m.group, selector(name)).await;
        channel.send_modify(|s| s.publish_book(Arc::new(m.group.view())));
        let request = hyper::Request::get("/snapshot")
            .body(hyper::Body::empty())
            .unwrap();
        let raw = hyper::body::to_bytes(super::super::response(request, &screen).into_body())
            .await
            .unwrap();
        let view: Value = serde_json::from_slice(&raw).unwrap();
        let book = &view["book"];
        assert_eq!(book["status"], "Ready");
        let a = &book["markets"][0];
        let b = &book["markets"][1];
        assert_eq!(a["name"], "Same");
        assert_eq!(b["name"], name);
        assert_eq!(
            a["yes"]["bid"][0].as_str().unwrap().parse::<D>().unwrap(),
            D::new(40, 2)
        );
        assert_eq!(
            b["yes"]["bid"][0].as_str().unwrap().parse::<D>().unwrap(),
            D::new(57, 2)
        );
        let keys = [a["id"].as_u64().unwrap(), b["id"].as_u64().unwrap()];
        assert_ne!(keys[0], keys[1]);
        if let Some(ids) = ids {
            assert_eq!(keys, ids);
        }
        ids = Some(keys);
        assert_eq!(view["desk"]["rates"][keys[0].to_string()], "0");
        assert_eq!(view["desk"]["rates"][keys[1].to_string()], "0.1");
        assert!(a.get("identity").is_none() && b.get("identity").is_none());
        assert!(view["desk"]["slots"][0].get("identity").is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn second_round_http_checks_effective_depth_at_entry_and_delivery() {
    let hedge_book = |timestamp, size| {
        json!({
            "event_type":"book","market":"b","asset_id":"2","timestamp":timestamp,
            "bids":[{"price":"0.57","size":"100"}],"asks":[{"price":"0.585","size":size}]
        })
    };
    for (size, drop_before_delivery) in [("25", false), ("50", true), ("50", false)] {
        let mut m = DeadlineMarket::new(0, 0, 0).await;
        m.frame(hedge_book("2", "25"));
        m.command("/desk/lag/20").await;
        m.command("/desk/fire").await;
        m.at(20_000).await;
        m.idle();
        m.at(21_000).await;
        m.frame(
            json!({"event_type":"last_trade_price","market":"a","asset_id":"1",
            "price":"0.40","size":"27","side":"SELL","timestamp":"1"}),
        );
        m.at(61_000).await;
        m.idle();
        let first = m.view().await;
        assert_eq!(first["slots"][0]["shares"], "25");
        assert_eq!(first["slots"][1]["shares"], "25");

        m.at(201_000).await;
        m.frame(hedge_book("3", size));
        m.at(1_200_000).await;
        let status = m.request("/desk/fire").await;
        assert_eq!(
            status,
            if size == "25" {
                hyper::StatusCode::CONFLICT
            } else {
                hyper::StatusCode::OK
            }
        );
        if drop_before_delivery {
            m.at(1_210_000).await;
            m.frame(hedge_book("4", "49"));
        }
        m.at(1_220_000).await;
        m.idle();
        let delivered = m.view().await;
        let replenished = size == "50" && !drop_before_delivery;
        assert_eq!(delivered["queue"].is_object(), replenished);
        assert_eq!(
            dec(&delivered["frozen"]).unwrap(),
            if replenished { D::TEN } else { D::ZERO }
        );
        if drop_before_delivery {
            assert!(delivered["log"].to_string().contains("可見補腿深度不足"));
        }
        m.at(1_221_000).await;
        m.frame(
            json!({"event_type":"last_trade_price","market":"a","asset_id":"1",
            "price":"0.40","size":"27","side":"SELL","timestamp":"2"}),
        );
        m.at(1_261_000).await;
        m.idle();
        let final_view = m.view().await;
        let shares = if replenished { "50" } else { "25" };
        assert_eq!(final_view["slots"][0]["shares"], shares);
        assert_eq!(final_view["slots"][1]["shares"], shares);
        assert_eq!(dec(&final_view["frozen"]).unwrap(), D::ZERO);
        assert_eq!(final_view["conserved"], true);
    }
}

#[tokio::test(start_paused = true)]
async fn capacity_isolation_tick_closes_only_off_grid_cockpit_orders() {
    use crate::test_http::{coarse_tick, full_book_at_capacity, overflow_full_book};

    for (bid, off_grid) in [("0.41", true), ("0.40", false)] {
        let mut m = DeadlineMarket::new(0, 0, 0).await;
        m.frame(
            json!({"event_type":"book","market":"a","asset_id":"1","timestamp":"100",
            "bids":[{"price":bid,"size":"2"}],"asks":[{"price":"0.60","size":"100"}]}),
        );
        m.command("/desk/lag/20").await;
        m.command("/desk/fire").await;
        m.at(20_000).await;
        m.idle();
        let frozen: D = (if off_grid { "10.25" } else { "10" }).parse().unwrap();
        assert_eq!(dec(&m.view().await["frozen"]).unwrap(), frozen);

        m.at(30_000).await;
        m.frame(full_book_at_capacity(bid));
        assert!(m.group.ready());
        m.at(31_000).await;
        m.frame(overflow_full_book());
        assert!(!m.group.ready());
        assert_eq!(
            dec(&m.replay.lock().unwrap().screen()["frozen"]).unwrap(),
            frozen
        );

        m.at(32_000).await;
        m.frame(coarse_tick());
        m.sell(); // The isolated order book must not match even at the retained price.
        assert!(!m.group.ready());
        m.at(130_000).await;
        m.idle();
        let v = m.view().await;
        assert_eq!(v["slots"][0]["shares"], "0");
        assert_eq!(v["conserved"], true);
        if off_grid {
            assert_eq!(
                v["orders"][0],
                json!({"order_id":1,"reason":"tick_changed","effective_ms":32,"filled":"0"})
            );
            assert_eq!(v["frozen"], "0");
        } else {
            assert_eq!(v["queue"]["remaining"], "2");
            assert_eq!(dec(&v["frozen"]).unwrap(), D::TEN);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn snapshot_publishes_a_timeout_receipt_between_periodic_samples() {
    let mut market = DeadlineMarket::new(0, 0, 0).await;
    market.command("/desk/lag/20").await;
    market.command("/desk/fire").await;
    market.at(20_000).await;
    market.idle();
    market.at(20_019_000).await;
    market.idle();
    assert_eq!(market.view().await["cancellations"], json!([]));

    market.at(20_040_000).await;
    market.idle();
    let view = market.view().await;
    assert_eq!(view["cancellations"][0]["reason"], "timeout");
    assert_eq!(view["cancellations"][0]["effective_ms"], 20_040);
    assert_eq!(view["frozen"], "0");
}

#[tokio::test(start_paused = true)]
async fn ioc_at_ledger_millisecond_uses_its_own_legs_exact_depth_age() {
    for (fraction, shares) in [(0, "0"), (500, "5")] {
        let mut m = DeadlineMarket::new(59_000, 0, fraction).await;
        m.send_hedge(1000, 940_000).await;
        m.at(60_000_000).await;
        m.idle();
        let v = m.view().await;
        assert_eq!(v["slots"][0]["shares"], "5");
        assert_eq!(v["slots"][1]["shares"], shares);
        assert_eq!(
            v["orders"]
                .as_array()
                .unwrap()
                .iter()
                .find(|o| o["order_id"] == 2)
                .and_then(|o| o["effective_ms"].as_u64()),
            (fraction != 0).then_some(60_000)
        );
        assert_eq!(v["conserved"], true);
    }
}
#[tokio::test(start_paused = true)]
async fn resting_maker_print_uses_exact_expiry() {
    for (us, shares) in [(60_000_499, "5"), (60_000_500, "0")] {
        let mut m = DeadlineMarket::new(0, 500, 500).await;
        m.at(59_000_000).await;
        m.command("/desk/lag/20").await;
        m.command("/desk/fire").await;
        m.at(59_020_000).await;
        m.idle();
        m.at(us).await;
        m.sell();
        m.at(60_150_000).await;
        m.idle();
        let v = m.view().await;
        assert_eq!(v["slots"][0]["shares"], shares);
        assert_eq!(v["conserved"], true);
    }
}
#[tokio::test(start_paused = true)]
async fn idle_sample_owns_reconciliation_cut_and_hedge_result() {
    for crosses in [true, false] {
        let mut m = DeadlineMarket::new(1000, 0, 0).await;
        m.send_hedge(0, 80_000).await;
        m.at(if crosses { 640_000 } else { 640_001 }).await;
        m.frame(json!({"event_type":"price_change","market":"a","timestamp":"2","price_changes":[{"asset_id":"91","side":"BUY","price":".41","size":"1","best_bid":".40","best_ask":".41"}]}));
        let deadline = m.group.reconciliation_deadline().unwrap();
        m.at(if crosses { 1_139_999 } else { 1_140_000 }).await;
        m.clocks.lock().unwrap().clear();
        if crosses {
            m.pause.store(true, std::sync::atomic::Ordering::SeqCst);
            let error = m.group.advance_input(m.source).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("crossed book reconciliation timed out")
            );
            assert!(m.clocks.lock().unwrap().is_empty());
            assert_eq!(*m.interrupts.lock().unwrap(), vec![deadline]);
            assert!(
                !m.group
                    .admit_event(m.source, crate::wire::Event::Other)
                    .unwrap()
            );
            assert!(m.group.open_input().is_some());
        } else {
            m.idle();
            assert_eq!(
                *m.clocks.lock().unwrap(),
                vec![deadline - Duration::from_micros(1)]
            );
        }
        let v = m.view().await;
        assert_eq!(v["slots"][0]["shares"], "5");
        assert_eq!(v["slots"][1]["shares"], if crosses { "0" } else { "5" });
        assert_eq!(v["conserved"], true);
    }
}

#[tokio::test(start_paused = true)]
async fn overdue_local_input_keeps_early_print_and_late_settlement() {
    for malformed in [false, true] {
        let mut m = DeadlineMarket::new(1000, 0, 0).await;
        m.command("/desk/lag/20").await;
        m.command("/desk/fire").await;
        m.at(20_000).await;
        m.idle();
        m.at(80_000).await;
        m.frame(json!({"event_type":"price_change","market":"a","timestamp":"2","price_changes":[{"asset_id":"91","side":"BUY","price":".41","size":"1","best_bid":".40","best_ask":".41"}]}));
        if malformed {
            m.at(579_998).await;
            m.group.admit_raw(m.source, "{".into()).unwrap();
        }
        for (at, size) in [(579_999, "7"), (580_000, "8")] {
            m.at(at).await;
            let trade = json!({"event_type":"last_trade_price","asset_id":"1","price":".40","size":size,"side":"SELL","timestamp":at});
            m.group.admit_raw(m.source, trade.to_string()).unwrap();
        }
        m.group
            .admit_raw(
                m.source,
                json!({"event_type":"market_resolved","market":"b"}).to_string(),
            )
            .unwrap();
        m.at(581_000).await;
        assert!(m.group.advance_input(m.source).is_err());
        let (assets, _) = m.group.subscription().unwrap().into_parts();
        assert_eq!(assets, ["1", "91"]);
        let v = m.view().await;
        assert_eq!(v["slots"][0]["shares"], if malformed { "0" } else { "5" });
        assert_eq!(v["slots"][1]["shares"], "0");
        assert_eq!(v["conserved"], true);
    }
}

#[tokio::test(start_paused = true)]
async fn settlement_cannot_erase_deadline_reached_before_frame_commit() {
    for during_prepare in [false, true] {
        let mut m = DeadlineMarket::new(1000, 0, 0).await;
        m.send_hedge(0, 80_000).await;
        m.at(640_000).await;
        m.frame(json!({"event_type":"price_change","market":"a","timestamp":"2","price_changes":[{"asset_id":"91","side":"BUY","price":".41","size":"1","best_bid":".40","best_ask":".41"}]}));
        let due = m.group.reconciliation_deadline().unwrap();
        m.at(1_139_999).await;
        m.group
            .admit_raw(
                m.source,
                json!({"event_type":"market_resolved","market":"a"}).to_string(),
            )
            .unwrap();
        if during_prepare {
            crate::book::frame::tests::before_next_commit(|| {
                use futures_util::FutureExt;
                let _ = tokio::time::advance(Duration::from_micros(1)).now_or_never();
            });
        } else {
            m.pause.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let err = m.group.advance_input(m.source).unwrap_err();
        assert!(
            err.to_string()
                .contains("crossed book reconciliation timed out")
        );
        assert_eq!(*m.interrupts.lock().unwrap(), vec![due]);
        assert_eq!(m.group.subscription().unwrap().into_parts().0, ["2", "92"]);
        let v = m.view().await;
        assert_eq!(v["slots"][0]["shares"], "5");
        assert_eq!(v["slots"][1]["shares"], "0");
    }
}

#[tokio::test(start_paused = true)]
async fn frame_validates_sequential_edits_before_publishing_trades() {
    for (split, backwards) in [(false, false), (false, true), (true, false)] {
        let mut m = DeadlineMarket::new(1000, 0, 0).await;
        m.command("/desk/lag/20").await;
        m.command("/desk/fire").await;
        m.at(20_000).await;
        m.idle();
        m.at(99_000).await;
        let trade = json!({"event_type":"last_trade_price","asset_id":"1","price":".40","size":"7","side":"SELL","timestamp":"1"});
        let mut events = Vec::new();
        if split {
            m.frame(trade);
        } else {
            events.push(trade);
        }
        if backwards {
            for timestamp in [3, 2] {
                events.push(json!({"event_type":"price_change","market":"a","timestamp":timestamp,"price_changes":[{"asset_id":"91","side":"BUY","price":".40","size":"11"}]}));
            }
        } else {
            events.push(json!({"event_type":"book","asset_id":"91","market":"wrong","timestamp":"2","bids":[],"asks":[]}));
        }
        events.push(json!({"event_type":"market_resolved","market":"b"}));
        m.group
            .admit_raw(m.source, json!(events).to_string())
            .unwrap();
        let err = m.group.advance_input(m.source).unwrap_err();
        assert!(err.to_string().contains(if backwards {
            "delta went backwards"
        } else {
            "unexpected condition ID"
        }));
        let v = m.view().await;
        assert_eq!(v["slots"][0]["shares"], if split { "5" } else { "0" });
        assert_eq!(v["slots"][1]["shares"], "0");
        assert_eq!(v["conserved"], true);
        assert_eq!(m.group.subscription().unwrap().into_parts().0, ["1", "91"]);
    }
}

#[tokio::test(start_paused = true)]
async fn public_trade_values_are_validated_before_publishing_the_frame() {
    for split in [false, true] {
        let (replay, mut group, source, guard) = connected_replay(true).await;
        tokio::time::advance(Duration::from_millis(99)).await;
        let print = |size| json!({"event_type":"last_trade_price","asset_id":"1","price":".40","size":size,"side":"SELL","timestamp":"1"});
        let frame = if split {
            group.admit_raw(source, print("7").to_string()).unwrap();
            group.advance_input(source).unwrap();
            json!([print("-1")])
        } else {
            json!([print("7"), print("-1")])
        };
        group.admit_raw(source, frame.to_string()).unwrap();
        assert!(
            group
                .advance_input(source)
                .unwrap_err()
                .to_string()
                .contains("invalid public trade")
        );
        let view = replay.lock().unwrap().screen();
        assert_eq!(view["slots"][0]["shares"], if split { "5" } else { "0" });
        assert_eq!(view["conserved"], true);
        guard.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn overdue_handoff_closes_at_the_same_cut_with_or_without_a_tail_frame() {
    for extra in [false, true] {
        let mut m = DeadlineMarket::new(1000, 0, 0).await;
        m.command("/desk/lag/20").await;
        m.command("/desk/fire").await;
        m.at(20_000).await;
        m.idle();
        m.at(80_000).await;
        m.frame(json!({"event_type":"price_change","market":"a","timestamp":"2","price_changes":[{"asset_id":"91","side":"BUY","price":".41","size":"1","best_bid":".40","best_ask":".41"}]}));
        m.at(580_000).await;
        if extra {
            m.group.admit_raw(m.source, "[]".into()).unwrap();
        }
        m.at(600_000).await;
        let mut retirement = m
            .group
            .local_retirement(crate::book::input::End::Handoff)
            .unwrap();
        while retirement.advance() {}
        let result = retirement.complete();
        assert!(
            m.group
                .finish_with(result)
                .await
                .unwrap_err()
                .to_string()
                .contains("reconciliation timed out")
        );
        let v = m.view().await;
        assert_eq!(v["orders"][0]["effective_ms"], 580);
        assert_eq!(v["orders"][0]["reason"], "session_ended");
        assert_eq!(v["frozen"], "0");
    }
}

#[tokio::test(start_paused = true)]
async fn idle_gamma_close_uses_its_acceptance_time_across_the_patience_deadline() {
    let (replay, mut group, source, guard) = connected_replay(true).await;
    let origin = replay.lock().unwrap().driver.origin;
    tokio::time::advance(Duration::from_millis(85)).await;
    group.close_input(source).unwrap();
    let accepted = origin + Duration::from_millis(20_019);
    tokio::time::advance(accepted + Duration::from_millis(2) - Instant::now()).await;
    group
        .update_discovery_at(accepted, Ok(Discovery::Closed))
        .unwrap();
    group.finish_with(Ok(())).await.unwrap();
    let view = replay.lock().unwrap().screen();
    assert_eq!(view["orders"][0]["effective_ms"], 20_019);
    assert_eq!(view["orders"][0]["reason"], "session_ended");
    assert_eq!(view["cancellations"], json!([]));
    assert_eq!(dec(&view["frozen"]).unwrap(), D::ZERO);
    guard.abort();
}
