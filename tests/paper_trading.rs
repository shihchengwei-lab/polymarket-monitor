use polymarket_monitor::paper::{
    AccountId, AssetId, Cash, ClientOrderId, Error, Price, Quantity, Shares, Side, Stamp,
    chaos_exchange::{ChaosExchange, Depth, ExecutionPair, MAX_ORDERS, Trade},
    fee_model::{FeeModel, MakerTier},
    hybrid_coordinator::{HybridCoordinator, Progress, Step},
    order_fsm::{CloseReason, Delivery, OrderMode, OrderRequest, OrderStatus},
};
use rust_decimal::Decimal;

fn decimal(s: &str) -> Decimal {
    s.parse().unwrap()
}
fn qty(s: &str) -> Quantity {
    Quantity::new(decimal(s)).unwrap()
}
fn shares(s: &str) -> Shares {
    Shares::new(decimal(s)).unwrap()
}
fn price(s: &str) -> Price {
    Price::new(decimal(s)).unwrap()
}
fn cash(s: &str) -> Cash {
    Cash::new(decimal(s)).unwrap()
}
fn id(n: u64) -> ClientOrderId {
    ClientOrderId::new(n).unwrap()
}

struct Fixture {
    exchange: ChaosExchange,
    asset: AssetId,
    account: AccountId,
    stamp: Stamp,
}

impl Fixture {
    fn new(funds: &str, position: &str, fee: FeeModel) -> Self {
        let asset = AssetId::new("123").unwrap();
        let mut exchange = ChaosExchange::new(100, cash(funds)).unwrap();
        exchange
            .register_asset(asset.clone(), fee, shares("1000"))
            .unwrap();
        let account = exchange
            .allocate(
                cash(funds),
                [(asset.clone(), shares(position))],
                MakerTier::Tier4,
            )
            .unwrap();
        let stamp = exchange.feed(&asset).unwrap().stamp(0, 0);
        Self {
            exchange,
            asset,
            account,
            stamp,
        }
    }

    fn advance(&mut self) -> Stamp {
        self.stamp = self
            .exchange
            .feed(&self.asset)
            .unwrap()
            .stamp(self.stamp.sequence() + 1, self.stamp.observed_ms() + 1);
        self.exchange.advance_to(self.stamp.observed_ms()).unwrap();
        self.stamp
    }

    fn observe(&mut self, bids: &[(&str, &str)], asks: &[(&str, &str)]) {
        let stamp = self.advance();
        let bids = bids.iter().map(|&(px, size)| (price(px), qty(size)));
        let asks = asks.iter().map(|&(px, size)| (price(px), qty(size)));
        self.exchange
            .observe(Depth::new(self.asset.clone(), stamp, price("0.000001"), bids, asks).unwrap())
            .unwrap();
    }

    fn trade(&mut self, side: Side, px: &str, size: &str) {
        let stamp = self.advance();
        self.exchange
            .trade(Trade::new(
                self.asset.clone(),
                stamp,
                side,
                price(px),
                qty(size),
            ))
            .unwrap();
    }

    fn order(&self, n: u64, side: Side, limit: &str, size: &str, mode: OrderMode) -> OrderRequest {
        OrderRequest::new(
            id(n),
            self.account,
            self.asset.clone(),
            side,
            price(limit),
            qty(size),
            mode,
        )
    }

    fn submit(&mut self, request: OrderRequest, receipt: Delivery) -> Result<OrderStatus, Error> {
        self.exchange
            .submit(request, self.stamp, receipt)
            .map(|r| r.status())
    }
}

#[test]
fn price_time_keeps_new_public_size_behind_existing_makers() {
    let mut f = Fixture::new("100", "0", FeeModel::FREE);
    f.observe(&[("0.40", "3")], &[("0.60", "10")]);
    for n in [10, 1] {
        assert_eq!(
            f.submit(
                f.order(n, Side::Buy, "0.40", "1", OrderMode::PostOnly),
                Delivery::Delivered
            ),
            Ok(OrderStatus::Working {
                remaining: qty("1"),
                filled: Shares::ZERO
            })
        );
    }
    f.observe(&[("0.40", "5")], &[("0.60", "10")]);
    f.trade(Side::Sell, "0.40", "4");
    assert_eq!(
        f.exchange.order_status(f.account.order(id(10))),
        Ok(OrderStatus::Closed {
            filled: shares("1"),
            reason: CloseReason::Filled
        })
    );
    assert_eq!(
        f.exchange.order_status(f.account.order(id(1))),
        Ok(OrderStatus::Working {
            filled: Shares::ZERO,
            remaining: qty("1")
        })
    );
    f.trade(Side::Sell, "0.40", "1");
    assert_eq!(
        f.exchange.order_status(f.account.order(id(1))),
        Ok(OrderStatus::Closed {
            filled: shares("1"),
            reason: CloseReason::Filled
        })
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().shares(),
        shares("2")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn taker_sweeps_best_first_and_never_remints_an_unchanged_snapshot() {
    let fees = FeeModel::new(decimal("0.05"), true).unwrap();
    let mut f = Fixture::new("20", "0", fees);
    let asks = [("0.48", "3"), ("0.46", "2"), ("0.55", "9")];
    f.observe(&[], &asks);
    let request = f.order(1, Side::Buy, "0.48", "4", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.submit(request, Delivery::Delivered),
        Ok(OrderStatus::Closed {
            filled: shares("4"),
            reason: CloseReason::Filled
        })
    );
    // Gross 2 * 0.46 + 2 * 0.48 = 1.88. Fees are 0.02484 + 0.02496.
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("18.07020")
    );
    f.observe(&[], &asks);
    let next = f.order(2, Side::Buy, "0.48", "2", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.submit(next, Delivery::Delivered),
        Ok(OrderStatus::Closed {
            filled: shares("1"),
            reason: CloseReason::UnfilledIoc
        })
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn duplicate_unknown_order_ids_do_not_execute_or_unlock_a_second_time() {
    let mut f = Fixture::new("1", "0", FeeModel::FREE);
    f.observe(&[], &[("0.40", "4")]);
    let request = f.order(1, Side::Buy, "0.50", "2", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.submit(request.clone(), Delivery::Lost),
        Ok(OrderStatus::Unconfirmed)
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        Cash::ZERO
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().shares(),
        Shares::ZERO
    );
    assert_eq!(
        f.submit(request.clone(), Delivery::Delivered),
        Ok(OrderStatus::Unconfirmed)
    );
    assert_eq!(
        f.submit(
            f.order(1, Side::Buy, "0.50", "1", OrderMode::ImmediateOrCancel),
            Delivery::Delivered
        ),
        Err(Error::ConflictingOrder)
    );
    assert_eq!(
        f.submit(
            f.order(2, Side::Sell, "0.50", "1", OrderMode::PostOnly),
            Delivery::Delivered
        ),
        Err(Error::InsufficientFunds)
    );
    assert_eq!(
        f.exchange
            .cancel(f.account.order(id(1)), Delivery::Delivered),
        Err(Error::Unconfirmed)
    );
    let final_status = Ok(OrderStatus::Closed {
        filled: shares("2"),
        reason: CloseReason::Filled,
    });
    assert_eq!(f.exchange.reconcile(f.account.order(id(1))), final_status);
    assert_eq!(f.exchange.reconcile(f.account.order(id(1))), final_status);
    assert_eq!(f.submit(request, Delivery::Lost), final_status);
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("0.20")
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().shares(),
        shares("2")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn a_dropped_cancel_keeps_order_priority_while_receipt_is_unknown() {
    let mut f = Fixture::new("0", "2", FeeModel::FREE);
    f.observe(&[], &[]);
    assert!(
        f.submit(
            f.order(1, Side::Sell, "0.40", "2", OrderMode::PostOnly),
            Delivery::Delivered
        )
        .is_ok()
    );
    assert_eq!(
        f.exchange.cancel(f.account.order(id(1)), Delivery::Dropped),
        Ok(OrderStatus::Unconfirmed)
    );
    f.trade(Side::Buy, "0.40", "1");
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        Cash::ZERO
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().shares(),
        Shares::ZERO
    );
    assert_eq!(
        f.exchange.reconcile(f.account.order(id(1))),
        Ok(OrderStatus::Working {
            filled: shares("1"),
            remaining: qty("1")
        })
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("0.40")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn hybrid_waits_for_cancel_receipt_then_takes_only_the_remainder() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[("0.60", "10")]);
    let maker = f.order(1, Side::Buy, "0.40", "5", OrderMode::PostOnly);
    let strategy = HybridCoordinator::hybrid(maker, id(2), price("0.60")).unwrap();
    let mut step = Step {
        escalate: false,
        submit_receipt: Delivery::Delivered,
        cancel_receipt: Delivery::Lost,
    };
    assert_eq!(
        strategy.step(&mut f.exchange, f.stamp, step),
        Ok(Progress::Resting {
            order: f.account.order(id(1)),
            filled: Shares::ZERO
        })
    );
    f.trade(Side::Sell, "0.40", "2");
    step.escalate = true;
    assert_eq!(
        strategy.step(&mut f.exchange, f.stamp, step),
        Ok(Progress::AwaitingReconciliation(f.account.order(id(1))))
    );
    assert_eq!(
        f.exchange.order_status(f.account.order(id(2))),
        Err(Error::UnknownOrder)
    );
    f.exchange.reconcile(f.account.order(id(1))).unwrap();
    step.escalate = false;
    assert_eq!(
        strategy.step(&mut f.exchange, f.stamp, step),
        Ok(Progress::Complete {
            filled: shares("5")
        })
    );
    assert_eq!(
        f.exchange.order_status(f.account.order(id(2))),
        Ok(OrderStatus::Closed {
            reason: CloseReason::Filled,
            filled: shares("3")
        })
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("7.40")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn a_public_trade_rejects_an_older_strategy_signal_even_if_depth_has_not_expired() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[("0.60", "10")]);
    let past = f.stamp;
    f.trade(Side::Buy, "0.60", "1");
    let request = f.order(1, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.exchange
            .submit(request.clone(), past, Delivery::Delivered),
        Err(Error::StaleSignal)
    );
    assert!(f.submit(request.clone(), Delivery::Delivered).is_ok());
    f.exchange.advance_to(102).unwrap();
    assert_eq!(
        f.submit(
            f.order(2, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel),
            Delivery::Delivered
        ),
        Err(Error::StaleSignal)
    );
    assert!(f.submit(request, Delivery::Lost).is_ok()); // ID replay is a lookup.
    assert!(f.exchange.is_conserved());
}

#[test]
fn disconnect_cancels_working_orders_without_unlocking_an_unknown_receipt() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[]);
    let request = f.order(1, Side::Buy, "0.40", "5", OrderMode::PostOnly);
    assert_eq!(
        f.submit(request, Delivery::Lost),
        Ok(OrderStatus::Unconfirmed)
    );
    f.exchange.invalidate_depth(&f.asset).unwrap();
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("8")
    );
    assert_eq!(f.exchange.current_stamp(&f.asset), Ok(None));
    assert_eq!(
        f.exchange.reconcile(f.account.order(id(1))),
        Ok(OrderStatus::Closed {
            reason: CloseReason::LostBook,
            filled: Shares::ZERO
        })
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("10")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn maker_rebate_is_funded_and_only_paid_after_a_simulated_day() {
    let fees = FeeModel::new(decimal("0.05"), true).unwrap();
    let mut f = Fixture::new("0", "1000", fees);
    f.observe(&[], &[]);
    let request = f.order(1, Side::Sell, "0.50", "1000", OrderMode::PostOnly);
    assert!(f.submit(request, Delivery::Delivered).is_ok());
    f.trade(Side::Buy, "0.50", "1000");
    let balance = f.exchange.balance(f.account, &f.asset).unwrap();
    assert_eq!(balance.cash(), cash("500"));
    assert_eq!(balance.accrued_rebates(), cash("1.875"));
    assert_eq!(f.exchange.settle_rebates(f.account), Ok(Cash::ZERO));
    f.exchange.advance_to(86_400_000).unwrap();
    assert_eq!(f.exchange.settle_rebates(f.account), Ok(cash("1.875")));
    assert_eq!(f.exchange.settle_rebates(f.account), Ok(Cash::ZERO));
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("501.875")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn reconciliation_keeps_the_original_rebate_earning_day() {
    let fees = FeeModel::new(decimal("0.05"), true).unwrap();
    let mut f = Fixture::new("0", "1000", fees);
    f.observe(&[], &[]);
    let request = f.order(1, Side::Sell, "0.50", "1000", OrderMode::PostOnly);
    assert_eq!(
        f.submit(request, Delivery::Lost),
        Ok(OrderStatus::Unconfirmed)
    );
    f.trade(Side::Buy, "0.50", "1000");
    f.exchange.advance_to(86_400_000).unwrap();
    assert_eq!(f.exchange.settle_rebates(f.account), Ok(Cash::ZERO));
    assert!(f.exchange.is_conserved());
    f.exchange.reconcile(f.account.order(id(1))).unwrap();
    assert_eq!(f.exchange.settle_rebates(f.account), Ok(cash("1.875")));
    assert!(f.exchange.is_conserved());
}

#[test]
fn announcing_trade_depletion_does_not_remove_the_next_public_queue_block() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[("0.40", "3")], &[("0.60", "10")]);
    let maker = f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly);
    f.submit(maker, Delivery::Delivered).unwrap();
    f.observe(&[("0.40", "5")], &[("0.60", "10")]);
    f.trade(Side::Sell, "0.40", "3");
    f.observe(&[("0.40", "2")], &[("0.60", "10")]);
    let taker = f.order(2, Side::Sell, "0.40", "1", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.submit(taker, Delivery::Delivered),
        Err(Error::InsufficientFunds)
    );
    f.trade(Side::Sell, "0.40", "1");
    let taker = f.order(2, Side::Sell, "0.40", "1", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.submit(taker, Delivery::Delivered),
        Ok(OrderStatus::Closed {
            filled: shares("1"),
            reason: CloseReason::Filled
        })
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn budget_and_order_limits_preserve_terminal_ids() {
    let mut f = Fixture::new("1", "0", FeeModel::FREE);
    f.observe(&[], &[]);
    assert_eq!(
        f.exchange.allocate(cash("1"), [], MakerTier::None),
        Err(Error::AlreadyStarted)
    );
    let original = f.order(1, Side::Buy, "0.4", "1", OrderMode::ImmediateOrCancel);
    for number in 1..=MAX_ORDERS {
        let request = f.order(
            number as u64,
            Side::Buy,
            "0.4",
            "1",
            OrderMode::ImmediateOrCancel,
        );
        assert!(f.submit(request, Delivery::Delivered).is_ok());
    }
    let beyond_limit = f.order(
        MAX_ORDERS as u64 + 1,
        Side::Buy,
        "0.4",
        "1",
        OrderMode::ImmediateOrCancel,
    );
    assert_eq!(
        f.submit(beyond_limit, Delivery::Delivered),
        Err(Error::Capacity)
    );
    assert_eq!(
        f.submit(original, Delivery::Lost),
        Ok(OrderStatus::Closed {
            filled: Shares::ZERO,
            reason: CloseReason::UnfilledIoc
        })
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn allocation_cannot_overcommit_the_configured_pool() {
    let asset = AssetId::new("123").unwrap();
    let mut exchange = ChaosExchange::new(100, cash("10")).unwrap();
    exchange
        .register_asset(asset.clone(), FeeModel::FREE, shares("4"))
        .unwrap();
    let first = exchange
        .allocate(cash("7"), [(asset.clone(), shares("2"))], MakerTier::None)
        .unwrap();
    assert_eq!(
        exchange.allocate(cash("3"), [(asset.clone(), shares("3"))], MakerTier::None),
        Err(Error::InsufficientFunds)
    );
    assert_eq!(
        exchange.allocate(cash("4"), [(asset.clone(), shares("2"))], MakerTier::None),
        Err(Error::InsufficientFunds)
    );
    let second = exchange
        .allocate(cash("3"), [(asset.clone(), shares("2"))], MakerTier::None)
        .unwrap();
    assert_eq!(exchange.balance(first, &asset).unwrap().cash(), cash("7"));
    assert_eq!(exchange.balance(second, &asset).unwrap().cash(), cash("3"));
    assert!(exchange.is_conserved());
}

#[test]
fn resting_orders_do_not_fill_on_l2_cancellation_or_crossed_observation() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[("0.40", "3")], &[("0.60", "5")]);
    let maker = f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly);
    f.submit(maker, Delivery::Delivered).unwrap();
    f.observe(&[], &[("0.60", "5")]);
    assert_eq!(
        f.exchange.order_status(f.account.order(id(1))),
        Ok(OrderStatus::Working {
            remaining: qty("1"),
            filled: Shares::ZERO
        })
    );
    f.observe(&[], &[("0.39", "1")]);
    assert_eq!(
        f.exchange.order_status(f.account.order(id(1))),
        Ok(OrderStatus::Closed {
            reason: CloseReason::BookCrossed,
            filled: Shares::ZERO
        })
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("10")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn pure_taker_stops_at_a_paper_order_without_trading_through_it() {
    let mut f = Fixture::new("10", "1", FeeModel::FREE);
    f.observe(&[], &[("0.60", "3")]);
    let maker = f.order(1, Side::Sell, "0.50", "1", OrderMode::PostOnly);
    f.submit(maker, Delivery::Delivered).unwrap();
    let taker = f.order(2, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel);
    let plan = HybridCoordinator::taker(taker).unwrap();
    assert_eq!(
        plan.step(
            &mut f.exchange,
            f.stamp,
            Step {
                escalate: false,
                submit_receipt: Delivery::Delivered,
                cancel_receipt: Delivery::Delivered
            }
        ),
        Ok(Progress::Complete {
            filled: Shares::ZERO
        })
    );
    assert_eq!(
        f.exchange.order_status(f.account.order(id(2))),
        Ok(OrderStatus::Closed {
            reason: CloseReason::SelfTradePrevented,
            filled: Shares::ZERO
        })
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn chaos_72_events_preserve_cash_and_every_assets_shares() {
    fn replay(seed: u64) -> Vec<String> {
        let mut random = seed;
        let mut next = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        let mut e = ChaosExchange::new(100, cash("400")).unwrap();
        let assets = [AssetId::new("1").unwrap(), AssetId::new("2").unwrap()];
        let fee = FeeModel::new(decimal("0.05"), true).unwrap();
        for asset in &assets {
            e.register_asset(asset.clone(), fee, shares("1000"))
                .unwrap();
        }
        let owners = [
            e.allocate(
                cash("200"),
                [(assets[0].clone(), shares("100"))],
                MakerTier::Tier4,
            )
            .unwrap(),
            e.allocate(
                cash("200"),
                [(assets[1].clone(), shares("100"))],
                MakerTier::Tier4,
            )
            .unwrap(),
        ];
        let mut checkpoint = Vec::new();
        let mut executed = 0;
        for cycle in 0..9 {
            let lane = (next() % 2) as usize;
            let side = if next().is_multiple_of(2) {
                Side::Buy
            } else {
                Side::Sell
            };
            let fault = match cycle % 3 {
                0 => Delivery::Lost,
                1 => Delivery::Dropped,
                _ => Delivery::Delivered,
            };
            let asset = &assets[lane];
            let maker = OrderRequest::new(
                id(2 * cycle + 1),
                owners[lane],
                asset.clone(),
                side,
                price("0.50"),
                qty("3"),
                OrderMode::PostOnly,
            );
            let taker = OrderRequest::new(
                id(2 * cycle + 2),
                owners[lane],
                asset.clone(),
                side,
                if side == Side::Buy {
                    price("0.60")
                } else {
                    price("0.40")
                },
                qty("2"),
                OrderMode::ImmediateOrCancel,
            );
            let stamp = e.feed(asset).unwrap().stamp(2 * cycle + 1, cycle * 3);
            for action in 0..8 {
                let outcome = match action {
                    0 => {
                        e.advance_to(stamp.observed_ms()).unwrap();
                        format!(
                            "{:?}",
                            e.observe(
                                Depth::new(
                                    asset.clone(),
                                    stamp,
                                    price("0.01"),
                                    [(price("0.40"), qty("12"))],
                                    [(price("0.60"), qty("12"))]
                                )
                                .unwrap()
                            )
                        )
                    }
                    1 => format!(
                        "{:?}",
                        e.submit(maker.clone(), stamp, fault).map(|r| r.status())
                    ),
                    2 => {
                        let trade_stamp = e
                            .feed(asset)
                            .unwrap()
                            .stamp(stamp.sequence() + 1, stamp.observed_ms() + 1);
                        e.advance_to(trade_stamp.observed_ms()).unwrap();
                        let opposite = if side == Side::Buy {
                            Side::Sell
                        } else {
                            Side::Buy
                        };
                        format!(
                            "{:?}",
                            e.trade(Trade::new(
                                asset.clone(),
                                trade_stamp,
                                opposite,
                                price("0.50"),
                                qty("1")
                            ))
                        )
                    }
                    3 => format!("{:?}", e.reconcile(maker.account().order(maker.id()))),
                    4 => format!("{:?}", e.cancel(maker.account().order(maker.id()), fault)),
                    5 => format!("{:?}", e.reconcile(maker.account().order(maker.id()))),
                    6 => format!(
                        "{:?}",
                        e.submit(
                            taker.clone(),
                            e.current_stamp(asset).unwrap().unwrap(),
                            fault
                        )
                        .map(|r| r.status())
                    ),
                    _ if cycle.is_multiple_of(2) => {
                        let before = e.balance(owners[lane], asset).unwrap();
                        assert_eq!(
                            e.observe(
                                Depth::new(asset.clone(), stamp, price("0.01"), [], []).unwrap()
                            ),
                            Err(Error::StaleSignal)
                        );
                        assert_eq!(e.balance(owners[lane], asset).unwrap(), before);
                        "stale snapshot dropped".to_owned()
                    }
                    _ => {
                        let before = e.balance(owners[lane], asset).unwrap();
                        let expected = e.order_status(taker.account().order(taker.id()));
                        assert_eq!(
                            e.submit(taker.clone(), stamp, Delivery::Delivered)
                                .map(|r| r.status()),
                            expected
                        );
                        assert_eq!(e.balance(owners[lane], asset).unwrap(), before);
                        format!("replay: {expected:?}")
                    }
                };
                assert!(
                    e.is_conserved(),
                    "event {executed}, cycle {cycle}: {outcome}"
                );
                checkpoint.push(outcome);
                executed += 1;
            }
        }
        assert_eq!(executed, 72);
        assert!(checkpoint.iter().any(|s| s.contains("Unconfirmed")));
        assert!(
            checkpoint
                .iter()
                .any(|s| s.contains("filled: Shares(1000000)"))
        );
        checkpoint
    }
    for seed in [7, 73, 2026] {
        assert_eq!(replay(seed), replay(seed));
    }
}

fn decision(escalate: bool) -> Step {
    Step {
        escalate,
        submit_receipt: Delivery::Delivered,
        cancel_receipt: Delivery::Delivered,
    }
}

#[test]
fn retired_depth_cannot_reopen() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[("0.60", "7")]);
    let old = f.stamp;
    let delayed = f.exchange.feed(&f.asset).unwrap().stamp(99, 2);
    let depth = |stamp| {
        Depth::new(
            f.asset.clone(),
            stamp,
            price("0.01"),
            [],
            [(price("0.60"), qty("7"))],
        )
        .unwrap()
    };
    f.exchange.advance_to(2).unwrap();
    f.exchange.invalidate_depth(&f.asset).unwrap();
    assert_eq!(f.exchange.observe(depth(delayed)), Err(Error::StaleSignal));
    let order = f.order(1, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.exchange.submit(order.clone(), old, Delivery::Delivered),
        Err(Error::StaleSignal)
    );
    let new = f.exchange.feed(&f.asset).unwrap().stamp(1, 2);
    f.exchange.observe(depth(new)).unwrap();
    assert_eq!(
        f.exchange.submit(order.clone(), old, Delivery::Delivered),
        Err(Error::StaleSignal)
    );
    assert!(matches!(
        f.exchange
            .submit(order, new, Delivery::Delivered)
            .unwrap()
            .status(),
        OrderStatus::Closed {
            reason: CloseReason::Filled,
            ..
        }
    ));
}

#[test]
fn foreign_working_id_cannot_be_cancelled_by_step_or_directly() {
    let mut a = Fixture::new("10", "0", FeeModel::FREE);
    let mut b = Fixture::new("10", "0", FeeModel::FREE);
    a.observe(&[], &[]);
    b.observe(&[], &[]);
    let maker = a.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly);
    let plan = HybridCoordinator::hybrid(maker.clone(), id(2), price("0.60")).unwrap();
    plan.step(&mut a.exchange, a.stamp, decision(false))
        .unwrap();
    let a_receipt = a
        .exchange
        .submit(maker, a.stamp, Delivery::Delivered)
        .unwrap();
    let other = b.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly);
    let b_receipt = b
        .exchange
        .submit(other, b.stamp, Delivery::Delivered)
        .unwrap();
    assert_eq!(
        plan.step(&mut b.exchange, b.stamp, decision(true)),
        Err(Error::UnknownAccount)
    );
    assert_eq!(
        b.exchange.cancel(a_receipt.key(), Delivery::Delivered),
        Err(Error::UnknownAccount)
    );
    assert_eq!(
        b.exchange.reconcile(a_receipt.key()),
        Err(Error::UnknownAccount)
    );
    assert_eq!(
        b.exchange.order_status(a_receipt.key()),
        Err(Error::UnknownAccount)
    );
    assert_eq!(
        a.exchange.order_status(a_receipt.key()),
        Ok(a_receipt.status())
    );
    assert_eq!(
        b.exchange.order_status(b_receipt.key()),
        Ok(b_receipt.status())
    );
    assert_eq!(
        b.exchange.order_status(b.account.order(id(2))),
        Err(Error::UnknownOrder)
    );
    assert_eq!(
        b.exchange.balance(b.account, &b.asset).unwrap().cash(),
        cash("9.6")
    );
}

#[test]
fn stale_escalation_leaves_no_intent() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[("0.60", "10")]);
    let initial = f.stamp;
    let plan = HybridCoordinator::hybrid(
        f.order(1, Side::Buy, "0.40", "5", OrderMode::PostOnly),
        id(2),
        price("0.60"),
    )
    .unwrap();
    plan.step(&mut f.exchange, initial, decision(false))
        .unwrap();
    f.trade(Side::Sell, "0.40", "2");
    assert_eq!(
        plan.step(&mut f.exchange, initial, decision(true)),
        Err(Error::StaleSignal)
    );
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, decision(false)),
        Ok(Progress::Resting {
            order: f.account.order(id(1)),
            filled: shares("2")
        })
    );
    assert_eq!(
        f.exchange.order_status(f.account.order(id(2))),
        Err(Error::UnknownOrder)
    );
    let wallet = f.exchange.balance(f.account, &f.asset).unwrap();
    assert_eq!((wallet.cash(), wallet.shares()), (cash("8"), shares("2")));
}

#[test]
fn fallback_collision_does_not_cancel_maker() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[]);
    let plan = HybridCoordinator::hybrid(
        f.order(1, Side::Buy, "0.40", "5", OrderMode::PostOnly),
        id(2),
        price("0.60"),
    )
    .unwrap();
    let resting = plan
        .step(&mut f.exchange, f.stamp, decision(false))
        .unwrap();
    f.submit(
        f.order(2, Side::Buy, "0.60", "5", OrderMode::ImmediateOrCancel),
        Delivery::Delivered,
    )
    .unwrap();
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, decision(true)),
        Err(Error::ConflictingOrder)
    );
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, decision(false)),
        Ok(resting)
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("8")
    );
}

#[test]
fn public_quote_blocks_post_only_after_paper_sweep() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[("0.60", "1")]);
    f.submit(
        f.order(1, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel),
        Delivery::Delivered,
    )
    .unwrap();
    let denied = OrderStatus::Closed {
        filled: Shares::ZERO,
        reason: CloseReason::WouldTake,
    };
    assert_eq!(
        f.submit(
            f.order(2, Side::Buy, "0.60", "1", OrderMode::PostOnly),
            Delivery::Delivered
        ),
        Ok(denied)
    );
    f.observe(&[], &[("0.60", "1")]);
    assert_eq!(f.exchange.order_status(f.account.order(id(2))), Ok(denied));
    assert_eq!(
        f.submit(
            f.order(3, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel),
            Delivery::Delivered
        ),
        Ok(OrderStatus::Closed {
            filled: Shares::ZERO,
            reason: CloseReason::UnfilledIoc
        })
    );
    let balance = f.exchange.balance(f.account, &f.asset).unwrap();
    assert_eq!(
        (balance.cash(), balance.shares()),
        (cash("9.4"), shares("1"))
    );
}

#[test]
fn paired_trade_consumes_public_depth_ahead_of_maker() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[("0.40", "1")], &[]);
    let working = f
        .submit(
            f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly),
            Delivery::Delivered,
        )
        .unwrap();
    let stamp = f.advance();
    let depth = Depth::new(f.asset.clone(), stamp, price("0.01"), [], []).unwrap();
    let print = Trade::new(f.asset.clone(), stamp, Side::Sell, price("0.40"), qty("1"));
    let pair = ExecutionPair::try_new(print, depth).unwrap();
    f.exchange.observe_execution(pair).unwrap();
    assert_eq!(f.exchange.order_status(f.account.order(id(1))), Ok(working));
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("9.6")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn standalone_print_replenishes_from_remaining_public_quantity() {
    let mut f = Fixture::new("0", "2", FeeModel::FREE);
    f.observe(&[("0.40", "1")], &[]);
    f.trade(Side::Sell, "0.40", "1");
    f.observe(&[("0.40", "2")], &[]);
    assert_eq!(
        f.submit(
            f.order(1, Side::Sell, "0.40", "2", OrderMode::ImmediateOrCancel),
            Delivery::Delivered
        ),
        Ok(OrderStatus::Closed {
            filled: shares("2"),
            reason: CloseReason::Filled
        })
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn independent_depth_removal_before_trade() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[("0.40", "1")], &[]);
    f.submit(
        f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly),
        Delivery::Delivered,
    )
    .unwrap();
    f.observe(&[], &[]);
    f.trade(Side::Sell, "0.40", "1");
    assert_eq!(
        f.exchange.order_status(f.account.order(id(1))),
        Ok(OrderStatus::Closed {
            reason: CloseReason::Filled,
            filled: shares("1")
        })
    );
}

#[test]
fn tick_change_closes_only_off_grid_orders() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[]);
    for (n, limit) in [(1, "0.41"), (2, "0.40")] {
        f.submit(
            f.order(n, Side::Buy, limit, "1", OrderMode::PostOnly),
            Delivery::Delivered,
        )
        .unwrap();
    }
    let stamp = f.advance();
    f.exchange
        .observe(Depth::new(f.asset.clone(), stamp, price("0.10"), [], []).unwrap())
        .unwrap();
    assert!(matches!(
        f.exchange.order_status(f.account.order(id(1))),
        Ok(OrderStatus::Closed { .. })
    ));
    assert!(matches!(
        f.exchange.order_status(f.account.order(id(2))),
        Ok(OrderStatus::Working { .. })
    ));
    assert_eq!(
        f.submit(
            f.order(3, Side::Buy, "0.41", "1", OrderMode::PostOnly),
            Delivery::Delivered
        ),
        Err(Error::InvalidValue)
    );
}

#[test]
fn rejected_fallback_preserves_position_and_dropped_cancel_needs_a_new_decision() {
    let mut f = Fixture::new("1", "0", FeeModel::FREE);
    f.observe(&[], &[]);
    let maker = f.order(1, Side::Buy, "0.40", "2", OrderMode::PostOnly);
    let key = f.account.order(id(1));
    let unaffordable = HybridCoordinator::hybrid(maker.clone(), id(2), price("0.60")).unwrap();
    unaffordable
        .step(&mut f.exchange, f.stamp, decision(false))
        .unwrap();
    assert_eq!(
        unaffordable.step(&mut f.exchange, f.stamp, decision(true)),
        Err(Error::InsufficientFunds)
    );
    let plan = HybridCoordinator::hybrid(maker, id(2), price("0.50")).unwrap();
    let mut dropped = decision(true);
    dropped.cancel_receipt = Delivery::Dropped;
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, dropped),
        Ok(Progress::AwaitingReconciliation(key))
    );
    f.trade(Side::Sell, "0.40", "1");
    assert_eq!(f.exchange.order_status(key), Ok(OrderStatus::Unconfirmed));
    f.exchange.reconcile(key).unwrap();
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, decision(false)),
        Ok(Progress::Resting {
            order: key,
            filled: shares("1")
        })
    );
    assert_eq!(
        f.exchange.order_status(f.account.order(id(2))),
        Err(Error::UnknownOrder)
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn execution_pair_requires_matching_asset_and_stamp() {
    let asset = AssetId::new("123").unwrap();
    let mut exchange = ChaosExchange::new(100, Cash::ZERO).unwrap();
    exchange
        .register_asset(asset.clone(), FeeModel::FREE, Shares::ZERO)
        .unwrap();
    let feed = exchange.feed(&asset).unwrap();
    let stamp = feed.stamp(1, 0);
    for (depth_asset, depth_stamp) in [
        (asset.clone(), feed.stamp(2, 0)),
        (AssetId::new("456").unwrap(), stamp),
    ] {
        let depth = Depth::new(depth_asset, depth_stamp, price("0.01"), [], []).unwrap();
        let print = Trade::new(asset.clone(), stamp, Side::Sell, price("0.40"), qty("1"));
        assert_eq!(
            ExecutionPair::try_new(print, depth).err(),
            Some(Error::InvalidDepth)
        );
    }
}

#[test]
fn newer_depth_makes_a_delayed_execution_pair_harmless() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[("0.40", "1")], &[]);
    let key = f.account.order(id(1));
    let working = f
        .submit(
            f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly),
            Delivery::Delivered,
        )
        .unwrap();
    let stamp = f.advance();
    let print = Trade::new(f.asset.clone(), stamp, Side::Sell, price("0.40"), qty("2"));
    let depth = Depth::new(f.asset.clone(), stamp, price("0.01"), [], []).unwrap();
    let delayed = ExecutionPair::try_new(print, depth).unwrap();
    f.observe(&[("0.40", "1")], &[]);
    let balance = f.exchange.balance(f.account, &f.asset).unwrap();
    assert_eq!(
        f.exchange.observe_execution(delayed),
        Err(Error::StaleSignal)
    );
    assert_eq!(f.exchange.current_stamp(&f.asset), Ok(Some(f.stamp)));
    assert_eq!(f.exchange.balance(f.account, &f.asset).unwrap(), balance);
    assert_eq!(f.exchange.order_status(key), Ok(working));
    // The next print must still consume the public share ahead of the maker.
    f.trade(Side::Sell, "0.40", "1");
    assert_eq!(f.exchange.order_status(key), Ok(working));
    f.trade(Side::Sell, "0.40", "1");
    assert_eq!(
        f.exchange.order_status(key),
        Ok(OrderStatus::Closed {
            reason: CloseReason::Filled,
            filled: shares("1")
        })
    );
}

#[test]
fn pair_replenishes_public_depth() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[("0.40", "1")], &[]);
    let st = f.advance();
    let print = Trade::new(f.asset.clone(), st, Side::Sell, price("0.40"), qty("1"));
    let depth = Depth::new(
        f.asset.clone(),
        st,
        price("0.01"),
        [(price("0.40"), qty("1"))],
        [],
    )
    .unwrap();
    f.exchange
        .observe_execution(ExecutionPair::try_new(print, depth).unwrap())
        .unwrap();
    f.submit(
        f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly),
        Delivery::Delivered,
    )
    .unwrap();
    f.trade(Side::Sell, "0.40", "1");
    assert!(
        matches!(f.exchange.order_status(f.account.order(id(1))),Ok(OrderStatus::Working{filled,..}) if filled==Shares::ZERO)
    );
}
#[test]
fn hybrid_rebuild_awaits_original_ioc() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[("0.60", "1")]);
    let req = f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly);
    let build = || HybridCoordinator::hybrid(req.clone(), id(2), price("0.60")).unwrap();
    let mut lost = decision(true);
    lost.submit_receipt = Delivery::Lost;
    assert!(matches!(
        build().step(&mut f.exchange, f.stamp, lost),
        Ok(Progress::AwaitingReconciliation(_))
    ));
    assert!(matches!(
        build().step(&mut f.exchange, f.stamp, decision(false)),
        Ok(Progress::AwaitingReconciliation(_))
    ));
    assert_eq!(
        f.submit(req.clone(), Delivery::Delivered),
        Err(Error::ConflictingOrder)
    );
    assert_eq!(
        f.exchange.order_status(f.account.order(id(1))),
        Err(Error::UnknownOrder)
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("9.4")
    );
    f.trade(Side::Sell, "0.40", "1");
    f.exchange.reconcile(f.account.order(id(2))).unwrap();
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().shares(),
        shares("1")
    );
    assert_eq!(
        build().step(&mut f.exchange, f.stamp, decision(false)),
        Ok(Progress::Complete {
            filled: shares("1")
        })
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn paired_print_retires_public_lot_behind_first_maker() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[]);
    f.submit(
        f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly),
        Delivery::Delivered,
    )
    .unwrap();
    f.observe(&[("0.40", "1")], &[]);
    f.submit(
        f.order(2, Side::Buy, "0.40", "1", OrderMode::PostOnly),
        Delivery::Delivered,
    )
    .unwrap();
    let stamp = f.advance();
    let print = Trade::new(f.asset.clone(), stamp, Side::Sell, price("0.40"), qty("1"));
    let depth = Depth::new(
        f.asset.clone(),
        stamp,
        price("0.01"),
        [(price("0.40"), qty("1"))],
        [],
    )
    .unwrap();
    f.exchange
        .observe_execution(ExecutionPair::try_new(print, depth).unwrap())
        .unwrap();
    f.trade(Side::Sell, "0.40", "1");
    for n in [1, 2] {
        assert_eq!(
            f.exchange.order_status(f.account.order(id(n))),
            Ok(OrderStatus::Closed {
                filled: shares("1"),
                reason: CloseReason::Filled
            })
        );
    }
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().shares(),
        shares("2")
    );
}

#[test]
fn hybrid_capacity_counts_orders_and_pending_fallback_once() {
    for direct in [true, false] {
        let mut f = Fixture::new("10", "0", FeeModel::FREE);
        f.observe(&[], &[("0.60", "1")]);
        let end = MAX_ORDERS as u64;
        for n in 1..(if direct { end } else { end - 1 }) {
            f.submit(
                f.order(n, Side::Buy, "0.30", "1", OrderMode::ImmediateOrCancel),
                Delivery::Delivered,
            )
            .unwrap();
        }
        let maker = f.order(end + 1, Side::Buy, "0.40", "1", OrderMode::PostOnly);
        let plan = || HybridCoordinator::hybrid(maker.clone(), id(end + 2), price("0.60")).unwrap();
        if !direct {
            plan()
                .step(&mut f.exchange, f.stamp, decision(false))
                .unwrap();
            let mut escalate = decision(true);
            escalate.cancel_receipt = Delivery::Lost;
            assert_eq!(
                plan().step(&mut f.exchange, f.stamp, escalate),
                Ok(Progress::AwaitingReconciliation(
                    f.account.order(maker.id())
                ))
            );
            assert_eq!(
                f.submit(
                    f.order(
                        end + 2,
                        Side::Buy,
                        "0.60",
                        "1",
                        OrderMode::ImmediateOrCancel
                    ),
                    Delivery::Delivered
                ),
                Err(Error::ConflictingOrder)
            );
            assert_eq!(
                f.submit(
                    f.order(
                        end + 3,
                        Side::Buy,
                        "0.30",
                        "1",
                        OrderMode::ImmediateOrCancel
                    ),
                    Delivery::Delivered
                ),
                Err(Error::Capacity)
            );
            f.exchange.reconcile(f.account.order(maker.id())).unwrap();
        }
        assert_eq!(
            plan().step(&mut f.exchange, f.stamp, decision(direct)),
            Ok(Progress::Complete {
                filled: shares("1")
            })
        );
        if direct {
            assert_eq!(
                f.exchange.order_status(f.account.order(maker.id())),
                Err(Error::UnknownOrder)
            );
        }
        assert_eq!(
            f.submit(
                f.order(
                    end + 3,
                    Side::Buy,
                    "0.30",
                    "1",
                    OrderMode::ImmediateOrCancel
                ),
                Delivery::Delivered
            ),
            Err(Error::Capacity)
        );
        assert_eq!(
            f.submit(
                f.order(1, Side::Buy, "0.30", "1", OrderMode::ImmediateOrCancel),
                Delivery::Delivered
            ),
            Ok(OrderStatus::Closed {
                reason: CloseReason::UnfilledIoc,
                filled: Shares::ZERO
            })
        );
    }
}

#[test]
fn completed_maker_strategy_cannot_start_again() {
    for (receipt, via_strategy) in [
        (Delivery::Delivered, true),
        (Delivery::Lost, true),
        (Delivery::Delivered, false),
    ] {
        let mut f = Fixture::new("10", "0", FeeModel::FREE);
        f.observe(&[], &[("0.60", "1")]);
        let maker = f.order(1, Side::Buy, "0.60", "1", OrderMode::PostOnly);
        let build = || HybridCoordinator::hybrid(maker.clone(), id(2), price("0.60")).unwrap();
        if via_strategy {
            let mut step = decision(false);
            step.submit_receipt = receipt;
            let result = build().step(&mut f.exchange, f.stamp, step).unwrap();
            if receipt == Delivery::Lost {
                assert_eq!(
                    result,
                    Progress::AwaitingReconciliation(f.account.order(id(1)))
                );
                f.exchange.reconcile(f.account.order(id(1))).unwrap();
            } else {
                assert_eq!(
                    result,
                    Progress::Complete {
                        filled: Shares::ZERO
                    }
                );
            }
        } else {
            f.submit(maker.clone(), receipt).unwrap();
        }
        assert_eq!(
            build().step(&mut f.exchange, f.stamp, decision(true)),
            Ok(Progress::Complete {
                filled: Shares::ZERO
            })
        );
        assert_eq!(
            f.exchange.order_status(f.account.order(id(2))),
            Err(Error::UnknownOrder)
        );
    }
}

#[test]
fn reconciliation_releases_a_dropped_cancels_slot_before_strategy_resumes() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[], &[("0.60", "1")]);
    let maker = f.order(1, Side::Buy, "0.40", "2", OrderMode::PostOnly);
    let key = maker.account().order(maker.id());
    let plan = HybridCoordinator::hybrid(maker, id(2), price("0.60")).unwrap();
    plan.step(&mut f.exchange, f.stamp, decision(false))
        .unwrap();
    let mut dropped = decision(true);
    dropped.cancel_receipt = Delivery::Dropped;
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, dropped),
        Ok(Progress::AwaitingReconciliation(key))
    );
    let other = f.order(2, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.submit(other.clone(), Delivery::Delivered),
        Err(Error::ConflictingOrder)
    );
    f.trade(Side::Sell, "0.40", "1");
    assert!(
        matches!(f.exchange.reconcile(key),Ok(OrderStatus::Working{filled,..}) if filled==shares("1"))
    );
    assert!(matches!(
        f.submit(other, Delivery::Delivered),
        Ok(OrderStatus::Closed {
            reason: CloseReason::Filled,
            ..
        })
    ));
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, decision(true)),
        Err(Error::ConflictingOrder)
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn cancel_handoff_funds_belong_to_the_pending_ioc() {
    for (limit, tick) in [("0.60", "0.01"), ("0.61", "0.10")] {
        let mut f = Fixture::new(limit, "0", FeeModel::FREE);
        f.observe(&[], &[("0.60", "1")]);
        let maker = f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly);
        let plan = HybridCoordinator::hybrid(maker, id(2), price(limit)).unwrap();
        plan.step(&mut f.exchange, f.stamp, decision(false))
            .unwrap();
        let mut cancel = decision(true);
        cancel.cancel_receipt = Delivery::Lost;
        plan.step(&mut f.exchange, f.stamp, cancel).unwrap();
        let key = f.account.order(id(1));
        for _ in 0..2 {
            f.exchange.reconcile(key).unwrap();
            assert!(f.exchange.is_conserved());
        }
        assert_eq!(
            f.submit(
                f.order(3, Side::Buy, "0.10", "1", OrderMode::PostOnly),
                Delivery::Delivered
            ),
            Err(Error::InsufficientFunds)
        );
        let stamp = f.advance();
        f.exchange
            .observe(
                Depth::new(
                    f.asset.clone(),
                    stamp,
                    price(tick),
                    [],
                    [(price("0.60"), qty("1"))],
                )
                .unwrap(),
            )
            .unwrap();
        let off_grid = limit == "0.61";
        assert_eq!(
            plan.step(&mut f.exchange, stamp, decision(false)),
            Ok(Progress::Complete {
                filled: if off_grid { Shares::ZERO } else { shares("1") }
            })
        );
        if off_grid {
            assert_eq!(
                f.submit(
                    f.order(2, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel),
                    Delivery::Delivered
                ),
                Ok(OrderStatus::Closed {
                    reason: CloseReason::Filled,
                    filled: shares("1")
                })
            );
            assert_eq!(
                plan.step(&mut f.exchange, stamp, decision(true)),
                Ok(Progress::Complete {
                    filled: Shares::ZERO
                })
            );
        }
        assert_eq!(
            f.exchange.balance(f.account, &f.asset).unwrap().cash(),
            if off_grid { cash("0.01") } else { Cash::ZERO }
        );
        assert!(f.exchange.is_conserved());
    }
}

#[test]
fn failed_attachment_returns_a_reconcilable_exchange() {
    use polymarket_monitor::book::NegRiskEventGroup;
    for mode in [OrderMode::PostOnly, OrderMode::ImmediateOrCancel] {
        let mut f = Fixture::new("10", "10", FeeModel::FREE);
        f.observe(&[], &[("0.60", "1")]);
        let limit = if mode == OrderMode::PostOnly {
            "0.40"
        } else {
            "0.60"
        };
        let key = f.account.order(id(1));
        assert_eq!(
            f.submit(f.order(1, Side::Buy, limit, "1", mode), Delivery::Lost),
            Ok(OrderStatus::Unconfirmed)
        );
        let mut group = NegRiskEventGroup::new("fixture");
        let error = group.attach_paper(f.exchange).unwrap_err();
        assert_eq!(error.reason, Error::AlreadyStarted);
        let mut exchange = error.into_exchange();
        let (status, cash_left, position) = if mode == OrderMode::PostOnly {
            (
                OrderStatus::Working {
                    filled: Shares::ZERO,
                    remaining: qty("1"),
                },
                "9.60",
                "10",
            )
        } else {
            (
                OrderStatus::Closed {
                    filled: shares("1"),
                    reason: CloseReason::Filled,
                },
                "9.40",
                "11",
            )
        };
        for _ in 0..2 {
            assert_eq!(exchange.reconcile(key), Ok(status));
            let balance = exchange.balance(f.account, &f.asset).unwrap();
            assert_eq!(
                (balance.cash(), balance.shares()),
                (cash(cash_left), shares(position))
            );
        }
        assert!(exchange.is_conserved());
    }
}

#[test]
fn sell_handoff_can_be_abandoned_after_confirmation_without_a_book() {
    let mut f = Fixture::new("0", "1", FeeModel::FREE);
    f.observe(&[("0.40", "1")], &[]);
    let maker = f.order(1, Side::Sell, "0.60", "1", OrderMode::PostOnly);
    let key = f.account.order(id(1));
    let plan = HybridCoordinator::hybrid(maker, id(2), price("0.40")).unwrap();
    plan.step(&mut f.exchange, f.stamp, decision(false))
        .unwrap();
    let mut lost = decision(true);
    lost.cancel_receipt = Delivery::Lost;
    plan.step(&mut f.exchange, f.stamp, lost).unwrap();
    assert_eq!(f.exchange.abandon_fallback(key), Err(Error::Unconfirmed));
    f.exchange.reconcile(key).unwrap();
    assert!(f.exchange.is_conserved());
    let sell = f.order(3, Side::Sell, "0.40", "1", OrderMode::ImmediateOrCancel);
    assert_eq!(
        f.submit(sell, Delivery::Delivered),
        Err(Error::InsufficientFunds)
    );
    f.exchange.invalidate_depth(&f.asset).unwrap();
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, decision(true)),
        Err(Error::StaleSignal)
    );
    assert_eq!(
        f.exchange.abandon_fallback(key),
        Ok(OrderStatus::Closed {
            filled: Shares::ZERO,
            reason: CloseReason::Cancelled
        })
    );
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().shares(),
        shares("1")
    );
    assert_eq!(
        plan.step(&mut f.exchange, f.stamp, decision(true)),
        Ok(Progress::Complete {
            filled: Shares::ZERO
        })
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn real_print_shares_one_budget_across_prices_and_queue_positions() {
    for (public, volume, filled_id, maker_price) in [
        (vec![("0.40", "1")], "1", 2, "0.41"),
        (vec![("0.42", "1"), ("0.40", "2")], "2", 1, "0.42"),
    ] {
        let multi = filled_id == 1;
        let mut f = Fixture::new("10", "2", FeeModel::FREE);
        f.observe(&public, &[]);
        if multi {
            f.submit(
                f.order(1, Side::Buy, "0.42", "1", OrderMode::PostOnly),
                Delivery::Delivered,
            )
            .unwrap();
        }
        let working = f
            .submit(
                f.order(2, Side::Buy, "0.41", "1", OrderMode::PostOnly),
                Delivery::Delivered,
            )
            .unwrap();
        f.trade(Side::Sell, "0.40", volume);
        assert_eq!(
            f.exchange.order_status(f.account.order(id(filled_id))),
            Ok(OrderStatus::Closed {
                filled: shares("1"),
                reason: CloseReason::Filled
            })
        );
        let balance = f.exchange.balance(f.account, &f.asset).unwrap();
        assert_eq!(balance.shares(), shares("3"));
        if multi {
            assert_eq!(f.exchange.order_status(f.account.order(id(2))), Ok(working));
            assert_eq!(
                f.submit(
                    f.order(3, Side::Sell, "0.40", "1", OrderMode::ImmediateOrCancel),
                    Delivery::Delivered
                ),
                Ok(OrderStatus::Closed {
                    filled: Shares::ZERO,
                    reason: CloseReason::SelfTradePrevented
                })
            );
        } else {
            assert_eq!(
                balance.cash().decimal(),
                decimal("10") - decimal(maker_price)
            );
        }
        assert!(f.exchange.is_conserved());
    }
}

#[test]
fn paired_buy_print_matches_lowest_ask_at_resting_price_in_fifo_order() {
    let mut f = Fixture::new("10", "2", FeeModel::FREE);
    f.observe(&[], &[("0.51", "1"), ("0.60", "3")]);
    for n in [9, 1] {
        f.submit(
            f.order(n, Side::Sell, "0.51", "1", OrderMode::PostOnly),
            Delivery::Delivered,
        )
        .unwrap();
    }
    let stamp = f.advance();
    let print = Trade::new(f.asset.clone(), stamp, Side::Buy, price("0.60"), qty("2"));
    let depth = Depth::new(
        f.asset.clone(),
        stamp,
        price("0.01"),
        [],
        [(price("0.51"), qty("1")), (price("0.60"), qty("1"))],
    )
    .unwrap();
    f.exchange
        .observe_execution(ExecutionPair::try_new(print, depth).unwrap())
        .unwrap();
    assert_eq!(
        f.exchange.order_status(f.account.order(id(9))),
        Ok(OrderStatus::Closed {
            filled: shares("1"),
            reason: CloseReason::Filled
        })
    );
    assert!(matches!(
        f.exchange.order_status(f.account.order(id(1))),
        Ok(OrderStatus::Working {
            filled: Shares::ZERO,
            ..
        })
    ));
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().cash(),
        cash("10.51")
    );
    assert!(f.exchange.is_conserved());
}

#[test]
fn expired_pretrade_depth_accepts_replenishment_without_backfilling_a_maker() {
    let mut f = Fixture::new("10", "2", FeeModel::FREE);
    f.observe(&[("0.40", "1")], &[]);
    let old = f.stamp;
    let maker = f
        .submit(
            f.order(1, Side::Buy, "0.40", "1", OrderMode::PostOnly),
            Delivery::Delivered,
        )
        .unwrap();
    let stamp = f
        .exchange
        .feed(&f.asset)
        .unwrap()
        .stamp(2, old.observed_ms() + 100);
    f.exchange.advance_to(stamp.observed_ms()).unwrap();
    let trade = || Trade::new(f.asset.clone(), stamp, Side::Sell, price("0.40"), qty("1"));
    let pair = || {
        ExecutionPair::try_new(
            trade(),
            Depth::new(
                f.asset.clone(),
                stamp,
                price("0.01"),
                [(price("0.40"), qty("1"))],
                [(price("0.60"), qty("1"))],
            )
            .unwrap(),
        )
        .unwrap()
    };
    assert_eq!(f.exchange.trade(trade()), Err(Error::StaleSignal));
    assert_eq!(f.exchange.observe_execution(pair()), Ok(()));
    assert_eq!(
        f.exchange.observe_execution(pair()),
        Err(Error::StaleSignal)
    );
    assert_eq!(f.exchange.current_stamp(&f.asset), Ok(Some(stamp)));
    assert_eq!(f.exchange.order_status(f.account.order(id(1))), Ok(maker));
    f.stamp = stamp;
    assert_eq!(
        f.submit(
            f.order(2, Side::Buy, "0.60", "1", OrderMode::ImmediateOrCancel),
            Delivery::Delivered
        ),
        Ok(OrderStatus::Closed {
            filled: shares("1"),
            reason: CloseReason::Filled
        })
    );
    f.trade(Side::Sell, "0.40", "1");
    assert_eq!(
        f.exchange.order_status(f.account.order(id(1))),
        Ok(OrderStatus::Closed {
            filled: shares("1"),
            reason: CloseReason::Filled
        })
    );
    let delayed = pair_for(
        &f,
        f.exchange
            .feed(&f.asset)
            .unwrap()
            .stamp(99, f.stamp.observed_ms()),
    );
    f.exchange.invalidate_depth(&f.asset).unwrap();
    assert_eq!(
        f.exchange.observe_execution(delayed),
        Err(Error::StaleSignal)
    );
    let first = f
        .exchange
        .feed(&f.asset)
        .unwrap()
        .stamp(1, f.stamp.observed_ms());
    f.exchange.observe_execution(pair_for(&f, first)).unwrap();
    assert_eq!(f.exchange.current_stamp(&f.asset), Ok(Some(first)));
    assert!(f.exchange.is_conserved());

    fn pair_for(f: &Fixture, stamp: Stamp) -> ExecutionPair {
        ExecutionPair::try_new(
            Trade::new(f.asset.clone(), stamp, Side::Sell, price("0.40"), qty("1")),
            Depth::new(f.asset.clone(), stamp, price("0.01"), [], []).unwrap(),
        )
        .unwrap()
    }
}
#[test]
fn expired_replay_depth_does_not_match_print_but_accepts_its_paired_refresh() {
    let mut f = Fixture::new("10", "0", FeeModel::FREE);
    f.observe(&[(".40", "1")], &[]);
    let maker = f.order(1, Side::Buy, ".40", "1", OrderMode::PostOnly);
    f.submit(maker.clone(), Delivery::Delivered).unwrap();
    f.exchange.advance_to(101).unwrap();
    let signal = f.exchange.feed(&f.asset).unwrap().stamp(2, 101);
    let trade = || Trade::new(f.asset.clone(), signal, Side::Sell, price(".40"), qty("1"));
    assert_eq!(f.exchange.trade(trade()), Err(Error::StaleSignal));
    let depth = Depth::new(
        f.asset.clone(),
        signal,
        price(".01"),
        [(price(".40"), qty("1"))],
        [(price(".60"), qty("1"))],
    )
    .unwrap();
    f.exchange
        .observe_execution(ExecutionPair::try_new(trade(), depth).unwrap())
        .unwrap();
    assert_eq!(
        f.exchange
            .order_status(f.account.order(maker.id()))
            .unwrap(),
        OrderStatus::Working {
            filled: Shares::ZERO,
            remaining: qty("1")
        }
    );
    f.stamp = signal;
    let ioc = f.order(2, Side::Buy, ".60", "1", OrderMode::ImmediateOrCancel);
    let filled = OrderStatus::Closed {
        reason: CloseReason::Filled,
        filled: shares("1"),
    };
    assert_eq!(f.submit(ioc.clone(), Delivery::Delivered), Ok(filled));
    f.exchange.advance_to(201).unwrap();
    assert_eq!(f.submit(ioc, Delivery::Delivered), Ok(filled));
    assert_eq!(
        f.exchange.balance(f.account, &f.asset).unwrap().shares(),
        shares("1")
    );
    assert!(f.exchange.is_conserved());
}
