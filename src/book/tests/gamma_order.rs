use super::*;
use crate::paper::{
    Shares,
    order_fsm::{CloseReason, Delivery, OrderStatus},
};
use crate::test_http::{
    CLOSED, GAMMA_FIXTURE, http_request, http_response, http_response_with_status,
};
use std::time::Duration;
use tokio::net::TcpListener;

fn failed(group: &NegRiskEventGroup) -> bool {
    group.render().contains("Gamma 更新失敗")
}

#[tokio::test]
async fn non_success_closure_body_keeps_ready_book_and_paper_stamp() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let (mut group, _, asset) = funded_group();
    let stamp = group
        .paper()
        .unwrap()
        .current_stamp(&asset)
        .unwrap()
        .unwrap();
    for status in ["302 Found", "304 Not Modified"] {
        let (outcome, ()) = tokio::join!(
            group.refresh_discovery(&client, &base, Duration::from_secs(30)),
            async {
                let (socket, _) = http_request(&listener).await;
                http_response_with_status(socket, status, CLOSED).await;
            }
        );
        assert!(outcome.unwrap_err().to_string().contains(&status[..3]));
        assert!(group.ready() && failed(&group));
        assert_eq!(
            group.paper().unwrap().current_stamp(&asset).unwrap(),
            Some(stamp)
        );
    }
}

#[tokio::test]
async fn followed_redirect_keeps_raw_gamma_fields_and_renders_each_on_one_line() {
    use tokio::io::AsyncWriteExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut group = NegRiskEventGroup::new("fixture");
    let (outcome, ()) = tokio::join!(
        group.refresh_discovery(&client, &base, Duration::from_secs(30)),
        async {
            let (mut socket, _) = http_request(&listener).await;
            socket.write_all(b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            let (socket, path) = http_request(&listener).await;
            assert!(path.starts_with("GET /final "));
            let body = GAMMA_FIXTURE
                .replace("Fixture", r"第一段\n第二段")
                .replace("First", r"市場\n問題");
            http_response(socket, &body).await;
        }
    );
    assert!(outcome.unwrap());
    assert_eq!(group.title, "第一段\n第二段");
    assert_eq!(group.markets()[0].name, "市場\n問題");
    let source = group.open_input().unwrap();
    sync(&mut group, source);
    let view = group.render();
    assert_eq!(view.lines().count(), 10);
    assert!(view.starts_with(r"第一段\n第二段 | Ready"));
    assert!(view.lines().any(|line| line == r"市場\n問題"));
}

#[tokio::test]
async fn augmented_event_syncs_named_quotes_without_enabling_a_basket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut group = NegRiskEventGroup::new("fixture");
    let mut event: serde_json::Value = serde_json::from_str(GAMMA_FIXTURE).unwrap();
    event["negRiskAugmented"] = true.into();
    let markets = event["markets"].as_array_mut().unwrap();
    for market in markets.iter_mut() {
        market["active"] = true.into();
        market["negRiskOther"] = false.into();
    }
    let mut extra = markets[0].clone();
    extra["question"] = "Candidate A".into();
    extra["conditionId"] = "c".into();
    extra["clobTokenIds"] = serde_json::json!(["5", "6"]);
    extra["active"] = false.into();
    markets.push(extra.clone());
    extra["question"] = "Other".into();
    extra["conditionId"] = "d".into();
    extra["clobTokenIds"] = serde_json::json!(["7", "8"]);
    extra["active"] = true.into();
    extra["negRiskOther"] = true.into();
    markets.push(extra);

    let (outcome, ()) = tokio::join!(
        group.refresh_discovery(&client, &base, Duration::from_secs(30)),
        async {
            let (socket, _) = http_request(&listener).await;
            http_response(socket, &event.to_string()).await;
        }
    );
    assert!(outcome.unwrap());
    assert_eq!(
        group.subscription().unwrap().into_parts().0,
        ["1", "2", "3", "4"]
    );
    let source = group.open_input().unwrap();
    for (asset, market) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
        assert!(!group.quotes_ready());
        assert!(
            group
                .admit_raw(source, snapshot(asset, market, 100))
                .unwrap()
        );
        assert!(group.advance_input(source).unwrap());
    }
    assert!(group.quotes_ready());
    assert!(!group.ready());
    assert!(group.buy_depth(25.into()).is_none());
    assert!(group.command_frontier().is_none());
    let view = group.render();
    assert!(view.contains("QuotesReady (2/2"));
    assert!(view.contains("YES 中間價總和 —"));
    assert!(view.contains("bid 0.4"));
    assert!(!view.contains("Candidate A") && !view.contains("Other"));
}

fn sync(group: &mut NegRiskEventGroup, source: SubscriptionId) {
    for (asset, market) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
        assert!(
            group
                .admit_raw(source, snapshot(asset, market, 100))
                .unwrap()
        );
        assert!(group.advance_input(source).unwrap());
    }
    assert!(group.ready());
}

#[tokio::test]
async fn stalled_gamma_leaves_ingress_live_and_commits_only_the_resumed_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let ttl = Duration::from_secs(30);
    let mut group = open_group();
    let source = group.open_input().unwrap();
    let socket = tokio::select! {
        socket = http_request(&listener) => socket.0,
        done = group.refresh_discovery(&client, &base, ttl) => panic!("early response: {done:?}"),
    };
    sync(&mut group, source);
    let (mut group, account, asset) = fund(group);
    let stamp = group
        .paper()
        .unwrap()
        .current_stamp(&asset)
        .unwrap()
        .unwrap();
    let order = |id| buy_one_with_id(account, asset.clone(), id);
    let receipt = group
        .paper()
        .unwrap()
        .submit(order(1), stamp, Delivery::Lost)
        .unwrap();
    assert_eq!(receipt.status(), OrderStatus::Unconfirmed);
    assert!(
        group
            .admit_event(source, Event::MarketResolved { market: "b".into() })
            .unwrap()
    );
    {
        use crate::paper::hybrid_coordinator::{HybridCoordinator, Step};
        let mut desk = group.paper().unwrap();
        assert_eq!(desk.current_stamp(&asset), Ok(None));
        assert_eq!(
            desk.submit(order(2), stamp, Delivery::Delivered),
            Err(PaperError::StaleSignal)
        );
        let taker = HybridCoordinator::taker(order(3)).unwrap();
        let step = Step {
            escalate: true,
            submit_receipt: Delivery::Delivered,
            cancel_receipt: Delivery::Delivered,
        };
        assert_eq!(desk.step(&taker, stamp, step), Err(PaperError::StaleSignal));
        assert_eq!(
            desk.submit(order(1), stamp, Delivery::Delivered)
                .unwrap()
                .key(),
            receipt.key()
        );
        assert!(matches!(
            desk.reconcile(receipt.key()).unwrap(),
            OrderStatus::Closed { .. }
        ));
    }
    assert!(group.advance_input(source).unwrap());
    assert_eq!(group.assets(), ["1", "2"]);
    // If this started another request, the invalid endpoint would fail.
    let (done, ()) = tokio::join!(
        group.refresh_discovery(&client, "invalid", Duration::ZERO),
        http_response(socket, CLOSED)
    );
    assert!(done.unwrap() && group.event_closed());
    assert!(group.open_input().is_none());
    assert!(
        !group
            .refresh_discovery(&client, "invalid", ttl)
            .await
            .unwrap()
    );
    let mut desk = group.paper().unwrap();
    assert_eq!(desk.current_stamp(&asset), Ok(None));
    assert_eq!(
        desk.submit(order(2), stamp, Delivery::Delivered),
        Err(PaperError::StaleSignal)
    );
    let filled = OrderStatus::Closed {
        filled: Shares::new(Decimal::ONE).unwrap(),
        reason: CloseReason::Filled,
    };
    for _ in 0..2 {
        assert_eq!(desk.reconcile(receipt.key()), Ok(filled));
        assert_eq!(
            desk.submit(order(1), stamp, Delivery::Delivered)
                .unwrap()
                .key(),
            receipt.key()
        );
        let balance = desk.balance(account, &asset).unwrap();
        assert_eq!(balance.cash().decimal(), Decimal::new(94, 1));
        assert_eq!(balance.shares().decimal(), Decimal::ONE);
    }
}

#[tokio::test]
async fn cancelled_closure_cannot_replace_a_later_approval() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut group = ready_group();
    let ttl = Duration::from_secs(30);
    let socket = tokio::select! {
        socket = http_request(&listener) => socket.0,
        done = group.refresh_discovery(&client, &base, ttl) => panic!("early response: {done:?}"),
    };
    http_response(socket, CLOSED).await;
    group.cancel_gamma_refresh();
    assert!(group.ready());
    let deadline = group.subscription().unwrap().into_parts().1;
    for ttl in [Duration::ZERO, ttl] {
        let (done, ()) = tokio::join!(group.refresh_discovery(&client, &base, ttl), async {
            let (socket, _) = http_request(&listener).await;
            http_response(socket, &GAMMA_FIXTURE.replace("Fixture", "Refreshed")).await;
        });
        assert!(!done.unwrap() && group.ready());
        assert_eq!(group.title == "Refreshed", !ttl.is_zero());
        if ttl.is_zero() {
            assert_eq!(group.subscription().unwrap().into_parts().1, deadline);
        }
    }
}
