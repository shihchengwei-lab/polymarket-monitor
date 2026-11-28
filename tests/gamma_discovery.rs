use std::time::Duration;

use hyper::{Body, Request, Response, server::conn::Http, service::service_fn};
use polymarket_monitor::gamma_discovery::{BasketEligibility, Discovery, GroupParts, discover};
use reqwest::Client;
use serde_json::json;
use tokio::net::TcpListener;

#[tokio::test]
async fn discovery_preserves_basket_eligibility_when_consumed_by_a_library_caller() {
    let client = Client::builder().no_proxy().build().unwrap();
    for (augmented, expected) in [
        (false, BasketEligibility::FullRoster),
        (true, BasketEligibility::QuotesOnly),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let serve = async {
            let (socket, _) = listener.accept().await.unwrap();
            let markets = [("a", ["1", "2"]), ("b", ["3", "4"])].map(|(id, tokens)| {
                json!({"conditionId":id,"question":id,"outcomes":["Yes","No"],
                    "clobTokenIds":tokens,"orderPriceMinTickSize":"0.01","closed":false,
                    "active":true,"negRiskOther":false})
            });
            let event = json!({"slug":"fixture","title":"Fixture","closed":false,
                "negRisk":true,"negRiskAugmented":augmented,"markets":markets});
            Http::new()
                .http1_keep_alive(false)
                .serve_connection(
                    socket,
                    service_fn(|req: Request<Body>| {
                        assert_eq!(req.uri().path(), "/events/slug/fixture");
                        std::future::ready(Ok::<_, std::convert::Infallible>(Response::new(
                            Body::from(event.to_string()),
                        )))
                    }),
                )
                .await
                .unwrap();
        };
        let (discovery, ()) = tokio::join!(
            discover(&client, &base, "fixture", Duration::from_secs(30)),
            serve
        );
        let Discovery::Open(spec) = discovery.unwrap() else {
            panic!("expected an open roster");
        };
        assert_eq!(spec.basket_eligibility(), expected);
        let deadline = spec.valid_until();
        let GroupParts {
            title,
            markets,
            valid_until,
            basket_eligibility,
        } = spec.into_parts();
        assert_eq!(basket_eligibility, expected);
        assert_eq!(title, "Fixture");
        assert_eq!(markets.len(), 2);
        assert_eq!(valid_until, deadline);
    }
}
