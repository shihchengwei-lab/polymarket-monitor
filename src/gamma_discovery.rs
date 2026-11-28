use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use reqwest::{Client, Url};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use tokio::time::Instant;

use crate::wire::{gamma_decimal, success_body, validate_tick};

pub const GAMMA: &str = "https://gamma-api.polymarket.com";
const GAMMA_BODY_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventRecord {
    slug: String,
    title: String,
    #[serde(default)]
    neg_risk: Option<bool>,
    neg_risk_augmented: Option<bool>,
    markets: Vec<MarketRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketRecord {
    question: String,
    condition_id: String,
    #[serde(deserialize_with = "string_array")]
    outcomes: Vec<String>,
    #[serde(deserialize_with = "string_array")]
    clob_token_ids: Vec<String>,
    #[serde(deserialize_with = "gamma_decimal")]
    order_price_min_tick_size: Decimal,
    closed: bool,
    active: Option<bool>,
    neg_risk_other: Option<bool>,
}

fn string_array<'de, D: Deserializer<'de>>(input: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Encoded(String),
        Array(Vec<String>),
    }
    match Value::deserialize(input)? {
        Value::Encoded(json) => serde_json::from_str(&json).map_err(serde::de::Error::custom),
        Value::Array(items) => Ok(items),
    }
}

#[derive(Debug)]
pub struct MarketSpec {
    name: String,
    condition: String,
    // YES, NO, in this order.
    tokens: [String; 2],
    tick: Decimal,
    closed: bool,
}

impl MarketSpec {
    pub fn identity(&self) -> (&str, &[String; 2], bool) {
        (&self.condition, &self.tokens, self.closed)
    }

    pub fn into_parts(self) -> (String, String, [String; 2], Decimal, bool) {
        (
            self.condition,
            self.name,
            self.tokens,
            self.tick,
            self.closed,
        )
    }
}

/// Whether discovery returned a complete event roster for basket monitoring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BasketEligibility {
    /// The full roster is available. A live group must still synchronize before trading.
    FullRoster,
    /// Only active named outcomes are included and a complete basket cannot be computed.
    QuotesOnly,
}

#[derive(Debug)]
pub struct GroupSpec {
    parts: GroupParts,
}

/// Owned discovery data, including the basket eligibility of its market list.
#[derive(Debug)]
pub struct GroupParts {
    pub title: String,
    pub markets: Vec<MarketSpec>,
    pub valid_until: Instant,
    pub basket_eligibility: BasketEligibility,
}

impl GroupSpec {
    pub fn basket_eligibility(&self) -> BasketEligibility {
        self.parts.basket_eligibility
    }

    pub fn markets(&self) -> &[MarketSpec] {
        &self.parts.markets
    }

    pub fn valid_until(&self) -> Instant {
        self.parts.valid_until
    }

    pub fn into_parts(self) -> GroupParts {
        self.parts
    }
}

#[derive(Debug)]
pub enum Discovery {
    Open(GroupSpec),
    Unavailable(anyhow::Error),
    Closed,
}

fn expiration(ttl: Duration) -> Result<Instant> {
    Instant::now()
        .checked_add(ttl)
        .context("Gamma TTL exceeds clock range")
}
impl EventRecord {
    fn into_group(self, valid_until: Instant) -> Result<GroupSpec> {
        ensure!(
            self.neg_risk == Some(true),
            "{} is not a NegRisk event",
            self.slug
        );
        ensure!(
            self.markets.len() >= 2,
            "{} has fewer than two markets",
            self.slug
        );
        let mut conditions = HashSet::new();
        let mut tokens = HashSet::new();
        let mut markets = Vec::new();
        let quotes_only = self.neg_risk_augmented == Some(true);
        for market in self.markets {
            if quotes_only && (market.active != Some(true) || market.neg_risk_other != Some(false))
            {
                continue;
            }
            ensure!(
                !market.condition_id.is_empty() && conditions.insert(market.condition_id.clone()),
                "empty or duplicate condition ID in {}",
                self.slug
            );
            ensure!(
                market.outcomes.len() == 2 && market.clob_token_ids.len() == 2,
                "{} does not have exactly two outcomes and token IDs",
                market.question
            );
            let mut pair = [String::new(), String::new()];
            for (outcome, token) in market.outcomes.iter().zip(market.clob_token_ids) {
                let index = match outcome.to_ascii_lowercase().as_str() {
                    "yes" => 0,
                    "no" => 1,
                    _ => anyhow::bail!("unexpected outcome {outcome} in {}", market.question),
                };
                ensure!(
                    pair[index].is_empty()
                        && !token.is_empty()
                        && token.bytes().all(|b| b.is_ascii_digit())
                        && tokens.insert(token.clone()),
                    "invalid or duplicate outcome/token in {}",
                    market.question
                );
                pair[index] = token;
            }
            validate_tick(market.order_price_min_tick_size)?;
            markets.push(MarketSpec {
                name: market.question,
                condition: market.condition_id,
                tokens: pair,
                tick: market.order_price_min_tick_size,
                closed: market.closed,
            });
        }
        ensure!(
            !markets.is_empty(),
            "{} has no named active markets",
            self.slug
        );
        markets.sort_unstable_by(|a, b| a.condition.cmp(&b.condition));
        Ok(GroupSpec {
            parts: GroupParts {
                title: self.title,
                markets,
                valid_until,
                basket_eligibility: if quotes_only {
                    BasketEligibility::QuotesOnly
                } else {
                    BasketEligibility::FullRoster
                },
            },
        })
    }
}

fn classify(body: &[u8], slug: &str, ttl: Duration) -> Result<Discovery> {
    let event: serde_json::Value =
        serde_json::from_slice(body).with_context(|| format!("decode Gamma event {slug}"))?;
    ensure!(
        event["slug"].as_str() == Some(slug),
        "Gamma returned an unexpected event slug"
    );
    match event["closed"].as_bool() {
        Some(true) => Ok(Discovery::Closed),
        Some(false) => {
            let valid_until = expiration(ttl)?;
            let group = serde_json::from_slice::<EventRecord>(body)
                .context("invalid Gamma market fields")
                .and_then(|event| event.into_group(valid_until));
            Ok(match group {
                Ok(group) => Discovery::Open(group),
                Err(err) => Discovery::Unavailable(err),
            })
        }
        None => Ok(Discovery::Unavailable(anyhow::anyhow!(
            "missing or invalid Gamma event closed flag"
        ))),
    }
}

pub async fn discover(client: &Client, base: &str, slug: &str, ttl: Duration) -> Result<Discovery> {
    expiration(ttl)?;
    let mut url = Url::parse(base)?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid Gamma base URL"))?
        .pop_if_empty()
        .extend(["events", "slug", slug]);
    let response = client.get(url).send().await?;
    let body = success_body(response, GAMMA_BODY_LIMIT, "Gamma event body").await?;
    classify(&body, slug, ttl)
}

#[cfg(test)]
pub(crate) fn fixture_spec(markets: &[(&str, &str, &str, Decimal, bool)]) -> GroupSpec {
    EventRecord {
        slug: "fixture".to_owned(),
        title: "Fixture".to_owned(),
        neg_risk: Some(true),
        neg_risk_augmented: None,
        markets: markets
            .iter()
            .map(|&(condition, yes, no, tick, closed)| MarketRecord {
                question: condition.to_owned(),
                condition_id: condition.to_owned(),
                outcomes: vec!["Yes".to_owned(), "No".to_owned()],
                clob_token_ids: vec![yes.to_owned(), no.to_owned()],
                order_price_min_tick_size: tick,
                closed,
                active: None,
                neg_risk_other: None,
            })
            .collect(),
    }
    .into_group(expiration(Duration::from_secs(30)).unwrap())
    .unwrap()
}

#[derive(Deserialize)]
pub struct Listing {
    pub slug: String,
    pub title: String,
    #[serde(rename = "negRisk")]
    neg_risk: Option<bool>,
    #[serde(rename = "volume24hr", default)]
    volume: Option<serde_json::Value>,
}

pub async fn list(client: &Client, limit: u32, offset: u32) -> Result<Vec<Listing>> {
    list_from(client, GAMMA, limit, offset).await
}

async fn list_from(
    client: &Client,
    base: &str,
    mut limit: u32,
    mut offset: u32,
) -> Result<Vec<Listing>> {
    let mut events = Vec::new();
    while limit > 0 {
        // Gamma requires gzip for pages larger than 50.
        let page_size = limit.min(50);
        let page: Vec<Listing> = list_page(client, base, page_size, offset, false).await?;
        let count = page.len();
        events.extend(
            page.into_iter()
                .filter(|event| event.neg_risk == Some(true)),
        );
        limit -= page_size;
        if count < page_size as usize || limit == 0 {
            break;
        }
        offset = offset
            .checked_add(page_size)
            .context("Gamma list offset overflow")?;
    }
    Ok(events)
}

#[derive(Debug)]
struct PageOverLimit {
    size: u32,
    offset: u32,
    returned: usize,
}

impl std::fmt::Display for PageOverLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Gamma returned too many events at offset {}: requested {}, received {}",
            self.offset, self.size, self.returned
        )
    }
}

impl std::error::Error for PageOverLimit {}

async fn list_page(
    client: &Client,
    base: &str,
    size: u32,
    offset: u32,
    ranked: bool,
) -> Result<Vec<Listing>> {
    let mut request = client.get(format!("{base}/events")).query(&[
        ("active", "true"),
        ("closed", "false"),
        ("limit", &size.to_string()),
        ("offset", &offset.to_string()),
    ]);
    if ranked {
        request = request.query(&[("order", "volume24hr"), ("ascending", "false")]);
    }
    let body = success_body(
        request.send().await?,
        GAMMA_BODY_LIMIT,
        "Gamma list page body",
    )
    .await?;
    let page: Vec<Listing> = serde_json::from_slice(&body)?;
    if page.len() > size as usize {
        return Err(PageOverLimit {
            size,
            offset,
            returned: page.len(),
        }
        .into());
    }
    Ok(page)
}

pub async fn popular(client: &Client, base: &str) -> Result<Vec<(Listing, Decimal)>> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    // Gamma returns full market rosters even for listings. Keep each body bounded.
    let mut offset = 0;
    let mut size = 3;
    while offset < 120 {
        let page: Vec<Listing> = match list_page(client, base, size, offset, true).await {
            Ok(page) => page,
            Err(e) if size > 1 && !e.is::<PageOverLimit>() => {
                size = 1;
                continue;
            }
            Err(e) => return Err(e),
        };
        let last = page.len() < size as usize;
        offset += size;
        size = 3;
        for mut e in page {
            if e.neg_risk != Some(true) || !seen.insert(e.slug.clone()) {
                continue;
            }
            let Some(raw) = e.volume.take() else { continue };
            let text = raw
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| raw.to_string());
            let n = Decimal::from_str_exact(&text)?;
            result.push((e, n));
        }
        if result.len() >= 10 || last {
            break;
        }
    }
    result.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    result.truncate(10);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_http::{http_request, http_response, http_response_with_status};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn unrepresentable_ttl_fails_before_http_admission() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let client = Client::new();
        let result = discover(&client, &base, "fixture", Duration::MAX);
        tokio::select! {
            result=result=>assert!(result.unwrap_err().to_string().contains("TTL")),
            _=listener.accept()=>panic!("invalid TTL sent a request"),
        }
    }

    #[tokio::test]
    async fn list_rejects_redirect_even_after_a_valid_page() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let server = async {
            let entry = r#"{"slug":"fixture","title":"Fixture","negRisk":true}"#;
            for (status, count) in [("200 OK", 50), ("302 Found", 1)] {
                let (socket, _) = http_request(&listener).await;
                http_response_with_status(
                    socket,
                    status,
                    &format!("[{}]", vec![entry; count].join(",")),
                )
                .await;
            }
        };
        let (_, result) = tokio::join!(server, list_from(&client, &base, 51, 0));
        assert!(result.err().unwrap().to_string().contains("302"));
    }

    #[tokio::test]
    async fn list_rejects_a_chunked_page_larger_than_gamma_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let client = Client::builder().no_proxy().build().unwrap();
        let server = async {
            let (mut stream, _) = http_request(&listener).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let data = vec![b' '; 64 * 1024];
            for _ in 0..GAMMA_BODY_LIMIT / data.len() {
                stream.write_all(b"10000\r\n").await.unwrap();
                stream.write_all(&data).await.unwrap();
                stream.write_all(b"\r\n").await.unwrap();
            }
            let _ = stream.write_all(b"1\r\n \r\n0\r\n\r\n").await;
        };
        let (_, result) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(server, list_from(&client, &url, 1, 0))
        })
        .await
        .unwrap();
        assert!(format!("{:#}", result.err().unwrap()).contains("HTTP body exceeds 4194304 bytes"));
    }

    #[tokio::test]
    async fn list_rejects_excess_rows_before_filtering_even_after_a_valid_page() {
        let client = Client::builder().no_proxy().build().unwrap();
        for (limit, page_counts, second_is_neg_risk) in [
            (1, vec![2], true),
            (1, vec![2], false),
            (51, vec![50, 2], false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let serve = async {
                for (index, count) in page_counts.into_iter().enumerate() {
                    let (socket, request) = http_request(&listener).await;
                    let size = if limit > 50 && index == 0 { 50 } else { 1 };
                    let offset = 7 + 50 * index;
                    assert!(request.contains(&format!("&limit={size}&offset={offset} ")));
                    let page: Vec<_> = (0..count)
                        .map(|i| {
                            serde_json::json!({"slug":format!("event-{i}"),
                            "title":"Fixture","negRisk":i == 0 || second_is_neg_risk})
                        })
                        .collect();
                    http_response(socket, &serde_json::to_string(&page).unwrap()).await;
                }
            };
            let (result, ()) = tokio::join!(list_from(&client, &base, limit, 7), serve);
            let error = result
                .err()
                .expect("an oversized page must discard the entire result");
            let offset = if limit == 51 { 57 } else { 7 };
            assert!(
                error
                    .to_string()
                    .contains(&format!("offset {offset}: requested 1, received 2"))
            );
        }
    }

    #[tokio::test]
    async fn popular_rejects_oversized_pages_without_the_http_failure_retry() {
        let client = Client::builder().no_proxy().build().unwrap();
        for oversized in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let serve = async {
                let (socket, request) = http_request(&listener).await;
                assert!(request.contains("&limit=3&offset=0&order=volume24hr"));
                if oversized {
                    let event = r#"{"slug":"fixture","title":"Fixture","negRisk":true}"#;
                    http_response(socket, &format!("[{}]", [event; 4].join(","))).await;
                } else {
                    http_response_with_status(socket, "503 Service Unavailable", "{}").await;
                    let (socket, request) = http_request(&listener).await;
                    assert!(request.contains("&limit=1&offset=0&order=volume24hr"));
                    http_response(socket, "[]").await;
                }
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(popular(&client, &base), serve)
            })
            .await
            .expect("ranked pagination must finish without an extra request");
            if oversized {
                assert!(result.err().unwrap().is::<PageOverLimit>());
            } else {
                assert!(result.unwrap().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn list_preserves_offsets_page_sizes_and_neg_risk_filter() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let client = Client::builder().no_proxy().build().unwrap();
        let server = async {
            for (count, offset) in [(50, 7), (2, 57)] {
                let (mut stream, request) = http_request(&listener).await;
                assert_eq!(
                    request,
                    format!(
                        "GET /events?active=true&closed=false&limit={count}&offset={offset} HTTP/1.1"
                    )
                );
                let events: Vec<_> = (0..count).map(|index| serde_json::json!({
                    "slug":format!("event-{offset}-{index}"), "title":"Fixture", "negRisk":index==0
                })).collect();
                let body = serde_json::to_string(&events).unwrap();
                stream.write_all(format!("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        };
        let (_, result) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(server, list_from(&client, &url, 52, 7))
        })
        .await
        .unwrap();
        assert_eq!(
            result
                .unwrap()
                .into_iter()
                .map(|event| event.slug)
                .collect::<Vec<_>>(),
            ["event-7-0", "event-57-0"]
        );
    }

    #[test]
    fn discovery_maps_outcomes_and_preserves_numeric_tick_and_closed_member() {
        let event: EventRecord = serde_json::from_str(r#"{
            "slug":"event","title":"Event","negRisk":true,"markets":[
              {"question":"A","conditionId":"a","outcomes":"[\"No\",\"Yes\"]",
               "clobTokenIds":"[\"222\",\"111\"]","orderPriceMinTickSize":0.0000000000000000000000000001,"closed":false},
              {"question":"B","conditionId":"b","outcomes":["Yes","No"],
               "clobTokenIds":["333","444"],"orderPriceMinTickSize":"0.01","closed":true}
            ]
        }"#).unwrap();
        let group = event
            .into_group(expiration(Duration::from_secs(30)).unwrap())
            .unwrap();
        assert_eq!(group.markets()[0].tokens, ["111", "222"]);
        assert_eq!(group.markets()[0].tick, Decimal::new(1, 28));
        assert!(group.markets()[1].closed);
    }

    #[test]
    fn classifies_terminal_event_before_members_and_rejects_missing_closure() {
        let ttl = Duration::from_secs(30);
        assert!(matches!(
            classify(br#"{"slug":"a","closed":true,"markets":[{}]}"#, "a", ttl).unwrap(),
            Discovery::Closed
        ));
        assert!(matches!(
            classify(
                br#"{"slug":"a","closed":false,"title":"A","negRisk":true,"markets":[]}"#,
                "a",
                ttl
            )
            .unwrap(),
            Discovery::Unavailable(_)
        ));
        assert!(matches!(
            classify(br#"{"slug":"a"}"#, "a", ttl).unwrap(),
            Discovery::Unavailable(_)
        ));
        assert!(classify(br#"{"slug":"b","closed":true}"#, "a", ttl).is_err());
    }

    #[test]
    fn approval_requires_at_least_two_markets_and_unique_tokens() {
        let market = |condition: &str, yes: &str, no: &str| {
            serde_json::json!({
                "question": condition,
                "conditionId": condition,
                "outcomes": ["Yes", "No"],
                "clobTokenIds": [yes, no],
                "orderPriceMinTickSize": "0.01",
                "closed": false
            })
        };
        for (markets, reason) in [
            (vec![market("a", "1", "2")], "fewer than two markets"),
            (
                vec![market("a", "1", "2"), market("b", "2", "4")],
                "invalid or duplicate outcome/token",
            ),
        ] {
            let event = serde_json::json!({
                "slug": "fixture", "title": "Fixture", "closed": false,
                "negRisk": true, "markets": markets
            });
            let Discovery::Unavailable(error) = classify(
                event.to_string().as_bytes(),
                "fixture",
                Duration::from_secs(30),
            )
            .unwrap() else {
                panic!("invalid Gamma members received an approval");
            };
            assert!(error.to_string().contains(reason));
        }
    }
}
