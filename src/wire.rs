use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

pub(crate) async fn success_body(
    response: reqwest::Response,
    limit: usize,
    operation: &'static str,
) -> Result<Vec<u8>> {
    let status = response.status();
    if !status.is_success() {
        let message = format!("{operation}: unexpected HTTP status {}", status.as_u16());
        return Err(match response.error_for_status() {
            Err(source) => anyhow::Error::new(source).context(message),
            Ok(_) => anyhow::anyhow!(message),
        });
    }
    bounded_body(response, limit).await.context(operation)
}

pub async fn bounded_body(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    ensure!(
        response
            .content_length()
            .is_none_or(|length| length <= limit as u64),
        "HTTP body exceeds {limit} bytes"
    );
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            chunk.len() <= limit - body.len(),
            "HTTP body exceeds {limit} bytes"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod body_tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[tokio::test]
    async fn body_limit_checks_declared_and_accumulated_chunk_lengths() {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        for (response, accepted) in [
            ("Content-Length: 6\r\n\r\n123456", false),
            (
                "Transfer-Encoding: chunked\r\n\r\n3\r\n123\r\n3\r\n456\r\n0\r\n\r\n",
                false,
            ),
            (
                "Transfer-Encoding: chunked\r\n\r\n3\r\n123\r\n2\r\n45\r\n0\r\n\r\n",
                true,
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                socket
                    .write_all(
                        format!("HTTP/1.1 200 OK\r\nConnection: close\r\n{response}").as_bytes(),
                    )
                    .await
                    .unwrap();
            };
            let download = async {
                let response = client.get(url).send().await.unwrap();
                let body = bounded_body(response, 5).await;
                if accepted {
                    assert_eq!(body.unwrap(), b"12345");
                } else {
                    assert!(body.unwrap_err().to_string().contains("exceeds 5 bytes"));
                }
            };
            tokio::join!(server, download);
        }
    }
}

pub fn gamma_decimal<'de, D: Deserializer<'de>>(input: D) -> Result<Decimal, D::Error> {
    let raw = Box::<RawValue>::deserialize(input)?;
    let text = if raw.get().starts_with('"') {
        serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?
    } else {
        raw.get().to_owned()
    };
    Decimal::from_str_exact(&text).map_err(serde::de::Error::custom)
}

fn decimal<'de, D: Deserializer<'de>>(input: D) -> Result<Decimal, D::Error> {
    Decimal::from_str_exact(&String::deserialize(input)?).map_err(serde::de::Error::custom)
}

fn timestamp<'de, D: Deserializer<'de>>(input: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Timestamp {
        Text(String),
        Integer(u64),
    }
    match Timestamp::deserialize(input)? {
        Timestamp::Text(s) => s.parse().map_err(serde::de::Error::custom),
        Timestamp::Integer(n) => Ok(n),
    }
}

pub fn validate_tick(tick: Decimal) -> Result<()> {
    ensure!(
        tick > Decimal::ZERO && tick < Decimal::ONE,
        "invalid tick size {tick}"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Level {
    #[serde(deserialize_with = "decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "decimal")]
    pub size: Decimal,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

// Advisory prices must never make a valid, uncrossed delta undecodable.
fn advisory_price<'de, D: Deserializer<'de>>(input: D) -> Result<Option<Decimal>, D::Error> {
    let value = serde_json::Value::deserialize(input)?;
    Ok(value.as_str().and_then(|s| Decimal::from_str_exact(s).ok()))
}
#[derive(Debug, Deserialize)]
pub struct Change {
    #[serde(default, deserialize_with = "advisory_price")]
    pub best_bid: Option<Decimal>,
    #[serde(default, deserialize_with = "advisory_price")]
    pub best_ask: Option<Decimal>,
    pub asset_id: String,
    pub side: Side,
    #[serde(flatten)]
    pub level: Level,
}

#[derive(Debug, Deserialize)]
pub struct Snapshot {
    pub asset_id: String,
    pub market: String,
    #[serde(deserialize_with = "timestamp")]
    pub timestamp: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug)]
pub struct RestBook {
    pub snapshot: Snapshot,
    pub tick_size: Decimal,
}

impl<'de> Deserialize<'de> for RestBook {
    fn deserialize<D: Deserializer<'de>>(input: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct RestFields {
            asset_id: String,
            market: String,
            #[serde(deserialize_with = "timestamp")]
            timestamp: u64,
            bids: Box<RawValue>,
            asks: Box<RawValue>,
            #[serde(deserialize_with = "decimal")]
            tick_size: Decimal,
        }
        let fields = RestFields::deserialize(input)?;
        validate_raw_rows([fields.bids.as_ref(), fields.asks.as_ref()])
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            snapshot: Snapshot {
                asset_id: fields.asset_id,
                market: fields.market,
                timestamp: fields.timestamp,
                bids: serde_json::from_str(fields.bids.get()).map_err(serde::de::Error::custom)?,
                asks: serde_json::from_str(fields.asks.get()).map_err(serde::de::Error::custom)?,
            },
            tick_size: fields.tick_size,
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PublicTrade {
    pub asset_id: String,
    #[serde(deserialize_with = "decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "decimal")]
    pub size: Decimal,
    pub side: Side,
    #[serde(deserialize_with = "timestamp")]
    pub timestamp: u64,
    #[serde(default)]
    pub transaction_hash: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum Event {
    LastTradePrice(PublicTrade),
    Book(Snapshot),
    PriceChange {
        market: String,
        #[serde(deserialize_with = "timestamp")]
        timestamp: u64,
        price_changes: Vec<Change>,
    },
    TickSizeChange {
        market: String,
        asset_id: String,
        #[serde(deserialize_with = "timestamp")]
        timestamp: u64,
        #[serde(deserialize_with = "decimal")]
        new_tick_size: Decimal,
    },
    MarketResolved {
        market: String,
    },
    #[serde(other)]
    Other,
}

pub(crate) const MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_EVENTS: usize = 4096;
pub(crate) const MAX_ROWS: usize = 16384;

impl Snapshot {
    pub(crate) fn validate_size(&self) -> Result<()> {
        validate_rows(self.bids.len().saturating_add(self.asks.len()))
    }
}
fn validate_rows(rows: usize) -> Result<()> {
    ensure!(rows <= MAX_ROWS, "market event exceeds {MAX_ROWS} rows");
    Ok(())
}
fn validate_raw_rows<'a>(arrays: impl IntoIterator<Item = &'a RawValue>) -> Result<()> {
    let mut count = 0;
    for values in arrays {
        visit_array(values.get(), |_| {
            count += 1;
            validate_rows(count)
        })?;
    }
    Ok(())
}

impl Event {
    // Charge retained allocations, including spare capacity from typed callers.
    pub(crate) fn retained_bytes(&self) -> usize {
        let bytes = match self {
            Self::Book(book) => book
                .asset_id
                .capacity()
                .saturating_add(book.market.capacity())
                .saturating_add(
                    book.bids
                        .capacity()
                        .saturating_add(book.asks.capacity())
                        .saturating_mul(size_of::<Level>()),
                ),
            Self::PriceChange {
                market,
                price_changes,
                ..
            } => price_changes.iter().fold(
                market
                    .capacity()
                    .saturating_add(price_changes.capacity().saturating_mul(size_of::<Change>())),
                |sum, change| sum.saturating_add(change.asset_id.capacity()),
            ),
            Self::TickSizeChange {
                asset_id, market, ..
            } => asset_id.capacity().saturating_add(market.capacity()),
            Self::MarketResolved { market } => market.capacity(),
            Self::LastTradePrice(t) => t
                .asset_id
                .capacity()
                .saturating_add(t.transaction_hash.capacity()),
            Self::Other => 0,
        };
        bytes.saturating_add(size_of::<Self>())
    }

    pub(crate) fn validate_size(&self) -> Result<()> {
        let rows = match self {
            Event::Book(book) => return book.validate_size(),
            Event::PriceChange { price_changes, .. } => price_changes.len(),
            _ => 0,
        };
        validate_rows(rows)
    }
}

// Visit one raw item at a time, including when draining a retired connection.
fn visit_array<'de>(
    json: &'de str,
    mut visit: impl FnMut(&'de RawValue) -> Result<()>,
) -> Result<()> {
    struct Items<F>(F);
    impl<'de, F: FnMut(&'de RawValue) -> Result<()>> serde::de::Visitor<'de> for Items<F> {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an array")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            mut self,
            mut seq: A,
        ) -> std::result::Result<(), A::Error> {
            while let Some(item) = seq.next_element()? {
                (self.0)(item).map_err(serde::de::Error::custom)?;
            }
            Ok(())
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(json);
    (&mut decoder).deserialize_seq(Items(&mut visit))?;
    decoder.end()?;
    Ok(())
}

pub fn events(frame: &str) -> Result<Vec<Event>> {
    ensure!(
        frame.len() <= MESSAGE_BYTES,
        "market frame exceeds {MESSAGE_BYTES} bytes"
    );
    let mut events = Vec::new();
    let mut decode = |item: &RawValue| -> Result<()> {
        ensure!(
            events.len() < MAX_EVENTS,
            "market frame exceeds {MAX_EVENTS} events"
        );
        // Count raw rows before the tagged enum decoder allocates their contents.
        #[derive(Deserialize)]
        struct Rows<'a> {
            #[serde(borrow)]
            event_type: std::borrow::Cow<'a, str>,
            #[serde(borrow)]
            bids: Option<&'a RawValue>,
            #[serde(borrow)]
            asks: Option<&'a RawValue>,
            #[serde(borrow)]
            price_changes: Option<&'a RawValue>,
        }
        let rows: Rows = serde_json::from_str(item.get())?;
        let arrays = match rows.event_type.as_ref() {
            "book" => [rows.bids, rows.asks],
            "price_change" => [rows.price_changes, None],
            _ => [None, None],
        };
        validate_raw_rows(arrays.into_iter().flatten())?;
        events.push(serde_json::from_str(item.get())?);
        Ok(())
    };
    if frame.trim_start().starts_with('[') {
        visit_array(frame, decode)?;
    } else {
        decode(serde_json::from_str(frame)?)?;
    }
    Ok(events)
}

// A retiring subscription no longer has usable quotes. Recognize settlement
// independently of the payload of unrelated events in the same frame.
pub fn for_each_resolution(frame: &str, mut resolve: impl FnMut(&str)) {
    if frame.len() > MESSAGE_BYTES {
        return;
    }
    #[derive(Deserialize)]
    struct Lifecycle<'a> {
        #[serde(borrow)]
        event_type: std::borrow::Cow<'a, str>,
        #[serde(borrow)]
        market: Option<std::borrow::Cow<'a, str>>,
    }
    let mut handle = |item: &str| {
        if let Ok(Lifecycle {
            event_type,
            market: Some(market),
        }) = serde_json::from_str(item)
            && event_type == "market_resolved"
        {
            resolve(&market);
        }
    };
    if frame.trim_start().starts_with('[') {
        let _ = visit_array(frame, |item| {
            handle(item.get());
            Ok(())
        });
    } else {
        handle(frame);
    }
}

#[cfg(test)]
mod event_limits {
    use super::*;

    #[test]
    fn rest_decode_checks_the_combined_budget_before_decoding_levels() {
        let valid = r#"{"price":"0.40","size":"5"}"#;
        let bids = vec![valid; MAX_ROWS - 1].join(",");
        let encode = |asks: &str| {
            format!(
                r#"{{"market":"a","asset_id":"1","timestamp":"1","tick_size":"0.01","bids":[{bids}],"asks":[{asks}]}}"#
            )
        };
        let book: RestBook = serde_json::from_reader(encode(valid).as_bytes()).unwrap();
        assert_eq!(
            book.snapshot.bids.len() + book.snapshot.asks.len(),
            MAX_ROWS
        );
        let err = serde_json::from_str::<RestBook>(&encode("null,null")).unwrap_err();
        assert!(err.to_string().contains("rows"));
    }

    #[test]
    fn reject_excess_bytes_events_and_rows_before_matching() {
        assert!(events(&format!("{}[]", " ".repeat(MESSAGE_BYTES))).is_err());
        let batch = vec![r#"{"event_type":"other"}"#; MAX_EVENTS + 1].join(",");
        assert!(events(&format!("[{batch}]")).is_err());
        let rows = vec!["null"; MAX_ROWS + 1].join(",");
        let error = events(&format!(
            r#"{{"event_type":"price_change","price_changes":[{rows}]}}"#
        ))
        .unwrap_err();
        assert!(error.to_string().contains("rows"));
    }
}
