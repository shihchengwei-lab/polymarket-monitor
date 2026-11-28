mod desk;
mod session;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    io::{self, Write},
    sync::Arc,
    time::Duration,
};

use crate::book::view::BookView;
#[cfg(test)]
use crate::output::Output;
use anyhow::{Context, Result};
use hyper::{Body, Method, Request, Response, StatusCode, server::conn::Http, service::service_fn};
use serde_json::json;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    time::timeout,
};

#[derive(Clone, Default)]
struct Screen {
    session: u64,
    slug: Arc<str>,
    sequence: u64,
    book: Option<Arc<BookView>>,
    market_ids: Arc<BTreeMap<String, u64>>,
    next_market_id: u64,
    diagnostic: Arc<str>,
    stopped: bool,
    desk: Arc<serde_json::Value>,
}

impl Screen {
    fn publish_book(&mut self, book: Arc<BookView>) {
        if !book.markets.is_empty()
            && (self.market_ids.len() != book.markets.len()
                || book
                    .markets
                    .iter()
                    .any(|market| !self.market_ids.contains_key(&market.identity)))
        {
            let mut next = BTreeMap::new();
            for market in &book.markets {
                let id = self
                    .market_ids
                    .get(&market.identity)
                    .copied()
                    .unwrap_or_else(|| {
                        self.next_market_id += 1;
                        self.next_market_id
                    });
                next.insert(market.identity.clone(), id);
            }
            self.market_ids = Arc::new(next);
        }
        self.book = Some(book);
        self.sequence += 1;
    }

    fn snapshot(&self) -> serde_json::Value {
        let book = self.book.as_ref().map(|book| {
            let mut data = json!(&**book);
            for (market, view) in data["markets"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .zip(&book.markets)
            {
                market["id"] = json!(self.market_ids[&view.identity]);
            }
            data
        });
        let mut desk = (*self.desk).clone();
        if let Some(rates) = desk
            .get_mut("rates")
            .and_then(serde_json::Value::as_object_mut)
        {
            *rates = std::mem::take(rates)
                .into_iter()
                .filter_map(|(identity, rate)| {
                    Some((self.market_ids.get(&identity)?.to_string(), rate))
                })
                .collect();
        }
        if let Some(slots) = desk
            .get_mut("slots")
            .and_then(serde_json::Value::as_array_mut)
        {
            for slot in slots {
                let slot = slot.as_object_mut().unwrap();
                if let Some(id) = slot
                    .remove("identity")
                    .and_then(|identity| self.market_ids.get(identity.as_str()?).copied())
                {
                    slot.insert("id".into(), json!(id));
                }
            }
        }
        json!({"session":self.session,"slug":&*self.slug,"sequence":self.sequence,
            "book":book,"diagnostic":&*self.diagnostic,"stopped":self.stopped,"desk":desk})
    }
}

struct DiagnosticSink(watch::Sender<Screen>);
impl Write for DiagnosticSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let text: Arc<str> = String::from_utf8_lossy(bytes).trim_end().into();
        self.0.send_modify(|screen| {
            screen.diagnostic = text;
            screen.sequence += 1;
        });
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn response(req: Request<Body>, state: &watch::Receiver<Screen>) -> Response<Body> {
    let (status, mime, body) = match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => (
            StatusCode::OK,
            "text/html; charset=utf-8",
            Body::from(include_str!("index.html")),
        ),
        (&Method::GET, "/app.js") => (
            StatusCode::OK,
            "text/javascript; charset=utf-8",
            Body::from(include_str!("app.js")),
        ),
        (&Method::GET, "/visuals.js") => (
            StatusCode::OK,
            "text/javascript; charset=utf-8",
            Body::from(include_str!("visuals.js")),
        ),
        (&Method::GET, "/style.css") => (
            StatusCode::OK,
            "text/css; charset=utf-8",
            Body::from(include_str!("style.css")),
        ),
        (&Method::GET, "/snapshot") => {
            let screen = state.borrow().clone();
            (
                StatusCode::OK,
                "application/json",
                Body::from(screen.snapshot().to_string()),
            )
        }
        _ => (StatusCode::NOT_FOUND, "text/plain", Body::empty()),
    };
    Response::builder()
        .status(status)
        .header("Content-Type", mime)
        .header("Cache-Control", "no-store")
        .header("X-Content-Type-Options", "nosniff")
        .header(
            "Content-Security-Policy",
            "default-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'none'",
        )
        .body(body)
        .unwrap()
}

async fn command(
    req: Request<Body>,
    route: session::Route,
    switches: &tokio::sync::mpsc::Sender<session::Switch>,
) -> Response<Body> {
    let reply = |status, text: String| {
        Response::builder()
            .status(status)
            .header("Cache-Control", "no-store")
            .body(Body::from(text))
            .unwrap()
    };
    if req.headers().get("origin").and_then(|v| v.to_str().ok()) != Some("http://127.0.0.1:8080")
        || req.headers().get("x-paper-control").is_none()
    {
        return reply(StatusCode::FORBIDDEN, "僅接受本機儀表板指令".into());
    }
    if req
        .headers()
        .get("x-cockpit-session")
        .and_then(|s| s.to_str().ok())
        .and_then(|s| s.parse().ok())
        != Some(route.id)
    {
        return reply(StatusCode::CONFLICT, "場次已變更，請重新操作".into());
    }
    if let Some(slug) = req.uri().path().strip_prefix("/event/") {
        if slug.is_empty()
            || slug.len() > 256
            || !slug
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return reply(StatusCode::BAD_REQUEST, "請輸入事件 slug".into());
        }
        let (sender, response) = tokio::sync::oneshot::channel();
        if switches
            .try_send(session::Switch {
                expected: route.id,
                slug: slug.into(),
                result: sender,
            })
            .is_err()
        {
            return reply(StatusCode::CONFLICT, "另一個場次正在切換".into());
        }
        return match response.await {
            Ok(Ok(id)) => reply(StatusCode::OK, id.to_string()),
            Ok(Err(error)) => reply(StatusCode::CONFLICT, error),
            Err(_) => reply(StatusCode::SERVICE_UNAVAILABLE, "場次服務已結束".into()),
        };
    }
    let action = match req.uri().path() {
        "/desk/fire" => desk::Command::Fire,
        "/desk/auto/on" => desk::Command::Auto(true),
        "/desk/auto/off" => desk::Command::Auto(false),
        path => match path
            .strip_prefix("/desk/lag/")
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n <= crate::paper::MAX_PAPER_LATENCY_MS)
        {
            Some(n) => desk::Command::Lag(n),
            None => return reply(StatusCode::BAD_REQUEST, "無效的指令".into()),
        },
    };
    let (sender, recv) = tokio::sync::oneshot::channel();
    if route.commands.try_send((action, sender)).is_err() {
        return reply(StatusCode::SERVICE_UNAVAILABLE, "紙上控制台未就緒".into());
    }
    match timeout(Duration::from_secs(2), recv).await {
        Ok(Ok(Ok(()))) => reply(StatusCode::OK, "已受理".into()),
        Ok(Ok(Err(e))) => reply(StatusCode::CONFLICT, e),
        _ => reply(
            StatusCode::SERVICE_UNAVAILABLE,
            "未確認受理，請核對帳本".into(),
        ),
    }
}

pub async fn run(client: reqwest::Client, slug: Option<String>) -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", 8080))
        .await
        .context("無法開啟 127.0.0.1:8080")?;
    let catalog = session::catalog(client.clone());
    let sessions = session::Sessions::new(client, slug)?;
    eprintln!("Cockpit  http://127.0.0.1:8080  (Ctrl+C 結束)");
    let slots = Arc::new(Semaphore::new(32));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        let (socket, _) = tokio::select! {
            signal = &mut shutdown => { signal?; sessions.close().await; return Ok(()); }
            result = listener.accept() => result?,
        };
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            continue;
        };
        let state = sessions.current.clone();
        let switches = sessions.switches.clone();
        let catalog = catalog.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let service = service_fn(move |req| {
                let switches = switches.clone();
                let route = state.borrow().clone();
                let catalog = catalog.clone();
                async move {
                    Ok::<_, Infallible>(if req.method() == Method::POST {
                        command(req, route, &switches).await
                    } else if req.method() == Method::GET && req.uri().path() == "/events" {
                        Response::builder()
                            .header("Cache-Control", "no-store")
                            .header("Content-Type", "application/json")
                            .body(Body::from(catalog.borrow().to_string()))
                            .unwrap()
                    } else {
                        response(req, &route.screen)
                    })
                }
            });
            let _ = timeout(
                Duration::from_secs(8),
                Http::new()
                    .http1_only(true)
                    .http1_keep_alive(false)
                    .max_buf_size(8192)
                    .serve_connection(socket, service),
            )
            .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn unread_response_cannot_hold_up_next_snapshot() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (channel, rx) = watch::channel(Screen::default());
        let output = Output::with_book_sink(
            move |book| channel.send_modify(|screen| screen.publish_book(book)),
            io::sink(),
        );
        let view = |title: &str| crate::book::NegRiskEventGroup::new(title).view();
        output.finish_book(view(&"x".repeat(4096))).await.unwrap();

        // A 64-byte socket cannot buffer the outstanding HTTP response.
        let (server, mut client) = tokio::io::duplex(64);
        let observer = rx.clone();
        let connection = tokio::spawn(async move {
            Http::new()
                .http1_only(true)
                .http1_keep_alive(false)
                .serve_connection(
                    server,
                    service_fn(move |req| {
                        std::future::ready(Ok::<_, Infallible>(response(req, &observer)))
                    }),
                )
                .await
        });
        client
            .write_all(b"GET /snapshot HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        client.read_u8().await.unwrap();
        for n in 0..1000 {
            output.view_book(Arc::new(view(&n.to_string())));
        }
        timeout(Duration::from_secs(2), output.finish_book(view("latest")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rx.borrow().book.as_ref().unwrap().title, "latest");
        assert!(!connection.is_finished());
        drop(client);
        let _ = connection.await.unwrap();
    }
}
