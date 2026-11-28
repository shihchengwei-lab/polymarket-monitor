use super::{DiagnosticSink, Screen, desk};
use crate::{
    gamma_discovery::{self, Discovery},
    monitor,
    output::Output,
};
use anyhow::Result;
use reqwest::Client;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

#[derive(Clone)]
pub(super) struct Route {
    pub id: u64,
    pub screen: watch::Receiver<Screen>,
    pub commands: mpsc::Sender<desk::Request>,
}
struct Worker {
    stop: oneshot::Sender<()>,
    monitor: JoinHandle<()>,
}
impl Worker {
    async fn retire(self) {
        let _ = self.stop.send(());
        let _ = self.monitor.await;
    }
}
async fn monitor_with_metadata(
    monitoring: impl Future<Output = Result<()>>,
    metadata: impl Future<Output = ()>,
) -> Result<()> {
    tokio::pin!(monitoring);
    tokio::select! {
        result = &mut monitoring => result,
        () = metadata => monitoring.await,
    }
}

fn start(client: &Client, id: u64, slug: Option<String>) -> Result<(Route, Option<Worker>)> {
    let (tx, screen) = watch::channel(Screen {
        session: id,
        slug: slug.clone().unwrap_or_default().into(),
        ..Screen::default()
    });
    let (commands, inbox) = mpsc::channel(8);
    let route = Route {
        id,
        screen,
        commands,
    };
    let Some(slug) = slug else {
        return Ok((route, None));
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (stop, stopped) = oneshot::channel();
    let quantity = crate::paper::Quantity::new(desk::SIZE.into())?;
    let client = client.clone();
    let monitor = tokio::task::spawn_blocking(move || {
        runtime.block_on(async move {
            let (fees, metadata) = mpsc::channel(1);
            let mut desk = desk::Controller::new(metadata, inbox, tx.clone());
            let group = crate::book::NegRiskEventGroup::with_market_consumer(
                &slug,
                quantity,
                move |at, input| desk.receive(at, input),
            );
            let feed = tx.clone();
            let output = Output::with_book_sink(
                move |book| feed.send_modify(|screen| screen.publish_book(book)),
                DiagnosticSink(tx.clone()),
            );
            let endpoints = monitor::Endpoints::default();
            let monitoring =
                monitor::watch_group_until(&client, &endpoints, group, &output, stopped);
            let result =
                monitor_with_metadata(monitoring, desk::metadata(client.clone(), slug, fees)).await;
            tx.send_modify(|s| {
                s.sequence += 1;
                s.stopped = true;
                if let Err(err) = result {
                    s.diagnostic = format!("{err:#}").into();
                }
            });
        })
    });
    Ok((route, Some(Worker { stop, monitor })))
}

pub(super) struct Switch {
    pub expected: u64,
    pub slug: String,
    pub result: oneshot::Sender<Result<u64, String>>,
}
pub(super) struct Sessions {
    pub current: watch::Receiver<Route>,
    pub switches: mpsc::Sender<Switch>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}
impl Sessions {
    pub fn new(client: Client, slug: Option<String>) -> Result<Self> {
        let first = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let (route, mut worker) = start(&client, first, slug)?;
        let (published, current) = watch::channel(route);
        let (switches, mut requests) = mpsc::channel::<Switch>(1);
        let (shutdown, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                let req = tokio::select! {
                    biased;
                    _=&mut stopped=>break,
                    req=requests.recv()=>match req{Some(r)=>r,None=>break}
                };
                if req.result.is_closed() {
                    continue;
                }
                let old = published.borrow().clone();
                if old.id != req.expected {
                    let _ = req.result.send(Err("場次已切換，請重新選擇".into()));
                    continue;
                }
                if old.screen.borrow().slug.as_ref() == req.slug {
                    let _ = req.result.send(Ok(old.id));
                    continue;
                }
                // Validate before touching the current account or socket.
                let validation = tokio::select! {
                    _=&mut stopped=>break,
                    result=gamma_discovery::discover(&client,gamma_discovery::GAMMA,&req.slug,Duration::from_secs(30))=>result
                };
                if !matches!(validation, Ok(Discovery::Open(_))) {
                    let _ = req.result.send(Err(
                        "Gamma 無法確認這個 slug 是開放的 NegRisk 事件，已保留原場次".into(),
                    ));
                    continue;
                }
                if req.result.is_closed() {
                    continue;
                }
                if let Some(old) = worker.take() {
                    old.retire().await;
                }
                match start(&client, old.id + 1, Some(req.slug)) {
                    Ok((route, new)) => {
                        let id = route.id;
                        published.send_replace(route);
                        worker = new;
                        let _ = req.result.send(Ok(id));
                    }
                    Err(e) => {
                        // Keep the service switchable if a local runtime cannot start.
                        let (idle, _) = start(&client, old.id + 1, None).expect("idle session");
                        published.send_replace(idle);
                        let _ = req.result.send(Err(e.to_string()));
                    }
                }
            }
            if let Some(worker) = worker {
                worker.retire().await;
            }
        });
        Ok(Self {
            current,
            switches,
            shutdown,
            task,
        })
    }
    pub async fn close(self) {
        let _ = self.shutdown.send(());
        let _ = self.task.await;
    }
}

pub(super) fn catalog(client: Client) -> watch::Receiver<Arc<serde_json::Value>> {
    let (tx, rx) = watch::channel(Arc::new(serde_json::json!({"loading":true,"events":[]})));
    tokio::spawn(async move {
        let mut events = Vec::new();
        loop {
            let result = tokio::select! {_=tx.closed()=>break,r=gamma_discovery::popular(&client,gamma_discovery::GAMMA)=>r};
            let err = match result {
                Ok(list) => {
                    events=list.into_iter().map(|(e,v)|serde_json::json!({"title":e.title,"slug":e.slug,"volume":v.to_string()})).collect();
                    None
                }
                Err(e) => Some(format!("熱門清單更新失敗：{e}")),
            };
            let delay = if err.is_some() { 15 } else { 60 };
            tx.send_replace(Arc::new(serde_json::json!({"events":events,"error":err})));
            tokio::select! {_=tx.closed()=>break,_=tokio::time::sleep(Duration::from_secs(delay))=>{}}
        }
    });
    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn ending_metadata_cannot_complete_a_pending_monitor() {
        for success in [true, false] {
            let (release, flush) = oneshot::channel();
            let (stop, metadata_done) = oneshot::channel();
            let task = monitor_with_metadata(
                async {
                    flush.await?;
                    anyhow::ensure!(success, "flush failed");
                    Ok(())
                },
                async {
                    metadata_done.await.unwrap();
                },
            );
            tokio::pin!(task);
            stop.send(()).unwrap();
            assert!(futures_util::poll!(&mut task).is_pending());
            release.send(()).unwrap();
            assert_eq!(task.await.is_ok(), success);
        }
    }
}
