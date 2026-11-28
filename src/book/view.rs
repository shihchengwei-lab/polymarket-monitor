use super::*;
use crate::output::single_line;
use serde::Serialize;

pub(crate) fn market_identity(condition: &str, yes: &str, no: &str) -> String {
    // Only internal consumers see this identity. The Cockpit assigns its own local key.
    serde_json::to_string(&(condition, yes, no)).expect("market identity contains only strings")
}

#[derive(Debug, PartialEq, Serialize)]
pub(crate) struct BookView {
    pub title: String,
    status: &'static str,
    synced: usize,
    total: usize,
    notes: Vec<String>,
    #[serde(skip)]
    yes_sum: Option<String>,
    pub markets: Vec<MarketView>,
}

#[derive(Debug, PartialEq, Serialize)]
pub(crate) struct MarketView {
    #[serde(skip)]
    pub identity: String,
    name: String,
    closed: bool,
    yes: TokenView,
    no: TokenView,
    #[serde(skip)]
    gap: Option<String>,
}

#[derive(Debug, PartialEq, Serialize)]
struct TokenView {
    state: &'static str,
    bid: Option<[String; 2]>,
    ask: Option<[String; 2]>,
    tick: Option<String>,
}

impl TokenView {
    fn from_book(book: &TokenBook) -> Self {
        let quotes = book.synced().map(|b| b.levels.quotes());
        let wire_level = |level: Level| [level.price.to_string(), level.size.to_string()];
        Self {
            state: book.label(),
            bid: quotes.and_then(|q| q.bid).map(wire_level),
            ask: quotes.and_then(|q| q.ask).map(wire_level),
            tick: book.synced().map(|b| b.tick.to_string()),
        }
    }
}

impl fmt::Display for BookView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{} | {} ({}/{} 子市場)",
            single_line(&self.title),
            self.status,
            self.synced,
            self.total
        )?;
        for note in &self.notes {
            writeln!(f, "{}", single_line(note))?;
        }
        if let Some(sum) = &self.yes_sum {
            writeln!(f, "YES 中間價總和 {sum}")?;
        }
        for market in &self.markets {
            writeln!(
                f,
                "{}{}",
                single_line(&market.name),
                if market.closed { " [已關閉]" } else { "" }
            )?;
            for (outcome, token) in [("YES", &market.yes), ("NO", &market.no)] {
                write!(f, "  {outcome} {}", token.state)?;
                if let Some(tick) = &token.tick {
                    let fmt_level = |level: &Option<[String; 2]>| {
                        level.as_ref().map_or_else(
                            || "—".to_owned(),
                            |[price, size]| format!("{price} × {size}"),
                        )
                    };
                    write!(
                        f,
                        " | bid {} / ask {} | tick {tick}",
                        fmt_level(&token.bid),
                        fmt_level(&token.ask)
                    )?;
                }
                writeln!(f)?;
            }
            if let Some(gap) = &market.gap {
                writeln!(f, "  YES + NO 補數檢核 {gap}")?;
            }
        }
        Ok(())
    }
}

impl NegRiskEventGroup {
    pub(crate) fn retiring_view(&self) -> BookView {
        BookView {
            title: self.title.clone(),
            status: "NotReady",
            synced: 0,
            total: self.markets().len(),
            notes: Vec::new(),
            yes_sum: None,
            markets: Vec::new(),
        }
    }

    pub(crate) fn view(&self) -> BookView {
        let markets = self.markets();
        let synced = markets.iter().filter(|m| self.market_synced(m)).count();
        let quotes_ready =
            matches!(self.membership, Membership::Open { .. }) && synced == markets.len();
        let quotes_only = self.quotes_only();
        let ready = quotes_ready && !quotes_only;
        let status = match (quotes_ready, quotes_only) {
            (true, false) => "Ready",
            (true, true) => "QuotesReady",
            (false, _) => "NotReady",
        };
        let mut notes = Vec::new();
        match &self.membership {
            Membership::Unavailable { reason, .. } => {
                notes.push(format!("Gamma 成員不可用：{reason}"))
            }
            Membership::Closed(..) => notes.push("事件已結束".into()),
            Membership::SettlementCapacity(_) => notes.push(SettlementCapacity.to_string()),
            Membership::Pending => notes.push("尚未取得 Gamma 成員資料".into()),
            Membership::Open { .. } => {}
        }
        if self.gamma_failed {
            let detail = if self.awaiting_gamma() {
                "等待重新查詢"
            } else {
                "使用上次確認的資料"
            };
            notes.push(format!("Gamma 更新失敗，{detail}"));
        }
        if quotes_only {
            notes.push("此事件只顯示已啟用的具名選項；整籃計算與紙上交易已停用".into());
        }
        let sum = self.yes_sum(ready).map_or_else(
            || "—".into(),
            |sum| {
                let difference = &sum - PROBABILITY_ONE;
                format!(
                    "{} (與 1 的差 {})",
                    format_probability(sum),
                    format_probability(difference)
                )
            },
        );
        let views = markets
            .iter()
            .map(|market| {
                let gap = quotes_ready.then(|| {
                    Self::midpoint(&market.yes)
                        .zip(Self::midpoint(&market.no))
                        .map_or_else(
                            || "—".into(),
                            |(yes, no)| {
                                let difference = yes + no - PROBABILITY_ONE;
                                if difference.sign() == Sign::NoSign {
                                    "符合".into()
                                } else {
                                    format!("偏差 {}", format_probability(difference))
                                }
                            },
                        )
                });
                MarketView {
                    identity: market_identity(
                        &market.condition,
                        &market.yes.asset,
                        &market.no.asset,
                    ),
                    name: market.name.clone(),
                    closed: self.event_closed() || self.market_closed(market),
                    yes: TokenView::from_book(&market.yes),
                    no: TokenView::from_book(&market.no),
                    gap,
                }
            })
            .collect();
        BookView {
            title: self.title.clone(),
            status,
            synced,
            total: markets.len(),
            notes,
            yes_sum: Some(sum),
            markets: views,
        }
    }
}
