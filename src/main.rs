use polymarket_monitor::run_cockpit;

use polymarket_monitor::{gamma_discovery, monitor, output};

use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Result, bail, ensure};
use reqwest::Client;

const HELP: &str = "Polymarket NegRisk 唯讀監控

用法
  polymarket-monitor cockpit [事件 slug]
  polymarket-monitor list [--limit N] [--offset N]
  polymarket-monitor watch <事件 slug> [--once]

list  從 Gamma 列出這一頁的開放 NegRisk 事件。預設每頁 100 筆，從第 0 筆開始。
watch 顯示指定事件的訂單簿。按 Ctrl+C 結束。
--once 在首個 Ready 畫面寫入成功後結束。同步或輸出失敗時回傳錯誤。";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(err) => {
            eprint!("{}", failure_line(&err));
            ExitCode::FAILURE
        }
    }
}

fn failure_line(err: &anyhow::Error) -> String {
    format!("錯誤：{}\n", output::single_line(&format!("{err:#}")))
}

async fn run() -> Result<ExitCode> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    match args.as_slice() {
        [] => println!("{HELP}"),
        [flag] if flag == "--help" || flag == "-h" => println!("{HELP}"),
        [command, args @ ..]
            if command == "cockpit"
                && args.len() <= 1
                && args.first().is_none_or(|s| !s.starts_with('-')) =>
        {
            run_cockpit(client, args.first().cloned()).await?;
        }
        [command, flags @ ..] if command == "list" => list(&client, flags).await?,
        [command, slug, flags @ ..]
            if command == "watch"
                && !slug.starts_with('-')
                && (flags.is_empty() || flags == ["--once"]) =>
        {
            let output = output::Output::terminal();
            let result = monitor::watch(
                &client,
                &monitor::Endpoints::default(),
                slug,
                !flags.is_empty(),
                &output,
            )
            .await;
            return Ok(match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    output.report_failure(failure_line(&err)).await;
                    ExitCode::FAILURE
                }
            });
        }
        _ => bail!("參數有誤。使用 --help 查看用法"),
    }
    Ok(ExitCode::SUCCESS)
}

async fn list(client: &Client, flags: &[String]) -> Result<()> {
    let mut limit = None;
    let mut offset = None;
    let (pairs, remainder) = flags.as_chunks::<2>();
    ensure!(remainder.is_empty(), "list 的選項需要數值");
    for [flag, value] in pairs {
        let slot = match flag.as_str() {
            "--limit" => &mut limit,
            "--offset" => &mut offset,
            flag => bail!("未知參數 {flag}"),
        };
        ensure!(slot.is_none(), "重複參數 {flag}");
        *slot = Some(value.parse::<u32>()?);
    }
    let limit = limit.unwrap_or(100);
    let offset = offset.unwrap_or(0);
    ensure!((1..=100).contains(&limit), "--limit 必須介於 1 至 100");
    let events = gamma_discovery::list(client, limit, offset).await?;
    if events.is_empty() {
        println!("Gamma 此頁沒有開放的 NegRisk 事件（offset {offset}）");
    }
    for event in events {
        println!("{}", listing_row(&event));
    }
    Ok(())
}

fn listing_row(event: &gamma_discovery::Listing) -> impl std::fmt::Display + '_ {
    std::fmt::from_fn(|f| {
        write!(
            f,
            "{}\t{}",
            output::single_line(&event.slug),
            output::single_line(&event.title)
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_and_error_fields_preserve_printable_text_and_layout() {
        let title = "台北 Cafe\u{301} 👩‍💻\n第二段";
        let event: gamma_discovery::Listing = serde_json::from_value(serde_json::json!({
            "slug": "a\tb", "title": title, "negRisk": true
        }))
        .unwrap();
        assert_eq!(
            listing_row(&event).to_string(),
            "a\\tb\t台北 Cafe\u{301} 👩‍💻\\n第二段"
        );
        assert_eq!(event.title, title);
        let err = anyhow::anyhow!("市場\n問題").context("unexpected outcome");
        assert_eq!(
            failure_line(&err),
            "錯誤：unexpected outcome: 市場\\n問題\n"
        );
        for ch in [
            '\0', '\r', '\u{7f}', '\u{85}', '\u{2028}', '\u{2029}', '\u{202e}', '\u{2067}',
        ] {
            let escaped = output::single_line(&ch.to_string()).to_string();
            assert!(escaped.is_ascii() && escaped.starts_with('\\'));
        }
    }
}
