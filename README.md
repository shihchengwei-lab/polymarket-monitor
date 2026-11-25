# Polymarket NegRisk 行情與紙上交易

在本機同步同一事件所有結果的 YES／NO 訂單簿，查看整籃價差，並用真實行情模擬掛單、撤單與補腿。程式只讀取 Polymarket 公開資料，交易與餘額都在本機模擬。價格、股數與手續費使用十進位定點數。

## 開啟儀表板

在有 Rust 與 Cargo 的電腦上，從專案目錄執行以下 PowerShell 指令。

```powershell
cargo run --locked -- cockpit
```

開啟 <http://127.0.0.1:8080>，從熱門事件選單選取標的，或貼上事件 slug，按「切換事件」。也可用 `cockpit '事件-slug'` 直接指定初始標的。即時行情需要連往 Polymarket。按 Ctrl+C 結束服務。

## 其他入口

| 用途 | 入口 |
|---|---|
| 在終端看即時盤面 | `cargo run --locked -- watch '事件-slug'`；加 `--once` 會在首個 Ready 畫面寫入成功後結束 |
| 查看下一頁事件 | `cargo run --locked -- list --offset 100`；預設檢查 100 筆 Gamma 事件，僅列出符合條件者 |
| 操作模擬帳戶、判讀價差與回放 | [Cockpit 操作指南](docs/cockpit.md) |
| 查同步、隔離、重連條件與程式庫介面 | [訂單簿同步](docs/synchronization.md) · [紙上撮合 API 說明](src/paper/mod.rs) |
| 維護與驗證 | [專案待辦](BACKLOG.md) · `cargo test --locked` |
