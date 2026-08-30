use chrono::{Datelike, NaiveDate};
use eframe::egui;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Country selection skeleton
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Country {
    Australia,
    Usa,
    China,
    Japan,
    India,
    Uk,
    Germany,
    Canada,
    Russia,
    SouthKorea,
}

impl Country {
    fn all() -> Vec<Country> {
        vec![
            Country::Australia,
            Country::Usa,
            Country::China,
            Country::Japan,
            Country::India,
            Country::Uk,
            Country::Germany,
            Country::Canada,
            Country::Russia,
            Country::SouthKorea,
        ]
    }

    fn name(&self) -> &'static str {
        match self {
            Country::Australia => "Australia",
            Country::Usa => "USA",
            Country::China => "China",
            Country::Japan => "Japan",
            Country::India => "India",
            Country::Uk => "United Kingdom",
            Country::Germany => "Germany",
            Country::Canada => "Canada",
            Country::Russia => "Russia",
            Country::SouthKorea => "South Korea",
        }
    }

    fn from_name(name: &str) -> Country {
        Self::all()
            .into_iter()
            .find(|c| c.name() == name)
            .unwrap_or(Country::Australia)
    }

    // Per-country dashboards are not implemented yet; the dashboard is
    // Australia-driven for now.
    fn fx_symbol(&self) -> &'static str {
        "AUDUSD"
    }
}

// ---------------------------------------------------------------------------
// Series registry
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Source {
    Frankfurter,
    Fred,
    Kraken,
    Lbma,
}

impl Source {
    fn name(&self) -> &'static str {
        match self {
            Source::Frankfurter => "Frankfurter (ECB)",
            Source::Fred => "FRED",
            Source::Kraken => "Kraken",
            Source::Lbma => "LBMA",
        }
    }
}

#[derive(Clone)]
struct SeriesSpec {
    symbol: &'static str,
    title: &'static str,
    unit: &'static str,
    source: Source,
    market: bool,
    yoy: bool,
}

fn registry(country: Country) -> Vec<SeriesSpec> {
    vec![
        SeriesSpec { symbol: country.fx_symbol(), title: "AUD/USD", unit: "", source: Source::Frankfurter, market: true, yoy: false },
        SeriesSpec { symbol: "SP500", title: "S&P 500", unit: "", source: Source::Fred, market: true, yoy: false },
        SeriesSpec { symbol: "NASDAQCOM", title: "NASDAQ Composite", unit: "", source: Source::Fred, market: true, yoy: false },
        SeriesSpec { symbol: "DJIA", title: "Dow Jones (DJIA)", unit: "", source: Source::Fred, market: true, yoy: false },
        SeriesSpec { symbol: "GOLD", title: "Gold (LBMA PM)", unit: "USD/oz", source: Source::Lbma, market: false, yoy: false },
        SeriesSpec { symbol: "SILVER", title: "Silver (LBMA)", unit: "USD/oz", source: Source::Lbma, market: false, yoy: false },
        SeriesSpec { symbol: "btcusd", title: "Bitcoin (BTC/USD)", unit: "USD", source: Source::Kraken, market: true, yoy: false },
        SeriesSpec { symbol: "xmrusd", title: "Monero (XMR/USD)", unit: "USD", source: Source::Kraken, market: true, yoy: false },
        SeriesSpec { symbol: "IRSTCI01AUM156N", title: "RBA Cash Rate", unit: "%", source: Source::Fred, market: false, yoy: false },
        SeriesSpec { symbol: "AUSCPIALLQINMEI", title: "AU CPI (YoY)", unit: "%", source: Source::Fred, market: false, yoy: true },
    ]
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Series {
    title: String,
    unit: String,
    source_name: String,
    obs: Vec<(NaiveDate, f64)>,
    error: Option<String>,
}

impl Series {
    fn latest(&self) -> Option<f64> {
        self.obs.last().map(|o| o.1)
    }

    fn latest_date(&self) -> Option<NaiveDate> {
        self.obs.last().map(|o| o.0)
    }

    fn change_pct(&self, days: u32) -> Option<f64> {
        let n = self.obs.len();
        if n < 2 {
            return None;
        }
        let last = self.obs[n - 1].1;
        let base = if days == 0 {
            self.obs[0].1
        } else {
            let cutoff = NaiveDate::from_num_days_from_ce_opt(
                chrono::Utc::now().date_naive().num_days_from_ce() - days as i32,
            )
            .unwrap_or(NaiveDate::MIN);
            let start = self.obs.partition_point(|o| o.0 < cutoff);
            if start >= n {
                return None;
            }
            self.obs[start].1
        };
        if base == 0.0 {
            None
        } else {
            Some((last - base) / base * 100.0)
        }
    }

    fn day_change_pct(&self) -> Option<f64> {
        let n = self.obs.len();
        if n < 2 {
            return None;
        }
        let prev = self.obs[n - 2].1;
        let last = self.obs[n - 1].1;
        if prev == 0.0 {
            None
        } else {
            Some((last - prev) / prev * 100.0)
        }
    }

    fn slice_range(&self, days: u32) -> &[(NaiveDate, f64)] {
        if days == 0 {
            return &self.obs;
        }
        let cutoff = NaiveDate::from_num_days_from_ce_opt(
            chrono::Utc::now().date_naive().num_days_from_ce() - days as i32,
        )
        .unwrap_or(NaiveDate::MIN);
        let start = self.obs.partition_point(|o| o.0 < cutoff);
        &self.obs[start..]
    }
}

enum DataEvent {
    SeriesUpdated { symbol: String, obs: Vec<(NaiveDate, f64)> },
    Point { symbol: String, obs: (NaiveDate, f64) },
    Error { symbol: String, message: String },
    Status(String),
    ChatDelta { content: String },
    ChatReasoningDelta { content: String },
    ChatToolLine { line: String },
    ChatDone {
        error: Option<String>,
        usage: Option<Usage>,
    },
}

fn http_get(url: &str) -> Result<String, String> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(30)))
        .build()
        .new_agent();
    let resp = agent.get(url).call().map_err(|e| e.to_string())?;
    let mut body = String::new();
    resp.into_body()
        .into_reader()
        .read_to_string(&mut body)
        .map_err(|e| e.to_string())?;
    Ok(body)
}

use std::io::{BufRead, Read};

fn parse_lbma(body: &str) -> Vec<(NaiveDate, f64)> {
    #[derive(serde::Deserialize)]
    struct Entry {
        d: String,
        v: Vec<Option<f64>>,
    }
    let Ok(entries) = serde_json::from_str::<Vec<Entry>>(body) else {
        return Vec::new();
    };
    entries
        .into_iter()
        .filter_map(|e| {
            let date = NaiveDate::parse_from_str(&e.d, "%Y-%m-%d").ok()?;
            let usd = e.v.first().copied().flatten()?;
            Some((date, usd))
        })
        .collect()
}

fn parse_frankfurter(body: &str, to: &str) -> Vec<(NaiveDate, f64)> {
    #[derive(serde::Deserialize)]
    struct Resp {
        rates: HashMap<String, HashMap<String, f64>>,
    }
    let Ok(resp) = serde_json::from_str::<Resp>(body) else {
        return Vec::new();
    };
    let mut out: Vec<(NaiveDate, f64)> = resp
        .rates
        .into_iter()
        .filter_map(|(d, m)| {
            let date = NaiveDate::parse_from_str(&d, "%Y-%m-%d").ok()?;
            let v = *m.get(to)?;
            Some((date, v))
        })
        .collect();
    out.sort_by_key(|o| o.0);
    out
}

fn parse_fred(body: &str) -> Vec<(NaiveDate, f64)> {
    #[derive(serde::Deserialize)]
    struct Resp {
        observations: Vec<Obs>,
    }
    #[derive(serde::Deserialize)]
    struct Obs {
        date: String,
        value: String,
    }
    let Ok(resp) = serde_json::from_str::<Resp>(body) else {
        return Vec::new();
    };
    resp.observations
        .into_iter()
        .filter_map(|o| {
            let date = NaiveDate::parse_from_str(&o.date, "%Y-%m-%d").ok()?;
            let v = o.value.parse::<f64>().ok()?;
            Some((date, v))
        })
        .collect()
}

fn parse_kraken_ohlc(body: &str) -> Vec<(NaiveDate, f64)> {
    #[derive(serde::Deserialize)]
    struct Resp {
        result: serde_json::Value,
    }
    let Ok(resp) = serde_json::from_str::<Resp>(body) else {
        return Vec::new();
    };
    let Some(arr) = resp
        .result
        .as_object()
        .and_then(|o| o.values().find(|v| v.is_array()))
        .and_then(|v| v.as_array())
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for candle in arr {
        let Some(parts) = candle.as_array() else { continue };
        if parts.len() < 5 {
            continue;
        }
        let Some(time) = parts[0].as_u64() else { continue };
        let Some(close) = parts[4].as_str().and_then(|s| s.parse::<f64>().ok()) else {
            continue;
        };
        let Some(date) = chrono::DateTime::from_timestamp(time as i64, 0)
            .map(|dt| dt.date_naive())
        else {
            continue;
        };
        // keep last candle per day
        if out.last().map(|(d, _)| *d == date).unwrap_or(false) {
            out.last_mut().unwrap().1 = close;
        } else {
            out.push((date, close));
        }
    }
    out
}

fn parse_kraken_ticker(body: &str) -> Option<f64> {
    #[derive(serde::Deserialize)]
    struct Resp {
        result: serde_json::Value,
    }
    let resp = serde_json::from_str::<Resp>(body).ok()?;
    let entry = resp.result.as_object()?.values().next()?;
    entry
        .get("c")?
        .get(0)?
        .as_str()?
        .parse::<f64>()
        .ok()
}

#[derive(Clone, Default)]
struct ApiKeys {
    fred: String,
}

fn fred_yoy(obs: &[(NaiveDate, f64)]) -> Vec<(NaiveDate, f64)> {
    // Convert a quarterly index level series to year-over-year percentage.
    let mut out = Vec::new();
    for (i, (d, v)) in obs.iter().enumerate() {
        let cutoff = *d - chrono::Days::new(300);
        let mut k = i;
        while k > 0 && obs[k - 1].0 > cutoff {
            k -= 1;
        }
        if k > 0 {
            let base = obs[k - 1].1;
            if base != 0.0 {
                out.push((*d, (v - base) / base * 100.0));
            }
        }
    }
    out
}

fn fetch_series(spec: &SeriesSpec, keys: &ApiKeys, from_year: i32) -> Result<Vec<(NaiveDate, f64)>, String> {
    match spec.source {
        Source::Frankfurter => {
            let url = format!(
                "https://api.frankfurter.app/{}-01-01..?from=AUD&to=USD",
                from_year
            );
            let body = http_get(&url)?;
            let obs = parse_frankfurter(&body, "USD");
            if obs.is_empty() {
                return Err("no data returned".into());
            }
            Ok(obs)
        }
        Source::Fred => {
            if keys.fred.trim().is_empty() {
                return Err("FRED API key not set (Settings)".into());
            }
            let url = format!(
                "https://api.stlouisfed.org/fred/series/observations?series_id={}&api_key={}&file_type=json&observation_start={}-01-01",
                spec.symbol,
                keys.fred.trim(),
                from_year
            );
            let body = http_get(&url)?;
            let obs = parse_fred(&body);
            if obs.is_empty() {
                return Err("no data returned".into());
            }
            if spec.yoy {
                let yoy = fred_yoy(&obs);
                if yoy.is_empty() {
                    return Err("no data after YoY transform".into());
                }
                return Ok(yoy);
            }
            Ok(obs)
        }
        Source::Lbma => {
            let file = match spec.symbol {
                "SILVER" => "silver.json",
                _ => "gold_pm.json",
            };
            let url = format!("https://prices.lbma.org.uk/json/{}", file);
            let body = http_get(&url)?;
            let obs = parse_lbma(&body);
            if obs.is_empty() {
                return Err("no data returned".into());
            }
            Ok(obs)
        }
        Source::Kraken => {
            let url = format!(
                "https://api.kraken.com/0/public/OHLC?pair={}&interval=1440&since=0",
                spec.symbol.to_uppercase()
            );
            let body = http_get(&url)?;
            let obs = parse_kraken_ohlc(&body);
            if obs.is_empty() {
                return Err("no data returned".into());
            }
            Ok(obs)
        }
    }
}

fn fetch_kraken_live(symbol: &str) -> Result<f64, String> {
    let url = format!(
        "https://api.kraken.com/0/public/Ticker?pair={}",
        symbol.to_uppercase()
    );
    let body = http_get(&url)?;
    parse_kraken_ticker(&body).ok_or_else(|| "no ticker data".to_string())
}

// ---------------------------------------------------------------------------
// AI research chat
// ---------------------------------------------------------------------------

const MODEL_PRESETS: &[&str] = &[
    "openai/gpt-4o-mini",
    "anthropic/claude-sonnet-4",
    "deepseek/deepseek-r1",
    "google/gemini-2.5-pro",
    "meta-llama/llama-4-maverick",
];

#[derive(Clone, Copy, PartialEq)]
enum ChatRole {
    User,
    Assistant,
}

impl ChatRole {
    fn api(&self) -> &'static str {
        match self {
            ChatRole::User => "user",
            ChatRole::Assistant => "assistant",
        }
    }
}

#[derive(Clone)]
struct ChatMessage {
    role: ChatRole,
    content: String,
    reasoning: String,
    tool_log: Vec<String>,
}

#[derive(Clone, Copy, Default)]
struct Usage {
    prompt: u64,
    completion: u64,
    total: u64,
    cost_usd: f64,
}

struct ChatState {
    messages: Vec<ChatMessage>,
    input: String,
    busy: bool,
    error: Option<String>,
    model: String,
    model_custom: String,
    stream_content: String,
    stream_reasoning: String,
    stream_tool_lines: Vec<String>,
    usage: Option<Usage>,
    cost_total: f64,
    md_cache: egui_commonmark::CommonMarkCache,
}

impl Default for ChatState {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            input: String::new(),
            busy: false,
            error: None,
            model: MODEL_PRESETS[0].to_string(),
            model_custom: String::new(),
            stream_content: String::new(),
            stream_reasoning: String::new(),
            stream_tool_lines: Vec::new(),
            usage: None,
            cost_total: 0.0,
            md_cache: egui_commonmark::CommonMarkCache::default(),
        }
    }
}

fn resolve_model(chat: &ChatState) -> String {
    if MODEL_PRESETS.contains(&chat.model.as_str()) {
        chat.model.clone()
    } else if !chat.model_custom.trim().is_empty() {
        chat.model_custom.trim().to_string()
    } else {
        MODEL_PRESETS[0].to_string()
    }
}

#[derive(Clone)]
struct LlmConfig {
    base_url: String,
    key: String,
    model: String,
}

fn http_get_ua(url: &str, timeout_secs: u64) -> Result<String, String> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs)))
        .build()
        .new_agent();
    let resp = agent
        .get(url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
        )
        .header("Accept", "text/html,application/xhtml+xml")
        .call()
        .map_err(|e| e.to_string())?;
    let mut body = String::new();
    resp.into_body()
        .into_reader()
        .read_to_string(&mut body)
        .map_err(|e| e.to_string())?;
    Ok(body)
}

fn percent_encode_qs(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn strip_html(html: &str) -> String {
    let lower = html.to_lowercase();
    let mut out = String::with_capacity(html.len() / 2);
    let mut i = 0usize;
    let mut skip: Option<&'static str> = None;
    while i < html.len() {
        let step = html[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        if lower[i..].starts_with('<') {
            let end = match lower[i..].find('>') {
                Some(p) => i + p,
                None => break,
            };
            let tag = &lower[i + 1..end];
            let name: String = tag
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect();
            if skip.is_none() {
                if name == "script" {
                    skip = Some("</script>");
                } else if name == "style" {
                    skip = Some("</style>");
                }
            } else if let Some(close) = skip {
                if lower[i..].starts_with(close) {
                    skip = None;
                }
            }
            i = end + 1;
            continue;
        }
        if skip.is_some() {
            i += step;
            continue;
        }
        let ch = html[i..].chars().next().unwrap();
        if ch == '&' {
            if let Some(semi_rel) = html[i..].find(';') {
                if semi_rel <= 8 {
                    let ent = &html[i + 1..i + semi_rel];
                    let rep = match ent {
                        "amp" => "&",
                        "lt" => "<",
                        "gt" => ">",
                        "quot" => "\"",
                        "apos" => "'",
                        "nbsp" => " ",
                        _ => "",
                    };
                    out.push_str(rep);
                    i += semi_rel + 1;
                    continue;
                }
            }
        }
        out.push(ch);
        i += ch.len_utf8();
    }
    let mut collapsed = String::new();
    let mut last_ws = false;
    for ch in out.chars() {
        if ch.is_whitespace() {
            if !last_ws {
                collapsed.push(' ');
            }
            last_ws = true;
        } else {
            collapsed.push(ch);
            last_ws = false;
        }
    }
    collapsed.trim().to_string()
}

fn parse_ddg_results(body: &str) -> String {
    let mut out = String::from("Web search results:\n");
    let mut rest = body;
    let mut n = 0;
    while n < 6 {
        let Some(pos) = rest.find("result__a") else { break };
        let chunk = &rest[pos..];
        let Some(hpos) = chunk.find("href=\"") else {
            rest = &rest[pos + 10..];
            continue;
        };
        let href_start = hpos + 6;
        let Some(href_end) = chunk[href_start..].find('"') else { break };
        let href_raw = &chunk[href_start..href_start + href_end];
        let title = match chunk[href_start + href_end..].find('>').map(|p| href_start + href_end + p) {
            Some(te) => chunk[te + 1..]
                .find("</a>")
                .map(|ae| strip_html(&chunk[te + 1..te + 1 + ae]))
                .unwrap_or_default(),
            None => String::new(),
        };
        let snippet = rest[pos..]
            .find("result__snippet")
            .and_then(|sp| {
                let s = &rest[pos + sp..];
                s.find('>').and_then(|te| {
                    s[te + 1..]
                        .find("</a>")
                        .map(|ae| strip_html(&s[te + 1..te + 1 + ae]))
                })
            })
            .unwrap_or_default();
        let mut url = href_raw.replace("&amp;", "&");
        if let Some(u) = url.find("uddg=") {
            let tail = &url[u + 5..];
            let end = tail.find('&').unwrap_or(tail.len());
            url = percent_decode(&tail[..end]);
        }
        if url.starts_with("//") {
            url = format!("https:{}", url);
        }
        if !url.is_empty() && !title.is_empty() {
            out.push_str(&format!("{}. {} — {}\n   {}\n", n + 1, title, url, snippet));
            n += 1;
        }
        rest = &rest[pos + 10..];
    }
    if n == 0 {
        out.push_str("(no results parsed — try web_fetch on a known URL)");
    }
    out
}

fn execute_chat_tool(name: &str, args: &serde_json::Value) -> (String, String) {
    match name {
        "web_search" => {
            let q = args.get("query").and_then(|v| v.as_str()).unwrap_or("");
            let line = format!("↳ web_search \"{}\"", q);
            let url = format!(
                "https://html.duckduckgo.com/html/?q={}",
                percent_encode_qs(q)
            );
            let res = http_get_ua(&url, 20)
                .map(|b| parse_ddg_results(&b))
                .unwrap_or_else(|e| format!("search failed: {}", e));
            (line, res)
        }
        "web_fetch" => {
            let u = args.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let line = format!("↳ web_fetch {}", u);
            let res = http_get_ua(u, 20)
                .map(|b| {
                    let text = strip_html(&b);
                    let trunc: String = text.chars().take(8000).collect();
                    if text.chars().count() > 8000 {
                        format!("{}\n…[truncated]", trunc)
                    } else {
                        trunc
                    }
                })
                .unwrap_or_else(|e| format!("fetch failed: {}", e));
            (line, res)
        }
        _ => (format!("↳ ? {}", name), format!("unknown tool: {}", name)),
    }
}

fn chat_tools_json() -> serde_json::Value {
    serde_json::json!([
        {"type":"function","function":{
            "name":"web_search",
            "description":"Search the web (DuckDuckGo). Returns numbered titles, URLs and snippets.",
            "parameters":{"type":"object","properties":{"query":{"type":"string","description":"The search query"}},"required":["query"]}}},
        {"type":"function","function":{
            "name":"web_fetch",
            "description":"Fetch an http(s) URL and return the page text (scripts/styles stripped, ~8KB truncated).",
            "parameters":{"type":"object","properties":{"url":{"type":"string","description":"Absolute http(s) URL"}},"required":["url"]}}}
    ])
}

#[derive(Default)]
struct ToolCallAcc {
    id: String,
    name: String,
    args: String,
}

struct StreamOutcome {
    content: String,
    reasoning: String,
    tool_calls: Vec<ToolCallAcc>,
    usage: Option<Usage>,
}

impl Default for StreamOutcome {
    fn default() -> Self {
        Self {
            content: String::new(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            usage: None,
        }
    }
}

fn stream_chat_completion(
    cfg: &LlmConfig,
    body: &serde_json::Value,
    tx: &mpsc::Sender<DataEvent>,
    ctx: &egui::Context,
) -> Result<StreamOutcome, String> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(600)))
        .build()
        .new_agent();
    let url = format!("{}/chat/completions", cfg.base_url.trim_end_matches('/'));
    let resp = agent
        .post(&url)
        .header("Authorization", format!("Bearer {}", cfg.key))
        .send_json(body.clone())
        .map_err(|e| e.to_string())?;
    let reader = std::io::BufReader::new(resp.into_body().into_reader());
    let mut out = StreamOutcome::default();
    for line in reader.lines() {
        let line = line.map_err(|e| e.to_string())?;
        let Some(data) = line.strip_prefix("data:") else { continue };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            break;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        if let Some(err) = v.get("error") {
            let fallback = err.to_string();
            let msg = err.get("message").and_then(|m| m.as_str()).unwrap_or(&fallback);
            return Err(msg.to_string());
        }
        if let Some(u) = v.get("usage").filter(|u| !u.is_null()) {
            let pt = u.get("prompt_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
            let ct = u
                .get("completion_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(0);
            let tt = u
                .get("total_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(pt + ct);
            let cost = u.get("cost").and_then(|x| x.as_f64()).unwrap_or(0.0);
            out.usage = Some(Usage {
                prompt: pt,
                completion: ct,
                total: tt,
                cost_usd: cost,
            });
        }
        let Some(choice) = v.get("choices").and_then(|c| c.get(0)) else {
            continue;
        };
        let delta = choice.get("delta").cloned().unwrap_or_default();
        if let Some(c) = delta.get("content").and_then(|c| c.as_str()) {
            if !c.is_empty() {
                out.content.push_str(c);
                let _ = tx.send(DataEvent::ChatDelta { content: c.to_string() });
                ctx.request_repaint();
            }
        }
        if let Some(r) = delta.get("reasoning").and_then(|r| r.as_str()) {
            if !r.is_empty() {
                out.reasoning.push_str(r);
                let _ = tx.send(DataEvent::ChatReasoningDelta { content: r.to_string() });
                ctx.request_repaint();
            }
        }
        if let Some(tcs) = delta.get("tool_calls").and_then(|t| t.as_array()) {
            for tc in tcs {
                let idx = tc.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as usize;
                while out.tool_calls.len() <= idx {
                    out.tool_calls.push(ToolCallAcc::default());
                }
                let acc = &mut out.tool_calls[idx];
                if let Some(id) = tc.get("id").and_then(|i| i.as_str()) {
                    acc.id = id.to_string();
                }
                let f = tc.get("function").cloned().unwrap_or_default();
                if let Some(n) = f.get("name").and_then(|n| n.as_str()) {
                    acc.name.push_str(n);
                }
                if let Some(a) = f.get("arguments").and_then(|a| a.as_str()) {
                    acc.args.push_str(a);
                }
            }
        }
    }
    Ok(out)
}

fn run_chat_agent(
    ctx: egui::Context,
    tx: mpsc::Sender<DataEvent>,
    cfg: LlmConfig,
    system: String,
    history: Vec<ChatMessage>,
    user_text: String,
) {
    let mut api_messages: Vec<serde_json::Value> =
        vec![serde_json::json!({"role": "system", "content": system})];
    let recent: Vec<&ChatMessage> = if history.len() > 16 {
        history[history.len() - 16..].iter().collect()
    } else {
        history.iter().collect()
    };
    for m in recent {
        api_messages.push(serde_json::json!({"role": m.role.api(), "content": m.content}));
    }
    api_messages.push(serde_json::json!({"role": "user", "content": user_text}));

    let mut tools_enabled = true;
    let mut usage_streaming = true;
    let mut usage_sum = Usage::default();
    let mut has_usage = false;
    for _round in 0..6 {
        let mut body = serde_json::json!({
            "model": cfg.model,
            "messages": api_messages,
            "stream": true,
        });
        if tools_enabled {
            body["tools"] = chat_tools_json();
        }
        if usage_streaming {
            body["stream_options"] = serde_json::json!({"include_usage": true});
        }
        let outcome = match stream_chat_completion(&cfg, &body, &tx, &ctx) {
            Ok(o) => o,
            Err(e) => {
                if e.contains("400") {
                    if tools_enabled {
                        tools_enabled = false;
                        continue;
                    }
                    if usage_streaming {
                        usage_streaming = false;
                        continue;
                    }
                }
                let _ = tx.send(DataEvent::ChatDone {
                    error: Some(e),
                    usage: if has_usage { Some(usage_sum) } else { None },
                });
                ctx.request_repaint();
                return;
            }
        };
        if let Some(u) = outcome.usage {
            usage_sum.prompt += u.prompt;
            usage_sum.completion += u.completion;
            usage_sum.total += u.total;
            usage_sum.cost_usd += u.cost_usd;
            has_usage = true;
        }
        if outcome.tool_calls.iter().all(|t| t.name.is_empty()) {
            let _ = tx.send(DataEvent::ChatDone {
                error: None,
                usage: if has_usage { Some(usage_sum) } else { None },
            });
            ctx.request_repaint();
            return;
        }
        let calls: Vec<serde_json::Value> = outcome
            .tool_calls
            .iter()
            .map(|t| {
                serde_json::json!({
                    "id": t.id, "type": "function",
                    "function": {
                        "name": t.name,
                        "arguments": if t.args.is_empty() { "{}".to_string() } else { t.args.clone() }
                    }
                })
            })
            .collect();
        api_messages.push(serde_json::json!({
            "role": "assistant",
            "content": outcome.content,
            "tool_calls": calls,
        }));
        for t in &outcome.tool_calls {
            if t.name.is_empty() {
                continue;
            }
            let args = serde_json::from_str::<serde_json::Value>(&t.args).unwrap_or_default();
            let (line, result) = execute_chat_tool(&t.name, &args);
            let _ = tx.send(DataEvent::ChatToolLine { line });
            ctx.request_repaint();
            let trunc: String = result.chars().take(6000).collect();
            api_messages.push(serde_json::json!({
                "role": "tool",
                "tool_call_id": t.id,
                "content": trunc,
            }));
        }
    }
    let _ = tx.send(DataEvent::ChatDone {
        error: Some("! stopped: max tool rounds reached".into()),
        usage: if has_usage { Some(usage_sum) } else { None },
    });
    ctx.request_repaint();
}

fn market_system_prompt(
    series: &HashMap<String, Series>,
    specs: &[SeriesSpec],
    range: Range,
    country: Country,
) -> String {
    let mut lines = vec![
        format!(
            "You are the AI research analyst embedded in a personal macroeconomics dashboard (country focus: {}).",
            country.name()
        ),
        format!(
            "Today is {}. Live dashboard snapshot — value, {}-window change, as-of date:",
            chrono::Local::now().format("%Y-%m-%d"),
            range.label()
        ),
    ];
    for spec in specs {
        let Some(s) = series.get(spec.symbol) else { continue };
        let Some(v) = s.latest() else { continue };
        let fmt_chg = |p: Option<f64>| match p {
            Some(x) => format!("{:+.2}%", x),
            None => "n/a".into(),
        };
        let day = fmt_chg(s.day_change_pct());
        let m1 = fmt_chg(s.change_pct(30));
        let y1 = fmt_chg(s.change_pct(365));
        let rng = fmt_chg(s.change_pct(range.days()));
        let date = s
            .latest_date()
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default();
        let unit = if s.unit.is_empty() {
            String::new()
        } else {
            format!(" {}", s.unit)
        };
        lines.push(format!(
            "• {} — {}{} | 1D {} | 1M {} | 1Y {} | view({}) {} | as of {}",
            s.title,
            fmt_value(v),
            unit,
            day,
            m1,
            y1,
            range.label(),
            rng,
            date
        ));
    }
    lines.push(
        "These are the live prices fetched from the data APIs (FRED, ECB/Frankfurter, Kraken, LBMA; mostly daily EOD, some intraday). Use them directly when asked about current levels. For anything more time-sensitive or beyond this snapshot, use the web_search and web_fetch tools before answering. Be concise, quantitative, and cite URLs you fetched.".into(),
    );
    lines.join("\n")
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

const DB_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS observations (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (symbol, date)
);
CREATE TABLE IF NOT EXISTS series_meta (
    symbol TEXT PRIMARY KEY,
    last_fetched TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    reasoning TEXT NOT NULL DEFAULT '',
    tool_log TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
";

fn db_path() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    std::path::PathBuf::from(home).join(".economy.db")
}

fn config_get(conn: &Connection, key: &str) -> String {
    conn.query_row(
        "SELECT value FROM config WHERE key = ?1",
        rusqlite::params![key],
        |row| row.get(0),
    )
    .unwrap_or_default()
}

fn config_set(conn: &Connection, key: &str, value: &str) {
    conn.execute(
        "INSERT INTO config (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )
    .ok();
}

fn db_load_series(conn: &Connection, symbol: &str) -> Vec<(NaiveDate, f64)> {
    let mut stmt = match conn.prepare("SELECT date, value FROM observations WHERE symbol = ?1 ORDER BY date") {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    stmt.query_map(rusqlite::params![symbol], |row| {
        let date: String = row.get(0)?;
        let value: f64 = row.get(1)?;
        Ok((NaiveDate::parse_from_str(&date, "%Y-%m-%d").ok(), value))
    })
    .map(|rows| {
        rows.filter_map(|r| r.ok())
            .filter_map(|(d, v)| d.map(|d| (d, v)))
            .collect()
    })
    .unwrap_or_default()
}

fn db_save_series(conn: &Connection, symbol: &str, obs: &[(NaiveDate, f64)]) {
    conn.execute_batch("BEGIN IMMEDIATE").ok();
    for (date, value) in obs {
        conn.execute(
            "INSERT INTO observations (symbol, date, value) VALUES (?1, ?2, ?3)
             ON CONFLICT(symbol, date) DO UPDATE SET value = excluded.value",
            rusqlite::params![symbol, date.format("%Y-%m-%d").to_string(), value],
        )
        .ok();
    }
    let now = chrono::Local::now().to_rfc3339();
    conn.execute(
        "INSERT INTO series_meta (symbol, last_fetched) VALUES (?1, ?2)
         ON CONFLICT(symbol) DO UPDATE SET last_fetched = excluded.last_fetched",
        rusqlite::params![symbol, now],
    )
    .ok();
    conn.execute_batch("COMMIT").ok();
}

fn db_save_chat_message(
    conn: &Connection,
    role: &str,
    content: &str,
    reasoning: &str,
    tool_log: &[String],
) {
    let now = chrono::Local::now().to_rfc3339();
    conn.execute(
        "INSERT INTO chat_messages (role, content, reasoning, tool_log, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![role, content, reasoning, tool_log.join("\n"), now],
    )
    .ok();
}

fn db_load_chat(conn: &Connection) -> Vec<ChatMessage> {
    let Ok(mut stmt) =
        conn.prepare("SELECT role, content, reasoning, tool_log FROM chat_messages ORDER BY id")
    else {
        return Vec::new();
    };
    stmt.query_map([], |row| {
        let role: String = row.get(0)?;
        let content: String = row.get(1)?;
        let reasoning: String = row.get(2)?;
        let tool_log: String = row.get(3)?;
        Ok(ChatMessage {
            role: if role == "user" {
                ChatRole::User
            } else {
                ChatRole::Assistant
            },
            content,
            reasoning,
            tool_log: tool_log
                .lines()
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        })
    })
    .map(|rows| rows.filter_map(|r| r.ok()).collect())
    .unwrap_or_default()
}

fn db_clear_chat(conn: &Connection) {
    conn.execute("DELETE FROM chat_messages", []).ok();
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

struct WorkerHandle {
    manual_trigger: Arc<AtomicBool>,
}

impl WorkerHandle {
    fn trigger(&self) {
        self.manual_trigger.store(true, Ordering::Relaxed);
    }
}

fn spawn_worker(
    ctx: egui::Context,
    specs: Vec<SeriesSpec>,
    keys: ApiKeys,
    market_refresh: u64,
    db_mutex: Arc<Mutex<Connection>>,
    tx: mpsc::Sender<DataEvent>,
) -> WorkerHandle {
    let stop_flag = Arc::new(AtomicBool::new(false));
    let manual_trigger = Arc::new(AtomicBool::new(false));
    let handle = WorkerHandle {
        manual_trigger: manual_trigger.clone(),
    };

    let mut last_history: HashMap<String, std::time::Instant> = HashMap::new();
    let mut last_quotes = std::time::Instant::now()
        .checked_sub(Duration::from_secs(24 * 60 * 60))
        .unwrap_or_else(std::time::Instant::now);

    thread::spawn(move || {
        let quote_iv = Duration::from_secs(market_refresh.max(15));
        let hist_iv = Duration::from_secs(6 * 60 * 60);
        let macro_iv = Duration::from_secs(24 * 60 * 60);
        let mut any_event = false;
        loop {
            if stop_flag.load(Ordering::Relaxed) {
                return;
            }
            let manual = manual_trigger.swap(false, Ordering::Relaxed);

            // Live quotes: kraken tickers (crypto + AUD/USD)
            if manual || last_quotes.elapsed() >= quote_iv {
                for spec in specs
                    .iter()
                    .filter(|s| s.market && matches!(s.source, Source::Kraken | Source::Frankfurter))
                {
                    match fetch_kraken_live(spec.symbol) {
                        Ok(price) => {
                            let obs = (chrono::Utc::now().date_naive(), price);
                            let _ = tx.send(DataEvent::Point {
                                symbol: spec.symbol.to_string(),
                                obs,
                            });
                            any_event = true;
                        }
                        Err(e) => {
                            let _ = tx.send(DataEvent::Error {
                                symbol: spec.symbol.to_string(),
                                message: e,
                            });
                        }
                    }
                }
                last_quotes = std::time::Instant::now();
            }

            // Full history refresh
            for spec in &specs {
                if stop_flag.load(Ordering::Relaxed) {
                    return;
                }
                let iv = if spec.market { hist_iv } else { macro_iv };
                let due = manual
                    || last_history
                        .get(spec.symbol)
                        .map(|t| t.elapsed() >= iv)
                        .unwrap_or(true);
                if !due {
                    continue;
                }
                match fetch_series(spec, &keys, 2015) {
                    Ok(obs) => {
                        if let Ok(conn) = db_mutex.lock() {
                            db_save_series(&conn, spec.symbol, &obs);
                        }
                        let _ = tx.send(DataEvent::SeriesUpdated {
                            symbol: spec.symbol.to_string(),
                            obs,
                        });
                        any_event = true;
                    }
                    Err(e) => {
                        let _ = tx.send(DataEvent::Error {
                            symbol: spec.symbol.to_string(),
                            message: e,
                        });
                    }
                }
                last_history.insert(spec.symbol.to_string(), std::time::Instant::now());
            }
            if manual {
                let _ = tx.send(DataEvent::Status("Refresh complete".into()));
                any_event = true;
            }
            // Only wake the UI when something actually happened
            if any_event {
                any_event = false;
                ctx.request_repaint();
            }
            thread::sleep(Duration::from_secs(1));
        }
    });

    handle
}

// ---------------------------------------------------------------------------
// UI helpers
// ---------------------------------------------------------------------------

fn change_badge(ui: &mut egui::Ui, pct: Option<f64>) {
    match pct {
        Some(p) if p >= 0.0 => {
            ui.colored_label(egui::Color32::from_rgb(76, 175, 80), format!("+{:.2}%", p));
        }
        Some(p) => {
            ui.colored_label(egui::Color32::from_rgb(244, 67, 54), format!("{:.2}%", p));
        }
        None => {
            ui.colored_label(egui::Color32::GRAY, "—");
        }
    }
}

fn thousands(v: f64) -> String {
    let s = format!("{:.0}", v);
    let (sign, digits) = if let Some(stripped) = s.strip_prefix('-') {
        ("-", stripped)
    } else {
        ("", s.as_str())
    };
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    format!("{}{}", sign, out)
}

fn fmt_value(v: f64) -> String {
    if v >= 1000.0 {
        thousands(v)
    } else if v >= 10.0 {
        format!("{:.2}", v)
    } else {
        format!("{:.4}", v)
    }
}

fn fmt_usd(v: f64) -> String {
    if v >= 1.0 {
        format!("${:.2}", v)
    } else {
        format!("${:.4}", v)
    }
}

fn plot_line(ui: &mut egui::Ui, id: &str, obs: &[(NaiveDate, f64)], width: f32, height: f32) {
    egui_plot::Plot::new(id)
        .width(width)
        .height(height)
        .allow_drag(false)
        .allow_scroll(false)
        .allow_zoom(false)
        .show_axes([false, true])
        .show_background(false)
        .show(ui, |plot_ui| {
            let points: Vec<[f64; 2]> = obs
                .iter()
                .map(|(d, v)| [d.num_days_from_ce() as f64, *v])
                .collect();
            plot_ui.line(
                egui_plot::Line::new(id, egui_plot::PlotPoints::from(points))
                    .color(egui::Color32::from_rgb(33, 150, 243))
                    .width(1.5),
            );
        });
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Range {
    M1,
    M6,
    Y1,
    Y5,
    Max,
}

impl Range {
    fn label(&self) -> &'static str {
        match self {
            Range::M1 => "1M",
            Range::M6 => "6M",
            Range::Y1 => "1Y",
            Range::Y5 => "5Y",
            Range::Max => "Max",
        }
    }

    fn days(&self) -> u32 {
        match self {
            Range::M1 => 30,
            Range::M6 => 183,
            Range::Y1 => 365,
            Range::Y5 => 1826,
            Range::Max => 0,
        }
    }
}

enum DialogState {
    None,
    Settings {
        fred_key: String,
        llm_base_url: String,
        llm_key: String,
        llm_model: String,
        refresh_mins: String,
    },
    About,
}

struct MacroApp {
    country: Country,
    selected_country: Country,
    series: HashMap<String, Series>,
    specs: Vec<SeriesSpec>,
    db: Arc<Mutex<Connection>>,
    data_rx: mpsc::Receiver<DataEvent>,
    chat_tx: mpsc::Sender<DataEvent>,
    worker: WorkerHandle,
    chat: ChatState,
    chat_maximized: bool,
    llm_base_url: String,
    llm_key: String,
    status_text: String,
    dialog_state: DialogState,
    range: Range,
    last_updated: String,
}

impl MacroApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Result<Self, String> {
        let conn = Connection::open(db_path()).map_err(|e| e.to_string())?;
        conn.execute_batch(DB_SCHEMA).map_err(|e| e.to_string())?;

        let country = Country::Australia;
        let specs = registry(country);
        let mut series = HashMap::new();
        for spec in &specs {
            let obs = db_load_series(&conn, spec.symbol);
            series.insert(
                spec.symbol.to_string(),
                Series {
                    title: spec.title.to_string(),
                    unit: spec.unit.to_string(),
                    source_name: spec.source.name().to_string(),
                    obs,
                    error: None,
                },
            );
        }

        let keys = ApiKeys {
            fred: config_get(&conn, "fred_key"),
        };
        let selected_country = Country::from_name(&config_get(&conn, "country"));
        let refresh_mins: u64 = config_get(&conn, "refresh_mins").parse().unwrap_or(1);
        let range = match config_get(&conn, "range").as_str() {
            "1M" => Range::M1,
            "6M" => Range::M6,
            "1Y" => Range::Y1,
            "5Y" => Range::Y5,
            _ => Range::Y1,
        };
        let llm_base_url = {
            let u = config_get(&conn, "llm_base_url");
            if u.trim().is_empty() {
                "https://openrouter.ai/api/v1".to_string()
            } else {
                u
            }
        };
        let llm_key = config_get(&conn, "llm_key");
        let stored_model = config_get(&conn, "llm_model");
        let chat = ChatState {
            messages: db_load_chat(&conn),
            model: if stored_model.is_empty() || MODEL_PRESETS.contains(&stored_model.as_str()) {
                if stored_model.is_empty() {
                    MODEL_PRESETS[0].to_string()
                } else {
                    stored_model.clone()
                }
            } else {
                "custom…".to_string()
            },
            model_custom: if stored_model.is_empty() || MODEL_PRESETS.contains(&stored_model.as_str()) {
                String::new()
            } else {
                stored_model
            },
            ..Default::default()
        };

        let db = Arc::new(Mutex::new(conn));
        let (chat_tx, data_rx) = mpsc::channel();
        let worker = spawn_worker(
            _cc.egui_ctx.clone(),
            specs.clone(),
            keys,
            refresh_mins * 60,
            db.clone(),
            chat_tx.clone(),
        );

        Ok(Self {
            country,
            selected_country,
            series,
            specs,
            db,
            data_rx,
            chat_tx,
            worker,
            chat,
            chat_maximized: false,
            llm_base_url,
            llm_key,
            status_text: "Ready".into(),
            dialog_state: DialogState::None,
            range,
            last_updated: String::new(),
        })
    }

    fn drain_events(&mut self) {
        while let Ok(event) = self.data_rx.try_recv() {
            match event {
                DataEvent::SeriesUpdated { symbol, obs } => {
                    if let Some(s) = self.series.get_mut(&symbol) {
                        s.obs = obs;
                        s.error = None;
                    }
                    self.last_updated = chrono::Local::now().format("%H:%M:%S").to_string();
                }
                DataEvent::Point { symbol, obs } => {
                    if let Some(s) = self.series.get_mut(&symbol) {
                        match s.obs.last_mut() {
                            Some(last) if last.0 == obs.0 => last.1 = obs.1,
                            Some(last) if last.0 < obs.0 => s.obs.push(obs),
                            Some(_) => {}
                            None => s.obs.push(obs),
                        }
                        if let Ok(conn) = self.db.lock() {
                            db_save_series(&conn, &symbol, std::slice::from_ref(&obs));
                        }
                    }
                    self.last_updated = chrono::Local::now().format("%H:%M:%S").to_string();
                }
                DataEvent::Error { symbol, message } => {
                    if let Some(s) = self.series.get_mut(&symbol) {
                        s.error = Some(message.clone());
                    }
                    self.status_text = format!("{}: {}", symbol, message);
                }
                DataEvent::Status(msg) => {
                    self.status_text = msg;
                }
                DataEvent::ChatDelta { content } => {
                    self.chat.stream_content.push_str(&content);
                }
                DataEvent::ChatReasoningDelta { content } => {
                    self.chat.stream_reasoning.push_str(&content);
                }
                DataEvent::ChatToolLine { line } => {
                    self.chat.stream_tool_lines.push(line);
                }
                DataEvent::ChatDone { error, usage } => {
                    if let Some(u) = usage {
                        self.chat.cost_total += u.cost_usd;
                    }
                    self.chat.usage = usage;
                    match error {
                        Some(e) => self.chat.error = Some(e),
                        None => {
                            let msg = ChatMessage {
                                role: ChatRole::Assistant,
                                content: std::mem::take(&mut self.chat.stream_content),
                                reasoning: std::mem::take(&mut self.chat.stream_reasoning),
                                tool_log: std::mem::take(&mut self.chat.stream_tool_lines),
                            };
                            self.chat.messages.push(msg.clone());
                            if let Ok(conn) = self.db.lock() {
                                db_save_chat_message(
                                    &conn,
                                    "assistant",
                                    &msg.content,
                                    &msg.reasoning,
                                    &msg.tool_log,
                                );
                            }
                        }
                    }
                    self.chat.busy = false;
                }
            }
        }
    }

    fn send_chat(&mut self, ctx: egui::Context) {
        if self.chat.busy {
            return;
        }
        let text = self.chat.input.trim().to_string();
        if text.is_empty() {
            return;
        }
        self.chat.input.clear();
        self.chat.error = None;
        self.chat.stream_content.clear();
        self.chat.stream_reasoning.clear();
        self.chat.stream_tool_lines.clear();
        self.chat
            .messages
            .push(ChatMessage {
                role: ChatRole::User,
                content: text.clone(),
                reasoning: String::new(),
                tool_log: Vec::new(),
            });
        if let Ok(conn) = self.db.lock() {
            db_save_chat_message(&conn, "user", &text, "", &[]);
            config_set(&conn, "llm_model", &resolve_model(&self.chat));
        }
        let model = resolve_model(&self.chat);
        if self.llm_key.trim().is_empty() {
            self.chat.error = Some("* no LLM API key — Settings".into());
            return;
        }
        let cfg = LlmConfig {
            base_url: self.llm_base_url.clone(),
            key: self.llm_key.clone(),
            model,
        };
        let history: Vec<ChatMessage> = self.chat.messages[..self.chat.messages.len() - 1].to_vec();
        let system = market_system_prompt(&self.series, &self.specs, self.range, self.country);
        let tx = self.chat_tx.clone();
        self.chat.busy = true;
        thread::spawn(move || {
            run_chat_agent(ctx, tx, cfg, system, history, text);
        });
    }

    fn clear_chat(&mut self) {
        self.chat.messages.clear();
        self.chat.error = None;
        self.chat.usage = None;
        self.chat.cost_total = 0.0;
        self.chat.stream_content.clear();
        self.chat.stream_reasoning.clear();
        self.chat.stream_tool_lines.clear();
        if let Ok(conn) = self.db.lock() {
            db_clear_chat(&conn);
        }
    }

    fn set_range(&mut self, range: Range) {
        self.range = range;
        if let Ok(conn) = self.db.lock() {
            config_set(&conn, "range", range.label());
        }
    }

    fn stat_card(ui: &mut egui::Ui, s: &Series, range: Range, plot_w: f32, plot_h: f32) {
        egui::Frame::group(ui.style())
            .stroke(egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(80, 80, 80)))
            .inner_margin(egui::Margin::same(8))
            .show(ui, |ui| {
                ui.set_min_width(plot_w);
                ui.set_max_width(plot_w);
                ui.vertical(|ui| {
                    ui.horizontal(|ui| {
                        ui.set_max_width(plot_w);
                        ui.label(egui::RichText::new(&s.title).strong());
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            change_badge(ui, s.change_pct(range.days()));
                        });
                    });
                    match s.latest() {
                        Some(v) => {
                            let text = if s.unit.is_empty() {
                                fmt_value(v)
                            } else {
                                format!("{} {}", fmt_value(v), s.unit)
                            };
                            ui.label(egui::RichText::new(text).size(22.0).strong());
                        }
                        None => {
                            ui.label(egui::RichText::new("…").size(22.0).strong());
                        }
                    }
                    ui.label(
                        egui::RichText::new(format!(
                            "{}  ·  {}",
                            s.latest_date()
                                .map(|d| d.format("%Y-%m-%d").to_string())
                                .unwrap_or_else(|| "no date".into()),
                            s.source_name
                        ))
                        .small()
                        .color(egui::Color32::GRAY),
                    );
                    if s.obs.len() > 1 {
                        plot_line(ui, &format!("spark_{}", s.title), s.slice_range(range.days()), plot_w, plot_h);
                    } else if let Some(ref err) = s.error {
                        ui.label(
                            egui::RichText::new(err)
                                .small()
                                .color(egui::Color32::from_rgb(244, 67, 54)),
                        );
                    } else {
                        ui.label(egui::RichText::new("loading…").small().color(egui::Color32::GRAY));
                    }
                });
            });
    }

    fn chat_message_ui(
        ui: &mut egui::Ui,
        idx: usize,
        msg: &ChatMessage,
        streaming: bool,
        cache: &mut egui_commonmark::CommonMarkCache,
    ) {
        ui.vertical(|ui| {
            for line in &msg.tool_log {
                ui.label(
                    egui::RichText::new(line)
                        .small()
                        .color(egui::Color32::from_rgb(120, 120, 120)),
                );
            }
            if !msg.reasoning.is_empty() {
                let mut header = egui::CollapsingHeader::new(
                    egui::RichText::new("* thinking")
                        .small()
                        .color(egui::Color32::from_rgb(120, 120, 120)),
                )
                .id_salt(format!("think_{}", idx))
                .default_open(false);
                if streaming {
                    header = header.open(Some(true));
                }
                header.show(ui, |ui| {
                    ui.label(
                        egui::RichText::new(&msg.reasoning)
                            .small()
                            .color(egui::Color32::from_rgb(130, 130, 130)),
                    );
                });
            }
            if !msg.content.is_empty() {
                let (prefix, color) = match msg.role {
                    ChatRole::User => ("* you", egui::Color32::from_rgb(235, 235, 235)),
                    ChatRole::Assistant => ("* ai", egui::Color32::from_rgb(33, 150, 243)),
                };
                if msg.role == ChatRole::User {
                    ui.horizontal_wrapped(|ui| {
                        ui.label(egui::RichText::new(prefix).strong().color(color));
                        ui.label(&msg.content);
                    });
                } else {
                    ui.label(egui::RichText::new(prefix).strong().color(color));
                    egui_commonmark::CommonMarkViewer::new().show(ui, cache, &msg.content);
                }
            }
        });
    }

    fn chat_panel(
        ui: &mut egui::Ui,
        chat: &mut ChatState,
        maximized: bool,
        width: f32,
        height: f32,
        send: &mut bool,
        clear: &mut bool,
        toggle_max: &mut bool,
    ) {
        egui::Frame::group(ui.style())
            .stroke(egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(80, 80, 80)))
            .inner_margin(egui::Margin::same(8))
            .show(ui, |ui| {
                ui.set_min_width(width);
                ui.set_max_width(width);
                ui.set_min_height((height - 16.0).max(80.0));
                ui.vertical(|ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new("* AI RESEARCH")
                                .strong()
                                .color(egui::Color32::from_rgb(33, 150, 243)),
                        );
                        let selected: String = if MODEL_PRESETS.contains(&chat.model.as_str()) {
                            chat.model.clone()
                        } else {
                            "custom…".to_string()
                        };
                        egui::ComboBox::new("chat_model", selected.clone())
                            .selected_text(selected)
                            .width(150.0)
                            .show_ui(ui, |ui| {
                                for m in MODEL_PRESETS {
                                    ui.selectable_value(&mut chat.model, m.to_string(), *m);
                                }
                                ui.selectable_value(&mut chat.model, "custom…".to_string(), "custom…");
                            });
                        if !MODEL_PRESETS.contains(&chat.model.as_str()) {
                            ui.add(
                                egui::TextEdit::singleline(&mut chat.model_custom)
                                    .desired_width(110.0)
                                    .hint_text("model id"),
                            );
                        }
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if chat.cost_total > 0.0 {
                                ui.label(
                                    egui::RichText::new(fmt_usd(chat.cost_total))
                                        .small()
                                        .color(egui::Color32::from_rgb(120, 120, 120)),
                                )
                                .on_hover_text("total cost since cleared (USD)");
                            }
                            if let Some(u) = chat.usage {
                                ui.label(
                                    egui::RichText::new(format!("{} tok", thousands(u.total as f64)))
                                        .small()
                                        .color(egui::Color32::from_rgb(120, 120, 120)),
                                )
                                .on_hover_text(format!(
                                    "last request: in {} / out {} / total {} tokens / cost {}",
                                    thousands(u.prompt as f64),
                                    thousands(u.completion as f64),
                                    thousands(u.total as f64),
                                    fmt_usd(u.cost_usd)
                                ));
                            }
                            let max_label = if maximized { "v" } else { "^" };
                            let max_tip = if maximized {
                                "restore dashboard"
                            } else {
                                "expand chat to full window"
                            };
                            if ui
                                .small_button(max_label)
                                .on_hover_text(max_tip)
                                .clicked()
                            {
                                *toggle_max = true;
                            }
                            if ui
                                .small_button("x")
                                .on_hover_text("clear conversation")
                                .clicked()
                            {
                                *clear = true;
                            }
                            if chat.busy {
                                ui.label(
                                    egui::RichText::new("* working")
                                        .small()
                                        .color(egui::Color32::from_rgb(255, 152, 0)),
                                );
                            }
                        });
                    });
                    ui.separator();
                    let hist_h = (ui.available_height() - 40.0).max(60.0);
                    ui.style_mut().url_in_tooltip = true;
                    egui::ScrollArea::vertical()
                        .id_salt("chat_history")
                        .stick_to_bottom(true)
                        .max_height(hist_h)
                        .show(ui, |ui| {
                            let base = chat.messages.len();
                            for (i, msg) in chat.messages.iter().enumerate() {
                                Self::chat_message_ui(ui, i, msg, false, &mut chat.md_cache);
                            }
                            let live_active = chat.busy
                                || !chat.stream_content.is_empty()
                                || !chat.stream_reasoning.is_empty()
                                || !chat.stream_tool_lines.is_empty();
                            if live_active {
                                let live = ChatMessage {
                                    role: ChatRole::Assistant,
                                    content: chat.stream_content.clone(),
                                    reasoning: chat.stream_reasoning.clone(),
                                    tool_log: chat.stream_tool_lines.clone(),
                                };
                                Self::chat_message_ui(ui, base, &live, true, &mut chat.md_cache);
                                let dots = ".".repeat(((ui.ctx().time() * 2.0) as usize) % 4);
                                ui.label(
                                    egui::RichText::new(format!("* thinking{}", dots))
                                        .small()
                                        .color(egui::Color32::from_rgb(255, 152, 0)),
                                );
                            }
                            if let Some(err) = &chat.error {
                                ui.label(
                                    egui::RichText::new(format!("! {}", err))
                                        .small()
                                        .color(egui::Color32::from_rgb(244, 67, 54)),
                                );
                            }
                        });
                    ui.add_space(4.0);
                    ui.horizontal(|ui| {
                        let enter_send = {
                            let resp = ui.add(
                                egui::TextEdit::singleline(&mut chat.input)
                                    .desired_width((width - 74.0).max(80.0))
                                    .hint_text("ask · research · analyse…"),
                            );
                            let enter =
                                resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                            if enter {
                                resp.request_focus();
                            }
                            enter
                        };
                        let send_clicked = ui.add(egui::Button::new("* send")).clicked();
                        if (enter_send || send_clicked) && !chat.busy && !chat.input.trim().is_empty() {
                            *send = true;
                        }
                    });
                });
            });
    }

    fn show_title_bar(&mut self, ui: &mut egui::Ui) {
        let ctx = ui.ctx().clone();
        egui::Panel::top("title_bar").show(ui, |ui| {
            let available = ui.available_rect_before_wrap();
            let drag_rect = available.intersect(ui.max_rect());
            let drag_response = ui.interact(drag_rect, ui.id().with("drag_area"), egui::Sense::drag());

            if drag_response.dragged() {
                ctx.send_viewport_cmd(egui::ViewportCommand::StartDrag);
            }

            egui::MenuBar::new().ui(ui, |ui| {
                ui.label(egui::RichText::new("marketeer").strong().size(14.0));
                ui.add_space(16.0);
                ui.menu_button("File", |ui| {
                    if ui.button("Refresh All").clicked() {
                        self.worker.trigger();
                        self.status_text = "Refreshing…".into();
                        ui.close();
                    }
                    ui.separator();
                    if ui.button("Exit").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
                ui.menu_button("Settings", |ui| {
                    if ui.button("API Keys / Refresh").clicked() {
                        let (fred_key, llm_base_url, llm_key, llm_model, refresh_mins) = self
                            .db
                            .lock()
                            .ok()
                            .map(|conn| {
                                (
                                    config_get(&conn, "fred_key"),
                                    config_get(&conn, "llm_base_url"),
                                    config_get(&conn, "llm_key"),
                                    config_get(&conn, "llm_model"),
                                    config_get(&conn, "refresh_mins"),
                                )
                            })
                            .unwrap_or_else(|| {
                                (
                                    String::new(),
                                    "https://openrouter.ai/api/v1".into(),
                                    String::new(),
                                    String::new(),
                                    "1".into(),
                                )
                            });
                        self.dialog_state = DialogState::Settings {
                            fred_key,
                            llm_base_url,
                            llm_key,
                            llm_model,
                            refresh_mins,
                        };
                        ui.close();
                    }
                });
                ui.menu_button("Help", |ui| {
                    if ui.button("About").clicked() {
                        self.dialog_state = DialogState::About;
                        ui.close();
                    }
                });

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Close").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                    if ui.button("Minimize").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
                    }
                    if ui.button("Maximize").clicked() {
                        let maximized = ui.input(|i| i.viewport().maximized.unwrap_or(false));
                        ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!maximized));
                    }
                });
            });
        });
    }

    fn show_controls(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("controls").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Dashboard");
                let combo = egui::ComboBox::new("country_select", self.selected_country.name())
                    .selected_text(self.selected_country.name())
                    .width(140.0)
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut self.selected_country, Country::Australia, "Australia");
                        ui.selectable_value(&mut self.selected_country, Country::Usa, "USA");
                        ui.selectable_value(&mut self.selected_country, Country::China, "China");
                        ui.selectable_value(&mut self.selected_country, Country::Japan, "Japan");
                        ui.selectable_value(&mut self.selected_country, Country::India, "India");
                        ui.selectable_value(&mut self.selected_country, Country::Uk, "United Kingdom");
                        ui.selectable_value(&mut self.selected_country, Country::Germany, "Germany");
                        ui.selectable_value(&mut self.selected_country, Country::Canada, "Canada");
                        ui.selectable_value(&mut self.selected_country, Country::Russia, "Russia");
                        ui.selectable_value(&mut self.selected_country, Country::SouthKorea, "South Korea");
                    });
                if combo.response.changed() {
                    if let Ok(conn) = self.db.lock() {
                        config_set(&conn, "country", self.selected_country.name());
                    }
                    if self.selected_country != self.country {
                        self.status_text = format!(
                            "{} dashboard coming soon — showing Australia",
                            self.selected_country.name()
                        );
                    } else {
                        self.status_text = "Ready".into();
                    }
                }
                ui.add_space(10.0);
                if ui.button("Refresh").clicked() {
                    self.worker.trigger();
                    self.status_text = "Refreshing…".into();
                }
                ui.separator();
                ui.label("Range:");
                for r in [Range::M1, Range::M6, Range::Y1, Range::Y5, Range::Max] {
                    if ui
                        .selectable_label(self.range == r, r.label())
                        .clicked()
                    {
                        self.set_range(r);
                    }
                }
            });
        });
    }

    fn show_status_bar(&mut self, ui: &mut egui::Ui) {
        egui::Panel::bottom("status_bar").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new(&self.status_text).small());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.colored_label(
                        egui::Color32::GRAY,
                        egui::RichText::new(format!(
                            "{}v0.1.0",
                            if self.last_updated.is_empty() {
                                String::new()
                            } else {
                                format!("Updated {}  ·  ", self.last_updated)
                            }
                        ))
                        .small(),
                    );
                });
            });
        });
    }
}

impl eframe::App for MacroApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.drain_events();

        self.show_title_bar(ui);
        self.show_status_bar(ui);
        self.show_controls(ui);

        let panel_frame = {
            let style = ui.style();
            egui::Frame::central_panel(style).inner_margin(egui::Margin::same(10))
        };
        let mut chat_send = false;
        let mut chat_clear = false;
        let mut chat_toggle = false;
        egui::CentralPanel::default().frame(panel_frame).show(ui, |ui| {
            let specs = self.specs.clone();
            let series = &self.series;
            let chat = &mut self.chat;
            let range = self.range;
            let chat_maximized = self.chat_maximized;
            let spacing = 12.0;
            if chat_maximized {
                let chat_w = ui.available_width() - 22.0;
                let chat_h = ui.available_height() - 2.0;
                MacroApp::chat_panel(
                    ui,
                    chat,
                    true,
                    chat_w,
                    chat_h,
                    &mut chat_send,
                    &mut chat_clear,
                    &mut chat_toggle,
                );
                return;
            }
            let cols = 3usize;
            let total_rows = specs.len().div_ceil(cols);
            let avail_w = ui.available_width();
            let avail_h = ui.available_height();
            let card_w = (((avail_w - spacing * (cols as f32 - 1.0)) / cols as f32) - 2.0).max(180.0);
            let card_h = ((avail_h - spacing * (total_rows as f32 - 1.0)) / total_rows as f32).max(140.0);
            let plot_w = card_w - 16.0;
            let plot_h = (card_h - 84.0).max(30.0);
            let grid_specs = &specs[..specs.len().saturating_sub(1).min(9)];
            egui::Grid::new("dashboard_grid")
                .num_columns(cols)
                .spacing([spacing, spacing])
                .min_col_width(card_w)
                .show(ui, |ui| {
                    for (i, spec) in grid_specs.iter().enumerate() {
                        let s = series
                            .get(spec.symbol)
                            .cloned()
                            .unwrap_or_default();
                        MacroApp::stat_card(ui, &s, range, plot_w, plot_h);
                        if i % cols == cols - 1 {
                            ui.end_row();
                        }
                    }
                });
            ui.add_space(spacing);
            ui.horizontal(|ui| {
                if let Some(last) = specs.last() {
                    let s = series
                        .get(last.symbol)
                        .cloned()
                        .unwrap_or_default();
                    MacroApp::stat_card(ui, &s, range, plot_w, plot_h);
                }
                ui.add_space(spacing);
                let chat_w = 2.0 * card_w + spacing - 16.0;
                MacroApp::chat_panel(
                    ui,
                    chat,
                    false,
                    chat_w,
                    card_h,
                    &mut chat_send,
                    &mut chat_clear,
                    &mut chat_toggle,
                );
            });
        });

        match std::mem::replace(&mut self.dialog_state, DialogState::None) {
            DialogState::None => {}
            DialogState::Settings {
                mut fred_key,
                mut llm_base_url,
                mut llm_key,
                mut llm_model,
                mut refresh_mins,
            } => {
                let mut open = true;
                let mut save = false;
                egui::Window::new("Settings")
                    .collapsible(false)
                    .resizable(false)
                    .open(&mut open)
                    .show(&ctx, |ui| {
                        ui.label(egui::RichText::new("DATA").strong());
                        ui.label("FRED API Key:");
                        ui.add(
                            egui::TextEdit::singleline(&mut fred_key)
                                .desired_width(320.0)
                                .password(true),
                        );
                        ui.add_space(8.0);
                        ui.separator();
                        ui.label(egui::RichText::new("AI").strong());
                        ui.label("LLM API Base URL:");
                        ui.add(
                            egui::TextEdit::singleline(&mut llm_base_url)
                                .desired_width(320.0)
                                .hint_text("https://openrouter.ai/api/v1"),
                        );
                        ui.add_space(4.0);
                        ui.label("LLM API Key:");
                        ui.add(
                            egui::TextEdit::singleline(&mut llm_key)
                                .desired_width(320.0)
                                .password(true),
                        );
                        ui.add_space(4.0);
                        ui.label("Default Model:");
                        ui.add(
                            egui::TextEdit::singleline(&mut llm_model)
                                .desired_width(320.0)
                                .hint_text("openai/gpt-4o-mini"),
                        );
                        ui.add_space(8.0);
                        ui.separator();
                        ui.label("Refresh interval (minutes):");
                        ui.add(egui::TextEdit::singleline(&mut refresh_mins).desired_width(80.0));
                        ui.add_space(8.0);
                        if ui.button("Save").clicked() {
                            save = true;
                        }
                    });
                if save {
                    if let Ok(conn) = self.db.lock() {
                        config_set(&conn, "fred_key", fred_key.trim());
                        config_set(&conn, "llm_base_url", llm_base_url.trim());
                        config_set(&conn, "llm_key", llm_key.trim());
                        config_set(&conn, "llm_model", llm_model.trim());
                        config_set(&conn, "refresh_mins", refresh_mins.trim());
                    }
                    self.llm_base_url = if llm_base_url.trim().is_empty() {
                        "https://openrouter.ai/api/v1".into()
                    } else {
                        llm_base_url.trim().to_string()
                    };
                    self.llm_key = llm_key.trim().to_string();
                    self.status_text = "Settings saved (restart to apply)".into();
                } else if open {
                    self.dialog_state = DialogState::Settings {
                        fred_key,
                        llm_base_url,
                        llm_key,
                        llm_model,
                        refresh_mins,
                    };
                }
            }
            DialogState::About => {
                let mut open = true;
                egui::Window::new("About")
                    .collapsible(false)
                    .resizable(false)
                    .open(&mut open)
                    .show(&ctx, |ui| {
                        ui.label("Marketeer v1.0");
                        ui.label("Data: Frankfurter (ECB), FRED, Kraken, LBMA");
                    });
                if open {
                    self.dialog_state = DialogState::About;
                }
            }
        }

        // Deferred chat actions from the panel
        if chat_clear {
            self.clear_chat();
        }
        if chat_toggle {
            self.chat_maximized = !self.chat_maximized;
        }
        if chat_send {
            self.send_chat(ctx.clone());
        }
        // Animated "thinking…" dots need periodic repaints while busy
        if self.chat.busy {
            ctx.request_repaint_after(Duration::from_millis(300));
        }

        // No unconditional repaint: the UI only redraws on input events or when
        // the worker delivers new data (worker calls ctx.request_repaint()).
    }
}

fn install_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        "ibm-plex-mono".into(),
        Arc::new(egui::FontData::from_static(include_bytes!(
            "../assets/fonts/IBMPlexMono-Regular.ttf"
        ))),
    );
    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
        fonts
            .families
            .get_mut(&family)
            .expect("font family exists")
            .insert(0, "ibm-plex-mono".into());
    }
    ctx.set_fonts(fonts);
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 700.0])
            .with_maximized(true)
            .with_title("marketeer")
            .with_decorations(false),
        ..Default::default()
    };
    eframe::run_native(
        "Marketeer",
        options,
        Box::new(|cc| {
            install_fonts(&cc.egui_ctx);
            match MacroApp::new(cc) {
                Ok(app) => Ok(Box::new(app)),
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }),
    )
}
