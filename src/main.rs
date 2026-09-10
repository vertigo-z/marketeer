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
    intraday: Vec<(f64, f64)>,
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

    fn year_stats(&self) -> Option<(f64, f64, f64, f64, f64)> {
        let mut vals: Vec<f64> = self.obs.iter().rev().take(365).map(|o| o.1).collect();
        if vals.len() < 30 {
            return None;
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let pick = |i: usize| vals[i.min(vals.len() - 1)];
        Some((
            vals[0],
            pick(vals.len() / 4),
            pick(vals.len() / 2),
            pick(vals.len() * 3 / 4),
            vals[vals.len() - 1],
        ))
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
    IntradayUpdated { symbol: String, points: Vec<(f64, f64)> },
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

fn parse_kraken_ohlc_ts(body: &str) -> Vec<(f64, f64)> {
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
        out.push((719_163.0 + time as f64 / 86400.0, close));
    }
    out
}

fn fetch_kraken_intraday(pair: &str) -> Vec<(f64, f64)> {
    let mut merged: Vec<(f64, f64)> = Vec::new();
    for interval in [5u64, 60] {
        let url = format!(
            "https://api.kraken.com/0/public/OHLC?pair={}&interval={}&since=0",
            pair, interval
        );
        if let Ok(body) = http_get(&url) {
            merged.extend(parse_kraken_ohlc_ts(&body));
        }
        thread::sleep(Duration::from_millis(150));
    }
    merged.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    merged.dedup_by(|a, b| a.0 == b.0);
    merged
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
            let pair = spec.symbol.to_uppercase();
            let mut all = fetch_kraken_ohlc_page(&pair, 0)?;
            if all.is_empty() {
                return Err("no data returned".into());
            }
            let base = spec.symbol.trim_end_matches("usd").to_uppercase();
            if let Ok(hist) = fetch_yahoo_daily(&format!("{}-USD", base)) {
                let oldest = all[0].0;
                for (d, v) in hist {
                    if d < oldest {
                        all.push((d, v));
                    }
                }
                all.sort_by_key(|(d, _)| *d);
            }
            Ok(all)
        }
    }
}

fn yahoo_intraday_ticker(symbol: &str) -> Option<&'static str> {
    match symbol {
        "AUDUSD" => Some("AUDUSD=X"),
        "SP500" => Some("^GSPC"),
        "NASDAQCOM" => Some("^IXIC"),
        "DJIA" => Some("^DJI"),
        "GOLD" => Some("GC=F"),
        "SILVER" => Some("SI=F"),
        _ => None,
    }
}

fn fetch_yahoo_intraday(ticker: &str) -> Result<Vec<(f64, f64)>, String> {
    let url = format!(
        "https://query2.finance.yahoo.com/v8/finance/chart/{}?range=7d&interval=60m",
        ticker
    );
    let body = http_get_ua(&url, 30)?;
    let v: serde_json::Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    let result = v["chart"]["result"]
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| "no result".to_string())?;
    let ts = result["timestamp"]
        .as_array()
        .ok_or_else(|| "no timestamps".to_string())?;
    let closes = result["indicators"]["quote"][0]["close"]
        .as_array()
        .ok_or_else(|| "no closes".to_string())?;
    let mut out = Vec::new();
    for (t, c) in ts.iter().zip(closes) {
        let (Some(t), Some(c)) = (t.as_f64(), c.as_f64()) else {
            continue;
        };
        out.push((719_163.0 + t / 86400.0, c));
    }
    Ok(out)
}

fn fetch_yahoo_daily(ticker: &str) -> Result<Vec<(NaiveDate, f64)>, String> {
    let url = format!(
        "https://query2.finance.yahoo.com/v8/finance/chart/{}?range=max&interval=1d",
        ticker
    );
    let body = http_get_ua(&url, 30)?;
    let v: serde_json::Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    let result = v["chart"]["result"]
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| "no result".to_string())?;
    let ts = result["timestamp"]
        .as_array()
        .ok_or_else(|| "no timestamps".to_string())?;
    let closes = result["indicators"]["quote"][0]["close"]
        .as_array()
        .ok_or_else(|| "no closes".to_string())?;
    let mut out = Vec::new();
    for (t, c) in ts.iter().zip(closes) {
        let (Some(t), Some(c)) = (t.as_f64(), c.as_f64()) else {
            continue;
        };
        let Some(date) =
            chrono::DateTime::from_timestamp(t as i64, 0).map(|dt| dt.date_naive())
        else {
            continue;
        };
        out.push((date, c));
    }
    out.sort_by_key(|(d, _)| *d);
    out.dedup_by(|a, b| a.0 == b.0);
    Ok(out)
}

fn fetch_kraken_ohlc_page(pair: &str, since: u64) -> Result<Vec<(NaiveDate, f64)>, String> {
    let url = format!(
        "https://api.kraken.com/0/public/OHLC?pair={}&interval=1440&since={}",
        pair, since
    );
    let body = http_get(&url)?;
    Ok(parse_kraken_ohlc(&body))
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
    usage: Option<Usage>,
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
    stream_content: String,
    stream_reasoning: String,
    stream_tool_lines: Vec<String>,
    cost_total: f64,
    tok_total: u64,
    cancel: Arc<AtomicBool>,
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
            stream_content: String::new(),
            stream_reasoning: String::new(),
            stream_tool_lines: Vec::new(),
            cost_total: 0.0,
            tok_total: 0,
            cancel: Arc::new(AtomicBool::new(false)),
            md_cache: egui_commonmark::CommonMarkCache::default(),
        }
    }
}

fn resolve_model(chat: &ChatState) -> String {
    if chat.model.trim().is_empty() {
        MODEL_PRESETS[0].to_string()
    } else {
        chat.model.trim().to_string()
    }
}

#[derive(Clone)]
struct LlmConfig {
    base_url: String,
    key: String,
    model: String,
    brave_key: String,
}

fn brave_search(key: &str, q: &str) -> Result<String, String> {
    let url = format!(
        "https://api.search.brave.com/res/v1/web/search?q={}&count=8",
        percent_encode_qs(q)
    );
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(20)))
        .build()
        .new_agent();
    let resp = agent
        .get(&url)
        .header("X-Subscription-Token", key)
        .header("Accept", "application/json")
        .call()
        .map_err(|e| e.to_string())?;
    let mut body = String::new();
    resp.into_body()
        .into_reader()
        .read_to_string(&mut body)
        .map_err(|e| e.to_string())?;
    let v: serde_json::Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    let mut out = String::new();
    if let Some(results) = v["web"]["results"].as_array() {
        for (n, r) in results.iter().enumerate() {
            let title = r["title"].as_str().unwrap_or("").trim();
            let link = r["url"].as_str().unwrap_or("").trim();
            let desc = strip_html(r["description"].as_str().unwrap_or(""))
                .trim()
                .to_string();
            if !title.is_empty() && !link.is_empty() {
                out.push_str(&format!("{}. {} — {}\n   {}\n", n + 1, title, link, desc));
            }
        }
    }
    if out.is_empty() {
        out.push_str("(no results parsed — try web_fetch on a known URL)");
    }
    Ok(out)
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

fn execute_chat_tool(name: &str, args: &serde_json::Value, brave_key: &str) -> (String, String) {
    match name {
        "web_search" => {
            let q = args.get("query").and_then(|v| v.as_str()).unwrap_or("");
            let line = format!("↳ web_search \"{}\"", q);
            let key = brave_key.trim();
            let res = if !key.is_empty() {
                brave_search(key, q)
            } else {
                let url = format!(
                    "https://html.duckduckgo.com/html/?q={}",
                    percent_encode_qs(q)
                );
                http_get_ua(&url, 20).map(|b| parse_ddg_results(&b))
            }
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
            "description":"Search the web (Brave Search API when a key is set, else DuckDuckGo). Returns numbered titles, URLs and snippets.",
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
    cancel: &AtomicBool,
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
        if cancel.load(Ordering::Relaxed) {
            break;
        }
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
    cancel: Arc<AtomicBool>,
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
    loop {
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
        let outcome = match stream_chat_completion(&cfg, &body, &tx, &ctx, &cancel) {
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
        if cancel.load(Ordering::Relaxed) {
            let _ = tx.send(DataEvent::ChatDone {
                error: None,
                usage: if has_usage { Some(usage_sum) } else { None },
            });
            ctx.request_repaint();
            return;
        }
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
            let (line, result) = execute_chat_tool(&t.name, &args, &cfg.brave_key);
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
            "Today is {}. Live dashboard snapshot — value, window changes, 52-week range stats, as-of date:",
            chrono::Local::now().format("%Y-%m-%d")
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
        let year_ctx = match s.year_stats() {
            Some((lo, p25, med, p75, hi)) => format!(
                " | 52w low {} / median {} / high {} | typically {} to {} | now {:+.1}% vs 52w high",
                fmt_value(lo),
                fmt_value(med),
                fmt_value(hi),
                fmt_value(p25),
                fmt_value(p75),
                (v / hi - 1.0) * 100.0
            ),
            None => String::new(),
        };
        lines.push(format!(
            "• {} — {}{} | 1D {} | 1M {} | 1Y {} | view({}) {}{} | as of {}",
            s.title,
            fmt_value(v),
            unit,
            day,
            m1,
            y1,
            range.label(),
            rng,
            year_ctx,
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
CREATE TABLE IF NOT EXISTS intraday (
    symbol TEXT NOT NULL,
    t REAL NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (symbol, t)
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
CREATE TABLE IF NOT EXISTS budget_categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    color TEXT NOT NULL DEFAULT '#FF9800',
    sort_order INTEGER NOT NULL DEFAULT 0,
    recurring INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS budget_months (
    category_id INTEGER NOT NULL REFERENCES budget_categories(id) ON DELETE CASCADE,
    month TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (category_id, month)
);
CREATE TABLE IF NOT EXISTS budget_income (
    month TEXT PRIMARY KEY,
    income REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS budget_cumulative (
    month TEXT PRIMARY KEY,
    value REAL NOT NULL DEFAULT 0
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

fn db_load_intraday(conn: &Connection, symbol: &str) -> Vec<(f64, f64)> {
    let mut stmt = match conn.prepare("SELECT t, value FROM intraday WHERE symbol = ?1 ORDER BY t")
    {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    stmt.query_map(rusqlite::params![symbol], |row| {
        let t: f64 = row.get(0)?;
        let value: f64 = row.get(1)?;
        Ok((t, value))
    })
    .map(|rows| rows.filter_map(|r| r.ok()).collect())
    .unwrap_or_default()
}

fn db_save_intraday(conn: &Connection, symbol: &str, points: &[(f64, f64)]) {
    conn.execute_batch("BEGIN IMMEDIATE").ok();
    for (t, value) in points {
        conn.execute(
            "INSERT INTO intraday (symbol, t, value) VALUES (?1, ?2, ?3)
             ON CONFLICT(symbol, t) DO UPDATE SET value = excluded.value",
            rusqlite::params![symbol, t, value],
        )
        .ok();
    }
    let now_x = 719_163.0 + chrono::Utc::now().timestamp() as f64 / 86400.0;
    conn.execute(
        "DELETE FROM intraday WHERE t < ?1",
        rusqlite::params![now_x - 8.0],
    )
    .ok();
    conn.execute_batch("COMMIT").ok();
}

fn db_save_series(conn: &Connection, symbol: &str, obs: &[(NaiveDate, f64)]) {    conn.execute_batch("BEGIN IMMEDIATE").ok();
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
    usage: Option<Usage>,
) {
    let now = chrono::Local::now().to_rfc3339();
    let usage_str = usage
        .map(|u| format!("{}|{}|{}|{}", u.prompt, u.completion, u.total, u.cost_usd))
        .unwrap_or_default();
    conn.execute(
        "INSERT INTO chat_messages (role, content, reasoning, tool_log, usage, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![role, content, reasoning, tool_log.join("\n"), usage_str, now],
    )
    .ok();
}

fn db_load_chat(conn: &Connection) -> Vec<ChatMessage> {
    let Ok(mut stmt) = conn
        .prepare("SELECT role, content, reasoning, tool_log, usage FROM chat_messages ORDER BY id")
    else {
        return Vec::new();
    };
    stmt.query_map([], |row| {
        let role: String = row.get(0)?;
        let content: String = row.get(1)?;
        let reasoning: String = row.get(2)?;
        let tool_log: String = row.get(3)?;
        let usage: String = row.get(4)?;
        let parts: Vec<&str> = usage.split('|').collect();
        let parsed = if parts.len() == 4 {
            Some(Usage {
                prompt: parts[0].parse().unwrap_or(0),
                completion: parts[1].parse().unwrap_or(0),
                total: parts[2].parse().unwrap_or(0),
                cost_usd: parts[3].parse().unwrap_or(0.0),
            })
        } else {
            None
        };
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
            usage: parsed,
        })
    })
    .map(|rows| rows.filter_map(|r| r.ok()).collect())
    .unwrap_or_default()
}

fn db_clear_chat(conn: &Connection) {
    conn.execute("DELETE FROM chat_messages", []).ok();
}

#[derive(Clone)]
struct BudgetCategory {
    id: i64,
    name: String,
    color: String,
    recurring: bool,
}

const BUDGET_DEFAULTS: &[(&str, &str)] = &[
    ("Housing", "#F44336"),
    ("Groceries", "#4CAF50"),
    ("Transport", "#2196F3"),
    ("Utilities", "#FFEB3B"),
    ("Dining", "#FF9800"),
    ("Health", "#E91E63"),
    ("Entertainment", "#9C27B0"),
    ("Subscriptions", "#00BCD4"),
    ("Other", "#9E9E9E"),
];

fn db_seed_budget(conn: &Connection) {
    let _ = conn.execute(
        "ALTER TABLE budget_categories ADD COLUMN recurring INTEGER NOT NULL DEFAULT 1",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE chat_messages ADD COLUMN usage TEXT NOT NULL DEFAULT ''",
        [],
    );
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM budget_categories", [], |r| r.get(0))
        .unwrap_or(0);
    if count > 0 {
        return;
    }
    for (i, (name, color)) in BUDGET_DEFAULTS.iter().enumerate() {
        conn.execute(
            "INSERT INTO budget_categories (name, color, sort_order) VALUES (?1, ?2, ?3)",
            rusqlite::params![name, color, i as i64],
        )
        .ok();
    }
}

fn db_budget_categories(conn: &Connection) -> Vec<BudgetCategory> {
    let Ok(mut stmt) = conn
        .prepare("SELECT id, name, color, recurring FROM budget_categories ORDER BY sort_order, id")
    else {
        return Vec::new();
    };
    stmt.query_map([], |row| {
        Ok(BudgetCategory {
            id: row.get(0)?,
            name: row.get(1)?,
            color: row.get(2)?,
            recurring: row.get::<_, i64>(3)? != 0,
        })
    })
    .map(|rows| rows.filter_map(|r| r.ok()).collect())
    .unwrap_or_default()
}

fn db_budget_amount(conn: &Connection, category_id: i64, month: &str) -> f64 {
    conn.query_row(
        "SELECT amount FROM budget_months WHERE category_id = ?1 AND month = ?2",
        rusqlite::params![category_id, month],
        |r| r.get(0),
    )
    .unwrap_or(0.0)
}

fn db_budget_set_amount(conn: &Connection, category_id: i64, month: &str, amount: f64) {
    conn.execute(
        "INSERT INTO budget_months (category_id, month, amount) VALUES (?1, ?2, ?3)
         ON CONFLICT(category_id, month) DO UPDATE SET amount = excluded.amount",
        rusqlite::params![category_id, month, amount],
    )
    .ok();
}

fn db_budget_income(conn: &Connection, month: &str) -> f64 {
    conn.query_row(
        "SELECT income FROM budget_income WHERE month = ?1",
        rusqlite::params![month],
        |r| r.get(0),
    )
    .unwrap_or(0.0)
}

fn db_budget_set_income(conn: &Connection, month: &str, income: f64) {
    conn.execute(
        "INSERT INTO budget_income (month, income) VALUES (?1, ?2)
         ON CONFLICT(month) DO UPDATE SET income = excluded.income",
        rusqlite::params![month, income],
    )
    .ok();
}

fn db_budget_get_cumulative(conn: &Connection, month: &str) -> f64 {
    conn.query_row(
        "SELECT value FROM budget_cumulative WHERE month = ?1",
        rusqlite::params![month],
        |r| r.get(0),
    )
    .unwrap_or(0.0)
}

fn db_budget_set_cumulative(conn: &Connection, month: &str, value: f64) {
    conn.execute(
        "INSERT INTO budget_cumulative (month, value) VALUES (?1, ?2)
         ON CONFLICT(month) DO UPDATE SET value = excluded.value",
        rusqlite::params![month, value],
    )
    .ok();
}

fn db_budget_add_category(conn: &Connection, name: &str, color: &str, recurring: bool) {
    let next: i64 = conn
        .query_row("SELECT COALESCE(MAX(sort_order), 0) + 1 FROM budget_categories", [], |r| r.get(0))
        .unwrap_or(0);
    conn.execute(
        "INSERT INTO budget_categories (name, color, sort_order, recurring) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![name, color, next, recurring as i64],
    )
    .ok();
}

const RAINBOW_POOL: &[&str] = &[
    "#F44336", "#FFEB3B", "#4CAF50", "#2196F3", "#9C27B0", "#E91E63",
    "#00BCD4", "#795548", "#607D8B", "#8BC34A", "#3F51B5", "#CDDC39",
    "#009688", "#673AB7", "#FFC107", "#03A9F4", "#D500F9", "#AEEA00",
    "#FF4081", "#64DD17", "#00B0FF", "#651FFF", "#FFAB00", "#00E5FF",
    "#D50000", "#304FFE", "#AA00FF", "#263238", "#FFD600",
];

fn db_budget_rainbow(conn: &Connection) {
    let cats = db_budget_categories(conn);
    let n = cats.len();
    if n == 0 {
        return;
    }
    for (i, c) in cats.iter().enumerate() {
        let hex = if i < RAINBOW_POOL.len() {
            RAINBOW_POOL[i].to_string()
        } else {
            let extra = i - RAINBOW_POOL.len();
            let h = (extra as f64 * 137.508) % 360.0;
            let l = 0.45 + 0.16 * ((extra % 3) as f64);
            hsl_to_rgb_hex(h, 0.7, l)
        };
        conn.execute(
            "UPDATE budget_categories SET color = ?1 WHERE id = ?2",
            rusqlite::params![hex, c.id],
        )
        .ok();
    }
}

fn hsl_to_rgb_hex(h: f64, s: f64, l: f64) -> String {
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let hp = (h / 60.0) % 6.0;
    let x = c * (1.0 - (hp % 2.0 - 1.0).abs());
    let (r1, g1, b1) = match hp as u32 {
        0 => (c, x, 0.0),
        1 => (x, c, 0.0),
        2 => (0.0, c, x),
        3 => (0.0, x, c),
        4 => (x, 0.0, c),
        _ => (c, 0.0, x),
    };
    let m = l - c / 2.0;
    format!(
        "#{:02X}{:02X}{:02X}",
        ((r1 + m) * 255.0).round().clamp(0.0, 255.0) as u8,
        ((g1 + m) * 255.0).round().clamp(0.0, 255.0) as u8,
        ((b1 + m) * 255.0).round().clamp(0.0, 255.0) as u8
    )
}

fn db_budget_update_category(conn: &Connection, id: i64, name: &str, color: &str) {
    conn.execute(
        "UPDATE budget_categories SET name = ?1, color = ?2 WHERE id = ?3",
        rusqlite::params![name, color, id],
    )
    .ok();
}

fn db_budget_delete_category(conn: &Connection, id: i64) {
    conn.execute("DELETE FROM budget_months WHERE category_id = ?1", rusqlite::params![id]).ok();
    conn.execute("DELETE FROM budget_categories WHERE id = ?1", rusqlite::params![id]).ok();
}

const PALETTE: &[(&str, egui::Color32)] = &[
    ("#F44336", egui::Color32::from_rgb(244, 67, 54)),
    ("#FF9800", egui::Color32::from_rgb(255, 152, 0)),
    ("#FFEB3B", egui::Color32::from_rgb(255, 235, 59)),
    ("#4CAF50", egui::Color32::from_rgb(76, 175, 80)),
    ("#2196F3", egui::Color32::from_rgb(33, 150, 243)),
    ("#9C27B0", egui::Color32::from_rgb(156, 39, 176)),
    ("#E91E63", egui::Color32::from_rgb(233, 30, 99)),
    ("#00BCD4", egui::Color32::from_rgb(0, 188, 212)),
    ("#FFFFFF", egui::Color32::from_rgb(255, 255, 255)),
    ("#9E9E9E", egui::Color32::from_rgb(158, 158, 158)),
];

fn hex_to_color(hex: &str) -> egui::Color32 {
    let h = hex.trim().trim_start_matches('#');
    if h.len() == 6 {
        if let Ok(r) = u8::from_str_radix(&h[0..2], 16) {
            if let Ok(g) = u8::from_str_radix(&h[2..4], 16) {
                if let Ok(b) = u8::from_str_radix(&h[4..6], 16) {
                    return egui::Color32::from_rgb(r, g, b);
                }
            }
        }
    }
    egui::Color32::GRAY
}

enum BudgetAction {
    OpenEdit(i64),
    OpenDelete(i64),
}

#[derive(Default)]
struct BudgetState {
    categories: Vec<BudgetCategory>,
    year: i32,
    month: u32,
    amounts: HashMap<i64, f64>,
    income: f64,
    cumulative: f64,
    editing_cumulative: bool,
    count_oneoff: bool,
    auto_color: bool,
    cards_h: f32,
}

impl BudgetState {
    fn month_str(&self) -> String {
        format!("{:04}-{:02}", self.year, self.month)
    }

    fn month_label(&self) -> String {
        let names = [
            "January", "February", "March", "April", "May", "June", "July", "August",
            "September", "October", "November", "December",
        ];
        format!("{} {}", names[(self.month.max(1).min(12) - 1) as usize], self.year)
    }

    fn shift(&mut self, delta: i32) {
        let m = self.year * 12 + (self.month as i32 - 1) + delta;
        self.year = m.div_euclid(12);
        self.month = (m.rem_euclid(12) + 1) as u32;
        self.editing_cumulative = false;
    }

    fn reload(&mut self, conn: &Connection) {
        if self.auto_color {
            db_budget_rainbow(conn);
        }
        self.categories = db_budget_categories(conn);
        self.amounts.clear();
        for c in &self.categories {
            self.amounts.insert(c.id, db_budget_amount(conn, c.id, &self.month_str()));
        }
        self.income = db_budget_income(conn, &self.month_str());
        self.cumulative = db_budget_get_cumulative(conn, &self.month_str());
    }

    fn total_spend(&self) -> f64 {
        self.categories.iter().map(|c| self.amounts.get(&c.id).copied().unwrap_or(0.0)).sum()
    }
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
    if let Ok(conn) = db_mutex.lock() {
        for spec in &specs {
            if !db_load_series(&conn, spec.symbol).is_empty() {
                last_history.insert(spec.symbol.to_string(), std::time::Instant::now());
            }
        }
    }
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
                for spec in specs
                    .iter()
                    .filter(|s| matches!(s.source, Source::Kraken))
                {
                    let points = fetch_kraken_intraday(&spec.symbol.to_uppercase());
                    if !points.is_empty() {
                        if let Ok(conn) = db_mutex.lock() {
                            db_save_intraday(&conn, &spec.symbol, &points);
                        }
                        let _ = tx.send(DataEvent::IntradayUpdated {
                            symbol: spec.symbol.to_string(),
                            points,
                        });
                        any_event = true;
                    }
                }
                for spec in specs.iter() {
                    let Some(ticker) = yahoo_intraday_ticker(spec.symbol) else {
                        continue;
                    };
                    if let Ok(points) = fetch_yahoo_intraday(ticker) {
                        if !points.is_empty() {
                            if let Ok(conn) = db_mutex.lock() {
                                db_save_intraday(&conn, &spec.symbol, &points);
                            }
                            let _ = tx.send(DataEvent::IntradayUpdated {
                                symbol: spec.symbol.to_string(),
                                points,
                            });
                            any_event = true;
                        }
                    }
                    thread::sleep(Duration::from_millis(400));
                }
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

fn color_swatch_row(ui: &mut egui::Ui, current: &mut String) {
    ui.horizontal(|ui| {
        for (hex, color) in PALETTE {
            let (rect, response) = ui.allocate_exact_size(
                egui::vec2(18.0, 18.0),
                egui::Sense::click(),
            );
            ui.painter().rect_filled(rect, 3.0, *color);
            if *current == *hex {
                ui.painter().rect_stroke(
                    rect,
                    3.0,
                    egui::Stroke::new(2.0_f32, egui::Color32::WHITE),
                    egui::StrokeKind::Inside,
                );
            }
            if response.clicked() {
                *current = hex.to_string();
            }
            ui.add_space(2.0);
        }
    });
}

fn color_dot(ui: &mut egui::Ui, color: egui::Color32) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(10.0, 10.0), egui::Sense::hover());
    ui.painter().rect_filled(rect, 2.0, color);
}

fn plot_line(ui: &mut egui::Ui, id: &str, obs: &[(f64, f64)], width: f32, height: f32) {
    egui_plot::Plot::new(id)
        .width(width)
        .height(height)
        .allow_drag(false)
        .allow_scroll(false)
        .allow_zoom(false)
        .show_axes([false, true])
        .show_background(false)
        .label_formatter(|pos| match pos {
            egui_plot::HoverPosition::NearDataPoint { position, .. } => {
                let date = NaiveDate::from_num_days_from_ce_opt(position.x as i32)
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_default();
                Some(format!("{}\n${}", date, fmt_value(position.y)))
            }
            egui_plot::HoverPosition::Elsewhere { .. } => None,
        })
        .show(ui, |plot_ui| {
            let points: Vec<[f64; 2]> = obs.iter().map(|(x, v)| [*x, *v]).collect();
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
    D1,
    W1,
    M1,
    M6,
    Y1,
    Y5,
    Max,
}

impl Range {
    fn label(&self) -> &'static str {
        match self {
            Range::D1 => "1D",
            Range::W1 => "1W",
            Range::M1 => "1M",
            Range::M6 => "6M",
            Range::Y1 => "1Y",
            Range::Y5 => "5Y",
            Range::Max => "Max",
        }
    }

    fn days(&self) -> u32 {
        match self {
            Range::D1 => 1,
            Range::W1 => 7,
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
        brave_key: String,
    },
    About,
    AddCategory {
        name: String,
        color: String,
        recurring: bool,
    },
    EditCategory {
        id: i64,
        name: String,
        color: String,
    },
    ConfirmDeleteCategory {
        id: i64,
        name: String,
    },
    CustomModel {
        value: String,
    },
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
    budget: BudgetState,
    current_tab: String,
    llm_base_url: String,
    llm_key: String,
    brave_key: String,
    key_prompt: Option<(String, String, String)>,
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
            let intraday = db_load_intraday(&conn, spec.symbol);
            series.insert(
                spec.symbol.to_string(),
                Series {
                    title: spec.title.to_string(),
                    unit: spec.unit.to_string(),
                    source_name: spec.source.name().to_string(),
                    obs,
                    intraday,
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
            "1D" => Range::D1,
            "1W" => Range::W1,
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
        let brave_key = config_get(&conn, "brave_key");
        let key_prompt = if keys.fred.trim().is_empty()
            && llm_key.trim().is_empty()
            && brave_key.trim().is_empty()
        {
            Some((String::new(), String::new(), String::new()))
        } else {
            None
        };
        let current_tab = {
            let t = config_get(&conn, "tab");
            if t == "Budget" { "Budget".to_string() } else { "Dashboard".to_string() }
        };
        db_seed_budget(&conn);
        let now = chrono::Local::now();
        let mut budget = BudgetState {
            year: now.year(),
            month: now.month(),
            count_oneoff: config_get(&conn, "count_oneoff") == "1",
            auto_color: config_get(&conn, "auto_color") == "1",
            cards_h: 300.0,
            ..Default::default()
        };
        budget.reload(&conn);
        let stored_model = config_get(&conn, "llm_model");
        let chat = ChatState {
            messages: db_load_chat(&conn),
            tok_total: config_get(&conn, "chat_tok_total").parse().unwrap_or(0),
            cost_total: config_get(&conn, "chat_cost_total").parse().unwrap_or(0.0),
            model: if stored_model.is_empty() {
                MODEL_PRESETS[0].to_string()
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
            budget,
            current_tab,
            llm_base_url,
            llm_key,
            brave_key,
            key_prompt,
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
                DataEvent::IntradayUpdated { symbol, points } => {
                    if let Some(s) = self.series.get_mut(&symbol) {
                        s.intraday = points;
                    }
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
                        self.chat.tok_total += u.total;
                        if let Ok(conn) = self.db.lock() {
                            config_set(&conn, "chat_tok_total", &self.chat.tok_total.to_string());
                            config_set(&conn, "chat_cost_total", &self.chat.cost_total.to_string());
                        }
                    }
                    match error {
                        Some(e) => self.chat.error = Some(e),
                        None => {
                            let msg = ChatMessage {
                                role: ChatRole::Assistant,
                                content: std::mem::take(&mut self.chat.stream_content),
                                reasoning: std::mem::take(&mut self.chat.stream_reasoning),
                                tool_log: std::mem::take(&mut self.chat.stream_tool_lines),
                                usage,
                            };
                            self.chat.messages.push(msg.clone());
                            if let Ok(conn) = self.db.lock() {
                                db_save_chat_message(
                                    &conn,
                                    "assistant",
                                    &msg.content,
                                    &msg.reasoning,
                                    &msg.tool_log,
                                    msg.usage,
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
                usage: None,
            });
        if let Ok(conn) = self.db.lock() {
            db_save_chat_message(&conn, "user", &text, "", &[], None);
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
            brave_key: self.brave_key.clone(),
        };
        let history: Vec<ChatMessage> = self.chat.messages[..self.chat.messages.len() - 1].to_vec();
        let system = market_system_prompt(&self.series, &self.specs, self.range, self.country);
        let tx = self.chat_tx.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        self.chat.cancel = cancel.clone();
        self.chat.busy = true;
        thread::spawn(move || {
            run_chat_agent(ctx, tx, cfg, system, history, text, cancel);
        });
    }

    fn clear_chat(&mut self) {
        self.chat.messages.clear();
        self.chat.error = None;
        self.chat.cost_total = 0.0;
        self.chat.tok_total = 0;
        self.chat.stream_content.clear();
        self.chat.stream_reasoning.clear();
        self.chat.stream_tool_lines.clear();
        if let Ok(conn) = self.db.lock() {
            config_set(&conn, "chat_tok_total", "0");
            config_set(&conn, "chat_cost_total", "0");
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
                    let span = range.days();
                    let use_intraday = matches!(range, Range::D1 | Range::W1) && !s.intraday.is_empty();
                    if use_intraday {
                        let now_x = 719_163.0
                            + chrono::Utc::now().timestamp() as f64 / 86400.0;
                        let pts: Vec<(f64, f64)> = s
                            .intraday
                            .iter()
                            .filter(|(x, _)| *x >= now_x - span as f64)
                            .cloned()
                            .collect();
                        if pts.len() > 1 {
                            plot_line(ui, &format!("spark_{}", s.title), &pts, plot_w, plot_h);
                        }
                    } else if s.obs.len() > 1 {
                        let sliced = s.slice_range(span);
                        let start = if sliced.len() >= 2 {
                            s.obs.len() - sliced.len()
                        } else {
                            s.obs.len().saturating_sub(2)
                        };
                        let pts: Vec<(f64, f64)> = s.obs[start..]
                            .iter()
                            .map(|(d, v)| (d.num_days_from_ce() as f64, *v))
                            .collect();
                        if pts.len() > 1 {
                            plot_line(ui, &format!("spark_{}", s.title), &pts, plot_w, plot_h);
                        }
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

    fn chat_markdown_ui(
        ui: &mut egui::Ui,
        text: &str,
        cache: &mut egui_commonmark::CommonMarkCache,
    ) {
        let split_cells = |line: &str| {
            let line = line.trim();
            let line = line.strip_prefix('|').unwrap_or(line);
            let mut cells = Vec::new();
            let mut cell = String::new();
            let mut escaped = false;
            for ch in line.chars() {
                if ch == '|' && !escaped {
                    cells.push(cell.trim().to_string());
                    cell.clear();
                } else {
                    cell.push(ch);
                }
                escaped = ch == '\\' && !escaped;
            }
            if !cell.trim().is_empty() || !line.ends_with('|') {
                cells.push(cell.trim().to_string());
            }
            cells
        };
        let width = ui.available_width();
        let lines: Vec<&str> = text.lines().collect();
        let mut prose = String::new();
        let mut fence: Option<(char, usize)> = None;
        let mut i = 0;
        while i < lines.len() {
            let line = lines[i].trim_start();
            let marker = line.chars().next().unwrap_or(' ');
            let count = line.chars().take_while(|&ch| ch == marker).count();
            if (marker == '`' || marker == '~') && count >= 3 {
                match fence {
                    None => fence = Some((marker, count)),
                    Some((ch, len)) if ch == marker && count >= len => fence = None,
                    _ => {}
                }
                prose.push_str(lines[i]);
                prose.push('\n');
                i += 1;
                continue;
            }
            if fence.is_none() && i + 1 < lines.len() && lines[i].contains('|') {
                let header = split_cells(lines[i]);
                let separator = split_cells(lines[i + 1]);
                if !header.is_empty() && separator.len() == header.len()
                    && separator.iter().all(|cell| {
                        let dashes = cell.trim_matches(':');
                        !dashes.is_empty() && dashes.chars().all(|ch| ch == '-')
                    })
                {
                    if !prose.is_empty() {
                        ui.push_id(("prose", i), |ui| {
                            egui_commonmark::CommonMarkViewer::new().show(ui, cache, &prose);
                        });
                        prose.clear();
                    }
                    let table_id = i;
                    let cols = header.len();
                    let mut rows = vec![header];
                    i += 2;
                    while i < lines.len() && !lines[i].trim().is_empty() && lines[i].contains('|') {
                        let mut row = split_cells(lines[i]);
                        row.resize(cols, String::new());
                        rows.push(row);
                        i += 1;
                    }
                    let gap = 6.0_f32.min(width / (cols as f32 * 4.0));
                    let cell_w = ((width - gap * (cols - 1) as f32) / cols as f32).max(1.0);
                    for (row_idx, row) in rows.iter().enumerate() {
                        let top = ui.next_widget_position();
                        let mut row_h = ui.text_style_height(&egui::TextStyle::Body);
                        for (col, cell) in row.iter().enumerate() {
                            let rect = egui::Rect::from_min_size(
                                top + egui::vec2(col as f32 * (cell_w + gap), 0.0),
                                egui::vec2(cell_w, f32::INFINITY),
                            );
                            let mut cell_ui = ui.new_child(
                                egui::UiBuilder::new()
                                    .id_salt(("table", table_id, row_idx, col))
                                    .max_rect(rect)
                                    .layout(egui::Layout::top_down(egui::Align::LEFT)),
                            );
                            cell_ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Wrap);
                            egui_commonmark::CommonMarkViewer::new().show(&mut cell_ui, cache, cell);
                            row_h = row_h.max(cell_ui.min_size().y);
                        }
                        ui.allocate_space(egui::vec2(width, row_h));
                        if row_idx == 0 { ui.separator(); }
                    }
                    continue;
                }
            }
            prose.push_str(lines[i]);
            prose.push('\n');
            i += 1;
        }
        if !prose.is_empty() {
            ui.push_id(("prose", i), |ui| {
                egui_commonmark::CommonMarkViewer::new().show(ui, cache, &prose);
            });
        }
    }

    fn chat_message_ui(
        ui: &mut egui::Ui,
        idx: usize,
        msg: &ChatMessage,
        streaming: bool,
        cache: &mut egui_commonmark::CommonMarkCache,
    ) {
        let width = ui.available_width();
        let mut message_ui = ui.new_child(
            egui::UiBuilder::new()
                .id_salt(("chat_message", idx))
                .max_rect(ui.available_rect_before_wrap())
                .layout(egui::Layout::top_down(egui::Align::LEFT)),
        );
        message_ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Wrap);
        {
            let ui = &mut message_ui;
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
                    ui.horizontal_wrapped(|ui| {
                        ui.label(egui::RichText::new(prefix).strong().color(color));
                        if let Some(u) = msg.usage {
                            ui.label(
                                egui::RichText::new(format!(
                                    "{} tok · {}",
                                    thousands(u.total as f64),
                                    fmt_usd(u.cost_usd)
                                ))
                                .small()
                                .color(egui::Color32::from_rgb(120, 120, 120)),
                            )
                            .on_hover_text(format!(
                                "prompt {} · completion {}",
                                u.prompt, u.completion
                            ));
                        }
                    });
                    Self::chat_markdown_ui(ui, &msg.content, cache);
                }
            }
        }
        ui.allocate_space(egui::vec2(width, message_ui.min_size().y));
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
        custom_model: &mut bool,
    ) {
        let right = ui.max_rect().right().min(ui.clip_rect().right());
        let width = width.min((right - ui.next_widget_position().x - 18.0).max(1.0));
        let height = height.min(ui.available_height());
        egui::Frame::group(ui.style())
            .stroke(egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(80, 80, 80)))
            .inner_margin(egui::Margin::same(8))
            .show(ui, |parent| {
                let (_, rect) = parent.allocate_space(
                    egui::vec2(width, (height - 18.0).max(1.0)),
                );
                let mut panel_ui = parent.new_child(
                    egui::UiBuilder::new()
                        .id_salt("chat_bounds")
                        .max_rect(rect)
                        .layout(egui::Layout::top_down(egui::Align::LEFT)),
                );
                panel_ui.set_clip_rect(panel_ui.clip_rect().intersect(rect));
                panel_ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Wrap);
                let ui = &mut panel_ui;
                ui.vertical(|ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.label(
                            egui::RichText::new("* AI RESEARCH")
                                .strong()
                                .color(egui::Color32::from_rgb(33, 150, 243)),
                        );
                        let selected: String = chat.model.clone();
                        egui::ComboBox::from_id_salt("chat_model")
                            .selected_text(selected)
                            .width(150.0_f32.min(width))
                            .truncate()
                            .show_ui(ui, |ui| {
                                for m in MODEL_PRESETS {
                                    ui.selectable_value(&mut chat.model, m.to_string(), *m);
                                }
                                if ui
                                    .selectable_label(
                                        !MODEL_PRESETS.contains(&chat.model.as_str()),
                                        "custom…",
                                    )
                                    .clicked()
                                {
                                    *custom_model = true;
                                    ui.close();
                                }
                            });
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
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
                            if !(cfg!(target_os = "macos") && !maximized) {
                                ui.label(
                                    egui::RichText::new(format!(
                                        "{} tok",
                                        thousands(chat.tok_total as f64)
                                    ))
                                    .small()
                                    .color(egui::Color32::from_rgb(120, 120, 120)),
                                )
                                .on_hover_text("total tokens since cleared");
                                ui.label(
                                    egui::RichText::new(fmt_usd(chat.cost_total))
                                        .small()
                                        .color(egui::Color32::from_rgb(120, 120, 120)),
                                )
                                .on_hover_text("total cost since cleared (USD)");
                            }
                        });
                    });
                    ui.separator();
                    ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
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
                            if chat.busy {
                                if ui
                                    .add(egui::Button::new(
                                        egui::RichText::new("x stop")
                                            .color(egui::Color32::from_rgb(244, 67, 54)),
                                    ))
                                    .clicked()
                                {
                                    chat.cancel.store(true, Ordering::Relaxed);
                                }
                            } else {
                                let send_clicked = ui.add(egui::Button::new("* send")).clicked();
                                if (enter_send || send_clicked) && !chat.input.trim().is_empty() {
                                    *send = true;
                                }
                            }
                        });
                         ui.add_space(4.0);
                         ui.style_mut().url_in_tooltip = true;
                         egui::ScrollArea::vertical()
                            .id_salt("chat_history")
                            .auto_shrink([false, false])
                            .stick_to_bottom(true)
                            .show(ui, |parent| {
                                let rect = parent.available_rect_before_wrap();
                                let mut history_ui = parent.new_child(
                                    egui::UiBuilder::new()
                                        .id_salt("chat_content")
                                        .max_rect(rect)
                                        .layout(egui::Layout::top_down(egui::Align::LEFT)),
                                );
                                let ui = &mut history_ui;
                                for (i, msg) in chat.messages.iter().enumerate() {
                                    Self::chat_message_ui(ui, i, msg, false, &mut chat.md_cache);
                                }
                                let base = chat.messages.len();
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
                                        usage: None,
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
                                parent.allocate_space(egui::vec2(rect.width(), history_ui.min_size().y));
                             });
                    });
                });
            });
    }

    fn budget_section(
        ui: &mut egui::Ui,
        budget: &mut BudgetState,
        db: &Arc<Mutex<Connection>>,
        action: &mut Option<BudgetAction>,
        grid_id: &str,
        recurring: bool,
        per_col: usize,
    ) {
        let cats = budget.categories.clone();
        let filtered: Vec<&BudgetCategory> =
            cats.iter().filter(|c| c.recurring == recurring).collect();
        if filtered.is_empty() {
            return;
        }
        ui.label(
            egui::RichText::new(if recurring { "RECURRING" } else { "ONE-OFF" })
                .small()
                .color(egui::Color32::GRAY),
        );
        ui.with_layout(egui::Layout::left_to_right(egui::Align::Min), |ui| {
            for (col, chunk) in filtered.chunks(per_col.max(1)).enumerate() {
                egui::Grid::new(format!("{}_{}", grid_id, col))
                    .num_columns(4)
                    .spacing([8.0, 5.0])
                    .min_col_width(30.0)
                    .show(ui, |ui| {
                        for cat in chunk {
                            let color = hex_to_color(&cat.color);
                            let (rect, resp) = ui.allocate_exact_size(
                                egui::vec2(14.0, 14.0),
                                egui::Sense::click(),
                            );
                            ui.painter().rect_filled(rect, 3.0, color);
                            if resp.clicked() {
                                *action = Some(BudgetAction::OpenEdit(cat.id));
                            }
                            resp.on_hover_text("edit category");
                            let name_resp = ui.add(
                                egui::Button::new(
                                    egui::RichText::new(&cat.name).color(color),
                                )
                                .frame(false)
                                .small(),
                            );
                            if name_resp.clicked() {
                                *action = Some(BudgetAction::OpenEdit(cat.id));
                            }
                            if recurring {
                                let month = budget.month_str();
                                name_resp.context_menu(|ui| {
                                    if ui.button("copy recurring from last month").clicked() {
                                        let t = budget.year * 12 + budget.month as i32 - 2;
                                        let last_month = format!("{:04}-{:02}", t / 12, t % 12 + 1);
                                        if let Ok(conn) = db.lock() {
                                            let v = db_budget_amount(&conn, cat.id, &last_month);
                                            db_budget_set_amount(&conn, cat.id, &month, v);
                                            budget.amounts.insert(cat.id, v);
                                        }
                                        ui.close();
                                    }
                                });
                            }
                            let month = budget.month_str();
                            let amount = budget
                                .amounts
                                .entry(cat.id)
                                .or_insert(0.0);
                            let resp = ui.add(
                                egui::DragValue::new(amount)
                                    .speed(1.0)
                                    .prefix("$")
                                    .range(-1_000_000_000.0..=1_000_000_000.0),
                            );
                            if resp.changed() {
                                if let Ok(conn) = db.lock() {
                                    db_budget_set_amount(
                                        &conn,
                                        cat.id,
                                        &month,
                                        *amount,
                                    );
                                }
                            }
                            if ui
                                .small_button("x")
                                .on_hover_text("delete category")
                                .clicked()
                            {
                                *action = Some(BudgetAction::OpenDelete(cat.id));
                            }
                            ui.end_row();
                        }
                    });
            }
        });
    }

    fn budget_screen(
        ui: &mut egui::Ui,
        budget: &mut BudgetState,
        db: &Arc<Mutex<Connection>>,
        action: &mut Option<BudgetAction>,
    ) {
        let spacing = 12.0;
        let avail_w = ui.available_width();
        let avail_h = ui.available_height();
        let left_w = (avail_w * 0.37).max(280.0);
        let right_w = (avail_w - left_w - spacing - 4.0).max(240.0);
        let pie_r = (((right_w - 20.0) / 3.0) * 1.2).min((((avail_h - 200.0) / 4.0) * 1.2).max(48.0));
        let per_col = (((avail_h - 340.0).max(80.0)) / 24.0).floor().max(4.0) as usize;
        egui::ScrollArea::vertical()
            .id_salt("budget_window_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.set_min_width(left_w);
                        ui.set_max_width(left_w);
                        let spend = budget.total_spend();
                        let saved = budget.income - spend;
                        MacroApp::budget_section(ui, budget, db, action, "budget_grid_rec", true, per_col);
                        ui.add_space(8.0);
                        MacroApp::budget_section(ui, budget, db, action, "budget_grid_one", false, per_col);
                        ui.add_space(6.0);
                        ui.label(
                            egui::RichText::new(format!("Total   {}", fmt_usd(spend))).strong(),
                        );
                        let used = ui.min_size().y;
                        let spring = (avail_h - used - budget.cards_h - 4.0).max(0.0);
                        ui.add_space(spring);
                        egui::Frame::group(ui.style())
                            .stroke(egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(80, 80, 80)))
                            .inner_margin(egui::Margin::same(8))
                            .show(ui, |ui| {
                                ui.set_min_width(left_w - 18.0);
                                ui.label(egui::RichText::new("SPENT THIS MONTH").small().color(egui::Color32::GRAY));
                                ui.label(
                                    egui::RichText::new(fmt_usd(spend))
                                        .size(22.0)
                                        .strong(),
                                );
                            });
                        ui.add_space(6.0);
                        let saved_col = if saved >= 0.0 {
                            egui::Color32::from_rgb(76, 175, 80)
                        } else {
                            egui::Color32::from_rgb(244, 67, 54)
                        };
                        let saved_txt = if saved < 0.0 {
                            format!("-{}", fmt_usd(-saved))
                        } else {
                            fmt_usd(saved)
                        };
                        egui::Frame::group(ui.style())
                            .stroke(egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(80, 80, 80)))
                            .inner_margin(egui::Margin::same(8))
                            .show(ui, |ui| {
                                ui.set_min_width(left_w - 18.0);
                                ui.label(egui::RichText::new("SAVED THIS MONTH").small().color(egui::Color32::GRAY));
                                ui.label(
                                    egui::RichText::new(saved_txt)
                                        .size(22.0)
                                        .strong()
                                        .color(saved_col),
                                );
                            });
                        ui.add_space(6.0);
                        egui::Frame::group(ui.style())
                            .stroke(egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(80, 80, 80)))
                            .inner_margin(egui::Margin::same(8))
                            .show(ui, |ui| {
                                ui.set_min_width(left_w - 18.0);
                                ui.label(egui::RichText::new("CUMULATIVE SAVINGS").small().color(egui::Color32::GRAY));
                                if budget.editing_cumulative {
                                    let resp = ui.add(
                                        egui::DragValue::new(&mut budget.cumulative)
                                            .speed(10.0)
                                            .prefix("$")
                                            .range(-1_000_000_000.0..=1_000_000_000.0),
                                    );
                                    if resp.changed() {
                                        if let Ok(conn) = db.lock() {
                                            db_budget_set_cumulative(
                                                &conn,
                                                &budget.month_str(),
                                                budget.cumulative,
                                            );
                                        }
                                    }
                                    if resp.lost_focus() {
                                        budget.editing_cumulative = false;
                                    }
                                } else {
                                    let col = if budget.cumulative >= 0.0 {
                                        egui::Color32::from_rgb(76, 175, 80)
                                    } else {
                                        egui::Color32::from_rgb(244, 67, 54)
                                    };
                                    if ui
                                        .add(
                                            egui::Button::new(
                                                egui::RichText::new(fmt_usd(budget.cumulative))
                                                    .size(26.0)
                                                    .strong()
                                                    .color(col),
                                            )
                                            .frame(false),
                                        )
                                        .on_hover_text("click to edit (saved per month)")
                                        .clicked()
                                    {
                                        budget.editing_cumulative = true;
                                    }
                                }
                                ui.label(
                                    egui::RichText::new(format!("as at {}", budget.month_label()))
                                        .small()
                                        .color(egui::Color32::GRAY),
                                );
                            });
                        budget.cards_h = (ui.min_size().y - used - spring).max(0.0);
            });
            ui.add_space(spacing);
            ui.vertical(|ui| {
                ui.set_min_width(right_w);
                ui.set_max_width(right_w);
                let spend = budget.total_spend();
                let saved = budget.income - spend;
                ui.label(egui::RichText::new("SAVINGS VS SPENDING").small().color(egui::Color32::GRAY));
                let slices1 = if budget.income > 0.0 {
                    if saved >= 0.0 {
                        vec![
                            ("Spent".to_string(), spend, egui::Color32::from_rgb(33, 150, 243)),
                            ("Saved".to_string(), saved, egui::Color32::from_rgb(76, 175, 80)),
                        ]
                    } else {
                        vec![
                            ("Spent".to_string(), budget.income, egui::Color32::from_rgb(33, 150, 243)),
                            ("Overspend".to_string(), -saved, egui::Color32::from_rgb(244, 67, 54)),
                        ]
                    }
                } else {
                    Vec::new()
                };
                ui.with_layout(egui::Layout::left_to_right(egui::Align::Min), |ui| {
                    MacroApp::draw_pie(ui, "pie_savings", pie_r, &slices1, "");
                    ui.add_space(spacing);
                    egui::Grid::new("legend_savings")
                        .num_columns(4)
                        .spacing([8.0, 4.0])
                        .show(ui, |ui| {
                            for (label, value, color) in &slices1 {
                                color_dot(ui, *color);
                                ui.label(egui::RichText::new(label).small());
                                ui.label(egui::RichText::new(fmt_usd(*value)).small());
                                let pct = if budget.income > 0.0 {
                                    value / budget.income * 100.0
                                } else {
                                    0.0
                                };
                                ui.label(egui::RichText::new(format!("{:.0}%", pct)).small());
                                ui.end_row();
                            }
                        });
                });
                ui.add_space(spacing);
                ui.separator();
                ui.add_space(spacing / 2.0);
                ui.label(egui::RichText::new("SPENDING BY CATEGORY").small().color(egui::Color32::GRAY));
                let mut slices2: Vec<(String, f64, egui::Color32)> = budget
                    .categories
                    .iter()
                    .filter(|c| c.recurring || budget.count_oneoff)
                    .map(|c| {
                        (
                            c.name.clone(),
                            budget.amounts.get(&c.id).copied().unwrap_or(0.0),
                            hex_to_color(&c.color),
                        )
                    })
                    .filter(|(_, v, _)| *v > 0.0)
                    .collect();
                slices2.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                let cat_spend: f64 = slices2.iter().map(|s| s.1).sum();
                ui.with_layout(egui::Layout::left_to_right(egui::Align::Min), |ui| {
                    MacroApp::draw_pie(ui, "pie_categories", pie_r, &slices2, "");
                    ui.add_space(spacing);
                    egui::Grid::new("legend_categories")
                        .num_columns(4)
                        .spacing([8.0, 4.0])
                        .show(ui, |ui| {
                            for (label, value, color) in slices2.iter() {
                                color_dot(ui, *color);
                                ui.label(egui::RichText::new(label).small());
                                ui.label(egui::RichText::new(fmt_usd(*value)).small());
                                let pct = if cat_spend > 0.0 { value / cat_spend * 100.0 } else { 0.0 };
                                ui.label(egui::RichText::new(format!("{:.0}%", pct)).small());
                                ui.end_row();
                            }
                        });
                });
                if slices2.is_empty() {
                    ui.label(egui::RichText::new("no spending this month").small().color(egui::Color32::GRAY));
                }
            });
        });
        });
    }

    fn draw_pie(
        ui: &mut egui::Ui,
        _id: &str,
        radius: f32,
        slices: &[(String, f64, egui::Color32)],
        center_text: &str,
    ) {
        let size = radius * 2.0;
        let (rect, response) = ui.allocate_exact_size(egui::vec2(size, size), egui::Sense::hover());
        let painter = ui.painter();
        let center = rect.center();
        let total: f64 = slices.iter().map(|s| s.1).sum();
        if total <= 0.0 {
            painter.circle_stroke(center, radius, egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(80, 80, 80)));
            painter.text(
                center,
                egui::Align2::CENTER_CENTER,
                "no data",
                egui::FontId::proportional(12.0),
                egui::Color32::GRAY,
            );
            return;
        }
        let tau = std::f32::consts::TAU;
        let start = -std::f32::consts::FRAC_PI_2;
        let mut a0 = start;
        let mut hovered: Option<(String, f64, egui::Color32)> = None;
        let mut hovered_pts: Vec<egui::Pos2> = Vec::new();
        let mut hovered_mid: f32 = 0.0;
        for (label, value, color) in slices {
            let frac = (*value / total) as f32;
            let a1 = a0 + frac * tau;
            let segs = (((a1 - a0) / tau) * 96.0).ceil().max(2.0) as usize;
            let mut pts = vec![center];
            for s in 0..=segs {
                let ang = a0 + (a1 - a0) * (s as f32 / segs as f32);
                pts.push(center + radius * egui::vec2(ang.cos(), ang.sin()));
            }
            painter.add(egui::Shape::convex_polygon(
                pts.clone(),
                *color,
                egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(40, 40, 40)),
            ));
            if hovered.is_none() {
                if let Some(p) = response.hover_pos() {
                    let d = p - center;
                    if d.length() <= radius {
                        let mut rel = d.angle() - start;
                        while rel < 0.0 {
                            rel += tau;
                        }
                        let slice_span = a1 - start;
                        if rel < slice_span {
                            hovered = Some((label.clone(), *value, *color));
                            hovered_pts = pts;
                            hovered_mid = (a0 + a1) * 0.5;
                        }
                    }
                }
            }
            a0 = a1;
        }
        if let Some((label, value, _)) = &hovered {
            painter.add(egui::Shape::convex_polygon(
                hovered_pts,
                egui::Color32::TRANSPARENT,
                egui::Stroke::new(2.5, egui::Color32::WHITE),
            ));
            let pos = center + (radius * 0.6) * egui::vec2(hovered_mid.cos(), hovered_mid.sin());
            painter.text(
                pos,
                egui::Align2::CENTER_CENTER,
                format!("{}\n{:.1}%", label, value / total * 100.0),
                egui::FontId::proportional(13.0),
                egui::Color32::WHITE,
            );
        }
        if !center_text.is_empty() {
            painter.text(
                center,
                egui::Align2::CENTER_CENTER,
                center_text,
                egui::FontId::monospace(radius * 0.34),
                egui::Color32::WHITE,
            );
        }
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
                     if ui.button("API Keys").clicked() {
                        let (fred_key, llm_base_url, llm_key, llm_model, refresh_mins, brave_key) = self
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
                                    config_get(&conn, "brave_key"),
                                )
                            })
                            .unwrap_or_else(|| {
                                (
                                    String::new(),
                                    "https://openrouter.ai/api/v1".into(),
                                    String::new(),
                                    String::new(),
                                    "1".into(),
                                    String::new(),
                                )
                            });
                        self.dialog_state = DialogState::Settings {
                            fred_key,
                            llm_base_url,
                            llm_key,
                            llm_model,
                            refresh_mins,
                            brave_key,
                        };
                        ui.close();
                    }
                });
                ui.menu_button("View", |ui| {
                    if ui.button("Dashboard").clicked() {
                        self.current_tab = "Dashboard".into();
                        if let Ok(conn) = self.db.lock() {
                            config_set(&conn, "tab", "Dashboard");
                        }
                        ui.close();
                    }
                    if ui.button("Budget").clicked() {
                        self.current_tab = "Budget".into();
                        if let Ok(conn) = self.db.lock() {
                            config_set(&conn, "tab", "Budget");
                        }
                        self.budget.reload(&self.db.lock().unwrap_or_else(|p| p.into_inner()));
                        ui.close();
                    }
                    ui.separator();
                    ui.menu_button("Budget", |ui| {
                        if ui
                            .checkbox(&mut self.budget.count_oneoff, "count one-off in spending")
                            .changed()
                        {
                            if let Ok(conn) = self.db.lock() {
                                config_set(
                                    &conn,
                                    "count_oneoff",
                                    if self.budget.count_oneoff { "1" } else { "0" },
                                );
                            }
                        }
                        if ui
                            .checkbox(&mut self.budget.auto_color, "auto color-sort")
                            .changed()
                        {
                            if let Ok(conn) = self.db.lock() {
                                config_set(
                                    &conn,
                                    "auto_color",
                                    if self.budget.auto_color { "1" } else { "0" },
                                );
                            }
                            if self.budget.auto_color {
                                if let Ok(conn) = self.db.lock() {
                                    db_budget_rainbow(&conn);
                                }
                                self.budget.reload(&self.db.lock().unwrap_or_else(|p| p.into_inner()));
                            }
                        }
                    });
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
            if self.current_tab == "Budget" {
                ui.horizontal(|ui| {
                    ui.heading("Budget");
                    if ui.button("<").clicked() {
                        self.budget.shift(-1);
                        if let Ok(conn) = self.db.lock() {
                            self.budget.reload(&conn);
                        }
                    }
                    ui.label(
                        egui::RichText::new(self.budget.month_label()).strong(),
                    );
                    if ui.button(">").clicked() {
                        self.budget.shift(1);
                        if let Ok(conn) = self.db.lock() {
                            self.budget.reload(&conn);
                        }
                    }
                    ui.add_space(14.0);
                    ui.label("Income:");
                    let mut income = self.budget.income;
                    if ui
                        .add(
                            egui::DragValue::new(&mut income)
                                .speed(10.0)
                                .prefix("$")
                                .range(0.0..=10_000_000.0),
                        )
                        .changed()
                    {
                        if let Ok(conn) = self.db.lock() {
                            db_budget_set_income(&conn, &self.budget.month_str(), income);
                        }
                        self.budget.income = income;
                    }
                    ui.add_space(14.0);
                    if ui.button("+ Category").clicked() {
                        self.dialog_state = DialogState::AddCategory {
                            name: String::new(),
                            color: "#2196F3".into(),
                            recurring: true,
                        };
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(
                            egui::RichText::new(format!(
                                "spend {}  ·  saved {}",
                                fmt_usd(self.budget.total_spend()),
                                fmt_usd(self.budget.income - self.budget.total_spend())
                            ))
                            .small()
                            .color(egui::Color32::GRAY),
                        );
                    });
                });
                return;
            }
            ui.horizontal(|ui| {
                ui.heading("Dashboard");
                let combo = egui::ComboBox::new("country_select", "")
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
                for r in [Range::D1, Range::W1, Range::M1, Range::M6, Range::Y1, Range::Y5, Range::Max] {
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

    fn show_key_prompt(&mut self, ui: &mut egui::Ui) {
        let mut done = false;
        egui::CentralPanel::default().show(ui, |ui| {
            ui.add_space(16.0);
            ui.heading("API Keys");
            ui.label("Optional — used for macro data, AI research and web search.");
            ui.add_space(10.0);
            let Some((fred, llm, brave)) = self.key_prompt.as_mut() else {
                return;
            };
            ui.label("FRED API key (macro data):");
            ui.add(
                egui::TextEdit::singleline(fred)
                    .desired_width(320.0)
                    .password(true),
            );
            ui.add_space(6.0);
            ui.label("LLM API key (AI research):");
            ui.add(
                egui::TextEdit::singleline(llm)
                    .desired_width(320.0)
                    .password(true),
            );
            ui.add_space(6.0);
            ui.label("Brave Search API key (web search):");
            ui.add(
                egui::TextEdit::singleline(brave)
                    .desired_width(320.0)
                    .password(true),
            );
            ui.add_space(10.0);
            if fred.trim().is_empty() && llm.trim().is_empty() && brave.trim().is_empty() {
                ui.label(
                    egui::RichText::new("warning: this will result in limited functionality")
                        .color(egui::Color32::from_rgb(244, 67, 54)),
                );
                ui.add_space(10.0);
            }
            if ui.button("Continue").clicked() {
                done = true;
            }
        });
        if done {
            if let Some((fred, llm, brave)) = self.key_prompt.take() {
                if let Ok(conn) = self.db.lock() {
                    if !fred.trim().is_empty() {
                        config_set(&conn, "fred_key", fred.trim());
                    }
                    if !llm.trim().is_empty() {
                        config_set(&conn, "llm_key", llm.trim());
                    }
                    if !brave.trim().is_empty() {
                        config_set(&conn, "brave_key", brave.trim());
                    }
                }
                self.llm_key = llm.trim().to_string();
                self.brave_key = brave.trim().to_string();
                self.status_text = "API keys saved".into();
            }
        }
    }

    fn show_status_bar(&mut self, ui: &mut egui::Ui) {
        egui::Panel::bottom("status_bar").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new(&self.status_text).small());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.colored_label(
                        egui::Color32::GRAY,
                        egui::RichText::new(format!(
                            "{}v1.0",
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
        if self.key_prompt.is_some() {
            self.show_key_prompt(ui);
            return;
        }
        self.show_status_bar(ui);
        self.show_controls(ui);

        let panel_frame = {
            let style = ui.style();
            egui::Frame::central_panel(style).inner_margin(egui::Margin::same(10))
        };
        let mut chat_send = false;
        let mut chat_clear = false;
        let mut chat_toggle = false;
        let mut custom_model = false;
        let mut budget_action: Option<BudgetAction> = None;
        egui::CentralPanel::default().frame(panel_frame).show(ui, |ui| {
            let tab = self.current_tab.clone();
            let specs = self.specs.clone();
            let series = &self.series;
            let chat = &mut self.chat;
            let budget = &mut self.budget;
            let db = self.db.clone();
            let range = self.range;
            let chat_maximized = self.chat_maximized;
            let spacing = 12.0;
            if tab == "Budget" {
                let mut action: Option<BudgetAction> = None;
                MacroApp::budget_screen(ui, budget, &db, &mut action);
                budget_action = action;
                return;
            }
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
                    &mut custom_model,
                );
                return;
            }
            let cols = 3usize;
            let total_rows = specs.len().div_ceil(cols);
            let avail_w = ui.available_width();
            let avail_h = ui.available_height();
            let card_w = (((avail_w - spacing * (cols as f32 - 1.0)) / cols as f32) - 2.0).max(180.0);
            let card_h = ((avail_h - spacing * total_rows as f32) / total_rows as f32).max(140.0);
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
            let row_w = ui.available_width();
            ui.horizontal(|ui| {
                if let Some(last) = specs.last() {
                    let s = series
                        .get(last.symbol)
                        .cloned()
                        .unwrap_or_default();
                    MacroApp::stat_card(ui, &s, range, plot_w, plot_h);
                }
                ui.add_space((spacing - 8.0).max(0.0));
                let chat_w = (row_w - card_w - 2.0 * spacing - 10.0).max(160.0);
                MacroApp::chat_panel(
                    ui,
                    chat,
                    false,
                    chat_w,
                    card_h,
                    &mut chat_send,
                    &mut chat_clear,
                    &mut chat_toggle,
                    &mut custom_model,
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
                mut brave_key,
            } => {
                let mut open = true;
                let mut save = false;
                egui::Window::new("Settings")
                    .collapsible(false)
                    .resizable(false)
                    .open(&mut open)
                    .show(&ctx, |ui| {
                        ui.label(egui::RichText::new("DATA").strong());
                        ui.horizontal(|ui| {
                            ui.label("FRED API Key:");
                            if ui.small_button("copy").on_hover_text("copy key to clipboard").clicked() {
                                ui.ctx().copy_text(fred_key.clone());
                            }
                        });
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
                        ui.horizontal(|ui| {
                            ui.label("LLM API Key:");
                            if ui.small_button("copy").on_hover_text("copy key to clipboard").clicked() {
                                ui.ctx().copy_text(llm_key.clone());
                            }
                        });
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
                        ui.add_space(4.0);
                        ui.horizontal(|ui| {
                            ui.label("Brave Search Key:");
                            if ui.small_button("copy").on_hover_text("copy key to clipboard").clicked() {
                                ui.ctx().copy_text(brave_key.clone());
                            }
                        });
                        ui.add(
                            egui::TextEdit::singleline(&mut brave_key)
                                .desired_width(320.0)
                                .password(true),
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
                        config_set(&conn, "brave_key", brave_key.trim());
                    }
                    self.llm_base_url = if llm_base_url.trim().is_empty() {
                        "https://openrouter.ai/api/v1".into()
                    } else {
                        llm_base_url.trim().to_string()
                    };
                    self.llm_key = llm_key.trim().to_string();
                    self.brave_key = brave_key.trim().to_string();
                    self.status_text = "Settings saved (restart to apply)".into();
                } else if open {
                    self.dialog_state = DialogState::Settings {
                        fred_key,
                        llm_base_url,
                        llm_key,
                        llm_model,
                        refresh_mins,
                        brave_key,
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
            DialogState::AddCategory {
                mut name,
                mut color,
                mut recurring,
            } => {
                let mut open = true;
                let mut save = false;
                egui::Window::new("Add Category")
                    .collapsible(false)
                    .resizable(false)
                    .open(&mut open)
                    .show(&ctx, |ui| {
                        ui.label("Name:");
                        ui.add(
                            egui::TextEdit::singleline(&mut name)
                                .desired_width(240.0),
                        );
                        ui.add_space(6.0);
                        ui.label("Color:");
                        color_swatch_row(ui, &mut color);
                        ui.horizontal(|ui| {
                            ui.label("Hex:");
                            ui.add(egui::TextEdit::singleline(&mut color).desired_width(90.0));
                        });
                        ui.add_space(6.0);
                        ui.horizontal(|ui| {
                            ui.label("Type:");
                            ui.radio_value(&mut recurring, true, "Recurring");
                            ui.radio_value(&mut recurring, false, "One-off");
                        });
                        ui.add_space(8.0);
                        if ui.button("Add").clicked() {
                            save = true;
                        }
                    });
                if save && !name.trim().is_empty() {
                    if let Ok(conn) = self.db.lock() {
                        db_budget_add_category(&conn, name.trim(), color.trim(), recurring);
                    }
                    self.budget.reload(&self.db.lock().unwrap_or_else(|p| p.into_inner()));
                    self.status_text = format!("Added category: {}", name.trim());
                } else if open {
                    self.dialog_state = DialogState::AddCategory { name, color, recurring };
                }
            }
            DialogState::EditCategory {
                id,
                mut name,
                mut color,
            } => {
                let mut open = true;
                let mut save = false;
                egui::Window::new("Edit Category")
                    .collapsible(false)
                    .resizable(false)
                    .open(&mut open)
                    .show(&ctx, |ui| {
                        ui.label("Name:");
                        ui.add(egui::TextEdit::singleline(&mut name).desired_width(240.0));
                        ui.add_space(6.0);
                        ui.label("Color:");
                        color_swatch_row(ui, &mut color);
                        ui.horizontal(|ui| {
                            ui.label("Hex:");
                            ui.add(egui::TextEdit::singleline(&mut color).desired_width(90.0));
                        });
                        ui.add_space(8.0);
                        if ui.button("Save").clicked() {
                            save = true;
                        }
                    });
                if save && !name.trim().is_empty() {
                    if let Ok(conn) = self.db.lock() {
                        db_budget_update_category(&conn, id, name.trim(), color.trim());
                    }
                    self.budget.reload(&self.db.lock().unwrap_or_else(|p| p.into_inner()));
                } else if open {
                    self.dialog_state = DialogState::EditCategory { id, name, color };
                }
            }
            DialogState::ConfirmDeleteCategory { id, name } => {
                let mut open = true;
                let mut delete = false;
                let mut cancel = false;
                egui::Window::new("Delete Category")
                    .collapsible(false)
                    .resizable(false)
                    .open(&mut open)
                    .show(&ctx, |ui| {
                        ui.label(format!("Delete '{}' and all its monthly amounts?", name));
                        ui.add_space(8.0);
                        ui.horizontal(|ui| {
                            if ui.button("Delete").clicked() {
                                delete = true;
                            }
                            if ui.button("Cancel").clicked() {
                                cancel = true;
                            }
                        });
                    });
                if delete {
                    if let Ok(conn) = self.db.lock() {
                        db_budget_delete_category(&conn, id);
                    }
                    self.budget.reload(&self.db.lock().unwrap_or_else(|p| p.into_inner()));
                } else if open && !cancel {
                    self.dialog_state = DialogState::ConfirmDeleteCategory { id, name };
                }
            }
            DialogState::CustomModel { mut value } => {
                let mut open = true;
                let mut save = false;
                let mut cancel = false;
                egui::Window::new("Custom Model")
                    .collapsible(false)
                    .resizable(false)
                    .open(&mut open)
                    .show(&ctx, |ui| {
                        ui.label("Model id:");
                        ui.add(
                            egui::TextEdit::singleline(&mut value)
                                .desired_width(280.0)
                                .hint_text("openai/gpt-4o-mini"),
                        );
                        ui.add_space(8.0);
                        ui.horizontal(|ui| {
                            if ui.button("Use").clicked() {
                                save = true;
                            }
                            if ui.button("Cancel").clicked() {
                                cancel = true;
                            }
                        });
                    });
                if save && !value.trim().is_empty() {
                    self.chat.model = value.trim().to_string();
                    if let Ok(conn) = self.db.lock() {
                        config_set(&conn, "llm_model", value.trim());
                    }
                } else if open && !cancel {
                    self.dialog_state = DialogState::CustomModel { value };
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
        if custom_model {
            self.dialog_state = DialogState::CustomModel {
                value: if MODEL_PRESETS.contains(&self.chat.model.as_str()) {
                    String::new()
                } else {
                    self.chat.model.clone()
                },
            };
        }
        match budget_action {
            Some(BudgetAction::OpenEdit(id)) => {
                if let Some(c) = self.budget.categories.iter().find(|c| c.id == id) {
                    self.dialog_state = DialogState::EditCategory {
                        id,
                        name: c.name.clone(),
                        color: c.color.clone(),
                    };
                }
            }
            Some(BudgetAction::OpenDelete(id)) => {
                if let Some(c) = self.budget.categories.iter().find(|c| c.id == id) {
                    self.dialog_state = DialogState::ConfirmDeleteCategory {
                        id,
                        name: c.name.clone(),
                    };
                }
            }
            None => {}
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
            .with_inner_size([1200.0, 800.0])
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
