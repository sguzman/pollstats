use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderValue};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{format_description, OffsetDateTime};
use tokio::io::AsyncWriteExt;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use regex::Regex;

#[derive(Debug, Clone, Deserialize)]
struct AppConfig {
    store_dir: PathBuf,
    sources_file: PathBuf,
    postgres: Option<PostgresConfig>,
    http_timeout_seconds: Option<u64>,
    user_agent: Option<String>,
    download_concurrency: Option<usize>,
    http_retries: Option<u32>,
    download_rate_limit_kbps: Option<u64>,
    aria: Option<AriaConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct AriaConfig {
    output_dir: Option<PathBuf>,
    urls_filename: Option<String>,
    conf_filename: Option<String>,
    conf_template_file: Option<PathBuf>,
    emit: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct PostgresConfig {
    user: String,
    pass: String,
    host: String,
    port: u16,
    db: String,
    sslmode: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SourcesConfig {
    sources: Vec<Source>,
}

#[derive(Debug, Clone, Deserialize)]
struct Source {
    id: String,
    name: Option<String>,
    #[allow(dead_code)]
    homepage: Option<String>,
    datasets: Vec<DatasetConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct DatasetConfig {
    id: String,
    name: Option<String>,
    #[serde(rename = "type")]
    kind: String,
    url: String,
    crawl_download: Option<bool>,
    crawl_max_links: Option<usize>,
    crawl_same_host_only: Option<bool>,
    crawl_strategy: Option<String>,
    dataverse_subtrees: Option<Vec<String>>,
    headers: Option<BTreeMap<String, String>>,
    cookie_file: Option<String>,
}

#[derive(Debug, Clone)]
struct Dataset {
    source_id: String,
    #[allow(dead_code)]
    source_name: String,
    dataset_id: String,
    dataset_name: String,
    kind: String,
    url: String,
    crawl_download: bool,
    crawl_max_links: usize,
    crawl_same_host_only: bool,
    crawl_strategy: CrawlStrategy,
    dataverse_subtrees: Vec<String>,
    headers: BTreeMap<String, String>,
    requires_cookie: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrawlStrategy {
    ExtensionsOnly,
    HeadProbe,
    Dataverse,
}

impl CrawlStrategy {
    fn from_opt(s: Option<&str>) -> Self {
        match s.unwrap_or("extensions").to_ascii_lowercase().as_str() {
            "head" | "headprobe" | "probe" => Self::HeadProbe,
            "dataverse" => Self::Dataverse,
            _ => Self::ExtensionsOnly,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Manifest {
    source_id: Option<String>,
    dataset_id: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    url: Option<String>,
    fetched_at: Option<String>,
    last_seen_at: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    content_length: Option<String>,
    sha256: Option<String>,
    local_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Check,
    UpdateIfNewer,
    DownloadAlways,
}

#[derive(Parser, Debug)]
#[command(name = "pollstats")]
#[command(about = "US politics dataset store + updater", long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Download ONLY if remote dataset is newer than local set
    Update {
        /// Restrict to `source_id/dataset_id` (repeatable)
        #[arg(long)]
        only: Vec<String>,
    },
    /// Check if remote is newer; do not download
    Check {
        /// Restrict to `source_id/dataset_id` (repeatable)
        #[arg(long)]
        only: Vec<String>,
    },
    /// Download regardless of newer or not; replace local
    Download {
        /// Restrict to `source_id/dataset_id` (repeatable)
        #[arg(long)]
        only: Vec<String>,
    },
    /// List configured sources/datasets
    List,
    /// Emit aria2 config + url list for cached enumerations
    AriaExport {
        /// Restrict to `source_id/dataset_id` (repeatable). Uses enumeration caches.
        #[arg(long)]
        only: Vec<String>,
    },
    /// Create pollstats schema/tables in Postgres
    PgInit,
    /// Load manifest snapshots into Postgres
    PgLoad {
        /// Restrict to `source_id/dataset_id` (repeatable)
        #[arg(long)]
        only: Vec<String>,
    },
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn config_path() -> PathBuf {
    env::var_os("POLLSTATS_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/pollstats.toml"))
}

fn load_config() -> Result<(AppConfig, SourcesConfig)> {
    let app_cfg_path = config_path();
    let app_cfg_text = fs::read_to_string(&app_cfg_path)
        .with_context(|| format!("read config {}", app_cfg_path.display()))?;
    let app_cfg: AppConfig =
        toml::from_str(&app_cfg_text).context("parse config/pollstats.toml")?;

    let sources_cfg_text = fs::read_to_string(&app_cfg.sources_file)
        .with_context(|| format!("read sources {}", app_cfg.sources_file.display()))?;
    let sources_cfg: SourcesConfig =
        toml::from_str(&sources_cfg_text).context("parse config/sources.toml")?;

    Ok((app_cfg, sources_cfg))
}

fn iter_datasets(sources_cfg: &SourcesConfig) -> Vec<Dataset> {
    let mut out = Vec::new();
    for s in &sources_cfg.sources {
        let source_name = s.name.clone().unwrap_or_else(|| s.id.clone());
        for d in &s.datasets {
            out.push(Dataset {
                source_id: s.id.clone(),
                source_name: source_name.clone(),
                dataset_id: d.id.clone(),
                dataset_name: d.name.clone().unwrap_or_else(|| d.id.clone()),
                kind: d.kind.clone(),
                url: d.url.clone(),
                crawl_download: d.crawl_download.unwrap_or(false),
                crawl_max_links: d.crawl_max_links.unwrap_or(200),
                crawl_same_host_only: d.crawl_same_host_only.unwrap_or(true),
                crawl_strategy: CrawlStrategy::from_opt(d.crawl_strategy.as_deref()),
                dataverse_subtrees: d.dataverse_subtrees.clone().unwrap_or_default(),
                headers: resolve_dataset_headers(d.headers.clone().unwrap_or_default(), d.cookie_file.as_deref()),
                requires_cookie: d.cookie_file.is_some(),
            });
        }
    }
    out
}

fn resolve_dataset_headers(mut headers: BTreeMap<String, String>, cookie_file: Option<&str>) -> BTreeMap<String, String> {
    if let Some(path) = cookie_file {
        match read_cookiejar_for_domain(Path::new(path), "electionstudies.org") {
            Ok(cookie_header) => {
                if !cookie_header.trim().is_empty() {
                    headers.insert("Cookie".to_string(), cookie_header);
                } else {
                    debug!(path, "cookie file had no usable cookies for electionstudies.org");
                }
            }
            Err(e) => {
                debug!(path, error = ?e, "failed to read cookie file");
            }
        }
    }
    headers
}

fn read_cookiejar_for_domain(path: &Path, domain: &str) -> Result<String> {
    let txt = fs::read_to_string(path).with_context(|| format!("read cookie file {}", path.display()))?;
    // If the user pasted a raw Cookie header value (common from DevTools "Copy request headers"),
    // accept it as-is.
    let trimmed = txt.trim();
    if !trimmed.is_empty() {
        let first_line = trimmed.lines().next().unwrap_or("").trim();
        if first_line.to_ascii_lowercase().starts_with("cookie:") {
            return Ok(first_line["cookie:".len()..].trim().to_string());
        }
        // Heuristic: if it looks like "a=b; c=d" and not a Netscape cookiejar, accept it.
        if !first_line.contains('\t') && first_line.contains('=') {
            let raw = trimmed
                .lines()
                .filter(|l| !l.trim().is_empty() && !l.trim().starts_with('#'))
                .collect::<Vec<_>>()
                .join(" ");
            return Ok(raw.trim().to_string());
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let mut parts: Vec<String> = Vec::new();
    for line in txt.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Netscape cookie file format:
        // domain \t flag \t path \t secure \t expiration \t name \t value
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 7 {
            continue;
        }
        let mut c_domain = cols[0].trim();
        if c_domain.starts_with("#HttpOnly_") {
            c_domain = c_domain.trim_start_matches("#HttpOnly_");
        }
        let exp: i64 = cols[4].trim().parse().unwrap_or(0);
        let name = cols[5].trim();
        let value = cols[6].trim();

        if name.is_empty() {
            continue;
        }
        if exp != 0 && exp < now {
            continue;
        }
        // match exact domain or suffix (.example.org)
        let want = domain.trim_start_matches('.');
        let cd = c_domain.trim_start_matches('.');
        if cd != want && !cd.ends_with(want) && !want.ends_with(cd) {
            continue;
        }
        parts.push(format!("{name}={value}"));
    }
    Ok(parts.join("; "))
}

fn to_headermap(headers: &BTreeMap<String, String>) -> Result<HeaderMap> {
    let mut out = HeaderMap::new();
    for (k, v) in headers {
        let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
            .with_context(|| format!("invalid header name {k}"))?;
        let value = HeaderValue::from_str(v).with_context(|| format!("invalid header value for {k}"))?;
        out.insert(name, value);
    }
    Ok(out)
}

fn manifest_path(store_dir: &Path, d: &Dataset) -> PathBuf {
    store_dir
        .join("manifests")
        .join(&d.source_id)
        .join(format!("{}.json", d.dataset_id))
}

fn enumeration_path(store_dir: &Path, d: &Dataset) -> PathBuf {
    store_dir
        .join("enumerations")
        .join(&d.source_id)
        .join(format!("{}.json", d.dataset_id))
}

fn latest_dir(store_dir: &Path, d: &Dataset) -> PathBuf {
    store_dir
        .join("raw")
        .join(&d.source_id)
        .join(&d.dataset_id)
        .join("latest")
}

fn history_dir(store_dir: &Path, d: &Dataset, stamp: &str) -> PathBuf {
    store_dir
        .join("raw")
        .join(&d.source_id)
        .join(&d.dataset_id)
        .join("history")
        .join(stamp)
}

fn load_manifest(path: &Path) -> Result<Manifest> {
    if !path.exists() {
        return Ok(Manifest::default());
    }
    let txt = fs::read_to_string(path).with_context(|| format!("read manifest {}", path.display()))?;
    let m: Manifest = serde_json::from_str(&txt).with_context(|| format!("parse manifest {}", path.display()))?;
    Ok(m)
}

fn write_manifest(path: &Path, m: &Manifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let txt = serde_json::to_string_pretty(m).context("serialize manifest")?;
    fs::write(&tmp, format!("{txt}\n")).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn write_json_file<T: Serialize>(path: &Path, obj: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let txt = serde_json::to_string_pretty(obj).context("serialize json")?;
    fs::write(&tmp, format!("{txt}\n")).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let txt = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let obj: T = serde_json::from_str(&txt).with_context(|| format!("parse {}", path.display()))?;
    Ok(obj)
}

fn now_iso() -> Result<String> {
    Ok(OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339)?)
}

fn stamp_utc() -> Result<String> {
    let fmt = format_description::parse("[year][month][day]T[hour][minute][second]Z")?;
    Ok(OffsetDateTime::now_utc().format(&fmt)?)
}

fn suggest_filename(url: &str, headers: &HeaderMap) -> String {
    if let Some(v) = headers.get(reqwest::header::CONTENT_DISPOSITION) {
        if let Ok(s) = v.to_str() {
            let lower = s.to_ascii_lowercase();
            if let Some(idx) = lower.find("filename=") {
                let mut frag = s[idx + "filename=".len()..].trim().trim_end_matches(';').trim().to_string();
                if (frag.starts_with('"') && frag.ends_with('"')) || (frag.starts_with('\'') && frag.ends_with('\'')) {
                    frag = frag[1..frag.len() - 1].to_string();
                }
                let base = Path::new(&frag)
                    .file_name()
                    .and_then(|x| x.to_str())
                    .unwrap_or("")
                    .to_string();
                if !base.is_empty() {
                    return base;
                }
            }
        }
    }

    let p = url::Url::parse(url).ok();
    if let Some(p) = p {
        if let Some(seg) = p.path_segments().and_then(|mut it| it.next_back()) {
            if !seg.is_empty() {
                return seg.to_string();
            }
        }
    }
    "download.bin".to_string()
}

fn is_downloadable_link(url: &url::Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    let exts = [
        ".csv", ".tsv", ".json", ".geojson", ".zip", ".gz", ".bz2", ".xz", ".parquet", ".feather",
        ".xlsx", ".xls", ".sav", ".dta",
    ];
    exts.iter().any(|e| path.ends_with(e))
}

fn is_html_content_type(ct: &str) -> bool {
    let ct = ct.to_ascii_lowercase();
    ct.starts_with("text/html") || ct.starts_with("application/xhtml")
}

fn head_says_downloadable(headers: &HeaderMap) -> bool {
    if let Some(v) = headers.get(reqwest::header::CONTENT_DISPOSITION) {
        if let Ok(s) = v.to_str() {
            if s.to_ascii_lowercase().contains("attachment") {
                return true;
            }
        }
    }
    let Some(ct) = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_ascii_lowercase())
    else {
        return false;
    };
    if is_html_content_type(&ct) {
        return false;
    }
    // Explicitly allow common data-ish MIME types.
    if ct.starts_with("text/csv")
        || ct.starts_with("text/tab-separated-values")
        || ct.starts_with("application/zip")
        || ct.starts_with("application/gzip")
        || ct.starts_with("application/x-gzip")
        || ct.starts_with("application/json")
        || ct.starts_with("application/octet-stream")
        || ct.starts_with("application/vnd.ms-excel")
        || ct.starts_with("application/vnd.openxmlformats-officedocument")
    {
        return true;
    }
    // Avoid pulling site assets.
    if ct.starts_with("text/css")
        || ct.starts_with("text/javascript")
        || ct.starts_with("application/javascript")
        || ct.starts_with("image/")
        || ct.starts_with("font/")
        || ct.starts_with("text/plain")
    {
        return false;
    }
    // Default deny.
    false
}

#[derive(Debug, Deserialize)]
struct DataverseSearchResp {
    data: DataverseSearchData,
}

#[derive(Debug, Deserialize)]
struct DataverseSearchData {
    items: Vec<DataverseSearchItem>,
}

#[derive(Debug, Deserialize)]
struct DataverseSearchItem {
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "global_id")]
    global_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DataverseDatasetResp {
    data: DataverseDatasetData,
}

#[derive(Debug, Deserialize)]
struct DataverseDatasetData {
    #[serde(rename = "latestVersion")]
    latest_version: DataverseDatasetVersion,
}

#[derive(Debug, Deserialize)]
struct DataverseDatasetVersion {
    files: Vec<DataverseDatasetFileWrapper>,
}

#[derive(Debug, Deserialize)]
struct DataverseDatasetFileWrapper {
    #[serde(rename = "dataFile")]
    data_file: DataverseDataFile,
}

#[derive(Debug, Deserialize)]
struct DataverseDataFile {
    id: i64,
    filename: String,
    #[serde(default)]
    filesize: Option<u64>,
}

async fn dataverse_list_datasets(client: &reqwest::Client, subtree: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut start: u32 = 0;
    let per_page: u32 = 100;
    loop {
        let url = format!(
            "https://dataverse.harvard.edu/api/search?q=*&type=dataset&subtree={}&per_page={}&start={}",
            urlencoding::encode(subtree),
            per_page,
            start
        );
        let r = client.get(&url).send().await.with_context(|| format!("dataverse search {subtree}"))?;
        if !r.status().is_success() {
            return Err(anyhow!("dataverse search failed {subtree} status {}", r.status()));
        }
        let resp: DataverseSearchResp = r.json().await.context("parse dataverse search json")?;
        let mut added = 0usize;
        for it in resp.data.items {
            if it.kind != "dataset" {
                continue;
            }
            if let Some(gid) = it.global_id {
                out.push(gid);
                added += 1;
            }
        }
        if added == 0 {
            break;
        }
        start += per_page;
        if start > 10_000 {
            break;
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

async fn dataverse_list_files(client: &reqwest::Client, persistent_id: &str) -> Result<Vec<(i64, String, Option<u64>)>> {
    let url = format!(
        "https://dataverse.harvard.edu/api/datasets/:persistentId/?persistentId={}",
        urlencoding::encode(persistent_id)
    );
    let r = client.get(&url).send().await.with_context(|| format!("dataverse dataset {persistent_id}"))?;
    if !r.status().is_success() {
        return Err(anyhow!("dataverse dataset fetch failed {persistent_id} status {}", r.status()));
    }
    let resp: DataverseDatasetResp = r.json().await.context("parse dataverse dataset json")?;
    let mut out = Vec::new();
    for f in resp.data.latest_version.files {
        out.push((f.data_file.id, f.data_file.filename, f.data_file.filesize));
    }
    Ok(out)
}

async fn dataverse_download_file_url(datafile_id: i64) -> String {
    format!("https://dataverse.harvard.edu/api/access/datafile/{datafile_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataverseFilePlanItem {
    subtree: String,
    persistent_id: String,
    dataset_title: Option<String>,
    dataset_description: Option<String>,
    file_id: i64,
    filename: String,
    size_bytes: Option<u64>,
    url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataverseEnumerationCache {
    source_id: String,
    dataset_id: String,
    subtrees: Vec<String>,
    enumerated_at: String,
    datasets: usize,
    files: usize,
    total_bytes: Option<u64>,
    items: Vec<DataverseFilePlanItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataverseDatasetMeta {
    title: Option<String>,
    description: Option<String>,
}

async fn dataverse_get_dataset_meta(client: &reqwest::Client, persistent_id: &str) -> Result<DataverseDatasetMeta> {
    let url = format!(
        "https://dataverse.harvard.edu/api/datasets/:persistentId/?persistentId={}",
        urlencoding::encode(persistent_id)
    );
    let r = client.get(&url).send().await.with_context(|| format!("dataverse dataset meta {persistent_id}"))?;
    if !r.status().is_success() {
        return Err(anyhow!("dataverse dataset meta fetch failed {persistent_id} status {}", r.status()));
    }
    let v: serde_json::Value = r.json().await.context("parse dataverse dataset meta json")?;

    let mut title: Option<String> = None;
    let mut descs: Vec<String> = Vec::new();

    let fields = v
        .get("data")
        .and_then(|d| d.get("latestVersion"))
        .and_then(|lv| lv.get("metadataBlocks"))
        .and_then(|mb| mb.get("citation"))
        .and_then(|c| c.get("fields"))
        .and_then(|f| f.as_array())
        .cloned()
        .unwrap_or_default();

    for f in fields {
        let Some(type_name) = f.get("typeName").and_then(|x| x.as_str()) else { continue };
        match type_name {
            "title" => {
                if let Some(v) = f.get("value").and_then(|x| x.as_str()) {
                    title = Some(v.to_string());
                }
            }
            "dsDescription" => {
                if let Some(arr) = f.get("value").and_then(|x| x.as_array()) {
                    for item in arr {
                        // item.dsDescriptionValue.value is typical
                        let val = item
                            .get("dsDescriptionValue")
                            .and_then(|x| x.get("value"))
                            .and_then(|x| x.as_str());
                        if let Some(s) = val {
                            let s = s.trim();
                            if !s.is_empty() {
                                descs.push(s.to_string());
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let description = if descs.is_empty() {
        None
    } else {
        let mut merged = String::new();
        for (i, d) in descs.into_iter().enumerate() {
            if i > 0 {
                merged.push('\n');
                merged.push('\n');
            }
            merged.push_str(&d);
        }
        Some(merged)
    };

    Ok(DataverseDatasetMeta { title, description })
}

async fn ensure_dataverse_enumeration(
    client: &reqwest::Client,
    store_dir: &Path,
    d: &Dataset,
    refresh: bool,
) -> Result<DataverseEnumerationCache> {
    let path = enumeration_path(store_dir, d);
    if !refresh && path.exists() {
        return read_json_file(&path);
    }

    let enumerated_at = now_iso()?;
    let mut subtrees = d.dataverse_subtrees.clone();
    if subtrees.is_empty() {
        let resp = client.get(&d.url).send().await.with_context(|| format!("get page {}", d.url))?;
        let body = resp.text().await.context("read page body")?;
        let re = Regex::new(r#"https://dataverse\.harvard\.edu/dataverse/([A-Za-z0-9_-]+)"#).expect("dv re");
        for cap in re.captures_iter(&body) {
            if let Some(m) = cap.get(1) {
                subtrees.push(m.as_str().to_string());
            }
        }
        subtrees.sort();
        subtrees.dedup();
    }

    if subtrees.is_empty() {
        return Err(anyhow!("no dataverse subtrees configured/found"));
    }

    let mut items: Vec<DataverseFilePlanItem> = Vec::new();
    let mut total_datasets: usize = 0;
    let mut total_bytes: u64 = 0;
    let mut any_size: bool = false;
    for subtree in &subtrees {
        let pids = dataverse_list_datasets(client, subtree).await?;
        total_datasets += pids.len();
        for pid in pids {
            let meta = match dataverse_get_dataset_meta(client, &pid).await {
                Ok(m) => m,
                Err(e) => {
                    warn!(persistent_id = %pid, error = ?e, "dataverse meta fetch failed");
                    DataverseDatasetMeta {
                        title: None,
                        description: None,
                    }
                }
            };
            let files = dataverse_list_files(client, &pid).await?;
            for (fid, fname, size) in files {
                let url = dataverse_download_file_url(fid).await;
                if let Some(sz) = size {
                    total_bytes = total_bytes.saturating_add(sz);
                    any_size = true;
                }
                items.push(DataverseFilePlanItem {
                    subtree: subtree.clone(),
                    persistent_id: pid.clone(),
                    dataset_title: meta.title.clone(),
                    dataset_description: meta.description.clone(),
                    file_id: fid,
                    filename: fname,
                    size_bytes: size,
                    url,
                });
            }
        }
    }

    let cache = DataverseEnumerationCache {
        source_id: d.source_id.clone(),
        dataset_id: d.dataset_id.clone(),
        subtrees,
        enumerated_at,
        datasets: total_datasets,
        files: items.len(),
        total_bytes: if any_size { Some(total_bytes) } else { None },
        items,
    };
    write_json_file(&path, &cache)?;
    Ok(cache)
}

async fn crawl_links(
    client: &reqwest::Client,
    base_url: &str,
    max_links: usize,
    same_host_only: bool,
) -> Result<Vec<String>> {
    let base = url::Url::parse(base_url).with_context(|| format!("parse url {base_url}"))?;
    let resp = client
        .get(base_url)
        .send()
        .await
        .with_context(|| format!("get page {base_url}"))?;
    if !resp.status().is_success() {
        return Err(anyhow!("page fetch failed {base_url} status {}", resp.status()));
    }
    let body = resp.text().await.context("read page body")?;

    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").expect("selector");

    let mut out: Vec<url::Url> = Vec::new();
    for el in doc.select(&sel) {
        let Some(href) = el.value().attr("href") else { continue };
        let href = href.trim();
        if href.is_empty() || href.starts_with('#') || href.starts_with("javascript:") || href.starts_with("mailto:") {
            continue;
        }
        let resolved = base.join(href).ok();
        let Some(u) = resolved else { continue };
        if u.scheme() != "http" && u.scheme() != "https" {
            continue;
        }
        if same_host_only && u.host_str() != base.host_str() {
            continue;
        }
        out.push(u);
        if out.len() >= max_links {
            break;
        }
    }

    // Fallback: embedded absolute/root-relative URLs (common in JS apps).
    if out.len() < max_links {
        let re_abs = Regex::new(r#"https?://[^\s"'<>\\]+(\?[^\s"'<>\\]+)?"#).expect("re_abs");
        for m in re_abs.find_iter(&body) {
            if let Ok(u) = url::Url::parse(m.as_str()) {
                if u.scheme() != "http" && u.scheme() != "https" {
                    continue;
                }
                if same_host_only && u.host_str() != base.host_str() {
                    continue;
                }
                out.push(u);
                if out.len() >= max_links {
                    break;
                }
            }
        }
    }

    if out.len() < max_links {
        let re_rel = Regex::new(r#""(/[^"'<>\\]+)""#).expect("re_rel");
        for cap in re_rel.captures_iter(&body) {
            let Some(rel) = cap.get(1) else { continue };
            if let Ok(u) = base.join(rel.as_str()) {
                if u.scheme() != "http" && u.scheme() != "https" {
                    continue;
                }
                if same_host_only && u.host_str() != base.host_str() {
                    continue;
                }
                out.push(u);
                if out.len() >= max_links {
                    break;
                }
            }
        }
    }

    out.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    out.dedup_by(|a, b| a.as_str() == b.as_str());
    Ok(out.into_iter().map(|u| u.to_string()).collect())
}

async fn sha256_file(path: &Path) -> Result<String> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    let mut h = Sha256::new();
    h.update(&bytes);
    Ok(hex::encode(h.finalize()))
}

fn is_selected(only: &[String], d: &Dataset) -> bool {
    if only.is_empty() {
        return true;
    }
    let key = format!("{}/{}", d.source_id, d.dataset_id);
    only.iter().any(|x| x == &key)
}

fn aria_output_paths(cfg: &AppConfig) -> (PathBuf, PathBuf) {
    let output_dir = cfg
        .aria
        .as_ref()
        .and_then(|a| a.output_dir.clone())
        .unwrap_or_else(|| cfg.store_dir.join("aria"));
    let urls_name = cfg
        .aria
        .as_ref()
        .and_then(|a| a.urls_filename.clone())
        .unwrap_or_else(|| "urls".to_string());
    let conf_name = cfg
        .aria
        .as_ref()
        .and_then(|a| a.conf_filename.clone())
        .unwrap_or_else(|| "aria2.conf".to_string());
    (output_dir.join(urls_name), output_dir.join(conf_name))
}

fn aria_template_file(cfg: &AppConfig) -> Option<PathBuf> {
    cfg.aria
        .as_ref()
        .and_then(|a| a.conf_template_file.clone())
        .or_else(|| Some(PathBuf::from("tmp/conf.conf")))
}

fn aria_emit_map(cfg: &AppConfig) -> BTreeMap<String, String> {
    cfg.aria
        .as_ref()
        .and_then(|a| a.emit.clone())
        .unwrap_or_default()
}

fn compute_hashed_dataset_id(parent_dataset_id: &str, url: &str) -> String {
    let mut h = Sha256::new();
    h.update(url.as_bytes());
    let url_hash = hex::encode(h.finalize());
    format!("{}-{}", parent_dataset_id, &url_hash[..16])
}

fn sanitize_aria_filename(name: &str) -> String {
    // Keep it simple: aria2 will handle most names, but avoid path traversal.
    let name = name.replace('\\', "_").replace('/', "_");
    if name.is_empty() {
        "download.bin".to_string()
    } else {
        name
    }
}

fn write_aria_conf(cfg: &AppConfig, conf_path: &Path, urls_path: &Path) -> Result<()> {
    if let Some(parent) = conf_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }

    let emit = aria_emit_map(cfg);
    let mut lines: Vec<String> = Vec::new();
    if !emit.is_empty() {
        lines.push("# aria2.conf (generated by pollstats)".to_string());
        for (k, v) in emit {
            lines.push(format!("{k}={v}"));
        }
    } else {
        let template = aria_template_file(cfg).ok_or_else(|| anyhow!("missing aria conf template"))?;
        let raw = fs::read_to_string(&template)
            .with_context(|| format!("read aria conf template {}", template.display()))?;
        lines = raw.lines().map(|s| s.to_string()).collect();
    }

    // Patch input-file to the urls filename (relative to conf dir).
    let urls_name = urls_path
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("urls");
    for l in &mut lines {
        if l.trim_start().starts_with("input-file=") {
            *l = format!("input-file={}", urls_name);
        }
        if l.trim_start().starts_with("max-download-limit=") {
            let rate_bps = cfg.download_rate_limit_kbps.unwrap_or(500).saturating_mul(1024);
            *l = format!("max-download-limit={}", rate_bps);
        }
    }

    let txt = lines.join("\n") + "\n";
    fs::write(conf_path, txt).with_context(|| format!("write {}", conf_path.display()))?;
    Ok(())
}

fn write_aria_urls(
    cfg: &AppConfig,
    urls_path: &Path,
    selections: &[String],
    sources_cfg: &SourcesConfig,
) -> Result<()> {
    if let Some(parent) = urls_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }

    let store_dir = &cfg.store_dir;
    let mut out = String::new();

    // For each configured dataset, if an enumeration cache exists (or can be generated by check),
    // emit per-file entries.
    for d in iter_datasets(sources_cfg) {
        if !selections.is_empty() {
            let key = format!("{}/{}", d.source_id, d.dataset_id);
            if !selections.iter().any(|x| x == &key) {
                continue;
            }
        }

        let enum_path = enumeration_path(store_dir, &d);
        if !enum_path.exists() {
            continue;
        }

        // Currently we only emit from the Dataverse enumeration schema.
        let cache: DataverseEnumerationCache = read_json_file(&enum_path)
            .with_context(|| format!("read enumeration {}", enum_path.display()))?;

        // Headers for aria (from dataset config), if any.
        let mut header_lines: Vec<String> = Vec::new();
        for (k, v) in d.headers.iter() {
            header_lines.push(format!("header={}: {}", k, v));
        }

        for it in cache.items {
            let hashed_dataset_id = compute_hashed_dataset_id(&d.dataset_id, &it.url);
            let dir = store_dir
                .join("raw")
                .join(&d.source_id)
                .join(&hashed_dataset_id)
                .join("latest");
            let out_name = sanitize_aria_filename(&it.filename);

            out.push_str(&it.url);
            out.push('\n');
            out.push_str(&format!("  dir={}\n", dir.to_string_lossy()));
            out.push_str(&format!("  out={}\n", out_name));
            for hl in &header_lines {
                out.push_str("  ");
                out.push_str(hl);
                out.push('\n');
            }
            out.push('\n');
        }
    }

    if out.is_empty() {
        return Err(anyhow!(
            "no enumeration caches found to emit; run `pollstats check --only <source/dataset>` first"
        ));
    }

    fs::write(urls_path, out).with_context(|| format!("write {}", urls_path.display()))?;
    Ok(())
}

async fn process_http_file(
    client: &reqwest::Client,
    mode: Mode,
    store_dir: &Path,
    source_id: &str,
    dataset_id: &str,
    url: &str,
    retries: u32,
    rate_limit_bps: u64,
    extra_headers: &BTreeMap<String, String>,
) -> Result<()> {
    let d = Dataset {
        source_id: source_id.to_string(),
        source_name: source_id.to_string(),
        dataset_id: dataset_id.to_string(),
        dataset_name: dataset_id.to_string(),
        kind: "http_file".to_string(),
        url: url.to_string(),
        crawl_download: false,
        crawl_max_links: 0,
        crawl_same_host_only: false,
        crawl_strategy: CrawlStrategy::ExtensionsOnly,
        dataverse_subtrees: Vec::new(),
        headers: extra_headers.clone(),
        requires_cookie: false,
    };

    let mp = manifest_path(store_dir, &d);
    let mut m = load_manifest(&mp)?;
    let now = now_iso()?;

    let mut etag: Option<String> = None;
    let mut last_modified: Option<String> = None;
    let mut content_length: Option<String> = None;
    let mut remote_newer = true;

    // Some hosts (notably Dataverse access endpoints) return 403 to HEAD. Treat that as "unknown",
    // and fall back to downloading (GET), which may still succeed.
    let extra = to_headermap(&d.headers)?;
    match client.head(&d.url).headers(extra.clone()).send().await {
        Ok(head) => {
            let status = head.status();
            if status == reqwest::StatusCode::FORBIDDEN || status == reqwest::StatusCode::METHOD_NOT_ALLOWED {
                warn!(source_id = %d.source_id, dataset_id = %d.dataset_id, url = %d.url, http_status = %status, "head not allowed; will download");
            } else {
                if !status.is_success() {
                    warn!(source_id = %d.source_id, dataset_id = %d.dataset_id, url = %d.url, http_status = %status, "head not success");
                }

                etag = head
                    .headers()
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                last_modified = head
                    .headers()
                    .get(reqwest::header::LAST_MODIFIED)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                content_length = head
                    .headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());

                remote_newer = match (&m.etag, &etag, &m.last_modified, &last_modified) {
                    (Some(a), Some(b), _, _) if a == b => false,
                    (_, _, Some(a), Some(b)) if a == b => false,
                    _ => true,
                };
            }
        }
        Err(e) => {
            warn!(source_id = %d.source_id, dataset_id = %d.dataset_id, url = %d.url, error = ?e, "head failed; will download");
        }
    }

    // If a file already exists on disk (e.g. downloaded via aria2) but no manifest exists yet,
    // adopt it as the latest and write a manifest so subsequent runs are idempotent across tools.
    if m.sha256.is_none() && !matches!(mode, Mode::DownloadAlways) {
        let ldir = latest_dir(store_dir, &d);
        if ldir.exists() {
            let mut entries: Vec<PathBuf> = Vec::new();
            if let Ok(rd) = fs::read_dir(&ldir) {
                for e in rd.flatten() {
                    let p = e.path();
                    if p.is_file() {
                        entries.push(p);
                    }
                }
            }
            if entries.len() == 1 {
                let p = entries.pop().unwrap();
                let sha = sha256_file(&p).await?;
                m.source_id = Some(d.source_id.clone());
                m.dataset_id = Some(d.dataset_id.clone());
                m.kind = Some(d.kind.clone());
                m.url = Some(d.url.clone());
                m.fetched_at = Some(now_iso()?);
                m.etag = etag.clone();
                m.last_modified = last_modified.clone();
                m.content_length = content_length.clone();
                m.sha256 = Some(sha);
                m.local_path = Some(p.to_string_lossy().to_string());
                write_manifest(&mp, &m)?;

                info!(
                    source_id = %d.source_id,
                    dataset_id = %d.dataset_id,
                    path = %m.local_path.as_deref().unwrap_or(""),
                    "adopted existing download (no manifest)"
                );

                // If we have ETag/Last-Modified from HEAD, treat as up-to-date.
                remote_newer = false;
            }
        }
    }

    match mode {
        Mode::Check => {
            info!(
                source_id = %d.source_id,
                dataset_id = %d.dataset_id,
                newer = remote_newer,
                etag = etag.as_deref().unwrap_or(""),
                last_modified = last_modified.as_deref().unwrap_or(""),
                "checked"
            );
            return Ok(());
        }
        Mode::UpdateIfNewer if !remote_newer => {
            info!(source_id = %d.source_id, dataset_id = %d.dataset_id, "unchanged; skip download");
            return Ok(());
        }
        Mode::UpdateIfNewer | Mode::DownloadAlways => {}
    }

    let mut headers = HeaderMap::new();
    if mode == Mode::UpdateIfNewer {
        if let Some(ref e) = m.etag {
            if let Ok(v) = HeaderValue::from_str(e) {
                headers.insert(reqwest::header::IF_NONE_MATCH, v);
            }
        }
        if let Some(ref lm) = m.last_modified {
            if let Ok(v) = HeaderValue::from_str(lm) {
                headers.insert(reqwest::header::IF_MODIFIED_SINCE, v);
            }
        }
    }
    // Merge in extra headers (e.g. Cookie) last so they win.
    for (k, v) in extra.iter() {
        headers.insert(k, v.clone());
    }

    let ldir = latest_dir(store_dir, &d);
    tokio::fs::create_dir_all(&ldir)
        .await
        .with_context(|| format!("mkdir {}", ldir.display()))?;

    let mut attempt: u32 = 0;
    #[allow(unused_assignments)]
    let mut bytes_written: u64 = 0;
    #[allow(unused_assignments)]
    let mut out: Option<PathBuf> = None;
    loop {
        attempt += 1;

        let resp = client
            .get(&d.url)
            .headers(headers.clone())
            .send()
            .await
            .with_context(|| format!("get {}", d.url))?;
        if d.source_id == "anes" && resp.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(anyhow!(
                "ANES cookie invalid/expired (got 403). Refusing to attempt electionstudies.org further."
            ));
        }
        if resp.content_length().is_none() {
            // Some hosts stream without length; allow, but log because timeouts are more likely.
            debug!(source_id = %d.source_id, dataset_id = %d.dataset_id, url = %d.url, "no content-length");
        }
        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            info!(source_id = %d.source_id, dataset_id = %d.dataset_id, "304 not modified");
            return Ok(());
        }
        if !resp.status().is_success() {
            return Err(anyhow!(
                "download not success for {}/{} (status {})",
                d.source_id,
                d.dataset_id,
                resp.status()
            ));
        }

        let fname = suggest_filename(&d.url, resp.headers());
        let path = ldir.join(fname);
        let tmp = path.with_extension("part");
        debug!(path = %path.display(), attempt, "writing download");

        let started = Instant::now();
        match (async {
            let mut written: u64 = 0;
            let mut file = tokio::fs::File::create(&tmp)
                .await
                .with_context(|| format!("create {}", tmp.display()))?;

            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.context("read http body chunk")?;
                file.write_all(&chunk).await.context("write chunk")?;
                written += chunk.len() as u64;
                if rate_limit_bps > 0 {
                    let expected_secs = written as f64 / rate_limit_bps as f64;
                    let elapsed_secs = started.elapsed().as_secs_f64();
                    if expected_secs > elapsed_secs {
                        let sleep_s = expected_secs - elapsed_secs;
                        tokio::time::sleep(std::time::Duration::from_secs_f64(sleep_s)).await;
                    }
                }
            }
            file.flush().await.ok();
            tokio::fs::rename(&tmp, &path)
                .await
                .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
            Ok::<u64, anyhow::Error>(written)
        })
        .await
        {
            Ok(written) => {
                bytes_written = written;
                out = Some(path);
                break;
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                let is_timeout = e
                    .downcast_ref::<reqwest::Error>()
                    .map(|re| re.is_timeout())
                    .unwrap_or(false);
                if is_timeout && attempt < retries.max(1) {
                    warn!(source_id = %d.source_id, dataset_id = %d.dataset_id, attempt, error = ?e, "download timed out; retrying");
                    // crude backoff
                    tokio::time::sleep(std::time::Duration::from_secs(2_u64.saturating_mul(attempt as u64))).await;
                    continue;
                }
                return Err(e);
            }
        }
    }

    let out = out.ok_or_else(|| anyhow!("missing output path"))?;
    let sha = sha256_file(&out).await?;
    if let Some(prev) = m.sha256.as_deref() {
        if prev != sha {
            let stamp = stamp_utc()?;
            let hdir = history_dir(store_dir, &d, &stamp);
            tokio::fs::create_dir_all(&hdir)
                .await
                .with_context(|| format!("mkdir {}", hdir.display()))?;
            let hout = hdir.join("download.bin");
            tokio::fs::copy(&out, &hout)
                .await
                .with_context(|| format!("copy {} -> {}", out.display(), hout.display()))?;
        }
    }

    m.source_id = Some(d.source_id.clone());
    m.dataset_id = Some(d.dataset_id.clone());
    m.kind = Some(d.kind.clone());
    m.url = Some(d.url.clone());
    m.fetched_at = Some(now);
    m.etag = etag;
    m.last_modified = last_modified;
    m.content_length = content_length;
    m.sha256 = Some(sha);
    m.local_path = Some(out.to_string_lossy().to_string());
    write_manifest(&mp, &m)?;

    info!(
        source_id = %d.source_id,
        dataset_id = %d.dataset_id,
        bytes = bytes_written,
        path = %m.local_path.as_deref().unwrap_or(""),
        "downloaded"
    );
    Ok(())
}

async fn process_dataset(
    client: &reqwest::Client,
    mode: Mode,
    store_dir: &Path,
    d: &Dataset,
    retries: u32,
    rate_limit_bps: u64,
    check_refresh: bool,
) -> Result<()> {
    if d.kind == "http_page" {
        let mp = manifest_path(store_dir, d);
        let mut m = load_manifest(&mp)?;
        let now = now_iso()?;

        m.source_id = Some(d.source_id.clone());
        m.dataset_id = Some(d.dataset_id.clone());
        m.kind = Some(d.kind.clone());
        m.url = Some(d.url.clone());
        m.last_seen_at = Some(now);
        write_manifest(&mp, &m)?;
        info!(source_id = %d.source_id, dataset_id = %d.dataset_id, url = %d.url, crawl_download = d.crawl_download, "page dataset");

        if !d.crawl_download {
            return Ok(());
        }

        match d.crawl_strategy {
            CrawlStrategy::Dataverse => {
                let refresh = matches!(mode, Mode::Check) && check_refresh;
                let cache = ensure_dataverse_enumeration(client, store_dir, d, refresh).await?;
                info!(
                    source_id = %d.source_id,
                    dataset_id = %d.dataset_id,
                    enumerated_at = %cache.enumerated_at,
                    datasets = cache.datasets,
                    files = cache.files,
                    total_bytes = cache.total_bytes.unwrap_or(0),
                    subtrees = ?cache.subtrees,
                    "dataverse enumeration"
                );

                if matches!(mode, Mode::Check) {
                    return Ok(());
                }

                let mut any_failed = false;
                for it in cache.items {
                    let mut h = Sha256::new();
                    h.update(it.url.as_bytes());
                    let url_hash = hex::encode(h.finalize());
                    let key_dataset_id = format!("{}-{}", d.dataset_id, &url_hash[..16]);
                    if let Err(e) = process_http_file(
                        client,
                        mode,
                        store_dir,
                        &d.source_id,
                        &key_dataset_id,
                        &it.url,
                        retries,
                        rate_limit_bps,
                        &BTreeMap::new(),
                    )
                    .await
                    {
                        any_failed = true;
                        error!(
                            source_id = %d.source_id,
                            dataset_id = %d.dataset_id,
                            subtree = %it.subtree,
                            persistent_id = %it.persistent_id,
                            file_id = it.file_id,
                            filename = %it.filename,
                            size_bytes = it.size_bytes.unwrap_or(0),
                            url = %it.url,
                            error = ?e,
                            "dataverse file failed"
                        );
                    }
                }
                if any_failed {
                    return Err(anyhow!("one or more dataverse downloads failed"));
                }
                Ok::<(), anyhow::Error>(())
            }
            _ => {
                if matches!(mode, Mode::Check) {
                    let links = crawl_links(client, &d.url, d.crawl_max_links, d.crawl_same_host_only).await?;
                    info!(source_id = %d.source_id, dataset_id = %d.dataset_id, links = links.len(), strategy = ?d.crawl_strategy, "discovered links (check only)");
                    return Ok(());
                }
                let links = crawl_links(client, &d.url, d.crawl_max_links, d.crawl_same_host_only).await?;
                info!(source_id = %d.source_id, dataset_id = %d.dataset_id, links = links.len(), strategy = ?d.crawl_strategy, "discovered links");
                let mut any_failed = false;
                for link in links {
                    // Strategy: either only download links with known extensions, or probe with HEAD.
                    if d.crawl_strategy == CrawlStrategy::ExtensionsOnly {
                        if let Ok(u) = url::Url::parse(&link) {
                            if !is_downloadable_link(&u) {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    } else if d.crawl_strategy == CrawlStrategy::HeadProbe {
                        match client.head(&link).send().await {
                            Ok(r) => {
                                if !r.status().is_success() || !head_says_downloadable(r.headers()) {
                                    continue;
                                }
                            }
                            Err(_) => continue,
                        }
                    }

                    let mut h = Sha256::new();
                    h.update(link.as_bytes());
                    let url_hash = hex::encode(h.finalize());
                    let key_dataset_id = format!("{}-{}", d.dataset_id, &url_hash[..16]);
                    if let Err(e) =
                        process_http_file(client, mode, store_dir, &d.source_id, &key_dataset_id, &link, retries, rate_limit_bps, &BTreeMap::new()).await
                    {
                        any_failed = true;
                        error!(source_id = %d.source_id, dataset_id = %d.dataset_id, link = %link, error = ?e, "link failed");
                    }
                }
                if any_failed {
                    return Err(anyhow!("one or more crawled links failed"));
                }
                Ok(())
            }
        }?;
        return Ok(());
    }

    if d.kind != "http_file" {
        return Err(anyhow!("unknown dataset type {} for {}/{}", d.kind, d.source_id, d.dataset_id));
    }
    if d.requires_cookie && !d.headers.contains_key("Cookie") {
        return Err(anyhow!(
            "cookie required but missing/invalid for {}/{}; refusing to attempt this domain",
            d.source_id,
            d.dataset_id
        ));
    }
    process_http_file(
        client,
        mode,
        store_dir,
        &d.source_id,
        &d.dataset_id,
        &d.url,
        retries,
        rate_limit_bps,
        &d.headers,
    )
    .await
}

async fn run_http(mode: Mode, only: Vec<String>, check_refresh: bool) -> Result<i32> {
    let (cfg, sources_cfg) = load_config()?;
    fs::create_dir_all(&cfg.store_dir).with_context(|| format!("mkdir {}", cfg.store_dir.display()))?;

    let timeout_s = cfg.http_timeout_seconds.unwrap_or(60);
    let user_agent = cfg
        .user_agent
        .clone()
        .unwrap_or_else(|| "pollstats/0.1 (+local)".to_string());

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_s))
        .user_agent(user_agent)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .context("build http client")?;

    let retries = cfg.http_retries.unwrap_or(3);
    let rate_limit_bps = cfg
        .download_rate_limit_kbps
        .unwrap_or(500)
        .saturating_mul(1024);

    let concurrency = cfg.download_concurrency.unwrap_or(1).max(1);
    info!(concurrency, mode = ?mode, "starting run");

    let datasets: Vec<Dataset> = iter_datasets(&sources_cfg)
        .into_iter()
        .filter(|d| is_selected(&only, d))
        .collect();

    let tasks = futures_util::stream::iter(datasets.into_iter().map(|d| {
        let client = client.clone();
        let store_dir = cfg.store_dir.clone();
        let key = format!("{}/{}", d.source_id, d.dataset_id);
        async move {
            process_dataset(&client, mode, &store_dir, &d, retries, rate_limit_bps, check_refresh)
                .await
                .with_context(|| format!("process {key}"))
        }
    }))
    .buffer_unordered(concurrency);

    let mut any_failed = false;
    futures_util::pin_mut!(tasks);
    while let Some(res) = tasks.next().await {
        match res {
            Ok(()) => {}
            Err(e) => {
                any_failed = true;
                error!(error = ?e, "dataset failed");
            }
        }
    }

    Ok(if any_failed { 1 } else { 0 })
}

async fn pg_url(cfg: &AppConfig) -> Result<String> {
    if let Ok(v) = env::var("POLLSTATS_PG_URL") {
        if !v.trim().is_empty() {
            return Ok(v);
        }
    }
    let Some(pg) = cfg.postgres.clone() else {
        return Err(anyhow!(
            "missing [postgres] config in config/pollstats.toml (or POLLSTATS_PG_URL)"
        ));
    };
    let sslmode = pg.sslmode.unwrap_or_else(|| "disable".to_string());
    Ok(format!(
        "postgresql://{}:{}@{}:{}/{}?sslmode={}",
        urlencoding::encode(&pg.user),
        urlencoding::encode(&pg.pass),
        pg.host,
        pg.port,
        pg.db,
        sslmode
    ))
}

async fn cmd_pg_init() -> Result<i32> {
    let (cfg, _) = load_config()?;
    let url = pg_url(&cfg).await?;
    let sql = fs::read_to_string("sql/pollstats.sql").context("read sql/pollstats.sql")?;
    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .context("connect postgres")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(error = %e, "postgres connection error");
        }
    });
    client.batch_execute(&sql).await.context("execute schema")?;
    info!("pg-init ok");
    Ok(0)
}

async fn cmd_pg_load(only: Vec<String>) -> Result<i32> {
    let (cfg, sources_cfg) = load_config()?;
    let url = pg_url(&cfg).await?;

    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .context("connect postgres")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(error = %e, "postgres connection error");
        }
    });

    let mut rows = Vec::new();
    for d in iter_datasets(&sources_cfg) {
        if !is_selected(&only, &d) {
            continue;
        }
        let mp = manifest_path(&cfg.store_dir, &d);
        if !mp.exists() {
            continue;
        }
        let m = load_manifest(&mp)?;
        rows.push((
            d.source_id,
            d.dataset_id,
            d.url,
            m.etag.unwrap_or_default(),
            m.last_modified.unwrap_or_default(),
            m.sha256.unwrap_or_default(),
            mp.to_string_lossy().to_string(),
        ));
    }

    if rows.is_empty() {
        info!("pg-load: no manifests found");
        return Ok(0);
    }

    let stmt = client
        .prepare(
            "insert into pollstats.ingest_files \
             (source_id, dataset_id, fetched_at, url, status, http_status, etag, last_modified, sha256, bytes, local_path, note) \
             values ($1,$2,now(),$3,'manifest',null,$4,$5,$6,null,$7,'manifest snapshot')",
        )
        .await
        .context("prepare insert")?;

    let mut inserted = 0u64;
    for r in rows {
        client
            .execute(&stmt, &[&r.0, &r.1, &r.2, &r.3, &r.4, &r.5, &r.6])
            .await
            .context("insert row")?;
        inserted += 1;
    }
    info!(inserted, "pg-load ok");
    Ok(0)
}

async fn cmd_aria_export(only: Vec<String>) -> Result<i32> {
    let (cfg, sources_cfg) = load_config()?;

    let timeout_s = cfg.http_timeout_seconds.unwrap_or(60);
    let user_agent = cfg
        .user_agent
        .clone()
        .unwrap_or_else(|| "pollstats/0.1 (+local)".to_string());
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_s))
        .user_agent(user_agent)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .context("build http client")?;

    // Ensure enumerations exist for selected Dataverse datasets.
    for d in iter_datasets(&sources_cfg) {
        if !only.is_empty() {
            let key = format!("{}/{}", d.source_id, d.dataset_id);
            if !only.iter().any(|x| x == &key) {
                continue;
            }
        }
        if d.kind == "http_page" && d.crawl_strategy == CrawlStrategy::Dataverse && d.crawl_download {
            let enum_path = enumeration_path(&cfg.store_dir, &d);
            if !enum_path.exists() {
                let _ = ensure_dataverse_enumeration(&client, &cfg.store_dir, &d, false).await?;
            }
        }
    }

    let (urls_path, conf_path) = aria_output_paths(&cfg);
    write_aria_urls(&cfg, &urls_path, &only, &sources_cfg)?;
    write_aria_conf(&cfg, &conf_path, &urls_path)?;

    info!(
        urls = %urls_path.display(),
        conf = %conf_path.display(),
        "aria export written"
    );
    Ok(0)
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let code = match cli.cmd {
        Command::List => {
            let (_, sources_cfg) = load_config()?;
            for d in iter_datasets(&sources_cfg) {
                println!(
                    "{}/{}\t{}\t{}\t{}",
                    d.source_id, d.dataset_id, d.kind, d.dataset_name, d.url
                );
            }
            0
        }
        Command::Check { only } => run_http(Mode::Check, only, true).await?,
        Command::Update { only } => run_http(Mode::UpdateIfNewer, only, false).await?,
        Command::Download { only } => run_http(Mode::DownloadAlways, only, false).await?,
        Command::AriaExport { only } => cmd_aria_export(only).await?,
        Command::PgInit => cmd_pg_init().await?,
        Command::PgLoad { only } => cmd_pg_load(only).await?,
    };
    if code != 0 {
        return Err(anyhow!("pollstats exited with {}", code));
    }
    Ok(())
}
