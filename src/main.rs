use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{format_description, OffsetDateTime};
use tokio::io::AsyncWriteExt;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Deserialize)]
struct AppConfig {
    store_dir: PathBuf,
    sources_file: PathBuf,
    postgres: Option<PostgresConfig>,
    http_timeout_seconds: Option<u64>,
    user_agent: Option<String>,
    download_concurrency: Option<usize>,
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
            });
        }
    }
    out
}

fn manifest_path(store_dir: &Path, d: &Dataset) -> PathBuf {
    store_dir
        .join("manifests")
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

async fn process_dataset(client: &reqwest::Client, mode: Mode, store_dir: &Path, d: &Dataset) -> Result<()> {
    let mp = manifest_path(store_dir, d);
    let mut m = load_manifest(&mp)?;
    let now = now_iso()?;

    if d.kind == "http_page" {
        m.source_id = Some(d.source_id.clone());
        m.dataset_id = Some(d.dataset_id.clone());
        m.kind = Some(d.kind.clone());
        m.url = Some(d.url.clone());
        m.last_seen_at = Some(now);
        write_manifest(&mp, &m)?;
        info!(source_id = %d.source_id, dataset_id = %d.dataset_id, url = %d.url, "skip page dataset");
        return Ok(());
    }

    if d.kind != "http_file" {
        return Err(anyhow!("unknown dataset type {} for {}/{}", d.kind, d.source_id, d.dataset_id));
    }

    let head = client
        .head(&d.url)
        .send()
        .await
        .with_context(|| format!("head {}", d.url))?;
    let status = head.status();
    if !status.is_success() {
        warn!(source_id = %d.source_id, dataset_id = %d.dataset_id, url = %d.url, http_status = %status, "head not success");
    }

    let etag = head
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let last_modified = head
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let content_length = head
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let remote_newer = match (&m.etag, &etag, &m.last_modified, &last_modified) {
        (Some(a), Some(b), _, _) if a == b => false,
        (_, _, Some(a), Some(b)) if a == b => false,
        _ => true,
    };

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
        if let Some(ref e) = etag {
            if let Ok(v) = HeaderValue::from_str(e) {
                headers.insert(reqwest::header::IF_NONE_MATCH, v);
            }
        }
        if let Some(ref lm) = last_modified {
            if let Ok(v) = HeaderValue::from_str(lm) {
                headers.insert(reqwest::header::IF_MODIFIED_SINCE, v);
            }
        }
    }

    let resp = client
        .get(&d.url)
        .headers(headers)
        .send()
        .await
        .with_context(|| format!("get {}", d.url))?;
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

    let ldir = latest_dir(store_dir, d);
    tokio::fs::create_dir_all(&ldir)
        .await
        .with_context(|| format!("mkdir {}", ldir.display()))?;

    let fname = suggest_filename(&d.url, resp.headers());
    let out = ldir.join(fname);
    debug!(path = %out.display(), "writing download");
    let mut file = tokio::fs::File::create(&out)
        .await
        .with_context(|| format!("create {}", out.display()))?;

    let mut stream = resp.bytes_stream();
    let mut bytes_written: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read http body chunk")?;
        file.write_all(&chunk).await.context("write chunk")?;
        bytes_written += chunk.len() as u64;
    }
    file.flush().await.ok();

    let sha = sha256_file(&out).await?;
    if let Some(prev) = m.sha256.as_deref() {
        if prev != sha {
            let stamp = stamp_utc()?;
            let hdir = history_dir(store_dir, d, &stamp);
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

async fn run_http(mode: Mode, only: Vec<String>) -> Result<i32> {
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

    let concurrency = cfg.download_concurrency.unwrap_or(1).max(1);
    info!(concurrency, mode = ?mode, "starting run");

    let datasets: Vec<Dataset> = iter_datasets(&sources_cfg)
        .into_iter()
        .filter(|d| is_selected(&only, d))
        .collect();

    let tasks = futures_util::stream::iter(datasets.into_iter().map(|d| {
        let client = client.clone();
        let store_dir = cfg.store_dir.clone();
        async move { process_dataset(&client, mode, &store_dir, &d).await }
    }))
    .buffer_unordered(concurrency);

    let mut any_failed = false;
    futures_util::pin_mut!(tasks);
    while let Some(res) = tasks.next().await {
        match res {
            Ok(()) => {}
            Err(e) => {
                any_failed = true;
                error!(error = %e, "dataset failed");
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
        Command::Check { only } => run_http(Mode::Check, only).await?,
        Command::Update { only } => run_http(Mode::UpdateIfNewer, only).await?,
        Command::Download { only } => run_http(Mode::DownloadAlways, only).await?,
        Command::PgInit => cmd_pg_init().await?,
        Command::PgLoad { only } => cmd_pg_load(only).await?,
    };
    if code != 0 {
        return Err(anyhow!("pollstats exited with {}", code));
    }
    Ok(())
}
