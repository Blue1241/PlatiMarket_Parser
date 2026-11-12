use anyhow::{Context, Result};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html as AxumHtml, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use clap::{Parser, Subcommand, ValueEnum};
use futures::stream::{self, StreamExt};
use regex::Regex;
use reqwest::{Client, Url};
use scraper::{ElementRef, Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_SELLER_URL: &str = "https://plati.market/seller/siberiaplay/1257289/";
const CATS_SITEMAP_URL: &str = "https://plati.market/cats.xml";
const PUBLIC_SEARCH_API: &str = "https://plati.io/api/search.ashx";
const DIGISELLER_PRODUCT_API: &str = "https://api.digiseller.com/api/products";

const WEB_UI_HTML: &str = include_str!("ui.html");

mod native_ui;

#[derive(Parser, Debug)]
#[command(
    name = "plati_rust_parser",
    about = "Нативное приложение, веб-интерфейс и парсер выгодных карточек и продавцов Plati.Market"
)]
struct Cli {
    /// Папка для сохранения отчётов (JSON, CSV)
    #[arg(short, long, default_value = "output", global = true)]
    out_dir: PathBuf,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug, Serialize, Deserialize)]
enum SortMode {
    RealPrice,
    StorePrice,
    Reviews,
    Smart,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Запустить дополнительный локальный веб-интерфейс
    Serve {
        /// Порт веб-сервера
        #[arg(short, long, default_value_t = 8080)]
        port: u16,
    },

    /// Поиск карточек товаров из терминала (CLI)
    Deals {
        #[arg(short, long, default_value = "ChatGPT Plus")]
        query: String,
        #[arg(long, default_value_t = 2)]
        pages: u32,
        #[arg(long, default_value_t = 100)]
        min_reviews: u64,
        #[arg(long, default_value_t = 5.0)]
        max_neg_percent: f64,
        #[arg(long, default_value = "plus")]
        option_keyword: String,
        #[arg(
            long,
            default_value = "оффлайн,offline,общий,shared,гайд,инструкция,аренда"
        )]
        exclude: String,
        #[arg(long, default_value_t = 50)]
        inspect_limit: usize,
        #[arg(long, value_enum, default_value_t = SortMode::RealPrice)]
        sort: SortMode,
        #[arg(long, default_value_t = 6)]
        concurrency: usize,
        #[arg(long, default_value_t = 100)]
        delay_ms: u64,
    },

    /// Разобрать одну витрину продавца по ссылке (CLI)
    Single {
        #[arg(short, long, default_value = DEFAULT_SELLER_URL)]
        url: String,
    },

    /// Собрать базу активных продавцов через cats.xml + Public API (CLI)
    Discover {
        #[arg(long, default_value_t = 20)]
        max_categories: usize,
        #[arg(long, default_value_t = 1)]
        pages_per_category: u32,
        #[arg(long, default_value_t = 25)]
        enrich_limit: usize,
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        #[arg(long, default_value_t = 300)]
        delay_ms: u64,
    },
}

// ======================= МОДЕЛИ ДАННЫХ =======================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProductDeal {
    item_id: u64,
    title: String,
    item_url: String,
    seller_id: u64,
    seller_name: String,
    seller_url: String,
    seller_rating: f64,
    storefront_price_rub: f64,
    real_tariff_price_rub: f64,
    price_bait_diff_rub: f64,
    matched_tariff: String,
    positive_reviews: u64,
    negative_reviews: u64,
    neg_percent: f64,
    returns_count: u64,
    return_percent: f64,
    sold_count: u64,
    ad_bid_rub: f64,
    smart_score: f64,
    options_checked: bool,
}

#[derive(Debug, Deserialize)]
struct SearchApiResponse {
    #[serde(rename = "Totalpages", default)]
    total_pages: u32,
    #[serde(default)]
    items: Option<Vec<SearchApiItem>>,
}

#[derive(Debug, Clone, Deserialize)]
struct SearchApiItem {
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    price_rur: Option<f64>,
    #[serde(default)]
    seller_id: Option<u64>,
    #[serde(default)]
    seller_name: Option<String>,
    #[serde(default)]
    seller_rating: Option<f64>,
    #[serde(default)]
    numsold: Option<i64>,
    #[serde(default)]
    count_positiveresponses: Option<i64>,
    #[serde(default)]
    count_negativeresponses: Option<i64>,
    #[serde(default)]
    count_returns: Option<i64>,
    #[serde(default)]
    payadv: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DigiProductEnvelope {
    #[serde(default)]
    product: Option<DigiProduct>,
}

#[derive(Debug, Deserialize)]
struct DigiProduct {
    #[serde(default)]
    prices: Option<DigiPrices>,
    #[serde(default)]
    options: Option<Vec<DigiOption>>,
}

#[derive(Debug, Deserialize)]
struct DigiPrices {
    #[serde(default)]
    initial: Option<DigiCurrencyMap>,
    #[serde(default)]
    default: Option<DigiCurrencyMap>,
}

#[derive(Debug, Clone, Deserialize)]
struct DigiCurrencyMap {
    #[serde(rename = "RUB", default)]
    rub: Option<f64>,
    #[serde(rename = "USD", default)]
    usd: Option<f64>,
    #[serde(rename = "EUR", default)]
    eur: Option<f64>,
    #[serde(rename = "UAH", default)]
    uah: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DigiOption {
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "type", default)]
    opt_type: Option<String>,
    #[serde(default)]
    variants: Option<Vec<DigiVariant>>,
}

#[derive(Debug, Deserialize)]
struct DigiVariant {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    modify_value: Option<f64>,
    #[serde(default)]
    modify_value_default: Option<f64>,
    #[serde(default)]
    modify_type: Option<String>,
    #[serde(default)]
    visible: Option<Value>,
    #[serde(default)]
    default: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SellerRecord {
    seller_id: u64,
    seller_name: String,
    profile_url: String,
    rating: Option<f64>,
    level: Option<String>,
    sales_count: Option<u64>,
    returns_count: Option<u64>,
    reviews_count: Option<u64>,
    items_count: Option<u64>,
    tenure: Option<String>,
    enriched_from_html: bool,
}

// ======================= СТРУКТУРЫ ВЕБ-СЕРВЕРА AXUM =======================

#[derive(Clone)]
struct AppState {
    client: Client,
    out_dir: PathBuf,
    niche_running: Arc<AtomicBool>,
}

#[derive(Debug, Deserialize)]
struct ApiDealsRequest {
    query: String,
    #[serde(default)]
    option_keyword: String,
    #[serde(default = "default_min_reviews")]
    min_reviews: u64,
    #[serde(default = "default_max_neg")]
    max_neg_percent: f64,
    #[serde(default = "default_inspect_limit")]
    inspect_limit: usize,
    #[serde(default)]
    exclude: String,
    #[serde(default = "default_pages")]
    pages: u32,
}

fn default_min_reviews() -> u64 {
    100
}
fn default_max_neg() -> f64 {
    5.0
}
fn default_inspect_limit() -> usize {
    50
}
fn default_pages() -> u32 {
    2
}

#[derive(Debug, Deserialize)]
struct ApiItemOptionsQuery {
    id: u64,
}

#[derive(Debug, Serialize)]
struct UiOptionGroup {
    label: String,
    opt_type: String,
    variants: Vec<UiOptionVariant>,
}

#[derive(Debug, Serialize)]
struct UiOptionVariant {
    text: String,
    modifier_rub: f64,
    is_default: bool,
}

#[derive(Debug, Deserialize)]
struct ApiSellerQuery {
    url: String,
}

#[derive(Debug, Deserialize)]
struct ApiDiscoverRequest {
    #[serde(default = "default_cats")]
    max_categories: usize,
    #[serde(default = "default_enrich")]
    enrich_limit: usize,
}

fn default_cats() -> usize {
    10
}
fn default_enrich() -> usize {
    15
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NicheProduct {
    item_id: u64,
    title: String,
    item_url: String,
    category: String,
    seller_name: String,
    seller_id: u64,
    price_rub: f64,
    sold_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NicheSnapshot {
    captured_at: u64,
    categories_scanned: usize,
    products: Vec<NicheProduct>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NicheCheckpoint {
    started_at: u64,
    categories: Vec<String>,
    completed_categories: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NicheProgress {
    status: String,
    started_at: Option<u64>,
    updated_at: u64,
    total_categories: usize,
    completed_categories: usize,
    active_categories: usize,
    completed_pages: usize,
    total_pages: usize,
    products_cached: usize,
    current_category: String,
    resumed: bool,
    error: Option<String>,
}

impl Default for NicheProgress {
    fn default() -> Self {
        Self {
            status: "idle".to_string(),
            started_at: None,
            updated_at: unix_now(),
            total_categories: 0,
            completed_categories: 0,
            active_categories: 0,
            completed_pages: 0,
            total_pages: 0,
            products_cached: 0,
            current_category: String::new(),
            resumed: false,
            error: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct NicheTrend {
    #[serde(flatten)]
    product: NicheProduct,
    previous_sold_count: Option<u64>,
    delta_sales: Option<i64>,
    listings_count: usize,
    sellers_count: usize,
    comparable_median_price_rub: Option<f64>,
    markup_percent: Option<f64>,
    opportunity_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NicheHistoryPoint {
    captured_at: u64,
    #[serde(default)]
    interval_seconds: u64,
    delta_sales: u64,
    products_tracked: usize,
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn snapshot_dir(out_dir: &Path) -> PathBuf {
    out_dir.join("niche_snapshots")
}

fn niche_checkpoint_path(out_dir: &Path) -> PathBuf {
    out_dir.join("niche_checkpoint.json")
}

fn niche_progress_path(out_dir: &Path) -> PathBuf {
    out_dir.join("niche_progress.json")
}

fn niche_category_cache_path(out_dir: &Path, category_index: usize) -> PathBuf {
    out_dir.join("niche_checkpoint_categories").join(format!("{}.json", category_index))
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp_path = path.with_extension("tmp");
    serde_json::to_writer_pretty(File::create(&temp_path)?, value)?;
    #[cfg(windows)]
    if path.exists() {
        fs::remove_file(path)?;
    }
    fs::rename(temp_path, path)?;
    Ok(())
}

fn read_niche_progress(out_dir: &Path) -> NicheProgress {
    fs::read_to_string(niche_progress_path(out_dir))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

async fn update_niche_progress<F>(
    progress: &tokio::sync::Mutex<NicheProgress>,
    out_dir: &Path,
    update: F,
) -> Result<()>
where
    F: FnOnce(&mut NicheProgress),
{
    let mut progress = progress.lock().await;
    update(&mut progress);
    progress.updated_at = unix_now();
    write_json_atomic(&niche_progress_path(out_dir), &*progress)
}

// ======================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =======================

fn build_client() -> Result<Client> {
    Client::builder()
        .user_agent("Mozilla/5.0 (compatible; PlatiRadar/0.2; +https://plati.market/api/)")
        .timeout(Duration::from_secs(25))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .context("Не удалось инициализировать HTTP-клиент")
}

fn clean_space(raw: &str) -> String {
    raw.replace('\u{a0}', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_digits_u64(raw: &str) -> Option<u64> {
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse::<u64>().ok()
    }
}

fn parse_rating_f64(raw: &str) -> Option<f64> {
    let cleaned: String = raw
        .replace('\u{a0}', "")
        .replace(' ', "")
        .replace(',', ".");
    cleaned.parse::<f64>().ok()
}

fn json_flag_true(v: &Option<Value>, default_if_none: bool) -> bool {
    match v {
        None | Some(Value::Null) => default_if_none,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().unwrap_or(1) != 0,
        Some(Value::String(s)) => s != "0" && !s.eq_ignore_ascii_case("false"),
        _ => default_if_none,
    }
}

// ======================= ЛОГИКА ПОИСКА ТОВАРОВ И РЕАЛЬНЫХ ТАРИФОВ =======================

fn variant_modifier_rub(
    v: &DigiVariant,
    base_rub: f64,
    init_cur: Option<&DigiCurrencyMap>,
    def_cur: Option<&DigiCurrencyMap>,
) -> f64 {
    let mtype = v.modify_type.as_deref().unwrap_or("").trim().to_uppercase();
    let mval = v.modify_value.unwrap_or(0.0);
    let mdef = v.modify_value_default.unwrap_or(mval);

    if mval == 0.0 && mdef == 0.0 {
        return 0.0;
    }
    if mtype == "%" {
        return base_rub * (mval / 100.0);
    }

    let rate_for = |cur_val_init: Option<f64>, cur_val_def: Option<f64>| -> f64 {
        if let (Some(dr), Some(dc)) = (def_cur.and_then(|m| m.rub), cur_val_def) {
            if dc > 0.0 {
                return dr / dc;
            }
        }
        if let (Some(ir), Some(ic)) = (init_cur.and_then(|m| m.rub), cur_val_init) {
            if ic > 0.0 {
                return ir / ic;
            }
        }
        1.0
    };

    match mtype.as_str() {
        "USD" => mval * rate_for(init_cur.and_then(|m| m.usd), def_cur.and_then(|m| m.usd)),
        "EUR" => mval * rate_for(init_cur.and_then(|m| m.eur), def_cur.and_then(|m| m.eur)),
        "UAH" => mval * rate_for(init_cur.and_then(|m| m.uah), def_cur.and_then(|m| m.uah)),
        _ => mdef,
    }
}

fn is_pure_tariff_match(text: &str, keyword_lower: &str, stop_words: &[String]) -> bool {
    if keyword_lower.is_empty() {
        return false;
    }
    let lower = text.to_lowercase();
    let matches_kw = if keyword_lower == "plus" || keyword_lower == "плюс" {
        lower.contains("plus") || lower.contains("плюс")
    } else {
        lower.contains(keyword_lower)
    };
    if !matches_kw {
        return false;
    }
    if stop_words.iter().any(|w| !w.is_empty() && lower.contains(w)) {
        return false;
    }

    if keyword_lower == "plus" || keyword_lower == "плюс" {
        static COMPETING_TIERS_RE: OnceLock<Regex> = OnceLock::new();
        let re = COMPETING_TIERS_RE.get_or_init(|| {
            Regex::new(r"(?i)(?:^|[^a-zа-яё0-9])(go|pro|про)(?:$|[^a-zа-яё0-9])")
                .expect("valid competing tier regex")
        });
        if re.is_match(&lower) {
            return false;
        }
    }

    true
}

async fn inspect_real_tariff_price(
    client: &Client,
    item_id: u64,
    storefront_rub: f64,
    option_keyword: &str,
    stop_words: &[String],
) -> Result<(f64, String)> {
    let api_url = format!("{}/{}/data?currency=RUB", DIGISELLER_PRODUCT_API, item_id);

    let resp = client
        .get(&api_url)
        .header("Accept", "application/json")
        .send()
        .await?
        .error_for_status()?;

    let env: DigiProductEnvelope = resp.json().await?;
    let Some(prod) = env.product else {
        return Ok((storefront_rub, "[единый тариф витрины]".to_string()));
    };

    let init_map = prod.prices.as_ref().and_then(|p| p.initial.as_ref());
    let def_map = prod.prices.as_ref().and_then(|p| p.default.as_ref());

    let default_rub = def_map
        .and_then(|m| m.rub)
        .filter(|&v| v > 0.0)
        .unwrap_or(storefront_rub);
    let initial_rub = init_map
        .and_then(|m| m.rub)
        .filter(|&v| v > 0.0)
        .unwrap_or(default_rub);

    let options = prod.options.unwrap_or_default();
    let kw_lower = option_keyword.trim().to_lowercase();

    let mut default_mods_sum = 0.0;
    for opt in &options {
        let otype = opt.opt_type.as_deref().unwrap_or("");
        if otype != "radio" && otype != "select" {
            continue;
        }
        if let Some(vars) = &opt.variants {
            for v in vars {
                if json_flag_true(&v.visible, true) && json_flag_true(&v.default, false) {
                    default_mods_sum += variant_modifier_rub(v, initial_rub, init_map, def_map);
                }
            }
        }
    }
    let base_checkout_rub = (default_rub - default_mods_sum).max(initial_rub);

    let mut total_rub = base_checkout_rub;
    let mut chosen_labels: Vec<String> = Vec::new();

    for opt in &options {
        let otype = opt.opt_type.as_deref().unwrap_or("");
        if otype != "radio" && otype != "select" {
            continue;
        }
        let visible_vars: Vec<&DigiVariant> = opt
            .variants
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|v| json_flag_true(&v.visible, true))
            .collect();

        if visible_vars.is_empty() {
            continue;
        }

        let matching_vars: Vec<&DigiVariant> = visible_vars
            .iter()
            .copied()
            .filter(|v| {
                let txt = v.text.as_deref().unwrap_or("");
                is_pure_tariff_match(txt, &kw_lower, stop_words)
            })
            .collect();

        if !matching_vars.is_empty() {
            if let Some(best_v) = matching_vars.into_iter().min_by(|a, b| {
                let ma = variant_modifier_rub(a, initial_rub, init_map, def_map);
                let mb = variant_modifier_rub(b, initial_rub, init_map, def_map);
                ma.partial_cmp(&mb).unwrap_or(std::cmp::Ordering::Equal)
            }) {
                total_rub += variant_modifier_rub(best_v, initial_rub, init_map, def_map);
                let lbl = clean_space(best_v.text.as_deref().unwrap_or(""));
                if !lbl.is_empty() {
                    chosen_labels.push(lbl);
                }
            }
        } else if let Some(cheapest_v) = visible_vars.into_iter().min_by(|a, b| {
            let ma = variant_modifier_rub(a, initial_rub, init_map, def_map);
            let mb = variant_modifier_rub(b, initial_rub, init_map, def_map);
            ma.partial_cmp(&mb).unwrap_or(std::cmp::Ordering::Equal)
        }) {
            total_rub += variant_modifier_rub(cheapest_v, initial_rub, init_map, def_map);
        }
    }

    let label = if chosen_labels.is_empty() {
        "[единый тариф карточки]".to_string()
    } else {
        chosen_labels.join(" + ")
    };

    Ok((total_rub.round(), label))
}

fn compute_smart_score(pos_reviews: u64, neg_percent: f64, ret_percent: f64, real_price: f64) -> f64 {
    if real_price <= 0.0 {
        return 0.0;
    }
    let trust = ((pos_reviews as f64) + 1.0).ln();
    let penalty = 1.0 + (neg_percent * 0.25) + (ret_percent * 0.15);
    let score = (trust * 10_000.0) / (real_price * penalty);
    (score * 100.0).round() / 100.0
}

async fn search_deals(
    client: &Client,
    query: &str,
    pages: u32,
    min_reviews: u64,
    max_neg_percent: f64,
    option_keyword: &str,
    exclude_csv: &str,
    inspect_limit: usize,
    sort_mode: SortMode,
    concurrency: usize,
    delay_ms: u64,
) -> Result<Vec<ProductDeal>> {
    let stop_words: Vec<String> = exclude_csv
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    let mut raw_items: BTreeMap<u64, SearchApiItem> = BTreeMap::new();

    for page in 1..=pages.clamp(1, 5) {
        let api_url = Url::parse_with_params(
            PUBLIC_SEARCH_API,
            &[
                ("query", query),
                ("pagesize", "500"),
                ("pagenum", &page.to_string()),
                ("visibleOnly", "true"),
                ("response", "json"),
            ],
        )?;

        let resp = client.get(api_url).send().await?.error_for_status()?;
        let parsed: SearchApiResponse = resp.json().await?;
        let batch = parsed.items.unwrap_or_default();
        let batch_len = batch.len();

        for item in batch {
            if let Some(id) = item.id {
                raw_items.entry(id).or_insert(item);
            }
        }

        if page >= parsed.total_pages || batch_len == 0 {
            break;
        }
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    let mut candidates: Vec<ProductDeal> = Vec::new();
    for item in raw_items.into_values() {
        let Some(item_id) = item.id else { continue };
        let title = clean_space(&item.name.unwrap_or_default());
        if title.is_empty() {
            continue;
        }
        let title_lower = title.to_lowercase();
        if stop_words.iter().any(|w| title_lower.contains(w)) {
            continue;
        }

        let pos = item.count_positiveresponses.unwrap_or(0).max(0) as u64;
        if pos < min_reviews {
            continue;
        }

        let neg = item.count_negativeresponses.unwrap_or(0).max(0) as u64;
        let total_rev = (pos + neg).max(1) as f64;
        let neg_pct = ((neg as f64 / total_rev) * 10000.0).round() / 100.0;
        if neg_pct > max_neg_percent {
            continue;
        }

        let sold = item.numsold.unwrap_or(0).max(0) as u64;
        let returns = item.count_returns.unwrap_or(0).max(0) as u64;
        let ret_pct = if sold > 0 {
            ((returns as f64 / sold as f64) * 10000.0).round() / 100.0
        } else {
            0.0
        };

        let store_price = item.price_rur.unwrap_or(0.0);
        if store_price <= 0.0 {
            continue;
        }

        let seller_id = item.seller_id.unwrap_or(0);
        let seller_name = clean_space(&item.seller_name.unwrap_or_else(|| "Unknown".to_string()));
        let item_url = item
            .url
            .unwrap_or_else(|| format!("https://plati.market/itm/{}", item_id));
        let seller_url = format!("https://plati.market/seller/s/{}/", seller_id);
        let seller_rating = ((item.seller_rating.unwrap_or(0.0)) * 10.0).round() / 10.0;
        let ad_bid = ((item.payadv.unwrap_or(0.0)) * 10.0).round() / 10.0;
        let score = compute_smart_score(pos, neg_pct, ret_pct, store_price);

        candidates.push(ProductDeal {
            item_id,
            title,
            item_url,
            seller_id,
            seller_name,
            seller_url,
            seller_rating,
            storefront_price_rub: store_price,
            real_tariff_price_rub: store_price,
            price_bait_diff_rub: 0.0,
            matched_tariff: "[тариф с витрины]".to_string(),
            positive_reviews: pos,
            negative_reviews: neg,
            neg_percent: neg_pct,
            returns_count: returns,
            return_percent: ret_pct,
            sold_count: sold,
            ad_bid_rub: ad_bid,
            smart_score: score,
            options_checked: false,
        });
    }

    candidates.sort_by(|a, b| b.positive_reviews.cmp(&a.positive_reviews));

    if inspect_limit > 0 && !candidates.is_empty() {
        let to_check_len = inspect_limit.min(candidates.len());
        let rest = candidates.split_off(to_check_len);
        let kw_owned = option_keyword.to_string();
        let stop_owned = stop_words.clone();

        let mut checked: Vec<ProductDeal> = stream::iter(candidates)
            .map(|mut deal| {
                let client = client.clone();
                let kw = kw_owned.clone();
                let stops = stop_owned.clone();
                async move {
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    if let Ok((real_p, label)) = inspect_real_tariff_price(
                        &client,
                        deal.item_id,
                        deal.storefront_price_rub,
                        &kw,
                        &stops,
                    )
                    .await
                    {
                        deal.real_tariff_price_rub = real_p;
                        deal.price_bait_diff_rub = (real_p - deal.storefront_price_rub).max(0.0);
                        deal.matched_tariff = label;
                        deal.options_checked = true;
                        deal.smart_score = compute_smart_score(
                            deal.positive_reviews,
                            deal.neg_percent,
                            deal.return_percent,
                            deal.real_tariff_price_rub,
                        );
                    }
                    deal
                }
            })
            .buffer_unordered(concurrency.max(1))
            .collect()
            .await;

        checked.extend(rest);
        candidates = checked;
    }

    sort_deals(&mut candidates, sort_mode);
    Ok(candidates)
}

fn sort_deals(deals: &mut [ProductDeal], mode: SortMode) {
    deals.sort_by(|a, b| match mode {
        SortMode::RealPrice => a
            .real_tariff_price_rub
            .partial_cmp(&b.real_tariff_price_rub)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.positive_reviews.cmp(&a.positive_reviews)),
        SortMode::StorePrice => a
            .storefront_price_rub
            .partial_cmp(&b.storefront_price_rub)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.positive_reviews.cmp(&a.positive_reviews)),
        SortMode::Reviews => b.positive_reviews.cmp(&a.positive_reviews).then_with(|| {
            a.real_tariff_price_rub
                .partial_cmp(&b.real_tariff_price_rub)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        SortMode::Smart => b
            .smart_score
            .partial_cmp(&a.smart_score)
            .unwrap_or(std::cmp::Ordering::Equal),
    });
}

fn save_deals_files(out_dir: &Path, deals: &[ProductDeal]) -> Result<(PathBuf, PathBuf)> {
    fs::create_dir_all(out_dir)?;
    let csv_path = out_dir.join("deals.csv");
    let json_path = out_dir.join("deals.json");

    let mut csv_file = File::create(&csv_path)?;
    csv_file.write_all(b"\xEF\xBB\xBF")?;
    let mut wtr = csv::Writer::from_writer(csv_file);
    for d in deals {
        wtr.serialize(d)?;
    }
    wtr.flush()?;

    let json_file = File::create(&json_path)?;
    serde_json::to_writer_pretty(json_file, deals)?;
    Ok((csv_path, json_path))
}

// ======================= ЛОГИКА ПАРСИНГА ПРОДАВЦОВ =======================

fn extract_seller_id_from_url(url: &str) -> Option<u64> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"/seller/[^/]+/(\d+)").expect("valid seller url regex"));
    re.captures(url)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<u64>().ok())
}

fn parse_seller_html(
    fallback_id: Option<u64>,
    final_url: String,
    html: &str,
) -> Option<SellerRecord> {
    let doc = Html::parse_document(html);

    let sel_name = Selector::parse("h2.headline2-bold").ok()?;
    let sel_ranking_block = Selector::parse("p.d-inline-flex.gap-1").ok()?;
    let sel_use_ranking = Selector::parse("[href=\"#ranking\"]").ok()?;
    let sel_span = Selector::parse("span").ok()?;
    let sel_stat_col = Selector::parse("p.d-inline-flex.flex-column").ok()?;
    let sel_item_cnt = Selector::parse("#item_cnt").ok()?;

    let seller_name = doc
        .select(&sel_name)
        .next()
        .map(|el| clean_space(&el.text().collect::<Vec<_>>().join(" ")))
        .filter(|s| !s.is_empty())?;

    let seller_id = extract_seller_id_from_url(&final_url).or(fallback_id)?;

    let mut rating = None;
    for p in doc.select(&sel_ranking_block) {
        if p.select(&sel_use_ranking).next().is_some() {
            if let Some(sp) = p.select(&sel_span).next() {
                let txt = clean_space(&sp.text().collect::<Vec<_>>().join(" "));
                rating = parse_rating_f64(&txt);
                break;
            }
        }
    }

    static REVIEWS_RE: OnceLock<Regex> = OnceLock::new();
    let reviews_re = REVIEWS_RE
        .get_or_init(|| Regex::new(r"Отзыв\S*\s+([\d\s\u{a0}]+)").expect("valid reviews regex"));
    let full_text = doc.root_element().text().collect::<Vec<_>>().join(" ");
    let reviews_count = reviews_re
        .captures(&full_text)
        .and_then(|c| c.get(1))
        .and_then(|m| parse_digits_u64(m.as_str()));

    let mut level = None;
    let mut sales_count = None;
    let mut returns_count = None;
    let mut tenure = None;

    for p in doc.select(&sel_stat_col) {
        let direct_spans: Vec<String> = p
            .children()
            .filter_map(ElementRef::wrap)
            .filter(|el| el.value().name() == "span")
            .map(|el| clean_space(&el.text().collect::<Vec<_>>().join(" ")))
            .collect();

        if direct_spans.len() == 2 {
            let val = &direct_spans[0];
            let label = &direct_spans[1];
            if label == "Уровень" {
                level = Some(val.clone());
            } else if label.starts_with("Продаж") {
                sales_count = parse_digits_u64(val);
            } else if label.starts_with("Возврат") {
                returns_count = parse_digits_u64(val);
            } else if label == "На сайте" {
                tenure = Some(val.clone());
            }
        }
    }

    let items_count = doc
        .select(&sel_item_cnt)
        .next()
        .and_then(|el| parse_digits_u64(&el.text().collect::<Vec<_>>().join(" ")));

    Some(SellerRecord {
        seller_id,
        seller_name,
        profile_url: final_url,
        rating,
        level,
        sales_count,
        returns_count,
        reviews_count,
        items_count,
        tenure,
        enriched_from_html: true,
    })
}

async fn fetch_seller_profile_with_attempts(
    client: &Client,
    url: &str,
    fallback_id: Option<u64>,
) -> (Result<SellerRecord>, usize) {
    let mut last_err = None;
    let mut attempts = 0usize;
    for attempt in 0..3u64 {
        attempts = (attempt + 1) as usize;
        match client.get(url).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.as_u16() == 429 || status.is_server_error() {
                    last_err = Some(anyhow::anyhow!("HTTP {} для {}", status, url));
                } else {
                    let resp = match resp.error_for_status() {
                        Ok(resp) => resp,
                        Err(error) => return (Err(error.into()), attempts),
                    };
                    let final_url = resp.url().to_string();
                    let body = match resp.text().await {
                        Ok(body) => body,
                        Err(error) => return (Err(error.into()), attempts),
                    };
                    return (
                        parse_seller_html(fallback_id, final_url, &body).with_context(|| {
                            format!("Не удалось извлечь карточку продавца со страницы {}", url)
                        }),
                        attempts,
                    );
                }
            }
            Err(e) => {
                last_err = Some(e.into());
            }
        }
        tokio::time::sleep(Duration::from_millis(800 * (attempt + 1))).await;
    }
    (
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Неизвестная ошибка при запросе {}", url))),
        attempts,
    )
}

async fn fetch_seller_profile(
    client: &Client,
    url: &str,
    fallback_id: Option<u64>,
) -> Result<SellerRecord> {
    fetch_seller_profile_with_attempts(client, url, fallback_id).await.0
}

async fn fetch_category_queries(client: &Client, limit: usize) -> Result<Vec<String>> {
    let xml = client
        .get(CATS_SITEMAP_URL)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let re = Regex::new(r"<loc>https://plati\.market/cat/([a-z0-9-]+)/\d+/</loc>")?;
    let mut seen = HashSet::new();
    let mut queries = Vec::new();

    for cap in re.captures_iter(&xml) {
        let slug = cap[1].replace('-', " ");
        if slug.chars().count() >= 3 && seen.insert(slug.clone()) {
            queries.push(slug);
            if limit > 0 && queries.len() >= limit {
                break;
            }
        }
    }
    Ok(queries)
}

async fn fetch_niche_search_page(client: &Client, category: &str, page: u32) -> Result<SearchApiResponse> {
    let mut last_error = None;
    for attempt in 0..3 {
        let result = async {
            let api_url = Url::parse_with_params(
                PUBLIC_SEARCH_API,
                &[
                    ("query", category),
                    ("pagesize", "500"),
                    ("pagenum", &page.to_string()),
                    ("visibleOnly", "true"),
                    ("response", "json"),
                ],
            )?;
            Ok::<SearchApiResponse, anyhow::Error>(
                client.get(api_url).send().await?.error_for_status()?.json().await?,
            )
        }.await;
        match result {
            Ok(response) => return Ok(response),
            Err(err) => last_error = Some(err),
        }
        tokio::time::sleep(Duration::from_millis(250 * (attempt + 1))).await;
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Не удалось загрузить страницу {} категории {}", page, category)))
}

fn merge_niche_product(products: &mut BTreeMap<u64, NicheProduct>, product: NicheProduct) {
    products.entry(product.item_id)
        .and_modify(|existing| {
            if product.sold_count > existing.sold_count {
                *existing = product.clone();
            }
        })
        .or_insert(product);
}

async fn capture_niche_snapshot(client: &Client, out_dir: &Path) -> Result<NicheSnapshot> {
    let categories = fetch_category_queries(client, 0).await?;
    if categories.is_empty() {
        anyhow::bail!("В cats.xml не найдено ни одной категории");
    }

    let checkpoint_path = niche_checkpoint_path(out_dir);
    let loaded_checkpoint = fs::read_to_string(&checkpoint_path)
        .ok()
        .and_then(|raw| serde_json::from_str::<NicheCheckpoint>(&raw).ok())
        .filter(|checkpoint| checkpoint.categories == categories);
    let resumed = loaded_checkpoint.is_some();
    let mut checkpoint = loaded_checkpoint.unwrap_or_else(|| NicheCheckpoint {
        started_at: unix_now(),
        categories: categories.clone(),
        completed_categories: BTreeSet::new(),
    });
    let category_indexes: BTreeMap<String, usize> = categories.iter().cloned().enumerate()
        .map(|(index, category)| (category, index)).collect();
    let mut products_by_id = BTreeMap::<u64, NicheProduct>::new();
    let cached_categories: Vec<String> = checkpoint.completed_categories.iter().cloned().collect();
    for category in cached_categories {
        let Some(index) = category_indexes.get(&category) else {
            checkpoint.completed_categories.remove(&category);
            continue;
        };
        let cache_path = niche_category_cache_path(out_dir, *index);
        let cached: Option<Vec<NicheProduct>> = fs::read_to_string(cache_path)
            .ok().and_then(|raw| serde_json::from_str(&raw).ok());
        if let Some(products) = cached {
            for product in products {
                merge_niche_product(&mut products_by_id, product);
            }
        } else {
            checkpoint.completed_categories.remove(&category);
        }
    }
    let progress = NicheProgress {
        status: "collecting".to_string(),
        started_at: Some(checkpoint.started_at),
        updated_at: unix_now(),
        total_categories: categories.len(),
        completed_categories: checkpoint.completed_categories.len(),
        active_categories: 0,
        completed_pages: 0,
        total_pages: 0,
        products_cached: products_by_id.len(),
        current_category: String::new(),
        resumed,
        error: None,
    };
    write_json_atomic(&niche_progress_path(out_dir), &progress)?;
    write_json_atomic(&checkpoint_path, &checkpoint)?;

    let progress_state = Arc::new(tokio::sync::Mutex::new(progress.clone()));
    let pending_categories: Vec<String> = categories
        .into_iter()
        .filter(|category| !checkpoint.completed_categories.contains(category))
        .collect();
    let mut category_requests = stream::iter(pending_categories.into_iter().map(|category| {
        let client = client.clone();
        let progress_state = progress_state.clone();
        let progress_dir = out_dir.to_owned();
        async move {
            let active_category = category.clone();
            let _ = update_niche_progress(&progress_state, &progress_dir, |p| {
                p.active_categories += 1;
                p.current_category = active_category;
            }).await;

            let result: std::result::Result<Vec<SearchApiItem>, String> = async {
                let first_page = fetch_niche_search_page(&client, &category, 1)
                    .await.map_err(|e| e.to_string())?;
                let mut items = first_page.items.unwrap_or_default();
                let last_page = first_page.total_pages.max(1);
                let _ = update_niche_progress(&progress_state, &progress_dir, |p| {
                    p.total_pages = p.total_pages.saturating_add(last_page as usize);
                    p.completed_pages = p.completed_pages.saturating_add(1);
                }).await;
                let page_results: Vec<std::result::Result<Vec<SearchApiItem>, String>> = stream::iter(2..=last_page)
                    .map(|page| {
                        let client = client.clone();
                        let category = category.clone();
                        let progress_state = progress_state.clone();
                        let progress_dir = progress_dir.clone();
                        async move {
                            let result = fetch_niche_search_page(&client, &category, page)
                                .await.map(|response| response.items.unwrap_or_default())
                                .map_err(|e| e.to_string());
                            let _ = update_niche_progress(&progress_state, &progress_dir, |p| {
                                p.completed_pages = p.completed_pages.saturating_add(1);
                            }).await;
                            result
                        }
                    })
                    .buffer_unordered(4)
                    .collect()
                    .await;
                for page in page_results {
                    items.extend(page?);
                }
                Ok(items)
            }.await;
            (category, result)
        }
    }))
    .buffer_unordered(8);

    let mut failed_categories = Vec::new();
    while let Some((category, result)) = category_requests.next().await {
        match result {
            Ok(items) => {
                let mut category_products = Vec::new();
                for item in items {
                    let (Some(item_id), Some(price)) = (item.id, item.price_rur) else { continue };
                    if price <= 0.0 {
                        continue;
                    }
                    let title = clean_space(item.name.as_deref().unwrap_or(""));
                    if title.is_empty() {
                        continue;
                    }
                    category_products.push(NicheProduct {
                        item_id,
                        title,
                        item_url: item.url.unwrap_or_else(|| format!("https://plati.market/itm/{}", item_id)),
                        category: category.clone(),
                        seller_name: clean_space(item.seller_name.as_deref().unwrap_or("Неизвестно")),
                        seller_id: item.seller_id.unwrap_or(0),
                        price_rub: price,
                        sold_count: item.numsold.unwrap_or(0).max(0) as u64,
                    });
                }
                let category_index = *category_indexes.get(&category)
                    .ok_or_else(|| anyhow::anyhow!("Категория отсутствует в checkpoint: {}", category))?;
                write_json_atomic(&niche_category_cache_path(out_dir, category_index), &category_products)?;
                for product in category_products {
                    merge_niche_product(&mut products_by_id, product);
                }
                checkpoint.completed_categories.insert(category.clone());
                write_json_atomic(&checkpoint_path, &checkpoint)?;
                let completed_categories = checkpoint.completed_categories.len();
                let products_cached = products_by_id.len();
                update_niche_progress(&progress_state, out_dir, |p| {
                    p.active_categories = p.active_categories.saturating_sub(1);
                    p.completed_categories = completed_categories;
                    p.products_cached = products_cached;
                    p.current_category = category;
                }).await?;
            }
            Err(_error) => {
                failed_categories.push(category.clone());
                update_niche_progress(&progress_state, out_dir, |p| {
                    p.active_categories = p.active_categories.saturating_sub(1);
                    p.current_category = category;
                }).await?;
            }
        }
    }

    if !failed_categories.is_empty() {
        let error_message = format!(
            "Не завершено категорий: {}. Частичный результат сохранён; повторный запуск продолжит сбор.",
            failed_categories.len()
        );
        update_niche_progress(&progress_state, out_dir, move |p| {
            p.status = "interrupted".to_string();
            p.active_categories = 0;
            p.error = Some(error_message);
        }).await?;
        anyhow::bail!("Не удалось обработать {} категорий; checkpoint сохранён для продолжения", failed_categories.len());
    }

    if products_by_id.is_empty() {
        let _ = fs::remove_file(&checkpoint_path);
        let _ = fs::remove_dir_all(out_dir.join("niche_checkpoint_categories"));
        anyhow::bail!("Не удалось получить товары ни из одной категории Plati.Market");
    }

    let snapshot = NicheSnapshot {
        captured_at: unix_now(),
        categories_scanned: checkpoint.completed_categories.len(),
        products: products_by_id.into_values().collect(),
    };
    let dir = snapshot_dir(out_dir);
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.json", snapshot.captured_at));
    write_json_atomic(&path, &snapshot)?;
    if let Ok(snapshots) = load_recent_niche_snapshots(out_dir) {
        if let Some(point) = build_niche_history_point(&snapshots) {
            append_niche_history(out_dir, point)?;
        }
    }
    let _ = fs::remove_file(checkpoint_path);
    let _ = fs::remove_dir_all(out_dir.join("niche_checkpoint_categories"));
    update_niche_progress(&progress_state, out_dir, |p| {
        p.status = "completed".to_string();
        p.active_categories = 0;
        p.completed_categories = p.total_categories;
        p.products_cached = snapshot.products.len();
        p.current_category.clear();
        p.error = None;
    }).await?;
    Ok(snapshot)
}

fn load_recent_niche_snapshots(out_dir: &Path) -> Result<Vec<NicheSnapshot>> {
    let dir = snapshot_dir(out_dir);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    paths.sort();
    paths.reverse();
    let mut snapshots = Vec::new();
    for path in paths {
        if let Ok(raw) = fs::read_to_string(path) {
            if let Ok(snapshot) = serde_json::from_str::<NicheSnapshot>(&raw) {
                snapshots.push(snapshot);
                if previous_daily_snapshot(&snapshots).is_some() {
                    break;
                }
            }
        }
    }
    Ok(snapshots)
}

fn niche_history_path(out_dir: &Path) -> PathBuf {
    out_dir.join("niche_history.json")
}

fn build_niche_history_point(snapshots: &[NicheSnapshot]) -> Option<NicheHistoryPoint> {
    let current = snapshots.first()?;
    let previous_snapshot = snapshots.get(1)?;
    let previous: BTreeMap<u64, u64> = previous_snapshot
        .products.iter().map(|p| (p.item_id, p.sold_count)).collect();
    let mut delta_sales = 0u64;
    let mut products_tracked = 0usize;
    for product in &current.products {
        if let Some(old) = previous.get(&product.item_id) {
            products_tracked += 1;
            if product.sold_count > *old {
                delta_sales = delta_sales.saturating_add(product.sold_count - old);
            }
        }
    }
    Some(NicheHistoryPoint {
        captured_at: current.captured_at,
        interval_seconds: current.captured_at.saturating_sub(previous_snapshot.captured_at),
        delta_sales,
        products_tracked,
    })
}

fn read_niche_history(out_dir: &Path) -> Vec<NicheHistoryPoint> {
    fs::read_to_string(niche_history_path(out_dir))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn niche_history_for_response(out_dir: &Path, snapshots: &[NicheSnapshot]) -> Vec<NicheHistoryPoint> {
    let mut history = read_niche_history(out_dir);
    if let Some(point) = build_niche_history_point(snapshots) {
        if !history.iter().any(|saved| saved.captured_at == point.captured_at) {
            history.push(point);
        }
    }
    history.sort_by_key(|point| point.captured_at);
    history
}

fn append_niche_history(out_dir: &Path, point: NicheHistoryPoint) -> Result<()> {
    let path = niche_history_path(out_dir);
    let mut history: Vec<NicheHistoryPoint> = fs::read_to_string(&path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default();
    if !history.iter().any(|item| item.captured_at == point.captured_at) {
        history.push(point);
    }
    history.sort_by_key(|item| item.captured_at);
    if history.len() > 365 {
        let remove_count = history.len() - 365;
        history.drain(0..remove_count);
    }
    serde_json::to_writer_pretty(File::create(path)?, &history)?;
    Ok(())
}

fn previous_daily_snapshot(snapshots: &[NicheSnapshot]) -> Option<&NicheSnapshot> {
    let latest = snapshots.first()?;
    snapshots.iter().skip(1).find(|snapshot| {
        latest.captured_at.saturating_sub(snapshot.captured_at) >= 20 * 60 * 60
    })
}

fn niche_title_key(title: &str) -> String {
    const GENERIC: &[&str] = &[
        "key", "ключ", "код", "активация", "activation", "delivery", "доставка",
        "купить", "buy", "лицензия", "license", "steam", "global", "ru",
    ];
    let mut tokens: Vec<String> = title
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| token.chars().count() > 1 && !GENERIC.contains(token))
        .map(str::to_string)
        .collect();
    tokens.sort();
    tokens.dedup();
    let key = tokens.join(" ");
    if key.is_empty() {
        title.to_lowercase().chars().filter(|c| c.is_alphanumeric()).collect()
    } else {
        key
    }
}

fn build_niche_trends(snapshots: &[NicheSnapshot]) -> Vec<NicheTrend> {
    let Some(current) = snapshots.first() else { return Vec::new() };
    let previous: BTreeMap<u64, u64> = previous_daily_snapshot(snapshots)
        .map(|s| s.products.iter().map(|p| (p.item_id, p.sold_count)).collect())
        .unwrap_or_default();
    let mut title_groups = BTreeMap::<String, Vec<f64>>::new();
    let mut title_counts = BTreeMap::<String, usize>::new();
    let mut seller_groups = BTreeMap::<String, HashSet<u64>>::new();
    for product in &current.products {
        let key = niche_title_key(&product.title);
        *title_counts.entry(key.clone()).or_default() += 1;
        if product.seller_id > 0 {
            seller_groups.entry(key.clone()).or_default().insert(product.seller_id);
        }
        title_groups.entry(key).or_default().push(product.price_rub);
    }
    let price_benchmarks: BTreeMap<String, Option<f64>> = title_groups.into_iter().map(|(key, mut prices)| {
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = if prices.len() > 1 {
            let n = prices.len();
            Some(if n % 2 == 0 { (prices[n / 2 - 1] + prices[n / 2]) / 2.0 } else { prices[n / 2] })
        } else { None };
        (key, median)
    }).collect();
    let mut trends: Vec<NicheTrend> = current.products.iter().map(|product| {
        let key = niche_title_key(&product.title);
        let median = price_benchmarks.get(&key).copied().flatten();
        let markup = median.filter(|m| *m > 0.0).map(|m| ((product.price_rub / m - 1.0) * 100.0).round());
        let old = previous.get(&product.item_id).copied();
        let delta = old.map(|old| {
            (product.sold_count as i128 - old as i128)
                .clamp(i64::MIN as i128, i64::MAX as i128) as i64
        });
        let sellers_count = seller_groups.get(&key).map(|sellers| sellers.len()).unwrap_or(0);
        let demand_score = delta.unwrap_or(0).max(0) as f64;
        let demand_score = (demand_score.ln_1p() / 500.0_f64.ln_1p() * 45.0).clamp(0.0, 45.0);
        let supply_score = 35.0 / (sellers_count.max(1) as f64).sqrt();
        let markup_score = markup.unwrap_or(0.0).clamp(0.0, 200.0) * 0.1;
        let opportunity_score = (demand_score + supply_score + markup_score).clamp(0.0, 100.0);
        NicheTrend {
            product: product.clone(),
            previous_sold_count: old,
            delta_sales: delta,
            listings_count: *title_counts.get(&key).unwrap_or(&1),
            sellers_count,
            comparable_median_price_rub: median,
            markup_percent: markup,
            opportunity_score: (opportunity_score * 10.0).round() / 10.0,
        }
    }).collect();
    trends.sort_by(|a, b| b.opportunity_score.partial_cmp(&a.opportunity_score).unwrap_or(std::cmp::Ordering::Equal));
    trends
}

async fn discover_sellers_via_api(
    client: &Client,
    queries: &[String],
    pages_per_category: u32,
    delay_ms: u64,
) -> Result<BTreeMap<u64, SellerRecord>> {
    let mut sellers: BTreeMap<u64, SellerRecord> = BTreeMap::new();

    for query in queries {
        let mut page = 1u32;
        while page <= pages_per_category.max(1) {
            let api_url = Url::parse_with_params(
                PUBLIC_SEARCH_API,
                &[
                    ("query", query.as_str()),
                    ("pagesize", "500"),
                    ("pagenum", &page.to_string()),
                    ("visibleOnly", "true"),
                    ("response", "json"),
                ],
            )?;

            match client.get(api_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let parsed: SearchApiResponse = resp.json().await?;
                    let items = parsed.items.unwrap_or_default();
                    let batch_len = items.len();
                    for item in items {
                        if let (Some(id), Some(name)) = (item.seller_id, item.seller_name) {
                            sellers.entry(id).or_insert_with(|| SellerRecord {
                                seller_id: id,
                                seller_name: clean_space(&name),
                                profile_url: format!("https://plati.market/seller/s/{}/", id),
                                rating: item.seller_rating,
                                level: None,
                                sales_count: None,
                                returns_count: None,
                                reviews_count: None,
                                items_count: None,
                                tenure: None,
                                enriched_from_html: false,
                            });
                        }
                    }
                    if page >= parsed.total_pages || batch_len == 0 {
                        break;
                    }
                }
                Ok(resp) => return Err(anyhow::anyhow!("HTTP {} при поиске продавцов по запросу {}", resp.status(), query)),
                Err(error) => return Err(error.into()),
            }

            page += 1;
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
    Ok(sellers)
}

fn save_sellers_outputs(out_dir: &Path, records: &[SellerRecord]) -> Result<(PathBuf, PathBuf)> {
    fs::create_dir_all(out_dir)?;
    let csv_path = out_dir.join("plati_sellers.csv");
    let json_path = out_dir.join("plati_sellers.json");

    let mut csv_file = File::create(&csv_path)?;
    csv_file.write_all(b"\xEF\xBB\xBF")?;
    let mut wtr = csv::Writer::from_writer(csv_file);
    for rec in records {
        wtr.serialize(rec)?;
    }
    wtr.flush()?;

    let json_file = File::create(&json_path)?;
    serde_json::to_writer_pretty(json_file, records)?;
    Ok((csv_path, json_path))
}

// ======================= HTTP-ОБРАБОТЧИКИ AXUM =======================

async fn index_handler() -> AxumHtml<&'static str> {
    AxumHtml(WEB_UI_HTML)
}

async fn api_niche_radar_handler(State(state): State<AppState>) -> impl IntoResponse {
    match load_recent_niche_snapshots(&state.out_dir) {
        Ok(snapshots) => {
            let mut trends = build_niche_trends(&snapshots);
            trends.retain(|trend| trend.delta_sales.is_some());
            trends.truncate(2_000);
            let latest = snapshots.first();
            let previous = previous_daily_snapshot(&snapshots);
            (StatusCode::OK, Json(json!({
                "trends": trends,
                "history": niche_history_for_response(&state.out_dir, &snapshots),
                "progress": read_niche_progress(&state.out_dir),
                "has_checkpoint": niche_checkpoint_path(&state.out_dir).exists(),
                "captured_at": latest.map(|s| s.captured_at),
                "previous_captured_at": previous.map(|s| s.captured_at),
                "categories_scanned": latest.map(|s| s.categories_scanned).unwrap_or(0),
                "product_count": latest.map(|s| s.products.len()).unwrap_or(0),
                "has_comparison": previous.is_some()
            })))
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": err.to_string() }))),
    }
}

fn start_niche_snapshot(state: AppState) -> bool {
    if state.niche_running.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
        return false;
    }
    let mut progress = read_niche_progress(&state.out_dir);
    progress.status = "starting".to_string();
    progress.updated_at = unix_now();
    progress.error = None;
    progress.resumed = niche_checkpoint_path(&state.out_dir).exists();
    let _ = write_json_atomic(&niche_progress_path(&state.out_dir), &progress);
    tokio::spawn(async move {
        match capture_niche_snapshot(&state.client, &state.out_dir).await {
            Ok(snapshot) => println!(
                "Слепок радара сохранён: {} карточек, {} категорий",
                snapshot.products.len(), snapshot.categories_scanned
            ),
            Err(err) => {
                eprintln!("Не удалось завершить сбор радара ниш: {}", err);
                let mut progress = read_niche_progress(&state.out_dir);
                progress.status = if niche_checkpoint_path(&state.out_dir).exists() {
                    "interrupted".to_string()
                } else {
                    "error".to_string()
                };
                progress.active_categories = 0;
                progress.error = Some(err.to_string());
                progress.updated_at = unix_now();
                let _ = write_json_atomic(&niche_progress_path(&state.out_dir), &progress);
            }
        }
        state.niche_running.store(false, Ordering::Release);
    });
    true
}

async fn api_niche_status_handler(State(state): State<AppState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "progress": read_niche_progress(&state.out_dir),
            "has_checkpoint": niche_checkpoint_path(&state.out_dir).exists(),
            "running": state.niche_running.load(Ordering::Acquire)
        })),
    )
}

async fn api_refresh_niche_radar_handler(State(state): State<AppState>) -> impl IntoResponse {
    let started = start_niche_snapshot(state.clone());
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "started": started,
            "message": if started { "Сбор запущен" } else { "Сбор уже выполняется" },
            "progress": read_niche_progress(&state.out_dir),
            "has_checkpoint": niche_checkpoint_path(&state.out_dir).exists()
        })),
    )
}

async fn api_initial_handler(State(state): State<AppState>) -> impl IntoResponse {
    let json_path = state.out_dir.join("deals.json");
    if let Ok(raw) = fs::read_to_string(&json_path) {
        if let Ok(deals) = serde_json::from_str::<Vec<ProductDeal>>(&raw) {
            return (StatusCode::OK, Json(json!({ "deals": deals })));
        }
    }
    (StatusCode::OK, Json(json!({ "deals": [] })))
}

async fn api_deals_handler(
    State(state): State<AppState>,
    Json(req): Json<ApiDealsRequest>,
) -> impl IntoResponse {
    let q = req.query.trim();
    if q.chars().count() < 3 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Поисковый запрос должен содержать минимум 3 символа" })),
        );
    }

    let started = Instant::now();
    match search_deals(
        &state.client,
        q,
        req.pages.clamp(1, 4),
        req.min_reviews,
        req.max_neg_percent,
        &req.option_keyword,
        &req.exclude,
        req.inspect_limit.min(200),
        SortMode::RealPrice,
        6,
        80,
    )
    .await
    {
        Ok(deals) => {
            let _ = save_deals_files(&state.out_dir, &deals);
            let elapsed_ms = started.elapsed().as_millis() as u64;
            (
                StatusCode::OK,
                Json(json!({
                    "elapsed_ms": elapsed_ms,
                    "deals": deals,
                })),
            )
        }
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": err.to_string() })),
        ),
    }
}

async fn api_item_options_handler(
    State(state): State<AppState>,
    Query(q): Query<ApiItemOptionsQuery>,
) -> impl IntoResponse {
    let api_url = format!("{}/{}/data?currency=RUB", DIGISELLER_PRODUCT_API, q.id);
    let resp = match state
        .client
        .get(&api_url)
        .header("Accept", "application/json")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };

    let env: DigiProductEnvelope = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };

    let Some(prod) = env.product else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Карточка товара не найдена" })),
        );
    };

    let init_map = prod.prices.as_ref().and_then(|p| p.initial.as_ref());
    let def_map = prod.prices.as_ref().and_then(|p| p.default.as_ref());
    let default_rub = def_map.and_then(|m| m.rub).unwrap_or(0.0);
    let initial_rub = init_map.and_then(|m| m.rub).unwrap_or(default_rub);

    let options = prod.options.unwrap_or_default();
    let mut default_mods_sum = 0.0;
    for opt in &options {
        let otype = opt.opt_type.as_deref().unwrap_or("");
        if otype != "radio" && otype != "select" {
            continue;
        }
        if let Some(vars) = &opt.variants {
            for v in vars {
                if json_flag_true(&v.visible, true) && json_flag_true(&v.default, false) {
                    default_mods_sum += variant_modifier_rub(v, initial_rub, init_map, def_map);
                }
            }
        }
    }
    let base_checkout_rub = (default_rub - default_mods_sum).max(initial_rub);

    let mut groups = Vec::new();
    for opt in options {
        let otype = opt.opt_type.unwrap_or_default();
        let vars = opt.variants.unwrap_or_default();
        let ui_vars: Vec<UiOptionVariant> = vars
            .into_iter()
            .filter(|v| json_flag_true(&v.visible, true))
            .map(|v| {
                let mod_rub = variant_modifier_rub(&v, initial_rub, init_map, def_map);
                UiOptionVariant {
                    text: clean_space(v.text.as_deref().unwrap_or("")),
                    modifier_rub: mod_rub.round(),
                    is_default: json_flag_true(&v.default, false),
                }
            })
            .collect();

        if !ui_vars.is_empty() {
            groups.push(UiOptionGroup {
                label: clean_space(
                    opt.label
                        .as_deref()
                        .or(opt.name.as_deref())
                        .unwrap_or("Опция"),
                ),
                opt_type: otype,
                variants: ui_vars,
            });
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "base_checkout_rub": base_checkout_rub.round(),
            "groups": groups,
        })),
    )
}

async fn api_seller_handler(
    State(state): State<AppState>,
    Query(q): Query<ApiSellerQuery>,
) -> impl IntoResponse {
    let fallback_id = extract_seller_id_from_url(&q.url);
    match fetch_seller_profile(&state.client, &q.url, fallback_id).await {
        Ok(seller) => (StatusCode::OK, Json(json!(seller))),
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": err.to_string() })),
        ),
    }
}

async fn api_discover_sellers_handler(
    State(state): State<AppState>,
    Json(req): Json<ApiDiscoverRequest>,
) -> impl IntoResponse {
    let queries = match fetch_category_queries(&state.client, req.max_categories.clamp(1, 389)).await
    {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };

    let mut sellers_map = match discover_sellers_via_api(&state.client, &queries, 1, 150).await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };

    let enrich_limit = req.enrich_limit.min(100);
    if enrich_limit > 0 && !sellers_map.is_empty() {
        let mut by_rating: Vec<(u64, f64)> = sellers_map
            .values()
            .map(|s| (s.seller_id, s.rating.unwrap_or(0.0)))
            .collect();
        by_rating.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let top_ids: Vec<u64> = by_rating.into_iter().take(enrich_limit).map(|x| x.0).collect();

        let enriched: Vec<SellerRecord> = stream::iter(top_ids)
            .map(|seller_id| {
                let client = state.client.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let url = format!("https://plati.market/seller/s/{}/", seller_id);
                    fetch_seller_profile(&client, &url, Some(seller_id)).await.ok()
                }
            })
            .buffer_unordered(4)
            .filter_map(|x| async move { x })
            .collect()
            .await;

        for p in enriched {
            sellers_map.insert(p.seller_id, p);
        }
    }

    let mut sellers: Vec<SellerRecord> = sellers_map.into_values().collect();
    sellers.sort_by(|a, b| {
        b.rating
            .unwrap_or(0.0)
            .partial_cmp(&a.rating.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let _ = save_sellers_outputs(&state.out_dir, &sellers);

    (StatusCode::OK, Json(json!({ "sellers": sellers })))
}

async fn prepare_web_app(client: Client, out_dir: PathBuf) -> Result<Router> {
    if !niche_checkpoint_path(&out_dir).exists() {
        let mut progress = read_niche_progress(&out_dir);
        if progress.status == "starting" || progress.status == "collecting" {
            let latest_capture = load_recent_niche_snapshots(&out_dir)
                .ok()
                .and_then(|snapshots| snapshots.first().map(|snapshot| snapshot.captured_at));
            if latest_capture.map(|captured| captured >= progress.started_at.unwrap_or(u64::MAX)).unwrap_or(false) {
                progress.status = "completed".to_string();
                progress.error = None;
            } else {
                progress.status = "interrupted".to_string();
                progress.error = Some("Процесс завершился до сохранения checkpoint; предыдущий снимок сохранён. Можно запустить новый сбор.".to_string());
            }
            progress.active_categories = 0;
            progress.updated_at = unix_now();
            write_json_atomic(&niche_progress_path(&out_dir), &progress)?;
        }
    }
    let state = AppState {
        client,
        out_dir,
        niche_running: Arc::new(AtomicBool::new(false)),
    };
    let scheduler_state = state.clone();
    tokio::spawn(async move {
        loop {
            let has_checkpoint = niche_checkpoint_path(&scheduler_state.out_dir).exists();
            let needs_capture = has_checkpoint || load_recent_niche_snapshots(&scheduler_state.out_dir)
                .ok()
                .and_then(|s| s.first().map(|x| x.captured_at.saturating_add(86_400) <= unix_now()))
                .unwrap_or(true);
            if needs_capture {
                start_niche_snapshot(scheduler_state.clone());
            }
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    });
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/initial", get(api_initial_handler))
        .route("/api/niche-radar", get(api_niche_radar_handler).post(api_refresh_niche_radar_handler))
        .route("/api/niche-radar/status", get(api_niche_status_handler))
        .route("/api/deals", post(api_deals_handler))
        .route("/api/item-options", get(api_item_options_handler))
        .route("/api/seller", get(api_seller_handler))
        .route("/api/discover-sellers", post(api_discover_sellers_handler))
        .with_state(state);
    Ok(app)
}

async fn run_web_server(client: Client, out_dir: PathBuf, port: u16) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Не удалось занять порт {}", port))?;
    let app = prepare_web_app(client, out_dir).await?;

    println!("======================================================");
    println!(" Plati Radar запущен!");
    println!(" Открой в браузере: http://localhost:{}", port);
    println!("======================================================");

    axum::serve(listener, app).await?;
    Ok(())
}


// ======================= MAIN =======================

async fn execute_cli(cli: Cli) -> Result<()> {
    let client = build_client()?;

    match cli.command.unwrap_or(Commands::Serve { port: 8080 }) {
        Commands::Serve { port } => {
            run_web_server(client, cli.out_dir, port).await?;
        }

        Commands::Deals {
            query,
            pages,
            min_reviews,
            max_neg_percent,
            option_keyword,
            exclude,
            inspect_limit,
            sort,
            concurrency,
            delay_ms,
        } => {
            let deals = search_deals(
                &client,
                &query,
                pages,
                min_reviews,
                max_neg_percent,
                &option_keyword,
                &exclude,
                inspect_limit,
                sort,
                concurrency,
                delay_ms,
            )
            .await?;

            for (i, d) in deals.iter().take(10).enumerate() {
                println!(
                    "{:>2}. {:>5.0} ₽ (витрина {:>4.0} ₽) | +{:>5} отз. | {:<16} | {}",
                    i + 1,
                    d.real_tariff_price_rub,
                    d.storefront_price_rub,
                    d.positive_reviews,
                    d.seller_name,
                    d.item_url
                );
            }
            let (csv_path, json_path) = save_deals_files(&cli.out_dir, &deals)?;
            println!("Сохранено {} карточек в {:?} и {:?}", deals.len(), csv_path, json_path);
        }

        Commands::Single { url } => {
            let fallback_id = extract_seller_id_from_url(&url);
            let seller = fetch_seller_profile(&client, &url, fallback_id).await?;
            println!(
                "Магазин: {} (ID {}) | Продаж: {:?} | Отзывов: {:?}",
                seller.seller_name, seller.seller_id, seller.sales_count, seller.reviews_count
            );
            let (csv_path, json_path) = save_sellers_outputs(&cli.out_dir, &[seller])?;
            println!("Сохранено в {:?} и {:?}", csv_path, json_path);
        }

        Commands::Discover {
            max_categories,
            pages_per_category,
            enrich_limit,
            concurrency,
            delay_ms,
        } => {
            let queries = fetch_category_queries(&client, max_categories).await?;
            let mut sellers_map =
                discover_sellers_via_api(&client, &queries, pages_per_category, delay_ms).await?;

            if enrich_limit > 0 && !sellers_map.is_empty() {
                let ids: Vec<u64> = sellers_map.keys().copied().take(enrich_limit).collect();
                let enriched: Vec<SellerRecord> = stream::iter(ids)
                    .map(|seller_id| {
                        let client = client.clone();
                        async move {
                            if delay_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            }
                            let url = format!("https://plati.market/seller/s/{}/", seller_id);
                            fetch_seller_profile(&client, &url, Some(seller_id)).await.ok()
                        }
                    })
                    .buffer_unordered(concurrency.max(1))
                    .filter_map(|x| async move { x })
                    .collect()
                    .await;

                for p in enriched {
                    sellers_map.insert(p.seller_id, p);
                }
            }

            let mut results: Vec<SellerRecord> = sellers_map.into_values().collect();
            results.sort_by(|a, b| {
                b.rating
                    .unwrap_or(0.0)
                    .partial_cmp(&a.rating.unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let (csv_path, json_path) = save_sellers_outputs(&cli.out_dir, &results)?;
            println!(
                "Уникальных продавцов: {}. Файлы: {:?} и {:?}",
                results.len(),
                csv_path,
                json_path
            );
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.command.is_none() {
        return native_ui::run(cli.out_dir);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(execute_cli(cli))
}
