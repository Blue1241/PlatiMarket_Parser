use super::*;
use eframe::egui::{self, Color32, RichText, Stroke, Vec2};
use futures::stream::{self, StreamExt};
use std::sync::mpsc::{self, Receiver, Sender};
use std::collections::{HashMap, VecDeque};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Page { Deals, Seller, Sellers, Radar, Watchlist, Activity }
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum RadarSort { Opportunity, Delta, Price, Sellers }
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SellerSort { Rating, Sales, Reviews }

#[derive(Clone, Serialize, Deserialize)]
struct SavedSearch {
    name: String,
    query: String,
    keyword: String,
    exclude: String,
    min_reviews: u64,
    max_negative: f64,
    pages: u32,
    inspect_limit: usize,
    sort: SortMode,
}

#[derive(Clone, Serialize, Deserialize)]
struct TaskLogEntry { at: u64, task: String, status: String, details: String, #[serde(default = "default_one")] attempts: usize }

#[derive(Clone)]
struct ExportDraft { title:String, filename:String, rows:Vec<serde_json::Value>, columns:Vec<String>, selected:HashSet<String> }
#[derive(Clone,Copy)]
enum ExportFormat { Csv, Xlsx, Json }
impl ExportFormat { fn extension(self)->&'static str {match self{Self::Csv=>"csv",Self::Xlsx=>"xlsx",Self::Json=>"json"}} }

#[derive(Clone)]
struct NoteEditor { item_id:Option<u64>, seller_id:Option<u64>, note:String, tags:String }

#[derive(Clone)]
struct RadarChange { title:String, detail:String, sales:i64, price:f64 }
#[derive(Clone)]
struct RadarComparison { current_at:u64, previous_at:u64, total_changes:usize, added:usize, missing:usize, top_changes:Vec<RadarChange> }

fn default_one() -> usize { 1 }
fn default_delay_ms() -> u64 { 250 }
fn default_request_concurrency() -> usize { 4 }
fn default_retention_days() -> u64 { 365 }
fn default_history_limit() -> usize { 500 }
fn default_deal_column_widths()->[f32;6]{[0.33,0.13,0.11,0.16,0.15,0.12]}
fn default_seller_column_widths()->[f32;6]{[0.30,0.13,0.14,0.14,0.13,0.16]}
fn default_radar_column_widths()->[f32;7]{[0.05,0.38,0.12,0.12,0.13,0.12,0.08]}

fn normalize_column_widths(widths:&mut [f32]){
    for width in widths.iter_mut(){*width=width.clamp(0.02,0.9);}
    let total: f32=widths.iter().sum();
    if total>0.0{for width in widths{*width/=total;}}
}

fn resizable_header_cell(ui:&mut egui::Ui,index:usize,widths:&mut [f32],total_width:f32,header:impl FnOnce(&mut egui::Ui,f32)){
    let has_handle=index+1<widths.len();
    let handle_width=if has_handle{9.0}else{0.0};
    let column_width=total_width*widths[index];
    ui.horizontal(|ui|{
        ui.spacing_mut().item_spacing.x=0.0;
        header(ui,(column_width-handle_width).max(36.0));
        if has_handle{
            let (rect,response)=ui.allocate_exact_size(Vec2::new(handle_width,28.0),egui::Sense::drag());
            let color=if response.hovered()||response.dragged(){Color32::from_rgb(56,189,248)}else{Color32::from_rgb(71,85,105)};
            ui.painter().line_segment([egui::pos2(rect.center().x,rect.top()+6.0),egui::pos2(rect.center().x,rect.bottom()-6.0)],Stroke::new(if response.hovered()||response.dragged(){2.0_f32}else{1.0_f32},color));
            if response.hovered()||response.dragged(){ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeHorizontal);}
            response.clone().on_hover_text("Перетащите разделитель, чтобы изменить ширину столбца");
            if response.dragged(){
                let delta=ui.ctx().input(|input|input.pointer.delta().x)/total_width.max(1.0);
                let pair=widths[index]+widths[index+1];
                let minimum=(54.0/total_width.max(1.0)).min(pair*0.5);
                let left=(widths[index]+delta).clamp(minimum,pair-minimum);
                widths[index]=left;widths[index+1]=pair-left;
            }
        }
    });
}

fn fixed_table_cell(ui:&mut egui::Ui,width:f32,height:f32,contents:impl FnOnce(&mut egui::Ui)){
    ui.allocate_ui_with_layout(Vec2::new(width,height),egui::Layout::top_down(egui::Align::LEFT),|cell|{
        let rect=cell.max_rect();
        contents(cell);
        cell.painter().line_segment([egui::pos2(rect.right(),rect.top()+5.0),egui::pos2(rect.right(),rect.bottom()-5.0)],Stroke::new(1.0_f32,Color32::from_rgb(37,51,72)));
    });
}

#[derive(Clone, Default)]
struct RadarAlertPrefs { enabled: bool, min_delta: i64, max_sellers: usize, text_filter: String, tracked_sellers_only: bool, seller_ids: Vec<u64> }

#[derive(Clone, Serialize, Deserialize)]
struct WatchObservation { at: u64, price: f64, sales: u64, reviews: u64 }

#[derive(Clone, Serialize, Deserialize)]
struct WatchedProduct {
    item_id: u64, title: String, url: String, seller_id: u64, seller_name: String,
    #[serde(default)] note:String,
    #[serde(default)] tags:Vec<String>,
    observations: Vec<WatchObservation>,
}

#[derive(Clone, Serialize, Deserialize)]
struct SellerObservation { at: u64, rating: Option<f64>, sales: Option<u64>, reviews: Option<u64>, returns: Option<u64> }

#[derive(Clone, Serialize, Deserialize)]
struct WatchedSeller {
    seller_id: u64, name: String, url: String,
    #[serde(default)] note:String,
    #[serde(default)] tags:Vec<String>,
    observations: Vec<SellerObservation>,
}

fn default_true() -> bool { true }
fn default_price_drop() -> f64 { 5.0 }
fn default_sales_growth() -> u64 { 1 }

#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct UiSettings {
    out_dir: PathBuf,
    window_size: [f32; 2],
    page: Page,
    query: String,
    deals_filter: String,
    seller_url: String,
    sellers_filter: String,
    niche_filter: String,
    keyword: String,
    exclude: String,
    min_reviews: u64,
    max_negative: f64,
    pages: u32,
    inspect_limit: usize,
    sort: SortMode,
    seller_sort: SellerSort,
    max_categories: usize,
    enrich_limit: usize,
    min_delta: i64,
    max_sellers: usize,
    max_price: f64,
    max_markup: f64,
    radar_sort: RadarSort,
    favorites_only: bool,
    notify_growth: bool,
    #[serde(default = "default_true")]
    price_alerts: bool,
    #[serde(default = "default_true")]
    sales_alerts: bool,
    #[serde(default = "default_price_drop")]
    price_drop_threshold: f64,
    #[serde(default = "default_sales_growth")]
    sales_growth_threshold: u64,
    #[serde(default)]
    alert_filter: String,
    #[serde(default)]
    tracked_sellers_only: bool,
    #[serde(default = "default_delay_ms")]
    request_delay_ms:u64,
    #[serde(default = "default_request_concurrency")]
    request_concurrency:usize,
    #[serde(default = "default_retention_days")]
    retention_days:u64,
    #[serde(default = "default_history_limit")]
    history_limit:usize,
    #[serde(default = "default_deal_column_widths")]
    deal_column_widths:[f32;6],
    #[serde(default = "default_seller_column_widths")]
    seller_column_widths:[f32;6],
    #[serde(default = "default_radar_column_widths")]
    radar_column_widths:[f32;7],
}

impl UiSettings {
    fn defaults(out_dir: PathBuf) -> Self {
        Self { out_dir, window_size: [1440.0, 940.0], page: Page::Deals,
            query: "ChatGPT Plus".into(), deals_filter: String::new(), seller_url: DEFAULT_SELLER_URL.into(), sellers_filter: String::new(), niche_filter: String::new(), keyword: "plus".into(),
            exclude: "оффлайн,offline,общий,shared,гайд,инструкция,аренда".into(),
            min_reviews: 100, max_negative: 5.0, pages: 2, inspect_limit: 50,
            sort: SortMode::RealPrice, seller_sort: SellerSort::Rating, max_categories: 20, enrich_limit: 25,
            min_delta: 1, max_sellers: 50, max_price: 1_000_000.0, max_markup: 1_000.0,
            radar_sort: RadarSort::Opportunity, favorites_only: false, notify_growth: true,
            price_alerts: true, sales_alerts: true, price_drop_threshold: 5.0, sales_growth_threshold: 1,
            alert_filter: String::new(), tracked_sellers_only: false, request_delay_ms:250, request_concurrency:4, retention_days:365, history_limit:500,
            deal_column_widths:default_deal_column_widths(),seller_column_widths:default_seller_column_widths(),radar_column_widths:default_radar_column_widths() }
    }
}

fn settings_file() -> PathBuf {
    #[cfg(windows)]
    let base = std::env::var_os("APPDATA").map(PathBuf::from);
    #[cfg(target_os="macos")]
    let base = std::env::var_os("HOME").map(|h|PathBuf::from(h).join("Library").join("Application Support"));
    #[cfg(all(not(windows),not(target_os="macos")))]
    let base = std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from).or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")));
    base.unwrap_or_else(|| std::env::current_dir().unwrap_or_default().join("output"))
        .join("PlatiRadar").join("settings.json")
}

fn load_settings(default_dir: PathBuf) -> UiSettings {
    fs::read(settings_file()).ok().and_then(|raw| serde_json::from_slice::<UiSettings>(&raw).ok()).unwrap_or_else(|| UiSettings::defaults(default_dir))
}

fn save_settings(settings: &UiSettings) {
    let path = settings_file();
    let _ = write_json_atomic(&path, settings);
}

enum UiMessage {
    Deals(Result<Vec<ProductDeal>, String>, usize),
    Seller(Result<SellerRecord, String>, usize),
    Sellers(Result<Vec<SellerRecord>, String>, usize),
    DiscoverProgress(usize, usize, String),
    Options(Result<(u64, String, f64, Vec<UiOptionGroup>), String>),
    NicheStarted,
    Niche(Result<(), String>),
    RadarSnapshotsLoaded(Vec<NicheSnapshot>, Vec<NicheHistoryPoint>),
    RadarDerived(Vec<NicheMetric>, RadarComparison),
    RadarVisible(String, Vec<usize>),
}

struct PlatiApp {
    settings: UiSettings,
    settings_saved: UiSettings,
    out_dir: PathBuf,
    output_shared: Arc<std::sync::RwLock<PathBuf>>,
    client: Client,
    runtime: tokio::runtime::Runtime,
    tx: Sender<UiMessage>,
    rx: Receiver<UiMessage>,
    page: Page,
    message: String,
    query: String,
    keyword: String,
    exclude: String,
    min_reviews: u64,
    max_negative: f64,
    pages: u32,
    inspect_limit: usize,
    sort: SortMode,
    deal_column_widths:[f32;6],
    seller_column_widths:[f32;6],
    radar_column_widths:[f32;7],
    responsive_mode:u8,
    deals_filter: String,
    deals: Vec<ProductDeal>,
    search_busy: bool,
    search_cancel: Option<tokio::task::AbortHandle>,
    seller_url: String,
    seller: Option<SellerRecord>,
    seller_busy: bool,
    seller_cancel: Option<tokio::task::AbortHandle>,
    max_categories: usize,
    enrich_limit: usize,
    sellers: Vec<SellerRecord>,
    seller_sort: SellerSort,
    sellers_filter: String,
    discover_busy: bool,
    discover_cancel: Option<tokio::task::AbortHandle>,
    discover_progress: (usize, usize, String),
    niche_running: Arc<AtomicBool>,
    capture_task: Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>,
    radar_prefs_shared: Arc<std::sync::RwLock<RadarAlertPrefs>>,

    progress: NicheProgress,
    snapshots: Arc<Vec<NicheSnapshot>>,
    history: Vec<NicheHistoryPoint>,
    radar_snapshots_loading: bool,
    radar_metrics: Arc<Vec<NicheMetric>>,
    radar_comparison: Option<RadarComparison>,
    radar_derived_loading: bool,
    radar_visible: Vec<usize>,
    radar_filter_key: String,
    radar_visible_loading: bool,
    niche_filter: String,
    min_delta: i64,
    max_sellers: usize,
    max_price: f64,
    max_markup: f64,
    radar_sort: RadarSort,
    favorites: HashSet<u64>,
    favorites_only: bool,
    notify_growth: bool,
    options_open: bool,
    options_busy: bool,
    options_id: u64,
    options_title: String,
    options_base: f64,
    options: Vec<UiOptionGroup>,
    option_selection: BTreeMap<usize, usize>,
    saved_searches: Vec<SavedSearch>,
    saved_search_name: String,
    selected_saved_search: usize,
    watchlist: Vec<WatchedProduct>,
    watched_sellers: Vec<WatchedSeller>,
    activity_log: Vec<TaskLogEntry>,
    price_alerts: bool,
    sales_alerts: bool,
    price_drop_threshold: f64,
    sales_growth_threshold: u64,
    alert_filter: String,
    tracked_sellers_only: bool,
    restore_candidate: Option<PathBuf>,
    backup_message: String,
    search_queue:VecDeque<SavedSearch>,
    search_queue_running:bool,
    queue_advance_pending:bool,
    request_delay_ms:u64,
    request_concurrency:usize,
    retention_days:u64,
    history_limit:usize,
    watch_filter:String,
    export_draft:Option<ExportDraft>,
    note_editor:Option<NoteEditor>,
}

impl PlatiApp {
    fn new(cc: &eframe::CreationContext<'_>, mut settings: UiSettings) -> Self {
        normalize_column_widths(&mut settings.deal_column_widths);
        normalize_column_widths(&mut settings.seller_column_widths);
        normalize_column_widths(&mut settings.radar_column_widths);
        theme(&cc.egui_ctx);
        let out_dir = settings.out_dir.clone();
        let output_shared = Arc::new(std::sync::RwLock::new(out_dir.clone()));
        let client = build_client().unwrap_or_else(|_| Client::new());
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Tokio runtime");
        let (tx, rx) = mpsc::channel();
        let deals = read_json_or(out_dir.join("deals.json"), Vec::new());
        let sellers = read_json_or(out_dir.join("plati_sellers.json"), Vec::new());
        let favorites = read_json_or(out_dir.join("niche_favorites.json"), HashSet::new());
        let snapshots = Vec::new();
        let history = Vec::new();
        let progress = read_niche_progress(&out_dir);
        let watchlist = read_json_or(out_dir.join("watchlist.json"), Vec::new());
        let watched_sellers = read_json_or(out_dir.join("watched_sellers.json"), Vec::new());
        let mut app = Self {
            settings: settings.clone(), settings_saved: settings.clone(), out_dir, output_shared, client, runtime, tx, rx,
            page: settings.page, message: String::new(),
            query: settings.query.clone(), keyword: settings.keyword.clone(), exclude: settings.exclude.clone(),
            min_reviews: settings.min_reviews, max_negative: settings.max_negative, pages: settings.pages, inspect_limit: settings.inspect_limit,
            sort: settings.sort, deal_column_widths:settings.deal_column_widths,seller_column_widths:settings.seller_column_widths,radar_column_widths:settings.radar_column_widths,responsive_mode:u8::MAX,
            deals_filter: settings.deals_filter.clone(), deals, search_busy: false, search_cancel: None,
            seller_url: settings.seller_url.clone(), seller: None, seller_busy: false, seller_cancel: None,
            max_categories: settings.max_categories, enrich_limit: settings.enrich_limit, sellers, seller_sort: settings.seller_sort, sellers_filter: settings.sellers_filter.clone(), discover_busy: false,
            discover_cancel: None, discover_progress: (0, 0, String::new()),
            niche_running: Arc::new(AtomicBool::new(false)), capture_task: Arc::new(std::sync::Mutex::new(None)),
            radar_prefs_shared: Arc::new(std::sync::RwLock::new(RadarAlertPrefs{enabled:settings.notify_growth,min_delta:settings.min_delta,max_sellers:settings.max_sellers,text_filter:settings.alert_filter.clone(),tracked_sellers_only:settings.tracked_sellers_only,seller_ids:watched_sellers.iter().map(|s:&WatchedSeller|s.seller_id).collect()})), progress, snapshots:Arc::new(snapshots), history,
            niche_filter: settings.niche_filter.clone(), min_delta: settings.min_delta, max_sellers: settings.max_sellers, max_price: settings.max_price, max_markup: settings.max_markup, radar_sort: settings.radar_sort, favorites,
            radar_metrics:Arc::new(Vec::new()),radar_comparison:None,radar_snapshots_loading:true,radar_derived_loading:false,radar_visible:Vec::new(),radar_filter_key:String::new(),radar_visible_loading:false,
            favorites_only: settings.favorites_only, notify_growth: settings.notify_growth, options_open: false, options_busy: false,
            options_id: 0, options_title: String::new(), options_base: 0.0,
            options: Vec::new(), option_selection: BTreeMap::new(),
            saved_searches: read_json_or(settings.out_dir.join("saved_searches.json"), Vec::new()),
            saved_search_name: String::new(), selected_saved_search: 0,
            watchlist,
            watched_sellers,
            activity_log: read_json_or(settings.out_dir.join("activity_log.json"), Vec::new()),
            price_alerts: settings.price_alerts, sales_alerts: settings.sales_alerts, price_drop_threshold: settings.price_drop_threshold, sales_growth_threshold: settings.sales_growth_threshold,
            alert_filter: settings.alert_filter.clone(), tracked_sellers_only: settings.tracked_sellers_only, restore_candidate: None, backup_message: String::new(),
            search_queue:VecDeque::new(),search_queue_running:false,queue_advance_pending:false,
            request_delay_ms:settings.request_delay_ms,request_concurrency:settings.request_concurrency,retention_days:settings.retention_days,history_limit:settings.history_limit,
            watch_filter:String::new(),export_draft:None,note_editor:None,
        };
        app.reload_radar();
        app.start_radar_scheduler();
        app
    }

    fn start_radar_scheduler(&self) {
        let client = self.client.clone();
        let output_shared = self.output_shared.clone();
        let tx = self.tx.clone();
        let running = self.niche_running.clone();
        let task_slot = self.capture_task.clone();
        let radar_prefs = self.radar_prefs_shared.clone();
        self.runtime.spawn(async move {
            let mut last_check = tokio::time::Instant::now();
            let mut first = true;
            loop {
                if first || last_check.elapsed() >= Duration::from_secs(3600) {
                    first = false;
                    last_check = tokio::time::Instant::now();
                    let out = output_shared.read().map(|p| p.clone()).unwrap_or_else(|_| PathBuf::from("output"));
                    let due = niche_checkpoint_path(&out).exists()
                        || load_recent_niche_snapshots(&out).ok()
                            .and_then(|v| v.first().map(|s| s.captured_at.saturating_add(86_400) <= unix_now()))
                            .unwrap_or(true);
                    if due {
                        let prefs = radar_prefs.read().map(|p| p.clone()).unwrap_or_default();
                        start_capture(client.clone(), out.clone(), tx.clone(), running.clone(), tokio::runtime::Handle::current(), task_slot.clone(), prefs);
                    }
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    fn send_search(&mut self) {
        if self.search_busy || self.query.trim().chars().count() < 3 { return; }
        self.search_busy = true;
        self.message.clear();
        self.record_task("Поиск предложений","Запущено",self.query.clone());
        let (client, tx) = (self.client.clone(), self.tx.clone());
        let (query, keyword, exclude) = (self.query.trim().to_owned(), self.keyword.clone(), self.exclude.clone());
        let (pages, min_reviews, neg, inspect) = (self.pages, self.min_reviews, self.max_negative, self.inspect_limit);
        let delay=self.request_delay_ms.clamp(50,5000);
        let concurrency=self.request_concurrency.clamp(1,12);
        let task = self.runtime.spawn(async move {
            let mut attempts=0usize;
            let result=loop {
                attempts+=1;
                match search_deals(&client,&query,pages,min_reviews,neg,&keyword,&exclude,inspect,SortMode::RealPrice,concurrency,delay).await {
                    Ok(items)=>break Ok(items),
                    Err(_error) if attempts<3=>{tokio::time::sleep(Duration::from_millis(delay.saturating_mul(attempts as u64))).await;continue;},
                    Err(error)=>break Err(error.to_string()),
                }
            };
            let _=tx.send(UiMessage::Deals(result,attempts));
        });
        self.search_cancel = Some(task.abort_handle());
    }

    fn enqueue_search(&mut self,index:usize) {
        if let Some(preset)=self.saved_searches.get(index).cloned(){
            self.search_queue.push_back(preset);
            self.message=format!("Поиск добавлен в очередь · {} в очереди",self.search_queue.len());
        }
    }

    fn start_search_queue(&mut self) {
        if self.search_queue_running { return; }
        if self.search_queue.is_empty(){self.message="Очередь поисков пуста".into();return;}
        self.search_queue_running=true;
        self.record_task("Очередь поисков","Запущено",format!("{} задач",self.search_queue.len()));
        self.run_next_queued_search();
    }

    fn run_next_queued_search(&mut self) {
        if self.search_busy { return; }
        while let Some(preset)=self.search_queue.pop_front(){
            if preset.query.trim().chars().count()<3 {self.record_task("Очередь поисков","Пропущено",format!("{}: запрос короче 3 символов",preset.name));continue;}
            self.query=preset.query;self.keyword=preset.keyword;self.exclude=preset.exclude;
            self.min_reviews=preset.min_reviews;self.max_negative=preset.max_negative;self.pages=preset.pages;self.inspect_limit=preset.inspect_limit;self.sort=preset.sort;
            self.message=format!("Очередь: поиск «{}» · осталось {}",preset.name,self.search_queue.len());
            self.send_search();return;
        }
        self.search_queue_running=false;
        self.record_task("Очередь поисков","Завершено","Все поиски обработаны".into());
        self.message="Очередь поисков завершена".into();
    }

    fn stop_search_queue(&mut self) {
        self.search_queue.clear();self.search_queue_running=false;self.queue_advance_pending=false;
        if self.search_busy { self.cancel_search(); }
        self.message="Очередь поисков остановлена".into();
        self.record_task("Очередь поисков","Отменено",String::new());
    }

    fn cancel_search(&mut self) {
        if let Some(task) = self.search_cancel.take() { task.abort(); }
        self.search_busy = false;
        self.message = "Поиск отменён".into();
        self.record_task("Поиск предложений","Отменено",self.query.clone());
        if self.search_queue_running {self.search_queue.clear();self.search_queue_running=false;self.record_task("Очередь поисков","Отменено","Текущий поиск отменён".into());}
    }

    fn save_search_preset(&mut self) {
        let name = self.saved_search_name.trim();
        if name.is_empty() { self.message = "Укажите название сохранённого поиска".into(); return; }
        let preset = SavedSearch { name:name.into(), query:self.query.clone(), keyword:self.keyword.clone(), exclude:self.exclude.clone(), min_reviews:self.min_reviews, max_negative:self.max_negative, pages:self.pages, inspect_limit:self.inspect_limit, sort:self.sort };
        if let Some(existing)=self.saved_searches.iter_mut().find(|p|p.name.eq_ignore_ascii_case(name)) { *existing=preset; }
        else { self.saved_searches.push(preset); }
        self.selected_saved_search=self.saved_searches.iter().position(|p|p.name.eq_ignore_ascii_case(name)).unwrap_or(0);
        let _=write_json_atomic(&self.out_dir.join("saved_searches.json"),&self.saved_searches);
        self.message=format!("Поиск «{name}» сохранён");
    }

    fn apply_saved_search(&mut self, index:usize) {
        if let Some(p)=self.saved_searches.get(index).cloned() {
            self.query=p.query; self.keyword=p.keyword; self.exclude=p.exclude; self.min_reviews=p.min_reviews;
            self.max_negative=p.max_negative; self.pages=p.pages; self.inspect_limit=p.inspect_limit; self.sort=p.sort;
            self.selected_saved_search=index; self.message=format!("Загружен поиск «{}»",p.name);
        }
    }

    fn record_task(&mut self, task:&str, status:&str, details:String) { self.record_task_attempts(task,status,details,1); }

    fn record_task_attempts(&mut self, task:&str, status:&str, details:String, attempts:usize) {
        let now=unix_now();
        let details=if status!="Запущено" {
            self.activity_log.iter().find(|e|e.task==task&&e.status=="Запущено").map(|start|format!("{details} · длительность {} сек",now.saturating_sub(start.at))).unwrap_or(details)
        } else { details };
        self.activity_log.insert(0,TaskLogEntry{at:now,task:task.into(),status:status.into(),details,attempts});
        self.activity_log.truncate(self.history_limit.clamp(50,5000));
        let _=write_json_atomic(&self.out_dir.join("activity_log.json"),&self.activity_log);
    }

    fn persist_watchlist(&self) {
        let _=write_json_atomic(&self.out_dir.join("watchlist.json"),&self.watchlist);
        let _=write_json_atomic(&self.out_dir.join("watched_sellers.json"),&self.watched_sellers);
    }

    fn watch_product(&mut self, d:&ProductDeal) {
        if self.watchlist.iter().any(|w|w.item_id==d.item_id) {
            self.watchlist.retain(|w|w.item_id!=d.item_id);self.persist_watchlist();self.message=format!("Отслеживание остановлено: {}",d.title);return;
        }
        self.watchlist.push(WatchedProduct{item_id:d.item_id,title:d.title.clone(),url:d.item_url.clone(),seller_id:d.seller_id,seller_name:d.seller_name.clone(),note:String::new(),tags:Vec::new(),observations:vec![WatchObservation{at:unix_now(),price:d.real_tariff_price_rub,sales:d.sold_count,reviews:d.positive_reviews+d.negative_reviews}]});
        self.persist_watchlist(); self.message=format!("Добавлено в отслеживание: {}",d.title);
    }

    fn watch_seller(&mut self, seller:&SellerRecord) {
        if self.watched_sellers.iter().any(|w|w.seller_id==seller.seller_id) {
            self.watched_sellers.retain(|w|w.seller_id!=seller.seller_id);self.persist_watchlist();self.message=format!("Отслеживание продавца {} остановлено",seller.seller_name);return;
        }
        self.watched_sellers.push(WatchedSeller{seller_id:seller.seller_id,name:seller.seller_name.clone(),url:seller.profile_url.clone(),note:String::new(),tags:Vec::new(),observations:vec![SellerObservation{at:unix_now(),rating:seller.rating,sales:seller.sales_count,reviews:seller.reviews_count,returns:seller.returns_count}]});
        self.persist_watchlist(); self.message=format!("Продавец {} добавлен в отслеживание",seller.seller_name);
    }

    fn observe_watched_products(&mut self, deals:&[ProductDeal]) {
        let mut alerts=Vec::new();
        for d in deals {
            let alert_matches=self.alert_target_matches(&d.title,"",d.seller_id);
            if let Some(w)=self.watchlist.iter_mut().find(|w|w.item_id==d.item_id) {
                let previous=w.observations.last().cloned();
                if let Some(old)=previous {
                    let drop=old.price-d.real_tariff_price_rub;
                    let growth=d.sold_count.saturating_sub(old.sales);
                    if alert_matches && self.price_alerts && drop>0.0 && drop>=self.price_drop_threshold { alerts.push(format!("{}: цена снизилась на {:.0} ₽",w.title,drop)); }
                    if alert_matches && self.sales_alerts && growth>0 && growth>=self.sales_growth_threshold { alerts.push(format!("{}: продажи выросли на {}",w.title,growth)); }
                }
                w.title=d.title.clone(); w.seller_name=d.seller_name.clone();
                w.observations.push(WatchObservation{at:unix_now(),price:d.real_tariff_price_rub,sales:d.sold_count,reviews:d.positive_reviews+d.negative_reviews});
                if w.observations.len()>self.history_limit.clamp(50,5000) {let extra=w.observations.len()-self.history_limit.clamp(50,5000);w.observations.drain(..extra);}
            }
        }
        if !deals.is_empty() { self.persist_watchlist(); }
        if !alerts.is_empty() {
            let body=alerts.iter().take(3).cloned().collect::<Vec<_>>().join("\n");
            self.record_task("Отслеживание","Оповещение",format!("{} изменений",alerts.len()));
            self.runtime.spawn_blocking(move||{let _=notify_rust::Notification::new().summary("Plati Radar · отслеживание").body(&body).show();});
        }
    }

    fn observe_watched_seller(&mut self, seller:&SellerRecord) {
        let mut updated=false;
        if let Some(w)=self.watched_sellers.iter_mut().find(|w|w.seller_id==seller.seller_id) {
            w.name=seller.seller_name.clone();
            w.observations.push(SellerObservation{at:unix_now(),rating:seller.rating,sales:seller.sales_count,reviews:seller.reviews_count,returns:seller.returns_count});
            if w.observations.len()>self.history_limit.clamp(50,5000) {let extra=w.observations.len()-self.history_limit.clamp(50,5000);w.observations.drain(..extra);}
            updated=true;
        }
        if updated { self.persist_watchlist(); }
    }

    fn backup_data(&mut self) {
        if let Some(destination)=rfd::FileDialog::new().set_directory(&self.out_dir).pick_folder() {
            if destination==self.out_dir || destination.starts_with(&self.out_dir) {
                self.backup_message="Выберите папку за пределами каталога данных".into(); return;
            }
            let backup=destination.join(format!("PlatiRadar-backup-{}",unix_now()));
            let copied=copy_tree(&self.out_dir,&backup).and_then(|_|{
                let settings=settings_file();
                if settings.exists(){fs::copy(settings,backup.join("ui_settings.json"))?;}
                write_json_atomic(&backup.join("manifest.json"),&serde_json::json!({"app":"Plati Radar","created_at":unix_now(),"format":1}))
            });
            match copied { Ok(())=>self.backup_message=format!("Резервная копия создана: {}",backup.display()), Err(e)=>self.backup_message=format!("Ошибка резервного копирования: {e}") }
            self.record_task("Резервная копия","Завершено",self.backup_message.clone());
        }
    }

    fn choose_restore(&mut self) {
        if let Some(path)=rfd::FileDialog::new().set_directory(&self.out_dir).pick_folder() {
            if path==self.out_dir || path.starts_with(&self.out_dir) { self.backup_message="Выберите резервную копию вне текущего каталога данных".into(); }
            else if path.join("manifest.json").exists() { self.restore_candidate=Some(path); }
            else { self.backup_message="В выбранной папке не найден manifest.json резервной копии".into(); }
        }
    }

    fn restore_data(&mut self) {
        let Some(source)=self.restore_candidate.take() else { return; };
        match copy_tree(&source,&self.out_dir).and_then(|_|{
            let saved_settings=source.join("ui_settings.json");
            if saved_settings.exists(){
                let raw=fs::read(saved_settings)?;
                let mut restored:UiSettings=serde_json::from_slice(&raw)?;
                restored.out_dir=self.out_dir.clone();
                write_json_atomic(&settings_file(),&restored)?;
            }
            Ok(())
        }) {
            Ok(())=>{
                self.saved_searches=read_json_or(self.out_dir.join("saved_searches.json"),Vec::new());
                self.watchlist=read_json_or(self.out_dir.join("watchlist.json"),Vec::new());
                self.watched_sellers=read_json_or(self.out_dir.join("watched_sellers.json"),Vec::new());
                self.activity_log=read_json_or(self.out_dir.join("activity_log.json"),Vec::new());
                self.deals=read_json_or(self.out_dir.join("deals.json"),Vec::new()); self.sellers=read_json_or(self.out_dir.join("plati_sellers.json"),Vec::new());
                self.reload_radar(); self.backup_message="Резервная копия восстановлена; настройки интерфейса применятся после перезапуска".into();
                self.record_task("Восстановление","Завершено",source.display().to_string());
            }
            Err(e)=>self.backup_message=format!("Ошибка восстановления: {e}"),
        }
    }

    fn apply_retention(&mut self) {
        let cutoff=unix_now().saturating_sub(self.retention_days.clamp(30,3650).saturating_mul(86_400));
        let limit=self.history_limit.clamp(50,5000);
        for watched in &mut self.watchlist {
            watched.observations.retain(|item|item.at>=cutoff);
            if watched.observations.len()>limit {let drop=watched.observations.len()-limit;watched.observations.drain(..drop);}
        }
        for watched in &mut self.watched_sellers {
            watched.observations.retain(|item|item.at>=cutoff);
            if watched.observations.len()>limit {let drop=watched.observations.len()-limit;watched.observations.drain(..drop);}
        }
        self.activity_log.retain(|entry|entry.at>=cutoff);
        self.activity_log.truncate(limit);
        let mut archived=0usize;
        let snapshots_dir=self.out_dir.join("niche_snapshots");
        let archive_dir=self.out_dir.join("archive").join("niche_snapshots");
        if let Ok(entries)=fs::read_dir(&snapshots_dir) {
            for entry in entries.flatten() {
                let path=entry.path();if path.extension().and_then(|e|e.to_str())!=Some("json"){continue;}
                let old=fs::read(&path).ok().and_then(|raw|serde_json::from_slice::<NicheSnapshot>(&raw).ok()).map(|s|s.captured_at<cutoff).unwrap_or(false);
                if old {
                    if fs::create_dir_all(&archive_dir).is_ok(){let destination=archive_dir.join(entry.file_name());if fs::rename(&path,destination).is_ok(){archived+=1;}}
                }
            }
        }
        let history_path=self.out_dir.join("niche_history.json");
        if let Ok(raw)=fs::read(&history_path){if let Ok(mut points)=serde_json::from_slice::<Vec<NicheHistoryPoint>>(&raw){points.retain(|p|p.captured_at>=cutoff);let _=write_json_atomic(&history_path,&points);}}
        self.persist_watchlist();
        let _=write_json_atomic(&self.out_dir.join("activity_log.json"),&self.activity_log);
        self.reload_radar();
        self.record_task("Очистка истории","Завершено",format!("архивировано снимков радара: {archived}; срок хранения: {} дн.; лимит записей: {}",self.retention_days,limit));
        self.message=format!("Очистка завершена · архивировано снимков: {archived}");
    }

    fn send_seller_lookup(&mut self) {
        if self.seller_busy || self.seller_url.trim().is_empty() { return; }
        self.seller_busy = true;
        self.record_task("Проверка продавца","Запущено",self.seller_url.clone());
        let (client, tx, url) = (self.client.clone(), self.tx.clone(), self.seller_url.trim().to_owned());
        let task = self.runtime.spawn(async move {
            let id = extract_seller_id_from_url(&url);
            let (result,attempts) = fetch_seller_profile_with_attempts(&client, &url, id).await;
            let _ = tx.send(UiMessage::Seller(result.map_err(|e| e.to_string()),attempts));
        });
        self.seller_cancel = Some(task.abort_handle());
    }

    fn cancel_seller(&mut self) {
        if let Some(task) = self.seller_cancel.take() { task.abort(); }
        self.seller_busy = false;
        self.message = "Проверка продавца отменена".into();
        self.record_task("Проверка продавца","Отменено",self.seller_url.clone());
    }

    fn cancel_discover(&mut self) {
        if let Some(task) = self.discover_cancel.take() { task.abort(); }
        self.discover_busy = false;
        self.message = "Сбор продавцов отменён".into();
        self.record_task("Сбор продавцов","Отменено",String::new());
    }

    fn cancel_capture(&mut self) {
        if let Ok(mut slot) = self.capture_task.lock() {
            if let Some(task) = slot.take() { task.abort(); }
        }
        self.niche_running.store(false, Ordering::Release);
        let mut progress = read_niche_progress(&self.out_dir);
        progress.status = "interrupted".into();
        progress.active_categories = 0;
        progress.updated_at = unix_now();
        progress.error = Some("Сбор отменён пользователем; сохранённый checkpoint можно продолжить.".into());
        let _ = write_json_atomic(&niche_progress_path(&self.out_dir), &progress);
        self.progress = progress;
        self.message = "Сбор радара отменён. Прогресс можно возобновить позже.".into();
        self.record_task("Сбор радара","Отменено",String::new());
    }

    fn send_discover(&mut self) {
        if self.discover_busy { return; }
        self.discover_busy = true;
        self.record_task("Сбор продавцов","Запущено",format!("категорий до {}",self.max_categories));
        self.discover_progress = (0, 0, "Получаем список категорий…".into());
        let (client, tx) = (self.client.clone(), self.tx.clone());
        let (max_cats, enrich) = (self.max_categories.clamp(1, 389), self.enrich_limit.min(100));
        let delay_ms=self.request_delay_ms.clamp(50,5000);
        let concurrency=self.request_concurrency.clamp(1,12);
        let task = self.runtime.spawn(async move {
            let mut attempts=0usize;
            let result=loop {
                attempts+=1;
                let attempt_result = async {
                let queries = fetch_category_queries(&client, max_cats).await?;
                let total = queries.len();
                let mut map = BTreeMap::new();
                for (index, query) in queries.into_iter().enumerate() {
                    let one_query = vec![query.clone()];
                    let found = discover_sellers_via_api(&client, &one_query, 1, delay_ms).await?;
                    map.extend(found);
                    let _ = tx.send(UiMessage::DiscoverProgress(index + 1, total, query));
                }
                if enrich > 0 { let _ = tx.send(UiMessage::DiscoverProgress(total, total, "Обогащаем профили продавцов…".into())); }
                if enrich > 0 && !map.is_empty() {
                    let mut ranked: Vec<(u64, f64)> = map.values().map(|s| (s.seller_id, s.rating.unwrap_or(0.0))).collect();
                    ranked.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                    let ids: Vec<u64> = ranked.into_iter().take(enrich).map(|x| x.0).collect();
                    let extra: Vec<SellerRecord> = stream::iter(ids).map(|id| {
                        let client = client.clone();
                        async move {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            fetch_seller_profile(&client, &format!("https://plati.market/seller/s/{id}/"), Some(id)).await.ok()
                        }
                    }).buffer_unordered(concurrency).filter_map(|x| async move { x }).collect().await;
                    for seller in extra { map.insert(seller.seller_id, seller); }
                }
                let mut result: Vec<SellerRecord> = map.into_values().collect();
                result.sort_by(|a,b| b.rating.unwrap_or(0.0).partial_cmp(&a.rating.unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal));
                Ok::<_, anyhow::Error>(result)
                }.await;
                match attempt_result {
                    Ok(records)=>break Ok(records),
                    Err(_error) if attempts<3=>{tokio::time::sleep(Duration::from_millis(delay_ms.saturating_mul(attempts as u64))).await;continue;},
                    Err(error)=>break Err(error),
                }
            };
            let result=result.map_err(|e:anyhow::Error|e.to_string());
            let _ = tx.send(UiMessage::Sellers(result,attempts));
        });
        self.discover_cancel = Some(task.abort_handle());
    }

    fn load_options(&mut self, deal: &ProductDeal) {
        self.options_open = true;
        self.options_busy = true;
        self.options_id = deal.item_id;
        self.options_title.clone_from(&deal.title);
        self.options.clear();
        let (client, tx, id, price, title) = (self.client.clone(), self.tx.clone(), deal.item_id, deal.storefront_price_rub, deal.title.clone());
        self.runtime.spawn(async move {
            let result = fetch_item_options(&client, id, price).await.map(|(base, groups)| (id, title, base, groups)).map_err(|e| e.to_string());
            let _ = tx.send(UiMessage::Options(result));
        });
    }

    fn poll(&mut self) {
        while let Ok(message) = self.rx.try_recv() {
            match message {
                UiMessage::Deals(result,attempts) => {
                    self.search_busy = false;
                    self.search_cancel = None;
                    self.queue_advance_pending=true;
                    match result {
                        Ok(items) => {
                            self.message = match save_deals_files(&self.out_dir, &items) {
                                Ok((c,j)) => format!("Найдено {} карточек · сохранено {} и {}", items.len(), c.display(), j.display()),
                                Err(e) => format!("Найдено {}, ошибка записи: {e}", items.len()),
                            };
                            self.observe_watched_products(&items);
                            self.record_task_attempts("Поиск предложений","Завершено",format!("найдено {} карточек",items.len()),attempts);
                            self.deals = items;
                        }
                        Err(e) => { self.record_task_attempts("Поиск предложений","Ошибка",e.clone(),attempts); self.message = format!("Ошибка поиска: {e}"); },
                    }
                }
                UiMessage::Seller(result,attempts) => {
                    self.seller_busy = false;
                    self.seller_cancel = None;
                    match result {
                        Ok(item) => {
                            let _ = save_sellers_outputs(&self.out_dir, std::slice::from_ref(&item));
                            self.observe_watched_seller(&item);
                            self.record_task_attempts("Проверка продавца","Завершено",item.seller_name.clone(),attempts);
                            self.seller = Some(item);
                            self.message = "Профиль продавца загружен".into();
                        }
                        Err(e) => { self.record_task_attempts("Проверка продавца","Ошибка",e.clone(),attempts); self.message = format!("Ошибка загрузки профиля: {e}"); },
                    }
                }
                UiMessage::DiscoverProgress(done, total, category) => {
                    self.discover_progress = (done, total, category);
                }
                UiMessage::Sellers(result,attempts) => {
                    self.discover_busy = false;
                    self.discover_cancel = None;
                    match result {
                        Ok(items) => {
                            self.message = match save_sellers_outputs(&self.out_dir, &items) {
                                Ok((c,j)) => format!("Найдено {} продавцов · сохранено {} и {}", items.len(), c.display(), j.display()),
                                Err(e) => format!("Найдено {}, ошибка записи: {e}", items.len()),
                            };
                            for seller in &items { self.observe_watched_seller(seller); }
                            self.record_task_attempts("Сбор продавцов","Завершено",format!("найдено {} продавцов",items.len()),attempts);
                            self.sellers = items;
                        }
                        Err(e) => { self.record_task_attempts("Сбор продавцов","Ошибка",e.clone(),attempts); self.message = format!("Ошибка сбора продавцов: {e}"); },
                    }
                }
                UiMessage::Options(result) => {
                    self.options_busy = false;
                    match result {
                        Ok((id,title,base,groups)) => {
                            self.options_id = id; self.options_title = title; self.options_base = base;
                            self.option_selection.clear();
                            for (i,g) in groups.iter().enumerate() {
                                self.option_selection.insert(i, g.variants.iter().position(|v| v.is_default).unwrap_or(0));
                            }
                            self.options = groups;
                        }
                        Err(e) => self.message = format!("Ошибка загрузки тарифов: {e}"),
                    }
                }
                UiMessage::RadarSnapshotsLoaded(snapshots,history) => {
                    self.snapshots=Arc::new(snapshots);
                    self.history=history;
                    self.radar_snapshots_loading=false;
                    self.refresh_radar_derived();
                }
                UiMessage::RadarDerived(metrics,comparison) => {
                    self.radar_metrics=Arc::new(metrics);
                    self.radar_comparison=Some(comparison);
                    self.radar_derived_loading=false;
                    self.radar_filter_key.clear();
                }
                UiMessage::RadarVisible(key,visible) => {
                    if key==self.radar_filter_key{self.radar_visible=visible;self.radar_visible_loading=false;}
                }
                UiMessage::NicheStarted => self.record_task("Сбор радара","Запущено",String::new()),
                UiMessage::Niche(result) => {
                    self.reload_radar();
                    match &result { Ok(())=>self.record_task("Сбор радара","Завершено",format!("снимков: {}",self.snapshots.len())), Err(e)=>self.record_task("Сбор радара","Ошибка",e.clone()) };
                    self.message = match result {
                        Err(e) => format!("Ошибка радара: {e}"),
                        Ok(()) => "Снимок радара сохранён · подготовка таблиц выполняется в фоне".into(),
                    };
                }
            }
        }
        if self.queue_advance_pending { self.queue_advance_pending=false; if self.search_queue_running {self.run_next_queued_search();} }
        self.progress = read_niche_progress(&self.out_dir);
    }

    fn reload_radar(&mut self) {
        self.radar_snapshots_loading=true;
        self.radar_derived_loading=true;
        let out_dir=self.out_dir.clone();let tx=self.tx.clone();
        self.runtime.spawn_blocking(move||{
            let snapshots=load_recent_niche_snapshots(&out_dir).unwrap_or_default();
            let history=niche_history_for_response(&out_dir,&snapshots);
            let _=tx.send(UiMessage::RadarSnapshotsLoaded(snapshots,history));
        });
    }

    fn refresh_radar_derived(&mut self) {
        self.radar_derived_loading=true;
        self.radar_filter_key.clear();
        let snapshots=self.snapshots.clone();
        let tx=self.tx.clone();
        self.runtime.spawn_blocking(move||{
            let metrics=build_niche_metrics(snapshots.as_slice());
            let comparison=build_snapshot_comparison(snapshots.as_slice());
            let _=tx.send(UiMessage::RadarDerived(metrics,comparison));
        });
    }

    fn refresh_radar_visible(&mut self) {
        let key=format!("{}|{}|{}|{}|{}|{}|{}|{}|{}",self.niche_filter.to_lowercase(),self.min_delta,self.max_sellers,self.max_price.to_bits(),self.max_markup.to_bits(),radar_sort_name(self.radar_sort),self.favorites_only,self.favorites.len(),self.radar_metrics.len());
        if key==self.radar_filter_key{return;}
        self.radar_filter_key=key.clone();
        self.radar_visible_loading=true;
        let metrics=self.radar_metrics.clone();let snapshots=self.snapshots.clone();let tx=self.tx.clone();
        let filter=self.niche_filter.trim().to_lowercase();let min_delta=self.min_delta;let max_sellers=self.max_sellers;
        let max_price=self.max_price;let max_markup=self.max_markup;let favorites=self.favorites.clone();
        let favorites_only=self.favorites_only;let sort=self.radar_sort;
        self.runtime.spawn_blocking(move||{
            let Some(current)=snapshots.first() else{let _=tx.send(UiMessage::RadarVisible(key,Vec::new()));return;};
            let mut visible:Vec<usize>=metrics.iter().enumerate().filter_map(|(i,m)|{
                let product=current.products.get(m.product_index)?;
                let matches=m.delta_sales.unwrap_or(i64::MIN)>=min_delta&&m.sellers_count<=max_sellers
                    &&product.price_rub<=max_price&&m.markup_percent.map(|x|x<=max_markup).unwrap_or(true)
                    &&(!favorites_only||favorites.contains(&product.item_id))
                    &&(filter.is_empty()||product.title.to_lowercase().contains(&filter)||product.category.to_lowercase().contains(&filter));
                matches.then_some(i)
            }).collect();
            let compare=|&ia:&usize,&ib:&usize|{
                let a=&metrics[ia];let b=&metrics[ib];
                match sort{RadarSort::Opportunity=>b.opportunity_score.partial_cmp(&a.opportunity_score).unwrap_or(std::cmp::Ordering::Equal),RadarSort::Delta=>b.delta_sales.unwrap_or(i64::MIN).cmp(&a.delta_sales.unwrap_or(i64::MIN)),RadarSort::Price=>current.products[a.product_index].price_rub.partial_cmp(&current.products[b.product_index].price_rub).unwrap_or(std::cmp::Ordering::Equal),RadarSort::Sellers=>a.sellers_count.cmp(&b.sellers_count)}
            };
            if visible.len()>2000{visible.select_nth_unstable_by(1999,compare);visible.truncate(2000);}
            visible.sort_by(compare);
            let _=tx.send(UiMessage::RadarVisible(key,visible));
        });
    }

    fn radar_alert_prefs(&self) -> RadarAlertPrefs {
        RadarAlertPrefs{enabled:self.notify_growth,min_delta:self.min_delta,max_sellers:self.max_sellers,text_filter:self.alert_filter.clone(),tracked_sellers_only:self.tracked_sellers_only,seller_ids:self.watched_sellers.iter().map(|s|s.seller_id).collect()}
    }

    fn alert_target_matches(&self,title:&str,category:&str,seller_id:u64)->bool {
        let prefs=self.radar_alert_prefs();
        matches_alert_target(&prefs,title,category,seller_id)
    }

    fn persist_settings(&mut self, ctx: &egui::Context) {
        self.settings.out_dir = self.out_dir.clone();
        self.settings.page = self.page;
        self.settings.query = self.query.clone();
        self.settings.deals_filter = self.deals_filter.clone();
        self.settings.seller_url = self.seller_url.clone();
        self.settings.sellers_filter = self.sellers_filter.clone();
        self.settings.niche_filter = self.niche_filter.clone();
        self.settings.keyword = self.keyword.clone();
        self.settings.exclude = self.exclude.clone();
        self.settings.min_reviews = self.min_reviews;
        self.settings.max_negative = self.max_negative;
        self.settings.pages = self.pages;
        self.settings.inspect_limit = self.inspect_limit;
        self.settings.sort = self.sort;
        self.settings.seller_sort = self.seller_sort;
        self.settings.deal_column_widths=self.deal_column_widths;
        self.settings.seller_column_widths=self.seller_column_widths;
        self.settings.radar_column_widths=self.radar_column_widths;
        self.settings.max_categories = self.max_categories;
        self.settings.enrich_limit = self.enrich_limit;
        self.settings.min_delta = self.min_delta;
        self.settings.max_sellers = self.max_sellers;
        self.settings.max_price = self.max_price;
        self.settings.max_markup = self.max_markup;
        self.settings.radar_sort = self.radar_sort;
        self.settings.favorites_only = self.favorites_only;
        self.settings.notify_growth = self.notify_growth;
        self.settings.price_alerts = self.price_alerts;
        self.settings.sales_alerts = self.sales_alerts;
        self.settings.price_drop_threshold = self.price_drop_threshold;
        self.settings.sales_growth_threshold = self.sales_growth_threshold;
        self.settings.alert_filter = self.alert_filter.clone();
        self.settings.tracked_sellers_only = self.tracked_sellers_only;
        self.settings.request_delay_ms=self.request_delay_ms;
        self.settings.request_concurrency=self.request_concurrency;
        self.settings.retention_days=self.retention_days;
        self.settings.history_limit=self.history_limit;
        let alert_prefs=self.radar_alert_prefs();
        if let Ok(mut prefs) = self.radar_prefs_shared.write() { *prefs = alert_prefs; }
        if let Some(size) = ctx.input(|i| i.viewport().inner_rect.map(|r| [r.width(), r.height()])) {
            if size[0] >= 500.0 && size[1] >= 400.0 { self.settings.window_size = size; }
        }
        if self.settings != self.settings_saved {
            save_settings(&self.settings);
            self.settings_saved = self.settings.clone();
        }
    }

    fn choose_output_dir(&mut self) {
        if let Some(path) = rfd::FileDialog::new().set_directory(&self.out_dir).pick_folder() {
            self.out_dir = path;
            if let Ok(mut shared) = self.output_shared.write() { *shared = self.out_dir.clone(); }
            self.deals = read_json_or(self.out_dir.join("deals.json"), Vec::new());
            self.sellers = read_json_or(self.out_dir.join("plati_sellers.json"), Vec::new());
            self.favorites = read_json_or(self.out_dir.join("niche_favorites.json"), HashSet::new());
            self.saved_searches = read_json_or(self.out_dir.join("saved_searches.json"), Vec::new());
            self.watchlist = read_json_or(self.out_dir.join("watchlist.json"), Vec::new());
            self.watched_sellers = read_json_or(self.out_dir.join("watched_sellers.json"), Vec::new());
            self.activity_log = read_json_or(self.out_dir.join("activity_log.json"), Vec::new());
            self.reload_radar();
            self.progress = read_niche_progress(&self.out_dir);
            self.message = format!("Папка данных: {}", self.out_dir.display());
        }
    }

    fn open_output_dir(&mut self) {
        let result = open_folder(&self.out_dir);
        self.message = result.map(|_| format!("Открыта папка {}", self.out_dir.display())).unwrap_or_else(|e| format!("Не удалось открыть папку: {e}"));
    }

    fn prepare_export_values(&mut self,title:&str,filename:&str,rows:Vec<serde_json::Value>) {
        let Some(serde_json::Value::Object(first))=rows.first() else {self.message="Экспорт невозможен: таблица пуста".into();return;};
        let columns:Vec<String>=first.keys().cloned().collect();
        let selected=columns.iter().cloned().collect();
        self.export_draft=Some(ExportDraft{title:title.into(),filename:filename.into(),rows,columns,selected});
    }

    fn prepare_export<T:Serialize>(&mut self,title:&str,filename:&str,records:&[T]) {
        match records.iter().map(serde_json::to_value).collect::<std::result::Result<Vec<_>,_>>() {
            Ok(rows)=>self.prepare_export_values(title,filename,rows),
            Err(error)=>self.message=format!("Не удалось подготовить экспорт: {error}"),
        }
    }

    fn export_dialog(&mut self,ctx:&egui::Context) {
        let Some(mut draft)=self.export_draft.take() else{return};let mut open=true;let mut action=None;let mut move_up=None;let mut move_down=None;
        let screen=ctx.input(|i|i.viewport().inner_rect.map(|r|r.size()).unwrap_or(Vec2::new(1000.0,720.0)));
        let dialog_width=(screen.x*0.72).clamp(440.0,920.0).min((screen.x-24.0).max(400.0));
        let list_height=(screen.y*0.32).clamp(120.0,280.0);
        egui::Window::new(format!("Экспорт · {}",draft.title)).open(&mut open).resizable(true).default_width(dialog_width).show(ctx,|ui|{
            ui.label(format!("{} строк · выберите столбцы и порядок; предпросмотр показывает первые 5 строк",draft.rows.len()));
            ui.separator();
            ui.columns(2,|columns|{
                columns[0].label(RichText::new("Столбцы (↑ / ↓ меняют порядок)").strong());
                egui::ScrollArea::vertical().max_height(list_height).show(&mut columns[0],|ui|{
                    for (i,key) in draft.columns.clone().iter().enumerate(){
                        ui.horizontal(|ui|{let mut selected=draft.selected.contains(key);if ui.checkbox(&mut selected,key).changed(){if selected{draft.selected.insert(key.clone());}else{draft.selected.remove(key);}}
                            if ui.small_button("↑").clicked()&&i>0{move_up=Some(i);}
                            if ui.small_button("↓").clicked()&&i+1<draft.columns.len(){move_down=Some(i);}
                        });
                    }
                });
                columns[1].label(RichText::new("Предпросмотр").strong());
                let active:Vec<&String>=draft.columns.iter().filter(|c|draft.selected.contains(*c)).collect();
                egui::ScrollArea::both().max_height(list_height).show(&mut columns[1],|ui|{
                    egui::Grid::new("export_preview_grid").striped(true).show(ui,|ui|{for key in &active{ui.label(RichText::new(key.as_str()).strong());}ui.end_row();for row in draft.rows.iter().take(5){for key in &active{ui.label(export_cell(row.get(*key).unwrap_or(&serde_json::Value::Null)));}ui.end_row();}});
                });
            });
            if let Some(i)=move_up{draft.columns.swap(i,i-1);}if let Some(i)=move_down{draft.columns.swap(i,i+1);}
            ui.separator();
            ui.horizontal(|ui|{
                let enabled=!draft.selected.is_empty();
                if ui.add_enabled(enabled,egui::Button::new("Экспорт CSV…")).clicked(){action=Some(ExportFormat::Csv);}
                if ui.add_enabled(enabled,egui::Button::new("Экспорт XLSX…")).clicked(){action=Some(ExportFormat::Xlsx);}
                if ui.add_enabled(enabled,egui::Button::new("Экспорт JSON…")).clicked(){action=Some(ExportFormat::Json);}
                if !enabled{ui.colored_label(Color32::LIGHT_RED,"Выберите хотя бы один столбец");}
            });
        });
        if let Some(format)=action {
            let columns:Vec<String>=draft.columns.iter().filter(|c|draft.selected.contains(*c)).cloned().collect();
            let filename=format!("{}.{}",draft.filename,format.extension());
            let filter=match format{ExportFormat::Csv=>"CSV",ExportFormat::Xlsx=>"Excel",ExportFormat::Json=>"JSON"};
            if let Some(path)=rfd::FileDialog::new().set_file_name(&filename).add_filter(filter,&[format.extension()]).save_file(){
                self.message=write_custom_export(&path,&columns,&draft.rows,format).map(|_|format!("Экспортировано: {}",path.display())).unwrap_or_else(|e|format!("Ошибка экспорта: {e}"));
                return;
            }
        }
        if open {self.export_draft=Some(draft);}
    }

    fn sidebar(&mut self, ui: &mut egui::Ui) {
        let compact=ui.available_width()<190.0;
        ui.add_space(7.0);
        ui.horizontal(|ui| {
            ui.label(RichText::new("PR").size(if compact{22.0}else{25.0}).strong().color(Color32::from_rgb(56,189,248)));
            ui.vertical(|ui| { ui.label(RichText::new("PLATI RADAR").strong()); ui.label(RichText::new("АНАЛИТИКА МАРКЕТПЛЕЙСА").small().color(Color32::GRAY)); });
        });
        ui.add_space(25.0);
        ui.label(RichText::new("РАБОЧЕЕ ПРОСТРАНСТВО").small().color(Color32::GRAY));
        ui.add_space(8.0);
        let pages=if compact {[(Page::Deals,"⌕  Поиск"),(Page::Seller,"◉  Продавец"),(Page::Sellers,"▤  База продавцов"),(Page::Radar,"◌  Радар"),(Page::Watchlist,"★  Отслеживание"),(Page::Activity,"≡  Журнал / копии")]} else {[(Page::Deals,"⌕  Поиск предложений"),(Page::Seller,"◉  Профиль продавца"),(Page::Sellers,"▤  База продавцов"),(Page::Radar,"◌  Радар ниш"),(Page::Watchlist,"★  Отслеживание"),(Page::Activity,"≡  Журнал и резервные копии")]};
        for (page,label) in pages {if ui.selectable_label(self.page==page,label).clicked(){self.page=page;}}
        ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
            ui.separator();
            if ui.add_sized([ui.available_width(),26.0],egui::Button::new(if compact{"Открыть папку"}else{"Открыть папку данных"})).clicked(){self.open_output_dir();}
            if ui.add_sized([ui.available_width(),26.0],egui::Button::new(if compact{"Выбрать папку…"}else{"Выбрать папку данных…"})).clicked(){self.choose_output_dir();}
            ui.label(RichText::new(format!("Данные: {}", self.out_dir.display())).small().color(Color32::GRAY));
            ui.label(RichText::new("Нативный интерфейс · Rust").small().color(Color32::GRAY));
        });
    }

    fn header(&self, ui: &mut egui::Ui) {
        let (title, desc) = match self.page {
            Page::Deals => ("Поиск предложений", "Сравнивайте карточки по реальной цене тарифа"),
            Page::Seller => ("Профиль продавца", "Проверьте статистику и репутацию магазина"),
            Page::Sellers => ("База продавцов", "Исследуйте активных продавцов площадки"),
            Page::Radar => ("Радар ниш", "Спрос, конкуренция и динамика продаж"),
            Page::Watchlist => ("Отслеживание", "История цен и показателей выбранных карточек и продавцов"),
            Page::Activity => ("Журнал задач", "История запусков, ошибок и резервные копии данных"),
        };
        ui.horizontal(|ui| {
            ui.vertical(|ui| { ui.heading(title); ui.label(RichText::new(desc).color(Color32::GRAY)); });
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| ui.label(RichText::new("●  РАБОТАЕТ ЛОКАЛЬНО").small().color(Color32::from_rgb(74,222,128))));
        });
        ui.add_space(8.0); ui.separator(); ui.add_space(8.0);
    }

    fn deals_view(&mut self, ui: &mut egui::Ui) {
        let available_height=ui.available_height().min(1200.0);
        let results_height=(available_height-300.0).clamp(170.0,available_height.max(170.0));
        card(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.label(RichText::new("Быстрый поиск").strong());
                for preset in ["ChatGPT Plus","Claude Pro","Spotify Premium","Xbox Game Pass","YouTube Premium","Telegram Stars","Steam"] {
                    if ui.small_button(preset).clicked() {
                        self.query = preset.into();
                        self.keyword = if preset.contains("Plus") || preset.contains("Pro") { "plus" } else { "" }.into();
                    }
                }
            });
            ui.horizontal(|ui| {
                ui.label("Запрос");
                ui.add_sized([ui.available_width()-120.0,32.0], egui::TextEdit::singleline(&mut self.query).hint_text("Например, ChatGPT Plus"));
                if self.search_busy {
                    ui.spinner();
                    if ui.button("Отменить").clicked() { self.cancel_search(); }
                } else if ui.add_enabled(self.query.trim().chars().count() >= 3, egui::Button::new("Найти").fill(Color32::from_rgb(2,132,199))).clicked() { self.send_search(); }
            });
            ui.horizontal_wrapped(|ui| {
                ui.add(egui::TextEdit::singleline(&mut self.saved_search_name).desired_width((ui.available_width()*0.2).clamp(110.0,200.0)).hint_text("Название фильтра"));
                if ui.button("Сохранить фильтр").clicked(){self.save_search_preset();}
                let selected=self.saved_searches.get(self.selected_saved_search).map(|p|p.name.as_str()).unwrap_or("Сохранённые поиски");
                let mut chosen_search=None;
                egui::ComboBox::from_id_salt("saved_searches").selected_text(selected).show_ui(ui,|ui|{
                    for (i,p) in self.saved_searches.iter().enumerate(){if ui.selectable_label(i==self.selected_saved_search,&p.name).clicked(){chosen_search=Some(i);}}
                });
                if let Some(i)=chosen_search{self.apply_saved_search(i);}
                if ui.button("Удалить фильтр").clicked() && self.selected_saved_search<self.saved_searches.len(){self.saved_searches.remove(self.selected_saved_search);self.selected_saved_search=self.selected_saved_search.min(self.saved_searches.len().saturating_sub(1));let _=write_json_atomic(&self.out_dir.join("saved_searches.json"),&self.saved_searches);}
                if ui.button("В очередь").clicked(){self.enqueue_search(self.selected_saved_search);}
                if !self.search_queue_running {
                    if ui.button(format!("Запустить очередь ({})",self.search_queue.len())).clicked(){self.start_search_queue();}
                } else if ui.button(format!("Остановить очередь ({})",self.search_queue.len())).clicked(){self.stop_search_queue();}
            });
            ui.horizontal_wrapped(|ui|{
                ui.label("Пауза API, мс");ui.add(egui::DragValue::new(&mut self.request_delay_ms).range(50..=5000));
                ui.label("Параллельность");ui.add(egui::DragValue::new(&mut self.request_concurrency).range(1..=12));
                if !self.search_queue.is_empty() && ui.small_button("Очистить очередь").clicked(){self.search_queue.clear();self.search_queue_running=false;}
            });
            egui::CollapsingHeader::new("Фильтры и параметры").default_open(true).show(ui, |ui| {
                if ui.available_width()<900.0 {
                    ui.columns(2,|columns|{
                        columns[0].vertical(|ui|{ui.label("Страниц (1–5)");ui.add(egui::DragValue::new(&mut self.pages).range(1..=5));});
                        columns[1].vertical(|ui|{ui.label("Мин. отзывов");ui.add(egui::DragValue::new(&mut self.min_reviews).range(0..=1_000_000));});
                    });
                    ui.columns(2,|columns|{
                        columns[0].vertical(|ui|{ui.label("Макс. негатив, %");ui.add(egui::DragValue::new(&mut self.max_negative).range(0.0..=100.0));});
                        columns[1].vertical(|ui|{ui.label("Опции карточек");ui.add(egui::DragValue::new(&mut self.inspect_limit).range(0..=200));});
                    });
                    ui.columns(2,|columns|{
                        columns[0].vertical(|ui|{ui.label("Ключ тарифа");ui.add_sized([ui.available_width(),28.0],egui::TextEdit::singleline(&mut self.keyword));});
                        columns[1].vertical(|ui|{ui.label("Минус-слова");ui.add_sized([ui.available_width(),28.0],egui::TextEdit::singleline(&mut self.exclude));});
                    });
                    ui.vertical(|ui|{ui.label("Сортировка");egui::ComboBox::from_id_salt("sort").selected_text(sort_name(self.sort)).width(ui.available_width()).show_ui(ui,|ui|{for m in [SortMode::RealPrice,SortMode::StorePrice,SortMode::Reviews,SortMode::Smart]{ui.selectable_value(&mut self.sort,m,sort_name(m));}});});
                } else {
                    ui.columns(4,|columns|{
                        columns[0].vertical(|ui|{ui.label("Страниц (1–5)");ui.add(egui::DragValue::new(&mut self.pages).range(1..=5));});
                        columns[1].vertical(|ui|{ui.label("Мин. отзывов");ui.add(egui::DragValue::new(&mut self.min_reviews).range(0..=1_000_000));});
                        columns[2].vertical(|ui|{ui.label("Макс. негатив, %");ui.add(egui::DragValue::new(&mut self.max_negative).range(0.0..=100.0));});
                        columns[3].vertical(|ui|{ui.label("Опции карточек");ui.add(egui::DragValue::new(&mut self.inspect_limit).range(0..=200));});
                    });
                    ui.add_space(6.0);
                    ui.columns(4,|columns|{
                        columns[0].vertical(|ui|{ui.label("Ключ тарифа");ui.add_sized([ui.available_width(),28.0],egui::TextEdit::singleline(&mut self.keyword));});
                        columns[1].vertical(|ui|{ui.label("Минус-слова (через запятую)");ui.add_sized([ui.available_width(),28.0],egui::TextEdit::singleline(&mut self.exclude));});
                        columns[2].vertical(|ui|{ui.label("Сортировка");egui::ComboBox::from_id_salt("sort").selected_text(sort_name(self.sort)).width(ui.available_width()).show_ui(ui,|ui|{for m in [SortMode::RealPrice,SortMode::StorePrice,SortMode::Reviews,SortMode::Smart]{ui.selectable_value(&mut self.sort,m,sort_name(m));}});});
                    });
                }
            });
        });
        self.message(ui);
        ui.horizontal_wrapped(|ui| {
            ui.heading("Результаты");
            ui.label(RichText::new(format!("{} карточек",self.deals.len())).color(Color32::GRAY));
            ui.label(RichText::new("↔ меняйте ширину, перетаскивая разделители заголовков").small().color(Color32::GRAY));
            ui.add(egui::TextEdit::singleline(&mut self.deals_filter).desired_width((ui.available_width()*0.28).clamp(130.0,260.0)).hint_text("Фильтр названия / продавца"));
            if ui.small_button("Сбросить ширину столбцов").clicked(){self.deal_column_widths=default_deal_column_widths();}
            if ui.button("Экспорт / настроить…").clicked(){let f=self.deals_filter.to_lowercase();let rows:Vec<ProductDeal>=self.deals.iter().filter(|d|f.is_empty()||d.title.to_lowercase().contains(&f)||d.seller_name.to_lowercase().contains(&f)).cloned().collect();self.prepare_export("Предложения","deals",&rows);}
        });
        card(ui,|ui|{
            let table_width=ui.available_width();
            // Keep a practical minimum width so columns remain usable in small windows;
            // the shared horizontal scroll then moves header and rows together.
            let column_budget=(table_width-5.0*8.0).max(1100.0);
            egui::ScrollArea::both().auto_shrink([false,false]).min_scrolled_height(results_height).max_height(results_height).show(ui,|ui|{
                egui::Grid::new("deal_rows").striped(true).spacing([8.0,6.0]).min_col_width(1.0).show(ui,|ui|{
                    resizable_header_cell(ui,0,&mut self.deal_column_widths,column_budget,|ui,width|{ui.add_sized([width,30.0],egui::Label::new(RichText::new("Карточка").strong().color(Color32::from_rgb(125,211,252))));});
                    resizable_header_cell(ui,1,&mut self.deal_column_widths,column_budget,|ui,width|{ui.add_sized([width,30.0],egui::Label::new(RichText::new("Тариф").strong().color(Color32::from_rgb(125,211,252))));});
                    for (index,(head,mode)) in [("Цена",SortMode::RealPrice),("Отзывы",SortMode::Reviews)].into_iter().enumerate(){
                        let column=index+2;
                        resizable_header_cell(ui,column,&mut self.deal_column_widths,column_budget,|ui,width|{
                            let color=if self.sort==mode{Color32::from_rgb(125,211,252)}else{Color32::WHITE};
                            if ui.add_sized([width,30.0],egui::Button::new(RichText::new(head).strong().color(color))).clicked(){self.sort=mode;}
                        });
                    }
                    resizable_header_cell(ui,4,&mut self.deal_column_widths,column_budget,|ui,width|{ui.add_sized([width,30.0],egui::Label::new(RichText::new("Продавец").strong().color(Color32::from_rgb(125,211,252))));});
                    resizable_header_cell(ui,5,&mut self.deal_column_widths,column_budget,|ui,width|{ui.add_sized([width,30.0],egui::Label::new(RichText::new("Действия").strong().color(Color32::from_rgb(125,211,252))));});
                    ui.end_row();
                    let widths=self.deal_column_widths.map(|ratio|ratio*column_budget);
                    let f=self.deals_filter.to_lowercase();
                    let mut rows:Vec<ProductDeal>=self.deals.iter().filter(|d|f.is_empty()||d.title.to_lowercase().contains(&f)||d.seller_name.to_lowercase().contains(&f)).cloned().collect();
                    rows.sort_by(|a,b|sort_deals(a,b,self.sort));
                    for d in rows {
                        fixed_table_cell(ui,widths[0],94.0,|cell|{cell.add(egui::Label::new(RichText::new(&d.title).strong()).wrap());cell.hyperlink_to(format!("#{} · открыть ↗",d.item_id),&d.item_url);});
                        fixed_table_cell(ui,widths[1],94.0,|cell|{cell.add(egui::Label::new(RichText::new(&d.matched_tariff).small()).wrap());});
                        fixed_table_cell(ui,widths[2],94.0,|cell|{cell.label(RichText::new(format!("{:.0} ₽",d.real_tariff_price_rub)).strong().color(Color32::from_rgb(134,239,172)));if d.price_bait_diff_rub>0.0{cell.add(egui::Label::new(RichText::new(format!("витрина {:.0} ₽",d.storefront_price_rub)).small().color(Color32::GRAY)).wrap());}});
                        fixed_table_cell(ui,widths[3],94.0,|cell|{cell.label(format!("+{} / -{}",d.positive_reviews,d.negative_reviews));cell.add(egui::Label::new(RichText::new(format!("негатив {:.1}% · возвраты {}",d.neg_percent,d.returns_count)).small().color(Color32::GRAY)).wrap());});
                        fixed_table_cell(ui,widths[4],94.0,|cell|{cell.add(egui::Label::new(&d.seller_name).wrap());cell.add(egui::Label::new(RichText::new(format!("рейтинг {:.1} · продаж {}",d.seller_rating,d.sold_count)).small().color(Color32::GRAY)).wrap());cell.hyperlink_to("Витрина ↗",&d.seller_url);});
                        fixed_table_cell(ui,widths[5],94.0,|cell|{
                            if cell.add_sized([cell.available_width(),25.0],egui::Button::new("Опции и тарифы")).clicked(){self.load_options(&d);}
                            if cell.add_sized([cell.available_width(),25.0],egui::Button::new(if self.watchlist.iter().any(|w|w.item_id==d.item_id){"★ Отслеживается"}else{"☆ Следить"})).clicked(){self.watch_product(&d);}
                            if cell.add_sized([cell.available_width(),25.0],egui::Button::new("Анализ продавца")).clicked(){self.seller_url=d.seller_url.clone();self.seller=None;self.page=Page::Seller;}
                        });
                        ui.end_row();
                    }
                    if self.deals.is_empty(){ui.add(egui::Label::new(RichText::new("Запустите поиск, чтобы увидеть предложения.").color(Color32::GRAY)).wrap());ui.end_row();}
                });
            });
        });
        if self.options_open { self.options_dialog(ui.ctx()); }
    }

    fn options_dialog(&mut self, ctx:&egui::Context) {
        let mut open=self.options_open;
        let screen_width=ctx.input(|i|i.viewport().inner_rect.map(|r|r.width()).unwrap_or(1000.0));
        let dialog_width=(screen_width*0.56).clamp(400.0,720.0).min((screen_width-24.0).max(360.0));
        egui::Window::new("Варианты и тарифы").open(&mut open).resizable(true).default_width(dialog_width).show(ctx,|ui|{
            ui.label(RichText::new(&self.options_title).strong());
            ui.hyperlink_to(format!("Карточка {} ↗",self.options_id),format!("https://plati.market/itm/{}",self.options_id));
            if self.options_busy {ui.spinner();ui.label("Загружаем настройки Digiseller…");}
            else if self.options.is_empty(){ui.label("Доступных тарифных опций нет.");}
            else {
                for (i,g) in self.options.iter().enumerate(){
                    let chosen=*self.option_selection.get(&i).unwrap_or(&0);
                    let text=g.variants.get(chosen).map(|v|format!("{} ({:+.0} ₽)",v.text,v.modifier_rub)).unwrap_or_else(||"Выберите".into());
                    egui::ComboBox::from_id_salt(("option",i)).selected_text(format!("{}: {}",g.label,text)).show_ui(ui,|ui|{
                        for (j,v) in g.variants.iter().enumerate(){ui.selectable_value(self.option_selection.entry(i).or_default(),j,format!("{} ({:+.0} ₽){}",v.text,v.modifier_rub,if v.is_default{" · default"}else{""}));}
                    });
                }
                let mods:f64=self.options.iter().enumerate().filter_map(|(i,g)|g.variants.get(*self.option_selection.get(&i).unwrap_or(&0))).map(|v|v.modifier_rub).sum();
                ui.separator();ui.label(RichText::new(format!("Расчётная цена: {:.0} ₽",self.options_base+mods)).size(18.0).strong().color(Color32::from_rgb(134,239,172)));
            }
        }); self.options_open=open;
    }

    fn seller_view(&mut self,ui:&mut egui::Ui){
        card(ui,|ui|{
            ui.label("Ссылка на витрину или ID продавца");
            ui.horizontal(|ui|{
                ui.add_sized([ui.available_width()-140.0,32.0],egui::TextEdit::singleline(&mut self.seller_url).hint_text("https://plati.market/seller/..."));
                if self.seller_busy { ui.spinner(); if ui.button("Отменить").clicked(){self.cancel_seller();} }
                else if ui.add_enabled(!self.seller_url.trim().is_empty(),egui::Button::new("Проверить").fill(Color32::from_rgb(2,132,199))).clicked(){self.send_seller_lookup();}
            });
        }); self.message(ui);
        if let Some(s)=self.seller.clone().as_ref(){
            card(ui,|ui|{
                ui.horizontal(|ui|{ui.vertical(|ui|{ui.heading(&s.seller_name);ui.label(format!("Продавец #{} · {}",s.seller_id,s.level.as_deref().unwrap_or("уровень не указан")));ui.hyperlink_to("Открыть витрину ↗",&s.profile_url);});ui.with_layout(egui::Layout::right_to_left(egui::Align::Center),|ui|ui.label(RichText::new(format!("★ {:.1}",s.rating.unwrap_or(0.0))).size(24.0).color(Color32::from_rgb(251,191,36))));});
                ui.separator();
                for (name,val) in [("Продажи",opt(s.sales_count)),("Отзывы",opt(s.reviews_count)),("Возвраты",opt(s.returns_count)),("Товаров",opt(s.items_count))]{ui.horizontal(|ui|{ui.label(RichText::new(val).strong().size(20.0).color(Color32::from_rgb(125,211,252)));ui.label(name);});}
                if ui.button(if self.watched_sellers.iter().any(|w|w.seller_id==s.seller_id){"★ Продавец отслеживается"}else{"☆ Следить за продавцом"}).clicked(){self.watch_seller(s);}
                if let Some(t)=&s.tenure{ui.label(format!("На площадке: {t}"));}
                ui.label(RichText::new(if s.enriched_from_html{"Профиль обогащён HTML-данными"}else{"Доступна статистика Public API"}).small().color(Color32::GRAY));
            });
        }else{empty(ui,"Проверьте продавца","Статистика продаж, отзывы, возвраты, рейтинг и товары появятся здесь.");}
    }

    fn discover_view(&mut self,ui:&mut egui::Ui){
        card(ui,|ui|{
            ui.heading("Сбор продавцов по категориям");ui.label("Категории cats.xml и обогащение профилей HTML-данными.");
            if ui.available_width()<700.0 {
                ui.columns(2,|columns|{
                    columns[0].vertical(|ui|{ui.label("Макс. категорий");ui.add(egui::DragValue::new(&mut self.max_categories).range(1..=389));});
                    columns[1].vertical(|ui|{ui.label("Профилей обогатить");ui.add(egui::DragValue::new(&mut self.enrich_limit).range(0..=100));if self.discover_busy{if ui.add_sized([ui.available_width(),28.0],egui::Button::new("Отменить сбор")).clicked(){self.cancel_discover();}}else if ui.add_sized([ui.available_width(),28.0],egui::Button::new("Начать сбор").fill(Color32::from_rgb(2,132,199))).clicked(){self.send_discover();}});
                });
            } else {
                ui.columns(3,|columns|{
                    columns[0].vertical(|ui|{ui.label("Макс. категорий");ui.add(egui::DragValue::new(&mut self.max_categories).range(1..=389));});
                    columns[1].vertical(|ui|{ui.label("Профилей обогатить");ui.add(egui::DragValue::new(&mut self.enrich_limit).range(0..=100));});
                    columns[2].vertical_centered(|ui|{ui.add_space(18.0);if self.discover_busy{if ui.add_sized([ui.available_width(),30.0],egui::Button::new("Отменить сбор")).clicked(){self.cancel_discover();}}else if ui.add_sized([ui.available_width(),30.0],egui::Button::new("Начать сбор").fill(Color32::from_rgb(2,132,199))).clicked(){self.send_discover();}});
                });
            }
            if self.discover_busy { if self.discover_progress.1>0 { let f=self.discover_progress.0 as f32/self.discover_progress.1 as f32;ui.add(egui::ProgressBar::new(f).show_percentage().text(format!("{} / {} категорий",self.discover_progress.0,self.discover_progress.1))); } ui.label(&self.discover_progress.2); }
        }); self.message(ui);
        ui.horizontal_wrapped(|ui|{ui.heading("Продавцы");ui.label(RichText::new(format!("{} записей",self.sellers.len())).color(Color32::GRAY));ui.label(RichText::new("↔ тяните разделители заголовков").small().color(Color32::GRAY));ui.add(egui::TextEdit::singleline(&mut self.sellers_filter).desired_width((ui.available_width()*0.24).clamp(130.0,240.0)).hint_text("Фильтр"));if ui.small_button("Сбросить ширину столбцов").clicked(){self.seller_column_widths=default_seller_column_widths();}if ui.button("Экспорт / настроить…").clicked(){let f=self.sellers_filter.to_lowercase();let rows:Vec<SellerRecord>=self.sellers.iter().filter(|s|f.is_empty()||s.seller_name.to_lowercase().contains(&f)||s.seller_id.to_string().contains(&f)).cloned().collect();self.prepare_export("Продавцы","sellers",&rows);}});
        card(ui,|ui|{
            let table_width=ui.available_width();
            let column_budget=(table_width-5.0*8.0).max(860.0);
            let seller_list_height=(ui.available_height()-150.0).clamp(180.0,720.0);
            egui::ScrollArea::both().auto_shrink([false,false]).max_height(seller_list_height).show(ui,|ui|{
                egui::Grid::new("seller_list").striped(true).spacing([8.0,6.0]).min_col_width(1.0).show(ui,|ui|{
                    resizable_header_cell(ui,0,&mut self.seller_column_widths,column_budget,|ui,width|{ui.add_sized([width,30.0],egui::Label::new(RichText::new("Продавец").strong().color(Color32::from_rgb(125,211,252))));});
                    for (index,(title,sort)) in [("Рейтинг",SellerSort::Rating),("Продажи",SellerSort::Sales),("Отзывы",SellerSort::Reviews)].into_iter().enumerate(){
                        let column=index+1;
                        resizable_header_cell(ui,column,&mut self.seller_column_widths,column_budget,|ui,width|{
                            let color=if self.seller_sort==sort{Color32::from_rgb(125,211,252)}else{Color32::WHITE};
                            if ui.add_sized([width,30.0],egui::Button::new(RichText::new(title).strong().color(color))).clicked(){self.seller_sort=sort;}
                        });
                    }
                    resizable_header_cell(ui,4,&mut self.seller_column_widths,column_budget,|ui,width|{ui.add_sized([width,30.0],egui::Label::new(RichText::new("Возвраты").strong().color(Color32::from_rgb(125,211,252))));});
                    resizable_header_cell(ui,5,&mut self.seller_column_widths,column_budget,|ui,width|{ui.add_sized([width,30.0],egui::Label::new(RichText::new("Профиль").strong().color(Color32::from_rgb(125,211,252))));});
                    ui.end_row();
                    let widths=self.seller_column_widths.map(|ratio|ratio*column_budget);
                    let f=self.sellers_filter.to_lowercase();
                    let mut records:Vec<SellerRecord>=self.sellers.iter().filter(|s|f.is_empty()||s.seller_name.to_lowercase().contains(&f)||s.seller_id.to_string().contains(&f)).cloned().collect();
                    records.sort_by(|a,b|match self.seller_sort{SellerSort::Rating=>b.rating.unwrap_or(0.0).partial_cmp(&a.rating.unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal),SellerSort::Sales=>b.sales_count.cmp(&a.sales_count),SellerSort::Reviews=>b.reviews_count.cmp(&a.reviews_count)});
                    for seller in &records{
                        fixed_table_cell(ui,widths[0],52.0,|cell|{cell.add(egui::Label::new(RichText::new(&seller.seller_name).strong()).wrap());});
                        fixed_table_cell(ui,widths[1],52.0,|cell|{cell.label(format!("{:.1}",seller.rating.unwrap_or(0.0)));});
                        fixed_table_cell(ui,widths[2],52.0,|cell|{cell.label(opt(seller.sales_count));});
                        fixed_table_cell(ui,widths[3],52.0,|cell|{cell.label(opt(seller.reviews_count));});
                        fixed_table_cell(ui,widths[4],52.0,|cell|{cell.label(opt(seller.returns_count));});
                        fixed_table_cell(ui,widths[5],52.0,|cell|{cell.hyperlink_to("Открыть ↗",&seller.profile_url);});
                        ui.end_row();
                    }
                    if records.is_empty(){ui.add(egui::Label::new(RichText::new("Продавцы не найдены.").color(Color32::GRAY)).wrap());ui.end_row();}
                });
            });
        });
    }

    fn radar_view(&mut self,ui:&mut egui::Ui){
        card(ui,|ui|{
            ui.horizontal(|ui|{
                ui.vertical(|ui|{ui.heading("Ниши и продажи");let s=self.snapshots.first();ui.label(format!("Снимок: {} · {} категорий · {} товаров",s.map(|x|date(x.captured_at)).unwrap_or_else(||"нет".into()),s.map(|x|x.categories_scanned).unwrap_or(0),s.map(|x|x.products.len()).unwrap_or(0)));});
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center),|ui|{
                    if self.niche_running.load(Ordering::Acquire) {if ui.button("Отменить сбор").clicked(){self.cancel_capture();}}
                    else if ui.add(egui::Button::new("Обновить снимок").fill(Color32::from_rgb(2,132,199))).clicked(){start_capture(self.client.clone(),self.out_dir.clone(),self.tx.clone(),self.niche_running.clone(),self.runtime.handle().clone(),self.capture_task.clone(),self.radar_alert_prefs());}
                });
            });
            if self.radar_snapshots_loading{ui.horizontal(|ui|{ui.spinner();ui.label("Загрузка снимков радара в фоне…");});}
            if self.niche_running.load(Ordering::Acquire)||matches!(self.progress.status.as_str(),"starting"|"collecting"){
                let f=if self.progress.total_categories==0{0.0}else{self.progress.completed_categories as f32/self.progress.total_categories as f32};
                ui.add(egui::ProgressBar::new(f).show_percentage().text(format!("{} / {} категорий · {} страниц · {} товаров",self.progress.completed_categories,self.progress.total_categories,self.progress.completed_pages,self.progress.products_cached)));
                if !self.progress.current_category.is_empty(){ui.label(format!("Сейчас: {}",self.progress.current_category));}
            }
            if let Some(e)=&self.progress.error{ui.colored_label(Color32::LIGHT_RED,e);}
        });
        self.message(ui);
        let viewport_height=ui.ctx().input(|i|i.viewport().inner_rect.map(|r|r.height()).unwrap_or(800.0));
        let chart_height=(viewport_height*0.12).clamp(58.0,120.0);
        card(ui,|ui|{ui.label(RichText::new("История изменения продаж").strong());history_plot(ui,&self.history,chart_height);});
        card(ui,|ui|{ui.label(RichText::new("Что изменилось с прошлого снимка").strong());self.snapshot_comparison(ui);});
        ui.horizontal_wrapped(|ui|{
            ui.heading("Возможности");ui.label(RichText::new("↔ ширина столбцов меняется перетаскиванием разделителей").small().color(Color32::GRAY));ui.add(egui::TextEdit::singleline(&mut self.niche_filter).desired_width((ui.available_width()*0.18).clamp(120.0,240.0)).hint_text("Товар / категория"));
            ui.label("Мин. Δ");ui.add(egui::DragValue::new(&mut self.min_delta).range(-1_000_000..=1_000_000));ui.label("Макс. продавцов");ui.add(egui::DragValue::new(&mut self.max_sellers).range(1..=100_000));
            ui.label("Макс. цена");ui.add(egui::DragValue::new(&mut self.max_price).range(0.0..=100_000_000.0));ui.label("Макс. наценка %");ui.add(egui::DragValue::new(&mut self.max_markup).range(-100.0..=100_000.0));
            egui::ComboBox::from_id_salt("radar_sort").selected_text(radar_sort_name(self.radar_sort)).show_ui(ui,|ui|{for s in [RadarSort::Opportunity,RadarSort::Delta,RadarSort::Price,RadarSort::Sellers]{ui.selectable_value(&mut self.radar_sort,s,radar_sort_name(s));}});
            ui.checkbox(&mut self.favorites_only,"Избранное");ui.checkbox(&mut self.notify_growth,"Оповещать о совпадениях");
            if ui.small_button("Сбросить ширину столбцов").clicked(){self.radar_column_widths=default_radar_column_widths();}
            if ui.button("Экспорт / настроить…").clicked(){let rows=self.radar_metrics.iter().filter_map(|m|self.snapshots.first()?.products.get(m.product_index).map(|p|NicheTrend{product:p.clone(),previous_sold_count:m.previous_sold_count,delta_sales:m.delta_sales,listings_count:m.listings_count,sellers_count:m.sellers_count,comparable_median_price_rub:m.comparable_median_price_rub,markup_percent:m.markup_percent,opportunity_score:m.opportunity_score})).collect::<Vec<_>>();self.prepare_export("Радар ниш","niche_radar",&rows);}
        });
        self.refresh_radar_visible();
        card(ui,|ui|{
            ui.horizontal(|ui|{ui.label(RichText::new(format!("Возможности · {}{}",self.radar_visible.len(),if self.radar_visible.len()==2000{"+"}else{""})).strong());if self.radar_derived_loading||self.radar_visible_loading{ui.spinner();ui.label("Подготовка данных в фоне…");}else if self.radar_metrics.is_empty(){ui.label("Нет данных радара");}});
            if self.radar_derived_loading||self.radar_visible_loading {return;}
            let Some(current)=self.snapshots.first() else{return;};
            let table_width=ui.available_width();
            let column_budget=(table_width-6.0*9.0).max(1100.0);
            let labels=["★","Карточка / категория","Δ продаж","Цена","Продавцы","Наценка","Балл"];
            let mut toggle_favorite=None;
            let row_height=68.0;
            let table_height=(ui.available_height()-36.0).clamp(120.0,900.0);
            // One horizontal viewport wraps both header and body; vertical scrolling is
            // confined to the rows so column edges stay aligned while scrolling sideways.
            egui::ScrollArea::horizontal().auto_shrink([false,false]).show(ui,|ui|{
                ui.horizontal(|ui|{
                    ui.spacing_mut().item_spacing.x=9.0;
                    for (index,label) in labels.into_iter().enumerate(){
                        resizable_header_cell(ui,index,&mut self.radar_column_widths,column_budget,|ui,width|{ui.add_sized([width,30.0],egui::Label::new(RichText::new(label).strong().color(Color32::from_rgb(125,211,252))));});
                    }
                });
                let widths=self.radar_column_widths.map(|ratio|ratio*column_budget);
                egui::ScrollArea::vertical().auto_shrink([false,false]).max_height(table_height).show_rows(ui,row_height,self.radar_visible.len(),|ui,range|{
                    for row in range {
                        let metric_index=self.radar_visible[row];let m=self.radar_metrics[metric_index];
                        let Some(product)=current.products.get(m.product_index) else{continue;};
                        let yes=self.favorites.contains(&product.item_id);
                        ui.allocate_ui_with_layout(Vec2::new(widths.iter().sum::<f32>()+6.0*9.0,row_height),egui::Layout::left_to_right(egui::Align::Center),|row_ui|{
                            let row_color=if row%2==0{Color32::from_rgb(17,26,45)}else{Color32::from_rgb(20,31,52)};
                            row_ui.painter().rect_filled(row_ui.max_rect(),4.0,row_color);
                            row_ui.spacing_mut().item_spacing.x=9.0;
                            fixed_table_cell(row_ui,widths[0],row_height,|cell|{cell.centered_and_justified(|cell|{if cell.add(egui::Button::new(if yes{"★"}else{"☆"})).clicked(){toggle_favorite=Some(product.item_id);}});});
                            fixed_table_cell(row_ui,widths[1],row_height,|cell|{cell.hyperlink_to(&product.title,&product.item_url).on_hover_text(&product.title);cell.add(egui::Label::new(RichText::new(&product.category).small().color(Color32::GRAY)).wrap());});
                            fixed_table_cell(row_ui,widths[2],row_height,|cell|{cell.label(m.delta_sales.map(|d|format!("{d:+}")).unwrap_or_else(||"—".into()));});
                            fixed_table_cell(row_ui,widths[3],row_height,|cell|{cell.label(format!("{:.0} ₽",product.price_rub));});
                            fixed_table_cell(row_ui,widths[4],row_height,|cell|{cell.label(format!("{} / {}",m.sellers_count,m.listings_count));});
                            fixed_table_cell(row_ui,widths[5],row_height,|cell|{cell.label(m.markup_percent.map(|x|format!("{x:+.1}%")).unwrap_or_else(||"—".into()));});
                            fixed_table_cell(row_ui,widths[6],row_height,|cell|{cell.label(format!("{:.1}",m.opportunity_score));});
                        });
                    }
                });
            });
            if let Some(id)=toggle_favorite{if self.favorites.remove(&id)==false{self.favorites.insert(id);}let _=write_json_atomic(&self.out_dir.join("niche_favorites.json"),&self.favorites);self.radar_filter_key.clear();}
            if self.snapshots.len()<2{ui.label(RichText::new("Для сравнения нужны два снимка; первый запускается автоматически.").small().color(Color32::GRAY));}
        });
    }

    fn watchlist_view(&mut self,ui:&mut egui::Ui) {
        card(ui,|ui|{
            ui.heading("Оповещения при повторных проверках");
            ui.horizontal_wrapped(|ui|{
                ui.checkbox(&mut self.price_alerts,"Снижение цены");ui.label("От, ₽");ui.add(egui::DragValue::new(&mut self.price_drop_threshold).range(0.0..=1_000_000.0));
                ui.checkbox(&mut self.sales_alerts,"Рост продаж");ui.label("От, шт.");ui.add(egui::DragValue::new(&mut self.sales_growth_threshold).range(1..=1_000_000));
            });
            ui.horizontal_wrapped(|ui|{ui.label("Текст / категории (через запятую)");ui.add(egui::TextEdit::singleline(&mut self.alert_filter).desired_width((ui.available_width()*0.45).clamp(140.0,380.0)).hint_text("ключевые слова или названия категорий"));ui.checkbox(&mut self.tracked_sellers_only,"Только выбранные продавцы");});
            ui.label(RichText::new("Карточки обновляются при следующих поисках, продавцы — при повторной проверке их профиля. Данные и история хранятся локально.").small().color(Color32::GRAY));
        });
        ui.horizontal_wrapped(|ui|{ui.heading(format!("Карточки · {}",self.watchlist.len()));ui.add(egui::TextEdit::singleline(&mut self.watch_filter).desired_width((ui.available_width()*0.32).clamp(140.0,300.0)).hint_text("Фильтр по метке, заметке, товару"));});
        let filter=self.watch_filter.trim().to_lowercase();let mut remove_products=Vec::new();let mut edit_request=None;
        egui::ScrollArea::vertical().max_height((ui.available_height()*0.48).clamp(150.0,520.0)).show(ui,|ui|{
            for w in &self.watchlist {
                let searchable=format!("{} {} {} {}",w.title,w.seller_name,w.note,w.tags.join(" ")).to_lowercase();
                if !filter.is_empty()&&!searchable.contains(&filter){continue;}
                let latest=w.observations.last();let previous=w.observations.iter().rev().nth(1);
                card(ui,|ui|{
                    ui.horizontal(|ui|{ui.vertical(|ui|{ui.hyperlink_to(&w.title,&w.url);ui.label(RichText::new(format!("{} · ID {} · {} наблюдений",w.seller_name,w.item_id,w.observations.len())).small().color(Color32::GRAY));if !w.tags.is_empty(){ui.label(RichText::new(w.tags.join(" · ")).small().color(Color32::from_rgb(125,211,252)));}if !w.note.is_empty(){ui.label(RichText::new(&w.note).small().color(Color32::GRAY));}});ui.with_layout(egui::Layout::right_to_left(egui::Align::Center),|ui|{if ui.small_button("Убрать").clicked(){remove_products.push(w.item_id);}if ui.small_button("Заметка / метки").clicked(){edit_request=Some(NoteEditor{item_id:Some(w.item_id),seller_id:None,note:w.note.clone(),tags:w.tags.join(", ")});}});});
                    if let Some(now)=latest {let delta=previous.map(|p|now.price-p.price).unwrap_or(0.0);let sales=previous.map(|p|now.sales as i64-p.sales as i64).unwrap_or(0);ui.label(format!("Цена {:.0} ₽ ({:+.0} ₽) · Продажи {} ({:+}) · отзывы {} · проверено {}",now.price,delta,now.sales,sales,now.reviews,date(now.at)));}
                });
            }
        });
        if !remove_products.is_empty(){self.watchlist.retain(|w|!remove_products.contains(&w.item_id));self.persist_watchlist();}
        ui.heading(format!("Продавцы · {}",self.watched_sellers.len()));
        let mut remove_sellers=Vec::new();
        egui::ScrollArea::vertical().max_height((ui.available_height()*0.42).clamp(140.0,480.0)).show(ui,|ui|{
            for w in &self.watched_sellers {
                let searchable=format!("{} {} {} {}",w.name,w.note,w.tags.join(" "),w.seller_id).to_lowercase();if !filter.is_empty()&&!searchable.contains(&filter){continue;}
                let latest=w.observations.last();let previous=w.observations.iter().rev().nth(1);
                card(ui,|ui|{ui.horizontal(|ui|{ui.vertical(|ui|{ui.hyperlink_to(format!("{} · #{}",w.name,w.seller_id),&w.url);ui.label(RichText::new(format!("{} наблюдений",w.observations.len())).small().color(Color32::GRAY));if !w.tags.is_empty(){ui.label(RichText::new(w.tags.join(" · ")).small().color(Color32::from_rgb(125,211,252)));}if !w.note.is_empty(){ui.label(RichText::new(&w.note).small().color(Color32::GRAY));}});ui.with_layout(egui::Layout::right_to_left(egui::Align::Center),|ui|{if ui.small_button("Убрать").clicked(){remove_sellers.push(w.seller_id);}if ui.small_button("Заметка / метки").clicked(){edit_request=Some(NoteEditor{item_id:None,seller_id:Some(w.seller_id),note:w.note.clone(),tags:w.tags.join(", ")});}});});if let Some(n)=latest{let sales_delta=match(n.sales,previous.and_then(|p|p.sales)){(Some(a),Some(b))=>format!("{:+}",a as i64-b as i64),_=>"—".into()};ui.label(format!("Рейтинг {:.1} · Продажи {} ({}) · Отзывы {} · Возвраты {} · {}",n.rating.unwrap_or(0.0),opt(n.sales),sales_delta,opt(n.reviews),opt(n.returns),date(n.at)));}});
            }
        });
        if !remove_sellers.is_empty(){self.watched_sellers.retain(|w|!remove_sellers.contains(&w.seller_id));self.persist_watchlist();}
        if let Some(editor)=edit_request{self.note_editor=Some(editor);}
    }

    fn note_dialog(&mut self,ctx:&egui::Context) {
        let Some(mut editor)=self.note_editor.take() else{return};let mut open=true;let mut save=false;
        egui::Window::new("Заметка и метки").open(&mut open).show(ctx,|ui|{
            ui.label("Заметка");ui.add(egui::TextEdit::multiline(&mut editor.note).desired_rows(4).desired_width(ui.available_width()));
            ui.label("Метки через запятую");ui.text_edit_singleline(&mut editor.tags);
            if ui.button("Сохранить").clicked(){save=true;}
        });
        if save {
            let tags:Vec<String>=editor.tags.split(',').map(str::trim).filter(|t|!t.is_empty()).map(str::to_owned).collect();
            if let Some(id)=editor.item_id {if let Some(w)=self.watchlist.iter_mut().find(|w|w.item_id==id){w.note=editor.note.clone();w.tags=tags.clone();}}
            if let Some(id)=editor.seller_id {if let Some(w)=self.watched_sellers.iter_mut().find(|w|w.seller_id==id){w.note=editor.note.clone();w.tags=tags.clone();}}
            self.persist_watchlist();
        } else if open {self.note_editor=Some(editor);}
    }

    fn snapshot_comparison(&self,ui:&mut egui::Ui) {
        if self.radar_derived_loading {ui.horizontal(|ui|{ui.spinner();ui.label("Сравнение снимков рассчитывается в фоне…");});return;}
        let Some(comparison)=&self.radar_comparison else{ui.label(RichText::new("Сравнение появится после второго снимка радара.").small().color(Color32::GRAY));return;};
        if comparison.previous_at==0 {ui.label(RichText::new("Сравнение появится после второго снимка радара.").small().color(Color32::GRAY));return;}
        ui.label(format!("{} → {} · найдено {} изменений · +{} новых · −{} пропавших",date(comparison.previous_at),date(comparison.current_at),comparison.total_changes,comparison.added,comparison.missing));
        egui::ScrollArea::vertical().max_height((ui.available_height()*0.22).clamp(72.0,150.0)).show(ui,|ui|{
            for change in &comparison.top_changes {
                ui.horizontal(|ui|{ui.label(RichText::new(&change.detail).strong().color(Color32::from_rgb(125,211,252)));ui.label(&change.title);});
            }
            if comparison.total_changes>comparison.top_changes.len(){ui.label(RichText::new(format!("Показаны наиболее заметные {} из {} изменений",comparison.top_changes.len(),comparison.total_changes)).small().color(Color32::GRAY));}
        });
    }

    fn activity_view(&mut self,ui:&mut egui::Ui) {
        card(ui,|ui|{
            ui.heading("Резервная копия и перенос данных");
            ui.horizontal_wrapped(|ui|{ui.label("Хранить, дней");ui.add(egui::DragValue::new(&mut self.retention_days).range(30..=3650));ui.label("Макс. наблюдений/задач");ui.add(egui::DragValue::new(&mut self.history_limit).range(50..=5000));if ui.button("Архивировать и очистить сейчас").clicked(){self.apply_retention();}});
            ui.label(RichText::new("Снимки радара старше срока перемещаются в archive/niche_snapshots; длинные истории наблюдений и журнал ограничиваются заданным числом записей.").small().color(Color32::GRAY));
            ui.horizontal_wrapped(|ui|{if ui.button("Создать резервную копию…").clicked(){self.backup_data();}if ui.button("Восстановить из копии…").clicked(){self.choose_restore();}if ui.button("Обновить журнал").clicked(){self.activity_log=read_json_or(self.out_dir.join("activity_log.json"),Vec::new());}});
            if !self.backup_message.is_empty(){ui.label(&self.backup_message);}
            ui.label(RichText::new("Копия включает файлы данных, настройки приложения, сохранённые поиски, избранное, отслеживание, журнал и снимки. Восстановление объединяет файлы и заменяет одноимённые.").small().color(Color32::GRAY));
        });
        if self.restore_candidate.is_some(){
            let mut confirm=true;let path=self.restore_candidate.clone().unwrap();
            egui::Window::new("Подтвердить восстановление").open(&mut confirm).show(ui.ctx(),|ui|{ui.label(format!("Восстановить данные из {}?",path.display()));ui.horizontal(|ui|{if ui.button("Восстановить").clicked(){self.restore_data();}if ui.button("Отмена").clicked(){self.restore_candidate=None;}});});
            if !confirm{self.restore_candidate=None;}
        }
        ui.horizontal(|ui|{ui.heading("История задач");if ui.button("Очистить журнал").clicked(){self.activity_log.clear();let _=write_json_atomic(&self.out_dir.join("activity_log.json"),&self.activity_log);}});
        card(ui,|ui|{egui::ScrollArea::both().max_height((ui.available_height()-160.0).clamp(180.0,720.0)).show(ui,|ui|{egui::Grid::new("task_log_grid").striped(true).show(ui,|ui|{for h in ["Время","Задача","Статус","Попытки","Подробности"]{ui.label(RichText::new(h).strong());}ui.end_row();for e in self.activity_log.iter().take(300){ui.label(date(e.at));ui.label(&e.task);ui.label(&e.status);ui.label(e.attempts.to_string());ui.label(&e.details);ui.end_row();}});});});
    }

    fn message(&mut self,ui:&mut egui::Ui){if !self.message.is_empty(){card(ui,|ui|{ui.label(RichText::new(&self.message).color(if self.message.starts_with("Ошибка"){Color32::LIGHT_RED}else{Color32::from_rgb(134,239,172)}));});}}
}

impl eframe::App for PlatiApp {
    fn update(&mut self,ctx:&egui::Context,_frame:&mut eframe::Frame){
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num1)){self.page=Page::Deals;}
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num2)){self.page=Page::Seller;}
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num3)){self.page=Page::Sellers;}
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num4)){self.page=Page::Radar;}
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num5)){self.page=Page::Watchlist;}
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num6)){self.page=Page::Activity;}
        self.poll();
        let viewport=ctx.input(|i|i.viewport().inner_rect.map(|r|r.size()).unwrap_or(Vec2::new(1280.0,800.0)));
        let responsive_mode=if viewport.x<900.0||viewport.y<600.0{0}else if viewport.x>1900.0&&viewport.y>1000.0{4}else if viewport.x>1500.0{2}else if viewport.y>900.0{3}else{1};
        if self.responsive_mode!=responsive_mode {
            let mut responsive_style=(*ctx.style()).clone();
            let text_scale=match responsive_mode{0=>0.92,4=>1.08,_=>1.0};
            let previous_scale=match self.responsive_mode{0=>0.92,4=>1.08,_=>1.0};
            if responsive_mode==0 {
                responsive_style.spacing.item_spacing=Vec2::new(6.0,5.0);
                responsive_style.spacing.button_padding=Vec2::new(7.0,3.0);
                responsive_style.spacing.interact_size=Vec2::new(30.0,26.0);
            } else {
                let spacious=responsive_mode==2||responsive_mode==4;
                responsive_style.spacing.item_spacing=if spacious{Vec2::new(11.0,8.0)}else{Vec2::new(9.0,7.0)};
                responsive_style.spacing.button_padding=if spacious{Vec2::new(11.0,6.0)}else{Vec2::new(8.0,4.0)};
                responsive_style.spacing.interact_size=if responsive_mode==3||responsive_mode==4{Vec2::new(40.0,32.0)}else{Vec2::new(36.0,29.0)};
            }
            for font in responsive_style.text_styles.values_mut(){font.size=font.size/previous_scale*text_scale;}
            ctx.set_style(responsive_style);
            self.responsive_mode=responsive_mode;
        }
        let sidebar_width=(viewport.x*0.15).clamp(164.0,240.0);
        let horizontal_margin=if viewport.x<1000.0{12}else if viewport.x<1300.0{16}else{22};
        let vertical_margin=if viewport.y<680.0{8}else{16};
        egui::SidePanel::left("nav").exact_width(sidebar_width).resizable(false).frame(egui::Frame::side_top_panel(&ctx.style()).fill(Color32::from_rgb(15,23,42)).inner_margin(egui::Margin::same(if viewport.x<1000.0{9}else{14}))).show(ctx,|ui|self.sidebar(ui));
        egui::CentralPanel::default().frame(egui::Frame::central_panel(&ctx.style()).fill(Color32::from_rgb(10,15,28)).inner_margin(egui::Margin::symmetric(horizontal_margin,vertical_margin))).show(ctx,|ui|{
            self.header(ui);
            if self.page==Page::Radar {
                // Keep the radar table as the only vertical scrolling region on this page.
                // A parent ScrollArea makes egui chain wheel input into both areas.
                self.radar_view(ui);
            } else {
                egui::ScrollArea::vertical().auto_shrink([false,false]).show(ui,|ui|match self.page{Page::Deals=>self.deals_view(ui),Page::Seller=>self.seller_view(ui),Page::Sellers=>self.discover_view(ui),Page::Radar=>{},Page::Watchlist=>self.watchlist_view(ui),Page::Activity=>self.activity_view(ui)});
            }
        });
        if self.note_editor.is_some(){self.note_dialog(ctx);}
        if self.export_draft.is_some(){self.export_dialog(ctx);}
        self.persist_settings(ctx);
        ctx.request_repaint_after(Duration::from_millis(500));
    }
}

fn start_capture(client:Client,out:PathBuf,tx:Sender<UiMessage>,running:Arc<AtomicBool>,handle:tokio::runtime::Handle,slot:Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>,prefs:RadarAlertPrefs){
    if running.compare_exchange(false,true,Ordering::AcqRel,Ordering::Acquire).is_err(){return;}
    let _=tx.send(UiMessage::NicheStarted);
    let task_slot=slot.clone();
    let task=handle.spawn(async move{
        let r=capture_niche_snapshot(&client,&out).await.map(|_|()).map_err(|e|e.to_string());
        if r.is_ok() && prefs.enabled {
            let snapshots=load_recent_niche_snapshots(&out).unwrap_or_default();
            let count=build_niche_trends(&snapshots).iter().filter(|t|matches_alert_target(&prefs,&t.product.title,&t.product.category,t.product.seller_id)&&t.delta_sales.unwrap_or(i64::MIN)>=prefs.min_delta&&t.sellers_count<=prefs.max_sellers).count();
            if count>0 {
                let body=format!("По текущим порогам найдено {count} возможностей");
                let _=tokio::task::spawn_blocking(move||{let _=notify_rust::Notification::new().summary("Plati Radar").body(&body).show();}).await;
            }
        }
        running.store(false,Ordering::Release);
        if let Ok(mut current)=task_slot.lock(){*current=None;}
        let _=tx.send(UiMessage::Niche(r));
    });
    if let Ok(mut current)=slot.lock(){*current=Some(task.abort_handle());}
}

async fn fetch_item_options(client:&Client,id:u64,store_price:f64)->Result<(f64,Vec<UiOptionGroup>)>{
    let url=format!("{}/{id}/data?currency=RUB",DIGISELLER_PRODUCT_API);
    let product=client.get(url).header("Accept","application/json").send().await?.error_for_status()?.json::<DigiProductEnvelope>().await?.product;
    let Some(p)=product else{return Ok((store_price,Vec::new()))};
    let initial=p.prices.as_ref().and_then(|x|x.initial.as_ref());let default=p.prices.as_ref().and_then(|x|x.default.as_ref());
    let dr=default.and_then(|x|x.rub).filter(|x|*x>0.0).unwrap_or(store_price);let ir=initial.and_then(|x|x.rub).filter(|x|*x>0.0).unwrap_or(dr);
    let opts=p.options.unwrap_or_default();let mut mods=0.0;
    for o in &opts{if !matches!(o.opt_type.as_deref(),Some("radio"|"select")){continue;}for v in o.variants.as_deref().unwrap_or(&[]){if json_flag_true(&v.visible,true)&&json_flag_true(&v.default,false){mods+=variant_modifier_rub(v,ir,initial,default);}}}
    let base=(dr-mods).max(ir);let mut groups=Vec::new();
    for o in opts{let kind=o.opt_type.unwrap_or_default();if kind!="radio"&&kind!="select"{continue;}let variants:Vec<UiOptionVariant>=o.variants.unwrap_or_default().into_iter().filter(|v|json_flag_true(&v.visible,true)).map(|v|UiOptionVariant{text:clean_space(v.text.as_deref().unwrap_or("")),modifier_rub:variant_modifier_rub(&v,ir,initial,default).round(),is_default:json_flag_true(&v.default,false)}).collect();if !variants.is_empty(){groups.push(UiOptionGroup{label:clean_space(o.label.as_deref().or(o.name.as_deref()).unwrap_or("Опция")),opt_type:kind,variants});}}
    Ok((base.round(),groups))
}

fn matches_alert_target(prefs:&RadarAlertPrefs,title:&str,category:&str,seller_id:u64)->bool {
    let terms:Vec<String>=prefs.text_filter.split(',').map(str::trim).filter(|x|!x.is_empty()).map(str::to_lowercase).collect();
    let text=format!("{title} {category}").to_lowercase();
    let matches_text=terms.is_empty()||terms.iter().any(|term|text.contains(term.as_str()));
    let matches_seller=!prefs.tracked_sellers_only||prefs.seller_ids.contains(&seller_id);
    matches_text&&matches_seller
}

fn build_snapshot_comparison(snapshots:&[NicheSnapshot])->RadarComparison {
    if snapshots.len()<2{return RadarComparison{current_at:snapshots.first().map(|s|s.captured_at).unwrap_or(0),previous_at:0,total_changes:0,added:0,missing:0,top_changes:Vec::new()};}
    let current=&snapshots[0];let previous=&snapshots[1];
    let old:HashMap<u64,&NicheProduct>=previous.products.iter().map(|p|(p.item_id,p)).collect();
    let new:HashMap<u64,&NicheProduct>=current.products.iter().map(|p|(p.item_id,p)).collect();
    let group_counts=|products:&[NicheProduct]|{
        let mut groups:HashMap<String,HashSet<u64>>=HashMap::new();
        for product in products{groups.entry(niche_title_key(&product.title)).or_default().insert(product.seller_id);}
        groups.into_iter().map(|(key,sellers)|(key,sellers.len())).collect::<HashMap<_,_>>()
    };
    let old_groups=group_counts(&previous.products);let new_groups=group_counts(&current.products);
    let mut total_changes=0;let mut added=0;let mut missing=0;let mut top_changes:Vec<RadarChange>=Vec::with_capacity(60);
    let mut consider=|title:&str,detail:String,sales:i64,price:f64|{
        total_changes+=1;
        let better=top_changes.len()<60||top_changes.last().map(|last|sales>last.sales||(sales==last.sales&&price<last.price)).unwrap_or(true);
        if better {
            let item=RadarChange{title:title.to_owned(),detail,sales,price};
            let position=top_changes.iter().position(|other|sales>other.sales||(sales==other.sales&&price<other.price)).unwrap_or(top_changes.len());
            top_changes.insert(position,item);if top_changes.len()>60{top_changes.pop();}
        }
    };
    for product in &current.products {
        if let Some(old_product)=old.get(&product.item_id){
            let sales=product.sold_count as i64-old_product.sold_count as i64;let price=product.price_rub-old_product.price_rub;
            let key=niche_title_key(&product.title);
            let competition=new_groups.get(&key).copied().unwrap_or(0) as i64-old_groups.get(&key).copied().unwrap_or(0) as i64;
            let mut parts=Vec::new();if sales!=0{parts.push(format!("Продажи {sales:+}"));}if price.abs()>=0.01{parts.push(format!("Цена {price:+.0} ₽"));}if competition!=0{parts.push(format!("Продавцы {competition:+}"));}
            if !parts.is_empty(){consider(&product.title,parts.join(" · "),sales,price);}
        }else{added+=1;consider(&product.title,"Новая карточка".into(),0,0.0);}
    }
    for product in &previous.products{if !new.contains_key(&product.item_id){missing+=1;consider(&product.title,"Не найдена в новом снимке".into(),0,0.0);}}
    drop(consider);
    RadarComparison{current_at:current.captured_at,previous_at:previous.captured_at,total_changes,added,missing,top_changes}
}

fn export_cell(value:&serde_json::Value)->String {
    match value {serde_json::Value::Null=>String::new(),serde_json::Value::String(s)=>s.clone(),serde_json::Value::Number(n)=>n.to_string(),serde_json::Value::Bool(b)=>b.to_string(),other=>other.to_string()}
}

fn selected_export_rows(columns:&[String],rows:&[serde_json::Value])->Vec<serde_json::Value> {
    rows.iter().map(|row|{
        let mut object=serde_json::Map::new();
        for column in columns {if let Some(value)=row.get(column){object.insert(column.clone(),value.clone());}}
        serde_json::Value::Object(object)
    }).collect()
}

fn write_custom_export(path:&Path,columns:&[String],rows:&[serde_json::Value],format:ExportFormat)->Result<()> {
    if columns.is_empty(){anyhow::bail!("Не выбраны столбцы для экспорта");}
    if let Some(parent)=path.parent().filter(|p|!p.as_os_str().is_empty()){fs::create_dir_all(parent)?;}
    let rows=selected_export_rows(columns,rows);
    match format {
        ExportFormat::Json=>serde_json::to_writer_pretty(File::create(path)?,&rows)?,
        ExportFormat::Csv=>{
            let mut file=File::create(path)?;file.write_all(b"\xEF\xBB\xBF")?;let mut writer=csv::Writer::from_writer(file);
            writer.write_record(columns.iter().map(String::as_str))?;
            for row in &rows {
                let values:Vec<String>=columns.iter().map(|column|{
                    let value=row.get(column).unwrap_or(&serde_json::Value::Null);let mut text=export_cell(value);
                    if matches!(value,serde_json::Value::String(_))&&matches!(text.trim_start().chars().next(),Some('='|'+'|'-'|'@')){text.insert(0,'\'');}
                    text
                }).collect();
                writer.write_record(values)?;
            }
            writer.flush()?;
        }
        ExportFormat::Xlsx=>{
            let mut workbook=rust_xlsxwriter::Workbook::new();let sheet=workbook.add_worksheet();
            for (col,key) in columns.iter().enumerate(){sheet.write_string(0,col as u16,key.as_str())?;}
            for (row_index,row) in rows.iter().enumerate(){for (col,key) in columns.iter().enumerate(){
                let value=row.get(key).unwrap_or(&serde_json::Value::Null);let r=(row_index+1) as u32;let c=col as u16;
                match value {serde_json::Value::String(text)=>{sheet.write_string(r,c,text.as_str())?;},serde_json::Value::Number(number)=>{if let Some(value)=number.as_f64(){sheet.write_number(r,c,value)?;}else{sheet.write_string(r,c,number.to_string())?;}},serde_json::Value::Bool(value)=>{sheet.write_boolean(r,c,*value)?;},_=>{if !value.is_null(){sheet.write_string(r,c,export_cell(value))?;}}}
            }}
            sheet.set_freeze_panes(1,0)?;for col in 0..columns.len(){sheet.set_column_width(col as u16,18.0)?;}workbook.save(path)?;
        }
    }
    Ok(())
}

fn copy_tree(source:&Path,destination:&Path)->Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry=entry?;let from=entry.path();let to=destination.join(entry.file_name());
        if from.is_dir(){copy_tree(&from,&to)?;}else if from.is_file(){fs::copy(&from,&to)?;}
    }
    Ok(())
}

fn read_json_or<T:for<'de>Deserialize<'de>>(path:PathBuf,fallback:T)->T{fs::read(path).ok().and_then(|raw|serde_json::from_slice(&raw).ok()).unwrap_or(fallback)}
fn sort_name(s:SortMode)->&'static str{match s{SortMode::RealPrice=>"Реальная цена",SortMode::StorePrice=>"Цена витрины",SortMode::Reviews=>"Отзывы",SortMode::Smart=>"Умный балл"}}
fn radar_sort_name(s:RadarSort)->&'static str{match s{RadarSort::Opportunity=>"Балл",RadarSort::Delta=>"Прирост",RadarSort::Price=>"Цена",RadarSort::Sellers=>"Конкуренция"}}
fn sort_deals(a:&ProductDeal,b:&ProductDeal,s:SortMode)->std::cmp::Ordering{match s{SortMode::RealPrice=>a.real_tariff_price_rub.partial_cmp(&b.real_tariff_price_rub).unwrap_or(std::cmp::Ordering::Equal),SortMode::StorePrice=>a.storefront_price_rub.partial_cmp(&b.storefront_price_rub).unwrap_or(std::cmp::Ordering::Equal),SortMode::Reviews=>b.positive_reviews.cmp(&a.positive_reviews),SortMode::Smart=>b.smart_score.partial_cmp(&a.smart_score).unwrap_or(std::cmp::Ordering::Equal)}}
fn opt(x:Option<u64>)->String{x.map(|v|v.to_string()).unwrap_or_else(||"—".into())}

fn theme(ctx:&egui::Context){let mut s=(*ctx.style()).clone();s.visuals=egui::Visuals::dark();s.visuals.panel_fill=Color32::from_rgb(10,15,28);s.visuals.window_fill=Color32::from_rgb(20,29,47);s.visuals.extreme_bg_color=Color32::from_rgb(13,20,35);s.visuals.widgets.inactive.bg_fill=Color32::from_rgb(30,41,59);s.visuals.widgets.hovered.bg_fill=Color32::from_rgb(41,61,82);s.visuals.selection.bg_fill=Color32::from_rgb(3,105,161);s.spacing.item_spacing=Vec2::new(9.0,8.0);ctx.set_style(s);}
fn card(ui:&mut egui::Ui,f:impl FnOnce(&mut egui::Ui)){
    let content_width=(ui.available_width()-26.0).max(0.0);
    egui::Frame::new().fill(Color32::from_rgb(20,29,47)).stroke(Stroke::new(1.0_f32,Color32::from_rgb(37,51,72))).corner_radius(egui::CornerRadius::same(10)).inner_margin(egui::Margin::same(13)).show(ui,|inner|{if content_width.is_finite(){inner.set_min_width(content_width);}f(inner);});
    ui.add_space(9.0);
}
fn empty(ui:&mut egui::Ui,title:&str,desc:&str){card(ui,|ui|{ui.vertical_centered(|ui|{ui.add_space(26.0);ui.label(RichText::new("◈").size(36.0).color(Color32::from_rgb(56,189,248)));ui.heading(title);ui.label(RichText::new(desc).color(Color32::GRAY));ui.add_space(26.0);});});}
fn date(ts:u64)->String{let age=unix_now().saturating_sub(ts);if age<3600{format!("{} мин. назад",age/60)}else if age<86_400{format!("{} ч. назад",age/3600)}else{format!("{} дн. назад",age/86_400)}}

fn history_plot(ui:&mut egui::Ui,history:&[NicheHistoryPoint],height:f32){
    let history=if history.len()>500{&history[history.len()-500..]}else{history};
    let (r,p)=ui.allocate_painter(Vec2::new(ui.available_width(),height),egui::Sense::hover());let rect=r.rect.shrink(8.0);p.rect_filled(rect,egui::CornerRadius::same(5),Color32::from_rgb(13,20,35));
    if history.is_empty(){p.text(rect.center(),egui::Align2::CENTER_CENTER,"История появится после следующих снимков",egui::FontId::proportional(13.0),Color32::GRAY);return;}
    let max=history.iter().map(|x|x.delta_sales).max().unwrap_or(1).max(1) as f32;let left=rect.left()+15.0;let right=rect.right()-10.0;let top=rect.top()+10.0;let bottom=rect.bottom()-10.0;
    let pts:Vec<_>=history.iter().enumerate().map(|(i,x)|{let xx=if history.len()==1{(left+right)/2.0}else{egui::lerp(left..=right,i as f32/(history.len()-1)as f32)};egui::pos2(xx,egui::lerp(bottom..=top,x.delta_sales as f32/max))}).collect();
    if pts.len()>1{p.add(egui::Shape::line(pts.clone(),Stroke::new(2.0_f32,Color32::from_rgb(56,189,248))));}
    for (point,item) in pts.iter().zip(history) {
        p.circle_filled(*point,3.5,Color32::from_rgb(134,239,172));
        if r.hover_pos().map(|hover|hover.distance(*point)<10.0).unwrap_or(false) {
            let interval=item.interval_seconds/3600;
            let text=format!("{} · Δ {} · {} карточек · интервал {} ч",date(item.captured_at),item.delta_sales,item.products_tracked,interval);
            p.text(*point+Vec2::new(7.0,-9.0),egui::Align2::LEFT_BOTTOM,text,egui::FontId::proportional(11.0),Color32::WHITE);
        }
    }
}

fn write_niche_csv(path:&Path,snapshots:&[NicheSnapshot])->Result<()> {
    if let Some(parent)=path.parent().filter(|p|!p.as_os_str().is_empty()){fs::create_dir_all(parent)?;}
    let mut w=csv::Writer::from_path(path)?;
    w.write_record(["item_id","title","item_url","category","seller_name","seller_id","price_rub","sold_count","delta_sales","listings_count","sellers_count","median_price_rub","markup_percent","opportunity_score"])?;
    for t in build_niche_trends(snapshots){w.write_record([t.product.item_id.to_string(),t.product.title,t.product.item_url,t.product.category,t.product.seller_name,t.product.seller_id.to_string(),t.product.price_rub.to_string(),t.product.sold_count.to_string(),t.delta_sales.map(|x|x.to_string()).unwrap_or_default(),t.listings_count.to_string(),t.sellers_count.to_string(),t.comparable_median_price_rub.map(|x|x.to_string()).unwrap_or_default(),t.markup_percent.map(|x|x.to_string()).unwrap_or_default(),t.opportunity_score.to_string()])?;}w.flush()?;Ok(())
}

fn export_records<T:Serialize>(path:&Path,records:&[T])->Result<()> {
    if let Some(parent)=path.parent().filter(|p|!p.as_os_str().is_empty()){fs::create_dir_all(parent)?;}
    if path.extension().and_then(|x|x.to_str()).map(|x|x.eq_ignore_ascii_case("csv")).unwrap_or(false){
        let mut file=File::create(path)?;file.write_all(b"\xEF\xBB\xBF")?;let mut w=csv::Writer::from_writer(file);for row in records{w.serialize(row)?;}w.flush()?;
    }else{serde_json::to_writer_pretty(File::create(path)?,records)?;}
    Ok(())
}

fn open_folder(path:&Path)->Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(target_os="windows")]
    std::process::Command::new("explorer").arg(path).spawn()?;
    #[cfg(target_os="macos")]
    std::process::Command::new("open").arg(path).spawn()?;
    #[cfg(all(unix,not(target_os="macos")))]
    std::process::Command::new("xdg-open").arg(path).spawn()?;
    Ok(())
}

fn app_icon()->egui::IconData {
    let (w,h)=(64_u32,64_u32);let mut rgba=vec![0_u8;(w*h*4) as usize];
    for y in 0..h { for x in 0..w {
        let dx=x as f32-31.5;let dy=y as f32-31.5;let r=(dx*dx+dy*dy).sqrt();
        let mut c=[15_u8,23,42,255];
        if r<27.0 { c=[17,37,59,255]; }
        if (r-11.0).abs()<1.4||(r-21.0).abs()<1.4 { c=[56,189,248,255]; }
        if dx.abs()<1.0||dy.abs()<1.0 { if r<26.0 { c=[71,85,105,255]; } }
        if dx>0.0&&dy<0.0&&r<23.0 { c=[14,165,233,255]; }
        if r<4.0 { c=[74,222,128,255]; }
        let i=((y*w+x)*4) as usize;rgba[i..i+4].copy_from_slice(&c);
    }}
    egui::IconData{rgba,width:w,height:h}
}

pub fn run(out_dir:PathBuf)->Result<()>{
    let explicit_out=std::env::args().any(|a|a=="--out-dir"||a=="-o");
    let mut settings=load_settings(out_dir.clone());
    if explicit_out{settings.out_dir=out_dir;}
    let size=settings.window_size;
    let options=eframe::NativeOptions{viewport:egui::ViewportBuilder::default().with_inner_size(size).with_min_inner_size([640.0,420.0]).with_maximized(true).with_icon(Arc::new(app_icon())),..Default::default()};
    eframe::run_native("Plati Radar",options,Box::new(move|cc|Ok(Box::new(PlatiApp::new(cc,settings))))).map_err(|e|anyhow::anyhow!("Не удалось запустить Plati Radar: {e}"))
}
