use super::*;
use eframe::egui::{self, Color32, RichText, Stroke, Vec2};
use futures::stream::{self, StreamExt};
use std::sync::mpsc::{self, Receiver, Sender};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Page { Deals, Seller, Sellers, Radar }
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum RadarSort { Opportunity, Delta, Price, Sellers }
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SellerSort { Rating, Sales, Reviews }

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
}

impl UiSettings {
    fn defaults(out_dir: PathBuf) -> Self {
        Self { out_dir, window_size: [1440.0, 940.0], page: Page::Deals,
            query: "ChatGPT Plus".into(), deals_filter: String::new(), seller_url: DEFAULT_SELLER_URL.into(), sellers_filter: String::new(), niche_filter: String::new(), keyword: "plus".into(),
            exclude: "оффлайн,offline,общий,shared,гайд,инструкция,аренда".into(),
            min_reviews: 100, max_negative: 5.0, pages: 2, inspect_limit: 50,
            sort: SortMode::RealPrice, seller_sort: SellerSort::Rating, max_categories: 20, enrich_limit: 25,
            min_delta: 1, max_sellers: 50, max_price: 1_000_000.0, max_markup: 1_000.0,
            radar_sort: RadarSort::Opportunity, favorites_only: false, notify_growth: true }
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
    Deals(Result<Vec<ProductDeal>, String>),
    Seller(Result<SellerRecord, String>),
    Sellers(Result<Vec<SellerRecord>, String>),
    DiscoverProgress(usize, usize, String),
    Options(Result<(u64, String, f64, Vec<UiOptionGroup>), String>),
    Niche(Result<(), String>),
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
    radar_prefs_shared: Arc<std::sync::RwLock<(bool, i64, usize)>>,

    progress: NicheProgress,
    snapshots: Vec<NicheSnapshot>,
    history: Vec<NicheHistoryPoint>,
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
}

impl PlatiApp {
    fn new(cc: &eframe::CreationContext<'_>, settings: UiSettings) -> Self {
        theme(&cc.egui_ctx);
        let out_dir = settings.out_dir.clone();
        let output_shared = Arc::new(std::sync::RwLock::new(out_dir.clone()));
        let client = build_client().unwrap_or_else(|_| Client::new());
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Tokio runtime");
        let (tx, rx) = mpsc::channel();
        let deals = read_json_or(out_dir.join("deals.json"), Vec::new());
        let sellers = read_json_or(out_dir.join("plati_sellers.json"), Vec::new());
        let favorites = read_json_or(out_dir.join("niche_favorites.json"), HashSet::new());
        let snapshots = load_recent_niche_snapshots(&out_dir).unwrap_or_default();
        let history = niche_history_for_response(&out_dir, &snapshots);
        let progress = read_niche_progress(&out_dir);
        let app = Self {
            settings: settings.clone(), settings_saved: settings.clone(), out_dir, output_shared, client, runtime, tx, rx,
            page: settings.page, message: String::new(),
            query: settings.query.clone(), keyword: settings.keyword.clone(), exclude: settings.exclude.clone(),
            min_reviews: settings.min_reviews, max_negative: settings.max_negative, pages: settings.pages, inspect_limit: settings.inspect_limit,
            sort: settings.sort, deals_filter: settings.deals_filter.clone(), deals, search_busy: false, search_cancel: None,
            seller_url: settings.seller_url.clone(), seller: None, seller_busy: false, seller_cancel: None,
            max_categories: settings.max_categories, enrich_limit: settings.enrich_limit, sellers, seller_sort: settings.seller_sort, sellers_filter: settings.sellers_filter.clone(), discover_busy: false,
            discover_cancel: None, discover_progress: (0, 0, String::new()),
            niche_running: Arc::new(AtomicBool::new(false)), capture_task: Arc::new(std::sync::Mutex::new(None)),
            radar_prefs_shared: Arc::new(std::sync::RwLock::new((settings.notify_growth, settings.min_delta, settings.max_sellers))), progress, snapshots, history,
            niche_filter: settings.niche_filter.clone(), min_delta: settings.min_delta, max_sellers: settings.max_sellers, max_price: settings.max_price, max_markup: settings.max_markup, radar_sort: settings.radar_sort, favorites,
            favorites_only: settings.favorites_only, notify_growth: settings.notify_growth, options_open: false, options_busy: false,
            options_id: 0, options_title: String::new(), options_base: 0.0,
            options: Vec::new(), option_selection: BTreeMap::new(),
        };
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
                        let prefs = radar_prefs.read().map(|p| *p).unwrap_or((true, 1, 50));
                        start_capture(client.clone(), out.clone(), tx.clone(), running.clone(), tokio::runtime::Handle::current(), task_slot.clone(), prefs.0, prefs.1, prefs.2);
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
        let (client, tx) = (self.client.clone(), self.tx.clone());
        let (query, keyword, exclude) = (self.query.trim().to_owned(), self.keyword.clone(), self.exclude.clone());
        let (pages, min_reviews, neg, inspect) = (self.pages, self.min_reviews, self.max_negative, self.inspect_limit);
        let task = self.runtime.spawn(async move {
            let result = search_deals(&client, &query, pages, min_reviews, neg, &keyword, &exclude, inspect, SortMode::RealPrice, 6, 80).await.map_err(|e| e.to_string());
            let _ = tx.send(UiMessage::Deals(result));
        });
        self.search_cancel = Some(task.abort_handle());
    }

    fn cancel_search(&mut self) {
        if let Some(task) = self.search_cancel.take() { task.abort(); }
        self.search_busy = false;
        self.message = "Поиск отменён".into();
    }

    fn send_seller_lookup(&mut self) {
        if self.seller_busy || self.seller_url.trim().is_empty() { return; }
        self.seller_busy = true;
        let (client, tx, url) = (self.client.clone(), self.tx.clone(), self.seller_url.trim().to_owned());
        let task = self.runtime.spawn(async move {
            let id = extract_seller_id_from_url(&url);
            let result = fetch_seller_profile(&client, &url, id).await.map_err(|e| e.to_string());
            let _ = tx.send(UiMessage::Seller(result));
        });
        self.seller_cancel = Some(task.abort_handle());
    }

    fn cancel_seller(&mut self) {
        if let Some(task) = self.seller_cancel.take() { task.abort(); }
        self.seller_busy = false;
        self.message = "Проверка продавца отменена".into();
    }

    fn cancel_discover(&mut self) {
        if let Some(task) = self.discover_cancel.take() { task.abort(); }
        self.discover_busy = false;
        self.message = "Сбор продавцов отменён".into();
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
    }

    fn send_discover(&mut self) {
        if self.discover_busy { return; }
        self.discover_busy = true;
        self.discover_progress = (0, 0, "Получаем список категорий…".into());
        let (client, tx) = (self.client.clone(), self.tx.clone());
        let (max_cats, enrich) = (self.max_categories.clamp(1, 389), self.enrich_limit.min(100));
        let task = self.runtime.spawn(async move {
            let result = async {
                let queries = fetch_category_queries(&client, max_cats).await?;
                let total = queries.len();
                let mut map = BTreeMap::new();
                for (index, query) in queries.into_iter().enumerate() {
                    let one_query = vec![query.clone()];
                    let found = discover_sellers_via_api(&client, &one_query, 1, 150).await?;
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
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            fetch_seller_profile(&client, &format!("https://plati.market/seller/s/{id}/"), Some(id)).await.ok()
                        }
                    }).buffer_unordered(4).filter_map(|x| async move { x }).collect().await;
                    for seller in extra { map.insert(seller.seller_id, seller); }
                }
                let mut result: Vec<SellerRecord> = map.into_values().collect();
                result.sort_by(|a,b| b.rating.unwrap_or(0.0).partial_cmp(&a.rating.unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal));
                Ok::<_, anyhow::Error>(result)
            }.await.map_err(|e| e.to_string());
            let _ = tx.send(UiMessage::Sellers(result));
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
                UiMessage::Deals(result) => {
                    self.search_busy = false;
                    self.search_cancel = None;
                    match result {
                        Ok(items) => {
                            self.message = match save_deals_files(&self.out_dir, &items) {
                                Ok((c,j)) => format!("Найдено {} карточек · сохранено {} и {}", items.len(), c.display(), j.display()),
                                Err(e) => format!("Найдено {}, ошибка записи: {e}", items.len()),
                            };
                            self.deals = items;
                        }
                        Err(e) => self.message = format!("Ошибка поиска: {e}"),
                    }
                }
                UiMessage::Seller(result) => {
                    self.seller_busy = false;
                    self.seller_cancel = None;
                    match result {
                        Ok(item) => {
                            let _ = save_sellers_outputs(&self.out_dir, std::slice::from_ref(&item));
                            self.seller = Some(item);
                            self.message = "Профиль продавца загружен".into();
                        }
                        Err(e) => self.message = format!("Ошибка загрузки профиля: {e}"),
                    }
                }
                UiMessage::DiscoverProgress(done, total, category) => {
                    self.discover_progress = (done, total, category);
                }
                UiMessage::Sellers(result) => {
                    self.discover_busy = false;
                    self.discover_cancel = None;
                    match result {
                        Ok(items) => {
                            self.message = match save_sellers_outputs(&self.out_dir, &items) {
                                Ok((c,j)) => format!("Найдено {} продавцов · сохранено {} и {}", items.len(), c.display(), j.display()),
                                Err(e) => format!("Найдено {}, ошибка записи: {e}", items.len()),
                            };
                            self.sellers = items;
                        }
                        Err(e) => self.message = format!("Ошибка сбора продавцов: {e}"),
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
                UiMessage::Niche(result) => {
                    self.reload_radar();
                    self.message = match result {
                        Err(e) => format!("Ошибка радара: {e}"),
                        Ok(()) => {
                            let count = build_niche_trends(&self.snapshots).iter().filter(|t| t.delta_sales.unwrap_or(i64::MIN) >= self.min_delta && t.sellers_count <= self.max_sellers).count();
                            if self.notify_growth && count > 0 { format!("Радар: найдено {count} возможностей по текущим порогам") } else { "Снимок радара сохранён".into() }
                        }
                    };
                }
            }
        }
        self.progress = read_niche_progress(&self.out_dir);
    }

    fn reload_radar(&mut self) {
        self.snapshots = load_recent_niche_snapshots(&self.out_dir).unwrap_or_default();
        self.history = niche_history_for_response(&self.out_dir, &self.snapshots);
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
        self.settings.max_categories = self.max_categories;
        self.settings.enrich_limit = self.enrich_limit;
        self.settings.min_delta = self.min_delta;
        self.settings.max_sellers = self.max_sellers;
        self.settings.max_price = self.max_price;
        self.settings.max_markup = self.max_markup;
        self.settings.radar_sort = self.radar_sort;
        self.settings.favorites_only = self.favorites_only;
        self.settings.notify_growth = self.notify_growth;
        if let Ok(mut prefs) = self.radar_prefs_shared.write() { *prefs = (self.notify_growth, self.min_delta, self.max_sellers); }
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
            self.reload_radar();
            self.progress = read_niche_progress(&self.out_dir);
            self.message = format!("Папка данных: {}", self.out_dir.display());
        }
    }

    fn open_output_dir(&mut self) {
        let result = open_folder(&self.out_dir);
        self.message = result.map(|_| format!("Открыта папка {}", self.out_dir.display())).unwrap_or_else(|e| format!("Не удалось открыть папку: {e}"));
    }

    fn export_deals_as(&mut self, csv: bool) {
        let (name, filter, ext) = if csv { ("deals.csv", "CSV", "csv") } else { ("deals.json", "JSON", "json") };
        if let Some(path) = rfd::FileDialog::new().set_file_name(name).add_filter(filter, &[ext]).save_file() {
            self.message = export_records(&path, &self.deals).map(|_| format!("Экспортировано: {}", path.display())).unwrap_or_else(|e| format!("Ошибка экспорта: {e}"));
        }
    }

    fn export_sellers_as(&mut self, csv: bool) {
        let (name, filter, ext) = if csv { ("plati_sellers.csv", "CSV", "csv") } else { ("plati_sellers.json", "JSON", "json") };
        if let Some(path) = rfd::FileDialog::new().set_file_name(name).add_filter(filter, &[ext]).save_file() {
            self.message = export_records(&path, &self.sellers).map(|_| format!("Экспортировано: {}", path.display())).unwrap_or_else(|e| format!("Ошибка экспорта: {e}"));
        }
    }

    fn export_radar_as(&mut self) {
        if let Some(path) = rfd::FileDialog::new().set_file_name("niche_radar.csv").add_filter("CSV", &["csv"]).save_file() {
            self.message = write_niche_csv(&path, &self.snapshots).map(|_| format!("Экспортировано: {}", path.display())).unwrap_or_else(|e| format!("Ошибка экспорта: {e}"));
        }
    }

    fn sidebar(&mut self, ui: &mut egui::Ui) {
        ui.add_space(7.0);
        ui.horizontal(|ui| {
            ui.label(RichText::new("PR").size(25.0).strong().color(Color32::from_rgb(56,189,248)));
            ui.vertical(|ui| { ui.label(RichText::new("PLATI RADAR").strong()); ui.label(RichText::new("АНАЛИТИКА МАРКЕТПЛЕЙСА").small().color(Color32::GRAY)); });
        });
        ui.add_space(25.0);
        ui.label(RichText::new("РАБОЧЕЕ ПРОСТРАНСТВО").small().color(Color32::GRAY));
        ui.add_space(8.0);
        for (page, label) in [(Page::Deals,"⌕  Поиск предложений"),(Page::Seller,"◉  Профиль продавца"),(Page::Sellers,"▤  База продавцов"),(Page::Radar,"◌  Радар ниш")] {
            if ui.selectable_label(self.page == page, label).clicked() { self.page = page; }
        }
        ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
            ui.separator();
            if ui.small_button("Открыть папку данных").clicked() { self.open_output_dir(); }
            if ui.small_button("Выбрать папку данных…").clicked() { self.choose_output_dir(); }
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
        };
        ui.horizontal(|ui| {
            ui.vertical(|ui| { ui.heading(title); ui.label(RichText::new(desc).color(Color32::GRAY)); });
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| ui.label(RichText::new("●  РАБОТАЕТ ЛОКАЛЬНО").small().color(Color32::from_rgb(74,222,128))));
        });
        ui.add_space(8.0); ui.separator(); ui.add_space(8.0);
    }

    fn deals_view(&mut self, ui: &mut egui::Ui) {
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
            egui::CollapsingHeader::new("Фильтры и параметры").default_open(true).show(ui, |ui| {
                egui::Grid::new("search_config").num_columns(4).spacing([12.0,8.0]).show(ui, |ui| {
                    ui.vertical(|ui| {ui.label("Страниц (1–5)");ui.add(egui::DragValue::new(&mut self.pages).range(1..=5));});
                    ui.vertical(|ui| {ui.label("Мин. отзывов");ui.add(egui::DragValue::new(&mut self.min_reviews).range(0..=1_000_000));});
                    ui.vertical(|ui| {ui.label("Макс. негатив, %");ui.add(egui::DragValue::new(&mut self.max_negative).range(0.0..=100.0));});
                    ui.vertical(|ui| {ui.label("Проверить опции карточек");ui.add(egui::DragValue::new(&mut self.inspect_limit).range(0..=200));});
                    ui.vertical(|ui| {ui.label("Ключ тарифа");ui.text_edit_singleline(&mut self.keyword);});
                    ui.vertical(|ui| {ui.label("Минус-слова через запятую");ui.text_edit_singleline(&mut self.exclude);});
                    ui.vertical(|ui| {ui.label("Сортировка");egui::ComboBox::from_id_salt("sort").selected_text(sort_name(self.sort)).show_ui(ui,|ui|{for m in [SortMode::RealPrice,SortMode::StorePrice,SortMode::Reviews,SortMode::Smart]{ui.selectable_value(&mut self.sort,m,sort_name(m));}});});
                    ui.end_row();
                });
            });
        });
        self.message(ui);
        ui.horizontal(|ui| {
            ui.heading("Результаты");
            ui.label(RichText::new(format!("{} карточек",self.deals.len())).color(Color32::GRAY));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center),|ui|{
                ui.add(egui::TextEdit::singleline(&mut self.deals_filter).desired_width(200.0).hint_text("Фильтр названия / продавца"));
                if ui.button("CSV…").clicked() { self.export_deals_as(true); }
                if ui.button("JSON…").clicked() { self.export_deals_as(false); }
            });
        });
        card(ui,|ui|{
            egui::ScrollArea::both().max_height(500.0).show(ui,|ui|{
                egui::Grid::new("deal_rows").striped(true).spacing([12.0,8.0]).min_col_width(55.0).show(ui,|ui|{
                    ui.label(RichText::new("Карточка").strong().color(Color32::from_rgb(125,211,252)));
                    ui.label(RichText::new("Тариф").strong().color(Color32::from_rgb(125,211,252)));
                    for (head, mode) in [("Цена",SortMode::RealPrice),("Отзывы",SortMode::Reviews)] { if ui.selectable_label(self.sort==mode,RichText::new(head).strong()).clicked(){self.sort=mode;} }
                    ui.label(RichText::new("Продавец").strong().color(Color32::from_rgb(125,211,252)));
                    ui.label(RichText::new("Действия").strong().color(Color32::from_rgb(125,211,252)));
                    ui.end_row();
                    let f=self.deals_filter.to_lowercase();
                    let mut rows:Vec<ProductDeal>=self.deals.iter().filter(|d|f.is_empty()||d.title.to_lowercase().contains(&f)||d.seller_name.to_lowercase().contains(&f)).cloned().collect();
                    rows.sort_by(|a,b|sort_deals(a,b,self.sort));
                    for d in rows {
                        ui.vertical(|ui|{ui.add(egui::Label::new(RichText::new(&d.title).strong()).wrap());ui.hyperlink_to(format!("#{} · открыть ↗",d.item_id),&d.item_url);});
                        ui.label(RichText::new(&d.matched_tariff).small());
                        ui.vertical(|ui|{ui.label(RichText::new(format!("{:.0} ₽",d.real_tariff_price_rub)).strong().color(Color32::from_rgb(134,239,172)));if d.price_bait_diff_rub>0.0{ui.label(RichText::new(format!("витрина {:.0} ₽",d.storefront_price_rub)).small().color(Color32::GRAY));}});
                        ui.vertical(|ui|{ui.label(format!("+{} / -{}",d.positive_reviews,d.negative_reviews));ui.label(RichText::new(format!("негатив {:.1}% · возвраты {}",d.neg_percent,d.returns_count)).small().color(Color32::GRAY));});
                        ui.vertical(|ui|{ui.label(&d.seller_name);ui.label(RichText::new(format!("рейтинг {:.1} · продаж {}",d.seller_rating,d.sold_count)).small().color(Color32::GRAY));ui.hyperlink_to("Витрина ↗",&d.seller_url);});
                        ui.vertical(|ui|{
                            if ui.small_button("Опции и тарифы").clicked(){self.load_options(&d);}
                            if ui.small_button("Анализ продавца").clicked(){self.seller_url=d.seller_url.clone();self.seller=None;self.page=Page::Seller;}
                        }); ui.end_row();
                    }
                    if self.deals.is_empty(){ui.label(RichText::new("Запустите поиск, чтобы увидеть предложения.").color(Color32::GRAY));ui.end_row();}
                });
            });
        });
        if self.options_open { self.options_dialog(ui.ctx()); }
    }

    fn options_dialog(&mut self, ctx:&egui::Context) {
        let mut open=self.options_open;
        egui::Window::new("Варианты и тарифы").open(&mut open).default_width(520.0).show(ctx,|ui|{
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
        if let Some(s)=self.seller.as_ref(){
            card(ui,|ui|{
                ui.horizontal(|ui|{ui.vertical(|ui|{ui.heading(&s.seller_name);ui.label(format!("Продавец #{} · {}",s.seller_id,s.level.as_deref().unwrap_or("уровень не указан")));ui.hyperlink_to("Открыть витрину ↗",&s.profile_url);});ui.with_layout(egui::Layout::right_to_left(egui::Align::Center),|ui|ui.label(RichText::new(format!("★ {:.1}",s.rating.unwrap_or(0.0))).size(24.0).color(Color32::from_rgb(251,191,36))));});
                ui.separator();
                for (name,val) in [("Продажи",opt(s.sales_count)),("Отзывы",opt(s.reviews_count)),("Возвраты",opt(s.returns_count)),("Товаров",opt(s.items_count))]{ui.horizontal(|ui|{ui.label(RichText::new(val).strong().size(20.0).color(Color32::from_rgb(125,211,252)));ui.label(name);});}
                if let Some(t)=&s.tenure{ui.label(format!("На площадке: {t}"));}
                ui.label(RichText::new(if s.enriched_from_html{"Профиль обогащён HTML-данными"}else{"Доступна статистика Public API"}).small().color(Color32::GRAY));
            });
        }else{empty(ui,"Проверьте продавца","Статистика продаж, отзывы, возвраты, рейтинг и товары появятся здесь.");}
    }

    fn discover_view(&mut self,ui:&mut egui::Ui){
        card(ui,|ui|{
            ui.heading("Сбор продавцов по категориям");ui.label("Категории cats.xml и обогащение профилей HTML-данными.");
            ui.horizontal(|ui|{ui.vertical(|ui|{ui.label("Макс. категорий");ui.add(egui::DragValue::new(&mut self.max_categories).range(1..=389));});ui.vertical(|ui|{ui.label("Профилей обогатить");ui.add(egui::DragValue::new(&mut self.enrich_limit).range(0..=100));});if self.discover_busy{if ui.button("Отменить сбор").clicked(){self.cancel_discover();}}else if ui.button("Начать сбор").fill(Color32::from_rgb(2,132,199)).clicked(){self.send_discover();}});
            if self.discover_busy { if self.discover_progress.1>0 { let f=self.discover_progress.0 as f32/self.discover_progress.1 as f32;ui.add(egui::ProgressBar::new(f).show_percentage().text(format!("{} / {} категорий",self.discover_progress.0,self.discover_progress.1))); } ui.label(&self.discover_progress.2); }
        }); self.message(ui);
        ui.horizontal(|ui|{ui.heading("Продавцы");ui.label(RichText::new(format!("{} записей",self.sellers.len())).color(Color32::GRAY));ui.add(egui::TextEdit::singleline(&mut self.sellers_filter).desired_width(180.0).hint_text("Фильтр"));if ui.button("CSV…").clicked(){self.export_sellers_as(true);}if ui.button("JSON…").clicked(){self.export_sellers_as(false);}});
        card(ui,|ui|{egui::ScrollArea::both().max_height(590.0).show(ui,|ui|{
            egui::Grid::new("seller_list").striped(true).spacing([16.0,7.0]).show(ui,|ui|{
                ui.label(RichText::new("Продавец").strong().color(Color32::from_rgb(125,211,252)));
                for (title,sort) in [("Рейтинг",SellerSort::Rating),("Продажи",SellerSort::Sales),("Отзывы",SellerSort::Reviews)] { if ui.selectable_label(self.seller_sort==sort,title).clicked(){self.seller_sort=sort;} }
                ui.label(RichText::new("Возвраты").strong().color(Color32::from_rgb(125,211,252)));ui.label(RichText::new("Профиль").strong().color(Color32::from_rgb(125,211,252)));ui.end_row();
                let f=self.sellers_filter.to_lowercase();
                let mut records:Vec<SellerRecord>=self.sellers.iter().filter(|s|f.is_empty()||s.seller_name.to_lowercase().contains(&f)||s.seller_id.to_string().contains(&f)).cloned().collect();
                records.sort_by(|a,b|match self.seller_sort{SellerSort::Rating=>b.rating.unwrap_or(0.0).partial_cmp(&a.rating.unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal),SellerSort::Sales=>b.sales_count.cmp(&a.sales_count),SellerSort::Reviews=>b.reviews_count.cmp(&a.reviews_count)});
                for s in &records{
                    ui.label(RichText::new(&s.seller_name).strong());ui.label(format!("{:.1}",s.rating.unwrap_or(0.0)));ui.label(opt(s.sales_count));ui.label(opt(s.reviews_count));ui.label(opt(s.returns_count));ui.hyperlink_to("Открыть ↗",&s.profile_url);ui.end_row();
                }
            });
        });});
    }

    fn radar_view(&mut self,ui:&mut egui::Ui){
        card(ui,|ui|{
            ui.horizontal(|ui|{
                ui.vertical(|ui|{ui.heading("Ниши и продажи");let s=self.snapshots.first();ui.label(format!("Снимок: {} · {} категорий · {} товаров",s.map(|x|date(x.captured_at)).unwrap_or_else(||"нет".into()),s.map(|x|x.categories_scanned).unwrap_or(0),s.map(|x|x.products.len()).unwrap_or(0)));});
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center),|ui|{
                    if self.niche_running.load(Ordering::Acquire) {
                        if ui.button("Отменить сбор").clicked() { self.cancel_capture(); }
                    } else if ui.add(egui::Button::new("Обновить снимок").fill(Color32::from_rgb(2,132,199))).clicked() {
                        start_capture(self.client.clone(),self.out_dir.clone(),self.tx.clone(),self.niche_running.clone(),self.runtime.handle().clone(),self.capture_task.clone(),self.notify_growth,self.min_delta,self.max_sellers);
                    }
                });
            });
            if self.niche_running.load(Ordering::Acquire)||matches!(self.progress.status.as_str(),"starting"|"collecting"){
                let f=if self.progress.total_categories==0{0.0}else{self.progress.completed_categories as f32/self.progress.total_categories as f32};
                ui.add(egui::ProgressBar::new(f).show_percentage().text(format!("{} / {} категорий · {} страниц · {} товаров",self.progress.completed_categories,self.progress.total_categories,self.progress.completed_pages,self.progress.products_cached)));
                if !self.progress.current_category.is_empty(){ui.label(format!("Сейчас: {}",self.progress.current_category));}
            }
            if let Some(e)=&self.progress.error{ui.colored_label(Color32::LIGHT_RED,e);}
        }); self.message(ui);
        card(ui,|ui|{ui.label(RichText::new("История изменения продаж").strong());history_plot(ui,&self.history);});
        ui.horizontal_wrapped(|ui|{
            ui.heading("Возможности");ui.add(egui::TextEdit::singleline(&mut self.niche_filter).desired_width(150.0).hint_text("Товар / категория"));
            ui.label("Мин. Δ");ui.add(egui::DragValue::new(&mut self.min_delta).range(-1_000_000..=1_000_000));ui.label("Макс. продавцов");ui.add(egui::DragValue::new(&mut self.max_sellers).range(1..=100_000));
            ui.label("Макс. цена");ui.add(egui::DragValue::new(&mut self.max_price).range(0.0..=100_000_000.0));ui.label("Макс. наценка %");ui.add(egui::DragValue::new(&mut self.max_markup).range(-100.0..=100_000.0));
            egui::ComboBox::from_id_salt("radar_sort").selected_text(radar_sort_name(self.radar_sort)).show_ui(ui,|ui|{for s in [RadarSort::Opportunity,RadarSort::Delta,RadarSort::Price,RadarSort::Sellers]{ui.selectable_value(&mut self.radar_sort,s,radar_sort_name(s));}});
            ui.checkbox(&mut self.favorites_only,"Избранное");
            ui.checkbox(&mut self.notify_growth,"Оповещать о совпадениях");
            if ui.button("Сохранить CSV…").clicked(){self.export_radar_as();}
        });
        card(ui,|ui|{
            let mut trends=build_niche_trends(&self.snapshots);
            let f=self.niche_filter.to_lowercase();
            trends.retain(|t|t.delta_sales.unwrap_or(i64::MIN)>=self.min_delta&&t.sellers_count<=self.max_sellers&&t.product.price_rub<=self.max_price&&t.markup_percent.map(|x|x<=self.max_markup).unwrap_or(true)&&(!self.favorites_only||self.favorites.contains(&t.product.item_id))&&(f.is_empty()||t.product.title.to_lowercase().contains(&f)||t.product.category.to_lowercase().contains(&f)));
            trends.sort_by(|a,b|match self.radar_sort{RadarSort::Opportunity=>b.opportunity_score.partial_cmp(&a.opportunity_score).unwrap_or(std::cmp::Ordering::Equal),RadarSort::Delta=>b.delta_sales.unwrap_or(i64::MIN).cmp(&a.delta_sales.unwrap_or(i64::MIN)),RadarSort::Price=>a.product.price_rub.partial_cmp(&b.product.price_rub).unwrap_or(std::cmp::Ordering::Equal),RadarSort::Sellers=>a.sellers_count.cmp(&b.sellers_count)});
            trends.truncate(2000);
            egui::ScrollArea::both().max_height(480.0).show(ui,|ui|{
                egui::Grid::new("niche_table").striped(true).spacing([12.0,7.0]).show(ui,|ui|{
                    for h in ["★","Карточка / категория","Δ продаж","Цена","Продавцы","Наценка","Балл"]{ui.label(RichText::new(h).strong().color(Color32::from_rgb(125,211,252)));}ui.end_row();
                    for t in trends{
                        let yes=self.favorites.contains(&t.product.item_id);
                        if ui.button(if yes{"★"}else{"☆"}).clicked(){if yes{self.favorites.remove(&t.product.item_id);}else{self.favorites.insert(t.product.item_id);}let _=write_json_atomic(&self.out_dir.join("niche_favorites.json"),&self.favorites);}
                        ui.vertical(|ui|{ui.hyperlink_to(&t.product.title,&t.product.item_url);ui.label(RichText::new(&t.product.category).small().color(Color32::GRAY));});
                        ui.label(t.delta_sales.map(|d|format!("{d:+}")).unwrap_or_else(||"—".into()));ui.label(format!("{:.0} ₽",t.product.price_rub));ui.label(format!("{} / {}",t.sellers_count,t.listings_count));ui.label(t.markup_percent.map(|x|format!("{x:+.1}%")).unwrap_or_else(||"—".into()));ui.label(format!("{:.1}",t.opportunity_score));ui.end_row();
                    }
                });
            });
            if self.snapshots.len()<2{ui.label(RichText::new("Для сравнения нужны два снимка; первый запускается автоматически.").small().color(Color32::GRAY));}
        });
    }

    fn message(&mut self,ui:&mut egui::Ui){if !self.message.is_empty(){card(ui,|ui|{ui.label(RichText::new(&self.message).color(if self.message.starts_with("Ошибка"){Color32::LIGHT_RED}else{Color32::from_rgb(134,239,172)}));});}}
}

impl eframe::App for PlatiApp {
    fn update(&mut self,ctx:&egui::Context,_frame:&mut eframe::Frame){
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num1)){self.page=Page::Deals;}
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num2)){self.page=Page::Seller;}
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num3)){self.page=Page::Sellers;}
        if ctx.input(|i|i.modifiers.command&&i.key_pressed(egui::Key::Num4)){self.page=Page::Radar;}
        self.poll();
        egui::SidePanel::left("nav").exact_width(225.0).resizable(false).frame(egui::Frame::side_top_panel(&ctx.style()).fill(Color32::from_rgb(15,23,42)).inner_margin(egui::Margin::same(14))).show(ctx,|ui|self.sidebar(ui));
        egui::CentralPanel::default().frame(egui::Frame::central_panel(&ctx.style()).fill(Color32::from_rgb(10,15,28)).inner_margin(egui::Margin::symmetric(22,16))).show(ctx,|ui|{
            self.header(ui);egui::ScrollArea::vertical().auto_shrink([false,false]).show(ui,|ui|match self.page{Page::Deals=>self.deals_view(ui),Page::Seller=>self.seller_view(ui),Page::Sellers=>self.discover_view(ui),Page::Radar=>self.radar_view(ui)});
        });
        self.persist_settings(ctx);
        ctx.request_repaint_after(Duration::from_millis(500));
    }
}

fn start_capture(client:Client,out:PathBuf,tx:Sender<UiMessage>,running:Arc<AtomicBool>,handle:tokio::runtime::Handle,slot:Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>,notify:bool,min_delta:i64,max_sellers:usize){
    if running.compare_exchange(false,true,Ordering::AcqRel,Ordering::Acquire).is_err(){return;}
    let task_slot=slot.clone();
    let task=handle.spawn(async move{
        let r=capture_niche_snapshot(&client,&out).await.map(|_|()).map_err(|e|e.to_string());
        if r.is_ok() && notify {
            let snapshots=load_recent_niche_snapshots(&out).unwrap_or_default();
            let count=build_niche_trends(&snapshots).iter().filter(|t|t.delta_sales.unwrap_or(i64::MIN)>=min_delta&&t.sellers_count<=max_sellers).count();
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

fn read_json_or<T:for<'de>Deserialize<'de>>(path:PathBuf,fallback:T)->T{fs::read(path).ok().and_then(|raw|serde_json::from_slice(&raw).ok()).unwrap_or(fallback)}
fn sort_name(s:SortMode)->&'static str{match s{SortMode::RealPrice=>"Реальная цена",SortMode::StorePrice=>"Цена витрины",SortMode::Reviews=>"Отзывы",SortMode::Smart=>"Умный балл"}}
fn radar_sort_name(s:RadarSort)->&'static str{match s{RadarSort::Opportunity=>"Балл",RadarSort::Delta=>"Прирост",RadarSort::Price=>"Цена",RadarSort::Sellers=>"Конкуренция"}}
fn sort_deals(a:&ProductDeal,b:&ProductDeal,s:SortMode)->std::cmp::Ordering{match s{SortMode::RealPrice=>a.real_tariff_price_rub.partial_cmp(&b.real_tariff_price_rub).unwrap_or(std::cmp::Ordering::Equal),SortMode::StorePrice=>a.storefront_price_rub.partial_cmp(&b.storefront_price_rub).unwrap_or(std::cmp::Ordering::Equal),SortMode::Reviews=>b.positive_reviews.cmp(&a.positive_reviews),SortMode::Smart=>b.smart_score.partial_cmp(&a.smart_score).unwrap_or(std::cmp::Ordering::Equal)}}
fn opt(x:Option<u64>)->String{x.map(|v|v.to_string()).unwrap_or_else(||"—".into())}

fn theme(ctx:&egui::Context){let mut s=(*ctx.style()).clone();s.visuals=egui::Visuals::dark();s.visuals.panel_fill=Color32::from_rgb(10,15,28);s.visuals.window_fill=Color32::from_rgb(20,29,47);s.visuals.extreme_bg_color=Color32::from_rgb(13,20,35);s.visuals.widgets.inactive.bg_fill=Color32::from_rgb(30,41,59);s.visuals.widgets.hovered.bg_fill=Color32::from_rgb(41,61,82);s.visuals.selection.bg_fill=Color32::from_rgb(3,105,161);s.spacing.item_spacing=Vec2::new(9.0,8.0);ctx.set_style(s);}
fn card(ui:&mut egui::Ui,f:impl FnOnce(&mut egui::Ui)){egui::Frame::new().fill(Color32::from_rgb(20,29,47)).stroke(Stroke::new(1.0,Color32::from_rgb(37,51,72))).corner_radius(egui::CornerRadius::same(10)).inner_margin(egui::Margin::same(13)).show(ui,f);ui.add_space(9.0);}
fn empty(ui:&mut egui::Ui,title:&str,desc:&str){card(ui,|ui|{ui.vertical_centered(|ui|{ui.add_space(26.0);ui.label(RichText::new("◈").size(36.0).color(Color32::from_rgb(56,189,248)));ui.heading(title);ui.label(RichText::new(desc).color(Color32::GRAY));ui.add_space(26.0);});});}
fn date(ts:u64)->String{let age=unix_now().saturating_sub(ts);if age<3600{format!("{} мин. назад",age/60)}else if age<86_400{format!("{} ч. назад",age/3600)}else{format!("{} дн. назад",age/86_400)}}

fn history_plot(ui:&mut egui::Ui,history:&[NicheHistoryPoint]){
    let (r,p)=ui.allocate_painter(Vec2::new(ui.available_width(),125.0),egui::Sense::hover());let rect=r.rect.shrink(8.0);p.rect_filled(rect,egui::CornerRadius::same(5),Color32::from_rgb(13,20,35));
    if history.is_empty(){p.text(rect.center(),egui::Align2::CENTER_CENTER,"История появится после следующих снимков",egui::FontId::proportional(13.0),Color32::GRAY);return;}
    let max=history.iter().map(|x|x.delta_sales).max().unwrap_or(1).max(1) as f32;let left=rect.left()+15.0;let right=rect.right()-10.0;let top=rect.top()+10.0;let bottom=rect.bottom()-10.0;
    let pts:Vec<_>=history.iter().enumerate().map(|(i,x)|{let xx=if history.len()==1{(left+right)/2.0}else{egui::lerp(left..=right,i as f32/(history.len()-1)as f32)};egui::pos2(xx,egui::lerp(bottom..=top,x.delta_sales as f32/max))}).collect();
    if pts.len()>1{p.add(egui::Shape::line(pts.clone(),Stroke::new(2.0,Color32::from_rgb(56,189,248))));}
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
    let options=eframe::NativeOptions{viewport:egui::ViewportBuilder::default().with_inner_size(size).with_min_inner_size([980.0,660.0]).with_icon(Arc::new(app_icon())),..Default::default()};
    eframe::run_native("Plati Radar",options,Box::new(move|cc|Ok(Box::new(PlatiApp::new(cc,settings))))).map_err(|e|anyhow::anyhow!("Не удалось запустить Plati Radar: {e}"))
}
