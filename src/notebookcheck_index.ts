import axios from 'axios';
import * as cheerio from 'cheerio';

// ══════════════════════════════════════════════════════════════════════════════
//  NOTEBOOKCHECK REVIEW INDEX — VERCEL SERVERLESS EDITION
//
//  KEY INSIGHT: Vercel = every request is a fresh process. Zero in-memory state.
//  ALL state lives in Upstash Redis. Every function reads/writes Redis directly.
//
//  REDIS KEYS:
//    nbc:index:v3:entries        → { [url]: IndexEntry }  (full index)
//    nbc:index:v3:crawl_stats    → CrawlStats
//    nbc:index:v3:crawl_lock     → "1" with TTL (prevent concurrent crawls)
//    nbc:index:v3:crawl_progress → { page, totalUrls, updatedAt }
//
//  CRAWL STRATEGY — two modes:
//  1. crawlSync(startPage, maxPages)  → crawls N pages per invocation
//     Vercel Pro: 300s timeout → ~250 pages  |  Hobby: 10s → ~10 pages
//     Call repeatedly: crawlSync(1,40), crawlSync(41,40), crawlSync(81,40)...
//  2. crawlOnePage(page)              → one page per call, client chains them
// ══════════════════════════════════════════════════════════════════════════════

const INDEX_VERSION = 'v3';
const ENTRIES_KEY   = `nbc:index:${INDEX_VERSION}:entries`;
const STATS_KEY     = `nbc:index:${INDEX_VERSION}:crawl_stats`;
const LOCK_KEY      = `nbc:index:${INDEX_VERSION}:crawl_lock`;
const PROGRESS_KEY  = `nbc:index:${INDEX_VERSION}:crawl_progress`;
const ENTRIES_TTL   = 30 * 24 * 3600;
const LOCK_TTL      = 300;

// Smartphone-specific listing pages on NotebookCheck
// Reviews.55.0.html = all reviews (mostly laptops), Smartphones.155.0.html = phones only
const NBC_REVIEWS_BASE  = 'https://www.notebookcheck.net/Reviews.55.0.html';
const NBC_PHONES_BASE   = 'https://www.notebookcheck.net/Smartphones.155.0.html';
// NBC Library: device database pages (spec/aggregator pages for devices NBC tracks but
// hasn't written a full review for, e.g. Vivo-X200.919417.0.html). These never appear
// on the Smartphones listing but ARE discoverable via the Library filtered to smartphones.
// NBC Library — all device types, paginated by date. extractPhoneUrls filters out
// laptops and tablets, keeping only phone aggregator pages like Vivo-X200.919417.0.html.
// This is the correct second source: no stype param needed, filtering is done in code.
const NBC_LIBRARY_BASE = 'https://www.notebookcheck.net/Library.279.0.html';

// ── TYPES ─────────────────────────────────────────────────────────────────────

export type ScrapeStatus = 'pending' | 'scraping' | 'done' | 'error';

export interface IndexEntry {
  url: string; title: string; brand: string; slug: string;
  discoveredAt: string; status: ScrapeStatus;
  scrapedAt?: string; errorMsg?: string; retries: number;
}

export interface CrawlStats {
  totalPages: number; totalUrls: number; newUrls: number;
  crawlMs: number; lastCrawledAt: string; error?: string;
}

export interface ScrapeQueueStats {
  total: number; pending: number; scraping: number;
  done: number; error: number; coverage: string;
}

export interface CrawlProgress {
  page: number; totalUrls: number; startedAt: string; updatedAt: string;
}

// ── REDIS ─────────────────────────────────────────────────────────────────────

const _rax = axios.create({ timeout: 6000 });

function rBase() {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) throw new Error('UPSTASH_REDIS_REST_URL / TOKEN not set');
  return { url, token };
}

async function rGet(k: string): Promise<unknown> {
  const { url, token } = rBase();
  const r = await _rax.get(`${url}/get/${encodeURIComponent(k)}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const val = r.data?.result;
  if (val == null) throw new Error(`redis:miss:${k}`);
  return JSON.parse(val);
}

async function rSet(k: string, v: unknown, ttl = 86400): Promise<void> {
  const { url, token } = rBase();
  await _rax.post(`${url}/pipeline`,
    [['SET', k, JSON.stringify(v), 'EX', ttl]],
    { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } },
  );
}

async function rDel(k: string): Promise<void> {
  try {
    const { url, token } = rBase();
    await _rax.post(`${url}/pipeline`,
      [['DEL', k]],
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } });
  } catch { /* ignore */ }
}

async function rDelForce(keys: string[]): Promise<void> {
  const { url, token } = rBase();
  await _rax.post(`${url}/pipeline`,
    keys.map(k => ['DEL', k]),
    { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } });
}

async function rSetPermanent(k: string, v: unknown): Promise<void> {
  const { url, token } = rBase();
  await _rax.post(`${url}/pipeline`,
    [['SET', k, JSON.stringify(v)]],
    { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } },
  );
}

async function rSetNX(k: string, v: string, ttl: number): Promise<boolean> {
  try {
    const { url, token } = rBase();
    const r = await _rax.post(`${url}/pipeline`,
      [['SET', k, v, 'EX', ttl, 'NX']],
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } },
    );
    return r.data?.[0]?.[1] === 'OK';
  } catch { return false; }
}

// ── ENTRY STORE ───────────────────────────────────────────────────────────────

async function loadEntries(): Promise<Record<string, IndexEntry>> {
  try { return await rGet(ENTRIES_KEY) as Record<string, IndexEntry>; }
  catch { return {}; }
}

async function saveEntries(e: Record<string, IndexEntry>): Promise<void> {
  await rSetPermanent(ENTRIES_KEY, e);
}

// ── HTTP ──────────────────────────────────────────────────────────────────────

async function fetchHtml(url: string, ms = 15000): Promise<string> {
  for (let i = 0; i < 3; i++) {
    try {
      const { data } = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          Referer: 'https://www.notebookcheck.net/',
        },
        timeout: ms, maxRedirects: 3,
      });
      return typeof data === 'string' ? data : JSON.stringify(data);
    } catch (e: any) {
      if (e?.response?.status >= 400 && e?.response?.status < 500) throw e;
      if (i === 2) throw e;
      await new Promise(r => setTimeout(r, 600 * (i + 1)));
    }
  }
  throw new Error('fetchHtml: retries exhausted');
}

// ── BRAND ─────────────────────────────────────────────────────────────────────

const KNOWN_BRANDS = [
  'Apple','Samsung','Google','OnePlus','Xiaomi','Oppo','Vivo','Realme',
  'Motorola','Sony','Asus','Honor','Huawei','Nothing','Nokia','HTC','LG',
  'ZTE','TCL','Infinix','Tecno','Itel','Fairphone','Blackview','Ulefone',
  'Doogee','Cubot','Oukitel','Umidigi','BLU','Wiko','Lenovo','BlackBerry',
  'Sharp','Meizu','Nubia','Poco','Redmi','iQOO','Lava','Panasonic',
  'Unihertz','Crosscall','AGM','Cat','Gigaset','Alcatel','Energizer','BQ',
];

const GENERIC = new Set([
  'smartphone','phone','review','test','the','a','an','with','for','new',
  'best','top','chic','slim','pro','plus','ultra','max','ai','this','most',
  'and','or','in','on','at','by','of','to','is','unusual','design',
]);

function extractBrand(title: string): string {
  const lo = title.toLowerCase();
  for (const b of KNOWN_BRANDS) if (lo.includes(b.toLowerCase())) return b;
  for (const w of title.trim().split(/\s+/)) {
    const c = w.toLowerCase().replace(/[^a-z]/g, '');
    if (c.length >= 3 && !GENERIC.has(c) && /^[A-Z]/.test(w)) return w;
  }
  return 'Unknown';
}

// ── URL EXTRACTOR ─────────────────────────────────────────────────────────────

export function extractPhoneUrls(html: string): Array<{ url: string; title: string }> {
  const $ = cheerio.load(html);
  const out: Array<{ url: string; title: string }> = [];
  const seen = new Set<string>();

  $('a[href]').each((_, el) => {
    let href = $(el).attr('href') || '';
    if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
    else if (!href.startsWith('http')) return;
    href = href.split('?')[0];

    if (!href.includes('notebookcheck.net')) return;
    if (!/\.\d{4,}\.0\.html$/.test(href)) return;

    const slug = href.split('/').pop() || '';
    // Skip known listing/navigation pages
    if (/^(Reviews|Smartphones|Search|Topics|RSS|index|Notebooks|News|Smartphone|Library|Comparison)\./i.test(slug)) return;
    if (/-Series\./i.test(slug)) return; // exclude series overview pages (e.g. Xiaomi-17-Series.1176125.0.html)

    // Exclude laptops, CPU/GPU analyses and non-phone hardware immediately
    // Tablets: -Pad-, Galaxy-Tab, iPad, MatePad, MediaPad, -Tab-, Lenovo Tab, etc.
    const tabletSlug = /[-_]pad[-_.0]|[-_]tab[-_.0]|ipad|galaxy[-_]tab|matepad|mediapad|magicpad|lenovo[-_]tab|honor[-_]pad|xiaomi[-_]pad|realme[-_]pad|oppo[-_]pad|oneplus[-_]pad|iqoo[-_]pad|tcl[-_]tab|nokia[-_]tab/i.test(slug);
    const notAPhone = tabletSlug || /headphone|earphone|microphone|vacuum|robot|calendar|smartwatch|tablet|laptop|notebook|macbook|chromebook|charger|powerbank|earbuds|speaker|monitor|drone|keyboard|mouse|printer|router|modem|television|projector|cpu[-_]analysis|gpu[-_]analysis|thinkpad|ideapad|vivobook|zenbook|matebook|xps-|inspiron|pavilion|envy|spectre|elitebook|probook|razer-blade|apple-m[0-9][-_]/i.test(slug);
    if (notAPhone) return;

    // Accept two URL patterns:
    //   1. Full NBC review:       "Vivo-X200-FE-review.1114877.0.html"  (-review in slug)
    //   2. Aggregator/spec page:  "Vivo-X200.919417.0.html"             (Brand-Model.ID.0.html)
    //      NBC tracks these devices and aggregates external reviews + specs even without
    //      writing their own review. Useful for base models, regional variants, etc.
    const isReviewUrl       = /[-_]review\.\d{4,}\.0\.html$/i.test(slug);
    const hasPhoneKeyword   = /smartphone|iphone|(?<![a-z])phone(?![a-z])|mobile|handset/i.test(slug);
    const looksLikePhoneModel = /^(samsung[-_]galaxy|google[-_]pixel|oneplus|xiaomi|oppo|vivo|realme|motorola[-_]moto|sony[-_]xperia|honor|huawei|nothing[-_]phone|nokia|poco|redmi|iqoo|tcl|infinix|tecno|lava|blackberry|meizu|nubia|fairphone|ulefone|doogee|blackview|oukitel|umidigi|blu|alcatel|zte|unihertz|crosscall|agm)/i.test(slug);

    if (!isReviewUrl && !hasPhoneKeyword && !looksLikePhoneModel) return;

    if (seen.has(href.toLowerCase())) return;
    seen.add(href.toLowerCase());

    const title = ($(el).attr('title') || $(el).text().trim() || '').replace(/\s+/g, ' ').trim().slice(0, 200);
    if (title.length < 5) return;

    out.push({ url: href, title });
  });

  return out;
}

// ══════════════════════════════════════════════════════════════════════════════
//  CRAWL ONE PAGE
// ══════════════════════════════════════════════════════════════════════════════

export interface CrawlPageResult {
  page: number; phonesFound: number; totalUrls: number;
  newUrls: number; done: boolean; nextPage: number | null; durationMs: number;
}

export async function crawlOnePage(page: number): Promise<CrawlPageResult> {
  const t0  = Date.now();

  // ── Source 1: Smartphones review listing ─────────────────────────────────
  // Full NBC-written reviews, sorted by date. Paginated via ?&ns_page=N.
  const reviewsUrl = page === 1 ? NBC_PHONES_BASE : `${NBC_PHONES_BASE}?&ns_page=${page}`;

  // ── Source 2: NBC Library (all-device external review aggregator) ─────────
  // Mixed feed of phones + tablets + laptops, sorted by date added.
  // extractPhoneUrls filters out tablets and laptops, keeping only phone
  // aggregator pages like Vivo-X200.919417.0.html that never appear in Source 1.
  const libraryUrl = page === 1 ? NBC_LIBRARY_BASE : `${NBC_LIBRARY_BASE}?&ns_page=${page}`;

  const [reviewsHtml, libraryHtml] = await Promise.all([
    fetchHtml(reviewsUrl),
    (async () => { try { return await fetchHtml(libraryUrl, 12000); } catch { return ''; } })(),
  ]);

  const foundFromReviews = extractPhoneUrls(reviewsHtml);
  const foundFromLibrary = extractPhoneUrls(libraryHtml);

  // Deduplicate — same device can appear in both sources
  const seenUrls = new Set(foundFromReviews.map(f => f.url.toLowerCase()));
  const dedupedLibrary = foundFromLibrary.filter(f => !seenUrls.has(f.url.toLowerCase()));
  const found = [...foundFromReviews, ...dedupedLibrary];

  // Done when BOTH sources have no links (a reviews page full of laptops is not done)
  const reviewsRaw = (reviewsHtml.match(/\.notebookcheck\.net\/[^"']+\.\d+\.0\.html/g) || []).length;
  const libraryRaw = (libraryHtml.match(/\.notebookcheck\.net\/[^"']+\.\d+\.0\.html/g) || []).length;
  const pageIsEmpty = reviewsRaw === 0 && libraryRaw === 0;

  const entries = await loadEntries();
  let newUrls = 0;
  for (const { url: u, title } of found) {
    if (!entries[u]) {
      entries[u] = { url: u, title, brand: extractBrand(title), slug: u.split('/').pop() || '',
        discoveredAt: new Date().toISOString(), status: 'pending', retries: 0 };
      newUrls++;
    }
  }
  await saveEntries(entries);

  const totalUrls = Object.keys(entries).length;
  const progress: CrawlProgress = { page, totalUrls, startedAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  await rSet(PROGRESS_KEY, progress, LOCK_TTL);

  return { page, phonesFound: found.length, totalUrls, newUrls, done: pageIsEmpty, nextPage: pageIsEmpty ? null : page + 1, durationMs: Date.now() - t0 };
}

// ══════════════════════════════════════════════════════════════════════════════
//  SYNC CRAWL — N pages per invocation
// ══════════════════════════════════════════════════════════════════════════════

export async function crawlSync(startPage = 1, maxPages = 40, delayMs = 600): Promise<CrawlStats & { nextPage: number | null }> {
  const t0 = Date.now();

  // Force-clear any stale lock before acquiring — prevents permanent lock-out.
  try { await rDelForce([LOCK_KEY, PROGRESS_KEY]); } catch { /* ignore */ }

  const dynamicTtl = Math.min(maxPages * 60 + 60, 3600);
  const locked = await rSetNX(LOCK_KEY, '1', dynamicTtl);
  if (!locked) {
    const stats = await getLastCrawlStats();
    return {
      ...(stats ?? { totalPages: 0, totalUrls: 0, newUrls: 0, crawlMs: 0, lastCrawledAt: new Date().toISOString() }),
      error: 'Crawl lock active — another crawl is running', nextPage: null,
    };
  }

  let page          = startPage;
  let newUrls       = 0;
  let pagesRead     = 0;
  let lastTotalUrls = 0;
  let crawlDone     = false;

  try {
    while (pagesRead < maxPages) {
      if (pagesRead > 0) await new Promise(r => setTimeout(r, delayMs));
      const result = await crawlOnePage(page);
      newUrls      += result.newUrls;
      pagesRead++;
      lastTotalUrls = result.totalUrls;
      console.log(`[crawl] p${page} → ${result.phonesFound} phones (total: ${result.totalUrls})`);
      if (result.done) { crawlDone = true; break; }
      page++;
    }

    const nextPage = crawlDone ? null : page + 1;
    const stats: CrawlStats & { nextPage: number | null } = {
      totalPages: pagesRead, totalUrls: lastTotalUrls, newUrls,
      crawlMs: Date.now() - t0, lastCrawledAt: new Date().toISOString(), nextPage,
    };
    await rSet(STATS_KEY, stats, ENTRIES_TTL);
    await rDelForce([LOCK_KEY, PROGRESS_KEY]);
    return stats;

  } catch (e: any) {
    await rDelForce([LOCK_KEY, PROGRESS_KEY]);
    const stats: CrawlStats & { nextPage: number | null } = {
      totalPages: pagesRead, totalUrls: lastTotalUrls, newUrls,
      crawlMs: Date.now() - t0, lastCrawledAt: new Date().toISOString(),
      error: e.message, nextPage: null,
    };
    await rSet(STATS_KEY, stats, ENTRIES_TTL);
    return stats;
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  READ FUNCTIONS
// ══════════════════════════════════════════════════════════════════════════════

export async function getLastCrawlStats(): Promise<CrawlStats | null> {
  try { return await rGet(STATS_KEY) as CrawlStats; } catch { return null; }
}

export async function getCrawlProgress(): Promise<CrawlProgress | null> {
  try { return await rGet(PROGRESS_KEY) as CrawlProgress; } catch { return null; }
}

export async function getCrawlInProgress(): Promise<boolean> {
  try { await rGet(LOCK_KEY); return true; } catch { return false; }
}

export async function getQueueStats(): Promise<ScrapeQueueStats> {
  const entries = await loadEntries();
  let pending = 0, scraping = 0, done = 0, error = 0;
  for (const e of Object.values(entries)) {
    if (e.status === 'pending')  pending++;
    else if (e.status === 'scraping') scraping++;
    else if (e.status === 'done')     done++;
    else if (e.status === 'error')    error++;
  }
  const total = Object.keys(entries).length;
  const pct   = total > 0 ? ((done / total) * 100).toFixed(1) : '0.0';
  return { total, pending, scraping, done, error, coverage: `${done}/${total} (${pct}%)` };
}

export async function getBrandBreakdown(): Promise<Record<string, { total: number; done: number; pending: number; error: number }>> {
  const entries = await loadEntries();
  const result: Record<string, { total: number; done: number; pending: number; error: number }> = {};
  for (const e of Object.values(entries)) {
    if (!result[e.brand]) result[e.brand] = { total: 0, done: 0, pending: 0, error: 0 };
    result[e.brand].total++;
    if (e.status === 'done')     result[e.brand].done++;
    else if (e.status === 'error') result[e.brand].error++;
    else result[e.brand].pending++;
  }
  return Object.fromEntries(Object.entries(result).sort((a, b) => b[1].total - a[1].total));
}

export async function getIndexEntries(opts: { status?: ScrapeStatus | 'all'; brand?: string; page?: number; limit?: number; search?: string } = {}): Promise<{ entries: IndexEntry[]; total: number; page: number; limit: number; pages: number }> {
  const { status = 'all', brand, page = 1, limit = 50, search } = opts;
  let arr = Object.values(await loadEntries());
  if (status !== 'all')  arr = arr.filter(e => e.status === status);
  if (brand)             arr = arr.filter(e => e.brand.toLowerCase().includes(brand.toLowerCase()));
  if (search)            arr = arr.filter(e => e.title.toLowerCase().includes(search.toLowerCase()) || e.url.toLowerCase().includes(search.toLowerCase()));
  const order: Record<ScrapeStatus, number> = { pending: 0, error: 1, scraping: 2, done: 3 };
  arr.sort((a, b) => order[a.status] - order[b.status] || a.title.localeCompare(b.title));
  const total = arr.length;
  return { entries: arr.slice((page - 1) * limit, page * limit), total, page, limit, pages: Math.ceil(total / limit) };
}

export async function getEntry(url: string): Promise<IndexEntry | null> {
  const entries = await loadEntries();
  return entries[url] ?? null;
}

// ══════════════════════════════════════════════════════════════════════════════
//  WRITE FUNCTIONS
// ══════════════════════════════════════════════════════════════════════════════

// ── SEARCH INDEX ─────────────────────────────────────────────────────────────
// Searches all indexed entries for the best match.
// Uses a Redis-cached search index (flat list of {url, title, slug}) to avoid
// loading the full entries object on every request.

const SEARCH_INDEX_KEY = `nbc:index:v4:search_index`;

export async function rebuildSearchIndex(): Promise<void> {
  const entries = await loadEntries();
  // Store raw title (snippet included) — cleanIndexTitle() strips it at query time
  const flat = Object.values(entries).map((e: IndexEntry) => ({ url: e.url, title: e.title, slug: e.slug }));
  await rSetPermanent(SEARCH_INDEX_KEY, flat);
}

// Strip the stored snippet from the title — index stores "88% Clean Title...snippet text"
// We only want to match against the clean review title, not the preview blurb
function cleanIndexTitle(raw: string): string {
  // Remove leading rating like "88% "
  let t = raw.replace(/^\d+%\s*/, '');
  // The clean title ends at the first occurrence of the subtitle/snippet separator
  // Titles look like: "Samsung Galaxy S25 Ultra review - The AI phone...SubtitleText"
  // Snippets are appended directly after the title with no separator in some entries
  // Safest: take only up to the first sentence end or 120 chars of the first segment
  // Split on common title-end patterns: " review" boundary or truncate after ~100 chars of title
  const reviewIdx = t.search(/\breview\b/i);
  if (reviewIdx !== -1) {
    // Include "review" and a few chars after (e.g. " review - subtitle") but cut the snippet
    const dashIdx = t.indexOf(' - ', reviewIdx);
    t = dashIdx !== -1 ? t.slice(0, dashIdx) : t.slice(0, reviewIdx + 10);
  } else {
    t = t.slice(0, 120);
  }
  return t.toLowerCase().trim();
}

// Pre-compiled word boundary regex cache: avoids re-compiling the same regex
// for every entry (1800+ iterations) on every request.
const _wordBoundaryCache = new Map<string, RegExp>();
function wordBoundaryRe(word: string): RegExp {
  let re = _wordBoundaryCache.get(word);
  if (!re) {
    re = new RegExp(`(?<![a-z0-9])${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?![a-z0-9])`);
    _wordBoundaryCache.set(word, re);
  }
  return re;
}

// Same scoring as GSMArena resolver — all words must match, variant penalty system
function scoreIndexMatch(entryTitle: string, query: string): number {
  // Score only against the clean title, NOT the snippet (which may contain false word matches)
  const d = cleanIndexTitle(entryTitle);
  const q = query.toLowerCase().trim();
  const qWords = q.split(/\s+/).filter((w: string) => w.length > 1);

  // ALL query words must appear as whole words in the clean title — hard reject otherwise
  // Use word boundary check: " word " or start/end — prevents "ultra" matching "ultra-slim"
  const wordIn = (word: string, text: string) => wordBoundaryRe(word).test(text);

  if (!qWords.every((w: string) => wordIn(w, d))) return -1;

  // Exact or substring match
  if (d === q) return 10000;
  if (d.includes(q)) return 8000;

  // Penalise entries that have variant words the query didn't ask for
  const variants = ['ultra', 'pro', 'plus', 'mini', 'lite', 'fe', 'max', 'edge', 'standard', 'turbo', 'fold', 'flip', 'xl', 'xr', 'se', '5g', '4g'];
  const lastQWord = qWords[qWords.length - 1];
  let penalty = 0;
  for (const v of variants) {
    if (v !== lastQWord && wordIn(v, d) && !q.includes(v)) penalty += 2000;
  }

  // Bonus for shorter title (fewer extra words = more precise match)
  const lengthBonus = Math.max(0, 500 - d.length * 5);

  return 5000 - penalty + lengthBonus;
}

export async function searchIndex(q: string): Promise<{ url: string; title: string } | null> {
  // Normalize query the same way SearXNG does (alias resolution, brand expansion, etc.)
  const { normalizeQuery } = await import('./notebookcheck');
  const normalized = normalizeQuery(q).toLowerCase().trim();

  // Load compact search index from Redis
  let flat: Array<{ url: string; title: string; slug: string }> = [];
  try {
    flat = await rGet(SEARCH_INDEX_KEY) as any[];
  } catch {
    await rebuildSearchIndex();
    try { flat = await rGet(SEARCH_INDEX_KEY) as any[]; } catch { return null; }
  }
  if (!flat?.length) return null;

  let best: { url: string; title: string } | null = null;
  let bestScore = -1;

  for (const entry of flat) {
    const score = scoreIndexMatch(entry.title, normalized);
    if (score > bestScore) {
      bestScore = score;
      best = { url: entry.url, title: entry.title };
    }
  }

  // Require a confident match — penalised scores (e.g. wrong variant) must not win.
  // Base threshold: 3000. A clean full-word match scores 5000+; a variant-penalised
  // match scores 3000 or less and should fall through to SearXNG.
  const MIN_SCORE = 3500;
  if (bestScore < MIN_SCORE || !best) return null;

  // Hard-reject: if the query contains NO variant suffix but the winning entry does,
  // the user asked for a base model that isn't in the index — don't return a wrong variant.
  const VARIANT_SUFFIXES = ['ultra', 'pro', 'plus', 'mini', 'lite', 'fe', 'max', 'edge',
    'standard', 'turbo', 'fold', 'flip', 'xl', 'xr', 'se', '5g', '4g'];
  const queryHasVariant = VARIANT_SUFFIXES.some(v => wordBoundaryRe(v).test(normalized));
  if (!queryHasVariant) {
    const cleanBestTitle = cleanIndexTitle(best.title).toLowerCase();
    const titleHasExtraVariant = VARIANT_SUFFIXES.some(
      v => wordBoundaryRe(v).test(cleanBestTitle) && !wordBoundaryRe(v).test(normalized)
    );
    if (titleHasExtraVariant) return null; // let SearXNG handle it
  }

  return best;
}


// ── SCRAPE INDEXED DEVICE ────────────────────────────────────────────────────
export async function clearScrapeCache(url: string): Promise<void> {
  const { clearDeviceCache } = await import('./notebookcheck');
  await clearDeviceCache(url);
}

export async function scrapeIndexedDevice(url: string): Promise<{ success: boolean; data?: any; error?: string; cached?: boolean }> {
  // scrapeNotebookCheckDevice has its own Redis cache (nbc:device:...) — use it directly
  // Avoids: double caching, double Redis round-trips, loading 1686 entries just for status update
  try {
    const { scrapeNotebookCheckDevice } = await import('./notebookcheck');
    const t0 = Date.now();
    const data = await scrapeNotebookCheckDevice(url);
    const ms = Date.now() - t0;
    // If scrape took <200ms it almost certainly came from Redis cache
    const cached = ms < 200;

    return { success: true, data, cached };
  } catch (e: any) {
    return { success: false, error: e?.message ?? String(e) };
  }
}

export async function resetErrors(): Promise<number> {
  const entries = await loadEntries();
  let count = 0;
  for (const [url, entry] of Object.entries(entries)) {
    if (entry.status === 'error') { entries[url] = { ...entry, status: 'pending', retries: 0, errorMsg: undefined }; count++; }
  }
  await saveEntries(entries);
  return count;
}

export async function resetEntry(url: string): Promise<boolean> {
  const entries = await loadEntries();
  if (!entries[url]) return false;
  entries[url] = { ...entries[url], status: 'pending', retries: 0, errorMsg: undefined, scrapedAt: undefined };
  await saveEntries(entries);
  return true;
}

export async function clearIndex(): Promise<void> {
  await rDel(ENTRIES_KEY); await rDel(STATS_KEY);
  await rDel(PROGRESS_KEY); await rDel(LOCK_KEY);
}

export async function resetCrawlLock(): Promise<void> {
  await rDel(LOCK_KEY); await rDel(PROGRESS_KEY);
}

export async function validateIndexUrl(url: string): Promise<{ url: string; valid: boolean; h1?: string; error?: string; checkMs: number }> {
  const entries = await loadEntries();
  const entry   = entries[url];
  const t0 = Date.now();
  try {
    const html = await fetchHtml(url, 10000);
    const $ = cheerio.load(html);
    const h1 = $('h1').first().text().trim();
    const tokens = (entry?.title ?? '').toLowerCase().replace(/[^a-z0-9\s]/g, '').split(/\s+/)
      .filter(t => t.length >= 3 && !['the','and','for','with','review','test','smartphone'].includes(t));
    const matches = tokens.filter(t => h1.toLowerCase().includes(t)).length;
    const valid = h1.length > 3 && (tokens.length === 0 || matches >= Math.ceil(tokens.length * 0.4));
    return { url, valid, h1, checkMs: Date.now() - t0 };
  } catch (e: any) {
    return { url, valid: false, error: e?.message, checkMs: Date.now() - t0 };
  }
}