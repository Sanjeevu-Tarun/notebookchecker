import axios from 'axios';
import * as cheerio from 'cheerio';
import * as http from 'http';
import * as https from 'https';

// ══════════════════════════════════════════════════════════════════════════════
//  NOTEBOOKCHECK REVIEW INDEX CRAWLER
//
//  PURPOSE:
//  ─────────────────────────────────────────────────────────────────────────────
//  Instead of using SearXNG to find each device URL one-by-one (slow, rate-limited),
//  this module crawls the NBC Reviews listing page with the smartphone filter to
//  collect ALL review URLs upfront. Those URLs are then fed directly to
//  scrapeNotebookCheckDevice() — bypassing SearXNG entirely for bulk scraping.
//
//  FLOW:
//  ─────
//  1. crawlNBCSmartphoneIndex()
//       → fetch https://www.notebookcheck.net/Reviews.55.0.html?cat=Smartphones&page=N
//       → extract all review URLs from each paginated page
//       → store in IndexEntry[] with title, url, brand, discovered timestamp
//
//  2. scrapeIndexedDevice(url, name)
//       → calls scrapeNotebookCheckDevice() directly (no SearXNG)
//       → tracks scrape status per URL (pending / scraping / done / error)
//
//  3. bulkScrapeAll(concurrency, onProgress)
//       → processes the index queue with controlled concurrency
//       → respects NBC rate limits (delay between requests)
//
//  STORAGE:
//  ─────────
//  In-memory Map (survives process lifetime, resets on restart).
//  Redis is used for persistence if UPSTASH_REDIS_REST_URL is configured:
//    Key: "nbc:index:v1:urls"        → JSON array of IndexEntry
//    Key: "nbc:index:v1:scrape:{url}" → scraped NBCDeviceData
//
//  The index itself is small (~2000 entries × ~100 bytes = ~200KB), safe to
//  keep fully in memory and in Redis.
// ══════════════════════════════════════════════════════════════════════════════

// ── LOGGER ────────────────────────────────────────────────────────────────────
type LogLevel = 'debug' | 'info' | 'warn' | 'error';
function log(level: LogLevel, msg: string, meta?: Record<string, unknown>): void {
  if (process.env.NODE_ENV === 'test' && (level === 'debug' || level === 'info')) return;
  const entry = { ts: new Date().toISOString(), level, msg, ...meta };
  (level === 'error' || level === 'warn' ? console.error : console.log)(JSON.stringify(entry));
}

// ── SHARED HTTP AGENT ─────────────────────────────────────────────────────────
const _httpAgent  = new (require('http').Agent)({ keepAlive: true, maxSockets: 10, maxFreeSockets: 5 });
const _httpsAgent = new (require('https').Agent)({ keepAlive: true, maxSockets: 10, maxFreeSockets: 5 });
const sharedAxios = axios.create({
  httpAgent: _httpAgent,
  httpsAgent: _httpsAgent,
  maxRedirects: 3,
  decompress: true,
});

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
];
function randomUA() { return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]; }

// ── REDIS HELPERS ─────────────────────────────────────────────────────────────
async function redisGet(k: string): Promise<unknown | null> {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  try {
    const resp = await sharedAxios.get(`${url}/get/${encodeURIComponent(k)}`, {
      headers: { Authorization: `Bearer ${token}` }, timeout: 3000,
    });
    const val = resp.data?.result;
    return val ? JSON.parse(val) : null;
  } catch (e) { log('warn', 'redis.get failed', { key: k, err: (e as Error).message }); return null; }
}

async function redisSet(k: string, d: unknown, ttlSec = 86400): Promise<void> {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return;
  try {
    await sharedAxios.post(
      `${url}/pipeline`,
      [['SET', k, JSON.stringify(d), 'EX', ttlSec]],
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, timeout: 3000 },
    );
  } catch (e) { log('warn', 'redis.set failed', { key: k, err: (e as Error).message }); }
}

// ══════════════════════════════════════════════════════════════════════════════
//  TYPES
// ══════════════════════════════════════════════════════════════════════════════

export type ScrapeStatus = 'pending' | 'scraping' | 'done' | 'error';

export interface IndexEntry {
  url:          string;   // full review URL e.g. https://www.notebookcheck.net/...1234567.0.html
  title:        string;   // review page title e.g. "Google Pixel 10 Pro XL smartphone review"
  brand:        string;   // extracted brand token e.g. "Google"
  slug:         string;   // URL slug e.g. "Google-Pixel-10-Pro-XL-Powerful-smartphone.1128379.0.html"
  discoveredAt: string;   // ISO timestamp when URL was found in the index
  status:       ScrapeStatus;
  scrapedAt?:   string;   // ISO timestamp when scrape completed
  errorMsg?:    string;   // last error message if status === 'error'
  retries:      number;   // number of scrape attempts
}

export interface CrawlStats {
  totalPages:    number;
  totalUrls:     number;
  newUrls:       number;
  crawlMs:       number;
  lastCrawledAt: string;
  error?:        string;
}

export interface ScrapeQueueStats {
  total:    number;
  pending:  number;
  scraping: number;
  done:     number;
  error:    number;
  coverage: string; // e.g. "847/2134 (39.7%)"
}

// ══════════════════════════════════════════════════════════════════════════════
//  IN-MEMORY INDEX STORE
// ══════════════════════════════════════════════════════════════════════════════

// url → IndexEntry  (canonical store)
const indexStore = new Map<string, IndexEntry>();

// crawl + bulk state — use _state object so getCrawlInProgress() getter always reads live value
let lastCrawlStats: CrawlStats | null = null;
const _state = {
  crawlInProgress:   false,
  bulkScrapeActive:  false,
  bulkScrapeAborted: false,
};

// ── INDEX VERSION ─────────────────────────────────────────────────────────────
// Bump this when IndexEntry shape changes to invalidate Redis cache
const INDEX_VERSION = 'v2';
const INDEX_REDIS_KEY = `nbc:index:${INDEX_VERSION}:urls`;

// ══════════════════════════════════════════════════════════════════════════════
//  KNOWN BRANDS — used for brand extraction from review titles
// ══════════════════════════════════════════════════════════════════════════════
const KNOWN_BRANDS = [
  'Apple', 'Samsung', 'Google', 'OnePlus', 'Xiaomi', 'Oppo', 'Vivo', 'Realme',
  'Motorola', 'Sony', 'Asus', 'Honor', 'Huawei', 'Nothing', 'Nokia', 'HTC',
  'LG', 'ZTE', 'TCL', 'Infinix', 'Tecno', 'Itel', 'Fairphone', 'Blackview',
  'Ulefone', 'Doogee', 'Cubot', 'Oukitel', 'Umidigi', 'BLU', 'Wiko',
  'Lenovo', 'BlackBerry', 'Cat', 'Energizer', 'Alcatel', 'Sharp', 'Meizu',
  'Nubia', 'Poco', 'Redmi', 'iQOO', 'Lava', 'Micromax', 'Panasonic',
];

function extractBrand(title: string): string {
  const titleLower = title.toLowerCase();
  for (const brand of KNOWN_BRANDS) {
    if (titleLower.includes(brand.toLowerCase())) return brand;
  }
  // Fallback: first capitalised word that isn't a generic word
  const genericWords = new Set(['smartphone', 'phone', 'review', 'test', 'the', 'a', 'an', 'with', 'for', 'new', 'best', 'top', 'chic', 'slim']);
  for (const word of title.trim().split(/\s+/)) {
    const w = word.toLowerCase().replace(/[^a-z]/g, '');
    if (w.length >= 2 && !genericWords.has(w) && /^[A-Z]/.test(word)) return word;
  }
  return 'Unknown';
}

// ══════════════════════════════════════════════════════════════════════════════
//  PAGE FETCHER — with retry + random UA
// ══════════════════════════════════════════════════════════════════════════════
async function fetchPage(url: string, timeoutMs = 12000, retries = 2): Promise<string> {
  for (let i = 0; i <= retries; i++) {
    try {
      const { data } = await sharedAxios.get(url, {
        headers: {
          'User-Agent': randomUA(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'gzip, deflate, br',
          'Referer': 'https://www.notebookcheck.net/',
        },
        timeout: timeoutMs,
      });
      return typeof data === 'string' ? data : JSON.stringify(data);
    } catch (e: any) {
      const isLast = i === retries;
      const status = e?.response?.status;
      if (status && status >= 400 && status < 500) throw e; // 4xx — no point retrying
      if (isLast) throw e;
      await new Promise(r => setTimeout(r, 500 * Math.pow(2, i)));
    }
  }
  throw new Error('fetchPage: exhausted retries');
}

// ══════════════════════════════════════════════════════════════════════════════
//  URL EXTRACTOR — pull review URLs from a listing page
// ══════════════════════════════════════════════════════════════════════════════
function extractReviewUrls(html: string): Array<{ url: string; title: string }> {
  const $ = cheerio.load(html);
  const results: Array<{ url: string; title: string }> = [];
  const seen = new Set<string>();

  $('a[href]').each((_, el) => {
    let href = $(el).attr('href') || '';

    // Resolve relative URLs
    if (href.startsWith('/')) {
      href = 'https://www.notebookcheck.net' + href;
    } else if (!href.startsWith('http')) {
      return;
    }

    // Strip query params — NBC review URLs never need them
    href = href.split('?')[0];

    // Must be a notebookcheck review URL with 4+ digit article ID: .1234567.0.html
    if (!href.includes('notebookcheck.net')) return;
    if (!/.\d{4,}\.0\.html$/.test(href)) return;

    // Reject listing/search/category pages by their known slug prefixes
    const slug = href.split('/').pop() || '';
    if (/^(Reviews|Smartphones|Search|Topics|RSS|index|Notebooks)\./i.test(slug)) return;

    // Reject clearly non-review content
    if (/users?[-_]complain|rumou?r|leaked?|price[-_]drop|hands?[-_]on(?!.*review)|first[-_]look|unboxing|teardown|external[-_]review/i.test(slug)) return;

    // NBC smartphone review slugs always contain "smartphone" or "phone"
    // BUT "headphone", "earphone", "microphone" also match "phone" — exclude those
    // Also exclude known non-phone categories that sometimes sneak through
    const hasPhone = /smartphone|(?<![a-z])phone(?![a-z])|iphone/i.test(slug);
    const isNotPhone = /headphone|earphone|microphone|earbuds|speaker|vacuum|robot|calendar|smartwatch|tablet|laptop|notebook|macbook|chromebook|case[-_]review|charger|powerbank/i.test(slug);
    if (!hasPhone || isNotPhone) return;

    const key = href.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);

    const rawTitle = $(el).attr('title') || $(el).text().trim() || '';
    const title = rawTitle.replace(/\s+/g, ' ').trim().slice(0, 200);

    if (title.length < 5) return;

    results.push({ url: href, title });
  });

  return results;
}

// ══════════════════════════════════════════════════════════════════════════════
//  PAGINATION DETECTOR — find total page count
// ══════════════════════════════════════════════════════════════════════════════
function detectTotalPages(html: string): number {
  const $ = cheerio.load(html);

  // NBC Reviews pagination uses page numbers in URLs like:
  // /Reviews.55.0.html?cat=Smartphones&page=12
  // or links with text "12", "13" etc.
  let maxPage = 1;

  // NBC pagination links look like: /Reviews.55.0.html?&ns_page=5
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const pageMatch = href.match(/ns_page=(\d+)/);
    if (pageMatch) {
      const p = parseInt(pageMatch[1]);
      if (p > maxPage) maxPage = p;
    }
  });

  // NBC also renders page numbers as plain text in pagination widget
  $('a, span').each((_, el) => {
    const href = $(el as any).attr?.('href') || '';
    const txt  = $(el).text().trim();
    // Only count short numeric strings that look like page numbers
    if (/^\d{1,3}$/.test(txt)) {
      const n = parseInt(txt);
      if (n > maxPage && n < 500) maxPage = n;
    }
    // Also catch high ns_page values from any remaining links
    const m = href.match(/ns_page=(\d+)/);
    if (m) {
      const p = parseInt(m[1]);
      if (p > maxPage) maxPage = p;
    }
  });

  log('debug', 'index.pagination', { maxPage });
  return maxPage;
}

// ══════════════════════════════════════════════════════════════════════════════
//  LOAD INDEX FROM REDIS (on startup)
// ══════════════════════════════════════════════════════════════════════════════
export async function loadIndexFromRedis(): Promise<number> {
  try {
    const cached = await redisGet(INDEX_REDIS_KEY) as IndexEntry[] | null;
    if (!cached || !Array.isArray(cached)) return 0;

    let loaded = 0;
    for (const entry of cached) {
      if (!entry.url || !entry.title) continue;
      // Don't overwrite in-memory entries that are actively scraping
      const existing = indexStore.get(entry.url);
      if (existing && existing.status === 'scraping') continue;
      indexStore.set(entry.url, entry);
      loaded++;
    }
    log('info', 'index.redis_loaded', { count: loaded });
    return loaded;
  } catch (e) {
    log('warn', 'index.redis_load_failed', { err: (e as Error).message });
    return 0;
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  SAVE INDEX TO REDIS
// ══════════════════════════════════════════════════════════════════════════════
async function saveIndexToRedis(): Promise<void> {
  const entries = Array.from(indexStore.values());
  // Store with 7-day TTL — long enough to survive redeploys
  await redisSet(INDEX_REDIS_KEY, entries, 7 * 24 * 3600);
  log('info', 'index.redis_saved', { count: entries.length });
}

// ══════════════════════════════════════════════════════════════════════════════
//  MAIN CRAWLER — crawlNBCSmartphoneIndex()
//
//  Fetches all pages of the NBC smartphone reviews listing and builds the
//  index of review URLs. Respects existing scrape status so re-crawling
//  doesn't reset "done" entries.
// ══════════════════════════════════════════════════════════════════════════════

// NBC Reviews listing — two known URL formats for the smartphone filter:
//   Format A: https://www.notebookcheck.net/Reviews.55.0.html?cat=Smartphones&page=N
//   Format B: https://www.notebookcheck.net/Smartphones.1311.0.html  (direct category page)
// We try Format A first; if it returns 0 results we fall back to Format B.
// NBC uses ns_page= for pagination (not page=) — discovered from live response
// The dedicated smartphone reviews category page is the correct starting point
// NBC Reviews listing — the only working URL. Smartphones.1311.0.html is 404.
// Each page has ~10-16 phone reviews mixed with laptops/tablets/accessories.
// Phone reviews are identified by "smartphone"/"phone"/"iPhone" in the URL slug.
// With ~2000+ phone reviews and ~12 per page, expect ~170 pages to crawl.
const NBC_REVIEWS_BASE = 'https://www.notebookcheck.net/Reviews.55.0.html';

export interface CrawlOptions {
  maxPages?:       number;   // safety cap (default: 999). Crawl auto-stops when a page has 0 phones.
  delayMs?:        number;   // delay between page fetches ms (default: 600)
  forceRecrawl?:   boolean;  // re-fetch even if index is fresh (default: false)
}

export async function crawlNBCSmartphoneIndex(opts: CrawlOptions = {}): Promise<CrawlStats> {
  if (_state.crawlInProgress) {
    return lastCrawlStats ?? {
      totalPages: 0, totalUrls: 0, newUrls: 0, crawlMs: 0,
      lastCrawledAt: new Date().toISOString(), error: 'Crawl already in progress'
    };
  }

  const { maxPages = 999, delayMs = 600, forceRecrawl = false } = opts;

  // If we already have a recent index (< 6 hours) and not forced, skip
  if (!forceRecrawl && lastCrawlStats && indexStore.size > 0) {
    const ageMs = Date.now() - new Date(lastCrawlStats.lastCrawledAt).getTime();
    if (ageMs < 6 * 60 * 60 * 1000) {
      log('info', 'index.crawl_skipped', { reason: 'fresh', ageHours: (ageMs / 3600000).toFixed(1) });
      return lastCrawlStats;
    }
  }

  _state.crawlInProgress = true;
  const t0 = Date.now();
  let totalUrls = 0;
  let newUrls = 0;
  let totalPages = 0;

  log('info', 'index.crawl_start', { maxPages });

  try {
    // ── STEP 1: Fetch page 1 to determine total page count ──────────────────
    // NBC Reviews.55.0.html — mixed reviews page, ~100 links per page, ~12 phones each.
    // Pagination: ?&ns_page=N. No server-side smartphone filter exists.
    // We filter client-side by slug pattern (smartphone|iPhone|phone in URL).
    // Blind pagination: keep fetching until a page returns 0 phone links.
    const baseUrl = NBC_REVIEWS_BASE;
    const firstPageUrl = NBC_REVIEWS_BASE;

    log('info', 'index.fetch_page', { page: 1, url: firstPageUrl });
    const firstHtml = await fetchPage(firstPageUrl);
    const page1Entries = extractReviewUrls(firstHtml);
    const pagesToFetch = maxPages; // blind — stop when page returns 0 results
    log('info', 'index.crawl_mode', { mode: 'blind_pagination', baseUrl, page1Count: page1Entries.length, maxPages });
    for (const { url, title } of page1Entries) {
      const isNew = !indexStore.has(url);
      if (isNew || forceRecrawl) {
        const existing = indexStore.get(url);
        indexStore.set(url, {
          url,
          title,
          brand: extractBrand(title),
          slug: url.split('/').pop() || '',
          discoveredAt: new Date().toISOString(),
          status: existing?.status ?? 'pending',
          scrapedAt: existing?.scrapedAt,
          retries: existing?.retries ?? 0,
        });
        if (isNew) newUrls++;
      }
    }
    totalUrls += page1Entries.length;
    totalPages = 1;

    // ── STEP 2: Fetch remaining pages ─────────────────────────────────────────
    for (let page = 2; page <= pagesToFetch; page++) {
      await new Promise(r => setTimeout(r, delayMs));

      const pageUrl = `${NBC_REVIEWS_BASE}?&ns_page=${page}`;
      log('info', 'index.fetch_page', { page, url: pageUrl });

      try {
        const html = await fetchPage(pageUrl);
        const entries = extractReviewUrls(html);

        if (entries.length === 0) {
          log('info', 'index.crawl_empty_page', { page, note: 'no reviews found — stopping' });
          break; // Reached end of pagination
        }

        for (const { url, title } of entries) {
          const isNew = !indexStore.has(url);
          if (isNew || forceRecrawl) {
            const existing = indexStore.get(url);
            indexStore.set(url, {
              url,
              title,
              brand: extractBrand(title),
              slug: url.split('/').pop() || '',
              discoveredAt: new Date().toISOString(),
              status: existing?.status ?? 'pending',
              scrapedAt: existing?.scrapedAt,
              retries: existing?.retries ?? 0,
            });
            if (isNew) newUrls++;
          }
        }
        totalUrls += entries.length;
        totalPages++;
      } catch (e: any) {
        log('warn', 'index.fetch_page_failed', { page, err: e.message });
        // Continue to next page — one failed page shouldn't stop the whole crawl
      }
    }

    // ── STEP 3: Persist to Redis ──────────────────────────────────────────────
    await saveIndexToRedis();

    const crawlMs = Date.now() - t0;
    lastCrawlStats = {
      totalPages,
      totalUrls: indexStore.size,
      newUrls,
      crawlMs,
      lastCrawledAt: new Date().toISOString(),
    };

    log('info', 'index.crawl_done', { totalPages, totalUrls: indexStore.size, newUrls, crawlMs });
    return lastCrawlStats;

  } catch (e: any) {
    log('error', 'index.crawl_failed', { err: e.message });
    const crawlMs = Date.now() - t0;
    const stats = {
      totalPages, totalUrls: indexStore.size, newUrls, crawlMs,
      lastCrawledAt: new Date().toISOString(), error: e.message,
    };
    lastCrawlStats = stats;
    return stats;
  } finally {
    _state.crawlInProgress = false;
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  SCRAPE QUEUE STATS
// ══════════════════════════════════════════════════════════════════════════════
export function getQueueStats(): ScrapeQueueStats {
  let pending = 0, scraping = 0, done = 0, error = 0;
  for (const e of indexStore.values()) {
    if (e.status === 'pending') pending++;
    else if (e.status === 'scraping') scraping++;
    else if (e.status === 'done') done++;
    else if (e.status === 'error') error++;
  }
  const total = indexStore.size;
  const pct = total > 0 ? ((done / total) * 100).toFixed(1) : '0.0';
  return { total, pending, scraping, done, error, coverage: `${done}/${total} (${pct}%)` };
}

// ══════════════════════════════════════════════════════════════════════════════
//  GET INDEX ENTRIES — with filtering/pagination for API endpoints
// ══════════════════════════════════════════════════════════════════════════════
export interface IndexListOptions {
  status?:  ScrapeStatus | 'all';
  brand?:   string;
  page?:    number;
  limit?:   number;
  search?:  string;
}

export interface IndexListResult {
  entries: IndexEntry[];
  total:   number;
  page:    number;
  limit:   number;
  pages:   number;
}

export function getIndexEntries(opts: IndexListOptions = {}): IndexListResult {
  const { status = 'all', brand, page = 1, limit = 50, search } = opts;

  let entries = Array.from(indexStore.values());

  // Filter by status
  if (status !== 'all') {
    entries = entries.filter(e => e.status === status);
  }

  // Filter by brand
  if (brand) {
    const b = brand.toLowerCase();
    entries = entries.filter(e => e.brand.toLowerCase().includes(b));
  }

  // Filter by search term
  if (search) {
    const s = search.toLowerCase();
    entries = entries.filter(e =>
      e.title.toLowerCase().includes(s) || e.url.toLowerCase().includes(s)
    );
  }

  // Sort: done last (so pending/error come first for queue review)
  entries.sort((a, b) => {
    const order: Record<ScrapeStatus, number> = { pending: 0, error: 1, scraping: 2, done: 3 };
    if (order[a.status] !== order[b.status]) return order[a.status] - order[b.status];
    return a.title.localeCompare(b.title);
  });

  const total = entries.length;
  const totalPages = Math.ceil(total / limit);
  const start = (page - 1) * limit;
  const sliced = entries.slice(start, start + limit);

  return { entries: sliced, total, page, limit, pages: totalPages };
}

// ══════════════════════════════════════════════════════════════════════════════
//  SINGLE DEVICE SCRAPER (uses URL directly — no SearXNG)
// ══════════════════════════════════════════════════════════════════════════════
export async function scrapeIndexedDevice(url: string): Promise<{ success: boolean; data?: any; error?: string }> {
  const entry = indexStore.get(url);
  if (!entry) {
    return { success: false, error: `URL not in index: ${url}` };
  }

  // Lazy import to avoid circular dependencies
  const { scrapeNotebookCheckDevice } = await import('./notebookcheck');

  entry.status = 'scraping';
  indexStore.set(url, entry);

  try {
    const data = await scrapeNotebookCheckDevice(url, entry.title);
    entry.status = 'done';
    entry.scrapedAt = new Date().toISOString();
    entry.errorMsg = undefined;
    indexStore.set(url, entry);

    // Persist updated status to Redis in background
    saveIndexToRedis().catch(() => {});

    return { success: true, data };
  } catch (e: any) {
    entry.status = 'error';
    entry.errorMsg = e?.message ?? String(e);
    entry.retries = (entry.retries ?? 0) + 1;
    indexStore.set(url, entry);

    saveIndexToRedis().catch(() => {});

    return { success: false, error: e?.message ?? String(e) };
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  BULK SCRAPER — process entire index queue with concurrency control
// ══════════════════════════════════════════════════════════════════════════════
export interface BulkScrapeOptions {
  concurrency?:    number;  // parallel requests (default: 2 — be nice to NBC)
  delayMs?:        number;  // delay between each request (default: 1200ms)
  maxRetries?:     number;  // retry failed scrapes N times (default: 2)
  onlyPending?:    boolean; // skip errors too, only process pending (default: false = retry errors)
  onProgress?:     (stats: ScrapeQueueStats) => void;
}

export interface BulkScrapeResult {
  processed: number;
  succeeded: number;
  failed:    number;
  skipped:   number;
  elapsedMs: number;
}

export async function bulkScrapeAll(opts: BulkScrapeOptions = {}): Promise<BulkScrapeResult> {
  if (_state.bulkScrapeActive) {
    throw new Error('Bulk scrape already in progress. Call abortBulkScrape() first.');
  }

  const {
    concurrency = 2,
    delayMs = 1200,
    maxRetries = 2,
    onlyPending = false,
    onProgress,
  } = opts;

  _state.bulkScrapeActive = true;
  _state.bulkScrapeAborted = false;

  const t0 = Date.now();
  let processed = 0, succeeded = 0, failed = 0, skipped = 0;

  // Build work queue: pending + (errors with retries left) unless onlyPending
  const queue = Array.from(indexStore.values()).filter(e => {
    if (e.status === 'done') return false;
    if (e.status === 'scraping') { e.status = 'pending'; return true; } // reset stale scraping
    if (e.status === 'pending') return true;
    if (!onlyPending && e.status === 'error' && (e.retries ?? 0) < maxRetries) return true;
    return false;
  }).map(e => e.url);

  log('info', 'bulk.start', { queueSize: queue.length, concurrency, delayMs });

  // Process queue with controlled concurrency using a semaphore pattern
  let queueIndex = 0;

  async function worker(): Promise<void> {
    while (!_state.bulkScrapeAborted) {
      // Grab next URL
      const url = queue[queueIndex++];
      if (!url) break; // queue exhausted

      const result = await scrapeIndexedDevice(url);
      processed++;

      if (result.success) succeeded++;
      else failed++;

      // Progress callback
      if (onProgress) {
        onProgress(getQueueStats());
      }

      log('info', 'bulk.progress', {
        processed, total: queue.length, succeeded, failed,
        url: url.split('/').pop(), ok: result.success,
      });

      // Delay between requests to be polite to NBC
      if (!_state.bulkScrapeAborted && queueIndex < queue.length) {
        await new Promise(r => setTimeout(r, delayMs));
      }
    }
  }

  try {
    // Launch N concurrent workers
    const workers: Promise<void>[] = [];
    for (let i = 0; i < concurrency; i++) {
      // Stagger worker starts by delayMs/concurrency to avoid burst
      await new Promise(r => setTimeout(r, Math.floor(delayMs / concurrency) * i));
      workers.push(worker());
    }
    await Promise.all(workers);
  } finally {
    _state.bulkScrapeActive = false;
    _state.bulkScrapeAborted = false;
  }

  const elapsedMs = Date.now() - t0;
  log('info', 'bulk.done', { processed, succeeded, failed, skipped, elapsedMs });

  return { processed, succeeded, failed, skipped, elapsedMs };
}

export function abortBulkScrape(): void {
  if (_state.bulkScrapeActive) {
    _state.bulkScrapeAborted = true;
    log('info', 'bulk.abort_requested');
  }
}

export function isBulkScrapeActive(): boolean { return _state.bulkScrapeActive; }

// ══════════════════════════════════════════════════════════════════════════════
//  URL VALIDATION — verify a single URL resolves to the correct device
//  This is the "first SearXNG hit check" — after crawling we verify that
//  the URL we have actually returns the device page we expect
// ══════════════════════════════════════════════════════════════════════════════
export interface UrlValidationResult {
  url:       string;
  title:     string;
  valid:     boolean;
  httpStatus?: number;
  h1?:       string;        // actual page H1
  titleMatch?: boolean;     // does page H1 match expected title
  error?:    string;
  checkMs:   number;
}

export async function validateIndexUrl(url: string): Promise<UrlValidationResult> {
  const entry = indexStore.get(url);
  const t0 = Date.now();

  try {
    const html = await fetchPage(url, 10000);
    const $ = cheerio.load(html);
    const h1 = $('h1').first().text().trim();
    const httpStatus = 200; // if we got here, it's 200

    // Check if the H1 contains recognisable device name tokens
    const expectedTokens = (entry?.title ?? url)
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, '')
      .split(/\s+/)
      .filter(t => t.length >= 3 && !['the','and','for','with','review','test','smartphone'].includes(t));

    const h1Lower = h1.toLowerCase();
    const matchCount = expectedTokens.filter(t => h1Lower.includes(t)).length;
    const titleMatch = expectedTokens.length > 0 && matchCount >= Math.ceil(expectedTokens.length * 0.5);

    return {
      url, title: entry?.title ?? '',
      valid: h1.length > 3 && titleMatch,
      httpStatus, h1, titleMatch,
      checkMs: Date.now() - t0,
    };
  } catch (e: any) {
    return {
      url, title: entry?.title ?? '',
      valid: false,
      error: e?.message ?? String(e),
      checkMs: Date.now() - t0,
    };
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  BRAND BREAKDOWN — how many URLs per brand
// ══════════════════════════════════════════════════════════════════════════════
export function getBrandBreakdown(): Record<string, { total: number; done: number; pending: number; error: number }> {
  const result: Record<string, { total: number; done: number; pending: number; error: number }> = {};

  for (const e of indexStore.values()) {
    if (!result[e.brand]) result[e.brand] = { total: 0, done: 0, pending: 0, error: 0 };
    result[e.brand].total++;
    if (e.status === 'done') result[e.brand].done++;
    else if (e.status === 'pending' || e.status === 'scraping') result[e.brand].pending++;
    else if (e.status === 'error') result[e.brand].error++;
  }

  return Object.fromEntries(
    Object.entries(result).sort((a, b) => b[1].total - a[1].total)
  );
}

// ══════════════════════════════════════════════════════════════════════════════
//  RESET HELPERS
// ══════════════════════════════════════════════════════════════════════════════

/** Reset all error entries back to pending so they'll be retried */
export function resetErrors(): number {
  let count = 0;
  for (const [url, entry] of indexStore.entries()) {
    if (entry.status === 'error') {
      entry.status = 'pending';
      entry.retries = 0;
      entry.errorMsg = undefined;
      indexStore.set(url, entry);
      count++;
    }
  }
  saveIndexToRedis().catch(() => {});
  return count;
}

/** Reset a single URL back to pending */
export function resetEntry(url: string): boolean {
  const entry = indexStore.get(url);
  if (!entry) return false;
  entry.status = 'pending';
  entry.retries = 0;
  entry.errorMsg = undefined;
  entry.scrapedAt = undefined;
  indexStore.set(url, entry);
  saveIndexToRedis().catch(() => {});
  return true;
}

/** Clear the entire index (useful for testing) */
export function clearIndex(): void {
  indexStore.clear();
  lastCrawlStats = null;
}

// ── GETTERS for mutable state (primitives can't be live-exported in CJS) ──────
export function getCrawlInProgress(): boolean { return _state.crawlInProgress; }
export function getLastCrawlStats(): CrawlStats | null { return lastCrawlStats; }
export function getIndexStore(): Map<string, IndexEntry> { return indexStore; }

// Keep these exports for backwards compat (object reference is stable)
export { indexStore };