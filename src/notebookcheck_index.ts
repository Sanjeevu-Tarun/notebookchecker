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

// SOURCE A — NBC Smartphones review listing (internal NBC reviews only, ~80 pages)
// Each entry has "-review-" in its slug and contains full benchmarks/specs/images.
const NBC_REVIEWS_BASE = 'https://www.notebookcheck.net/Reviews.55.0.html?&items_per_page=100&hide_youtube=1&hide_external_reviews=1&showHighlightedTags=1&tagArray%5B%5D=10&typeArray%5B%5D=1&id=55';

// SOURCE B — Chronological listing (all device types, library/external pages)
// Used only for phones that NBC hasn't written their own review for yet.
// We skip any URL that already exists from SOURCE A.
const NBC_CHRONO_BASE = 'https://www.notebookcheck.net/Chronological-sorting.2690.0.html';

// SOURCE C — Smartphone listing (phones only, tagArray[]=10, typeArray[]=1)
// page 0 = no ns_page param, page 1+ = &ns_page=N
// This page surfaces phones that may not appear in Source A (no internal review yet)
// but are listed under the smartphones category.
const NBC_SMARTPHONE_BASE = 'https://www.notebookcheck.net/Smartphone.305158.0.html?&tagArray%5B%5D=10&typeArray%5B%5D=1&id=305158';

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

// ── RECOVER DELETED REVIEW URLS ───────────────────────────────────────────────
// Looks at all library URLs currently in the index, calls resolveToReviewUrl on each
// (hits Redis cache instantly — no live fetches needed), and adds the review URL if missing.
// Much faster than scanning Redis keys — works purely from existing entries + resolve cache.
export async function recoverDeletedReviewUrls(): Promise<{ recovered: number; alreadyPresent: number; totalCacheKeys: number }> {
  const entries = await loadEntries();

  // Find all library URLs still in the index (non-review URLs)
  const libraryUrls = Object.keys(entries).filter(u => !/-review-/i.test(u));
  const totalCacheKeys = libraryUrls.length;

  let recovered = 0, alreadyPresent = 0;

  // Resolve in parallel batches of 20 — hits Redis resolve cache, no live HTTP fetches
  const CONCURRENCY = 20;
  for (let i = 0; i < libraryUrls.length; i += CONCURRENCY) {
    const batch = libraryUrls.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(async (libraryUrl) => {
      try {
        const reviewUrl = await resolveToReviewUrl(libraryUrl);
        if (!reviewUrl || reviewUrl === libraryUrl) return; // no review found

        if (entries[reviewUrl]) { alreadyPresent++; return; }

        // Review URL missing from index — re-add it
        // Derive clean title from slug: "Samsung-Galaxy-S25-Ultra-review-....0.html" → "Samsung Galaxy S25 Ultra"
        const slug = reviewUrl.split('/').pop() || '';
        const rawTitle = slug.split('-review-')[0].replace(/-/g, ' ').trim();
        const title = rawTitle.split(' ').map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');

        entries[reviewUrl] = {
          url: reviewUrl, title,
          brand: extractBrand(title), slug,
          discoveredAt: new Date().toISOString(),
          status: 'pending', retries: 0,
        };
        recovered++;
      } catch { /* skip on error */ }
    }));
  }

  if (recovered > 0) {
    await saveEntries(entries);
    await rebuildSearchIndex();
  }

  return { recovered, alreadyPresent, totalCacheKeys };
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

// ── REVIEW URL RESOLVER ───────────────────────────────────────────────────────
// The Chronological page always links to the NBC *library* page for a device
// (e.g. Samsung-Galaxy-S25-Ultra.975474.0.html — specs + aggregated external reviews).
// When NBC has written their own full internal review, the library page links to it
// in its "Reviews" section as the first item (e.g. "89.4% Samsung Galaxy S25 Ultra review...").
// That internal review URL (slug contains "-review-") has the full benchmarks, images,
// and detailed measurements that the scraper needs.
//
// This function fetches the library page and returns the NBC internal review URL if found,
// or the original library URL if no internal review exists yet.
// Result is cached in Redis (nbc:review_resolve:URL) with a 7-day TTL to avoid
// refetching the library page on every crawl.

const RESOLVE_TTL = 7 * 24 * 3600; // 7 days

export async function resolveToReviewUrl(libraryUrl: string): Promise<string> {
  const ck = `nbc:review_resolve:${libraryUrl}`;
  try {
    const cached = await rGet(ck) as string;
    if (cached) return cached;
  } catch { /* miss */ }

  let html: string;
  try {
    html = await fetchHtml(libraryUrl, 10000);
  } catch {
    return libraryUrl;
  }

  const $ = cheerio.load(html);

  // Extract the device name from the library URL slug so we can find THE CORRECT review.
  // Library URL slug: "Google-Pixel-9-Pro-XL.873451.0.html"
  // We want: "google-pixel-9-pro-xl" (lowercase, no numeric suffix)
  const librarySlug = (libraryUrl.split('/').pop() || '')
    .replace(/\.\d+\.0\.html$/, '')
    .toLowerCase();

  let reviewUrl: string | null = null;

  // Strategy 1: Look inside the NBC "Reviews" section on the library page.
  // NBC wraps their own review link(s) in a specific table or div with class
  // "reviews", "nbc-reviews", or a <section> with id/class containing "review".
  // The link text contains a percentage rating like "89.4%".
  const reviewSectionSelectors = [
    '.reviews a[href]',
    '#reviews a[href]',
    '.nbc_reviews a[href]',
    'table.reviews a[href]',
    '[class*="review"] a[href]',
    '[id*="review"] a[href]',
  ];

  for (const sel of reviewSectionSelectors) {
    if (reviewUrl) break;
    $(sel).each((_, el) => {
      if (reviewUrl) return;
      let href = $(el).attr('href') || '';
      if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
      if (!href.startsWith('https://www.notebookcheck.net')) return;
      href = href.split('?')[0];
      if (!/-review-/i.test(href)) return;
      if (!/\.\d{4,}\.0\.html$/.test(href)) return;
      reviewUrl = href;
    });
  }

  // Strategy 2: Scan ALL links but match slug prefix — only accept a review URL
  // whose slug starts with the same words as the library page slug.
  // This prevents picking up sidebar/nav links to unrelated device reviews.
  // e.g. library="google-pixel-9-pro-xl" → only accept review slugs that start with
  // "google-pixel-9-pro-xl-review" or close variant.
  if (!reviewUrl) {
    // Build a loose prefix: first 3+ meaningful words of the library slug
    // "google-pixel-9-pro-xl" → check review slug contains "google" and "pixel" and "9"
    const slugWords = librarySlug.split('-').filter(w => w.length > 0);
    // Use first 3 words as a minimum match requirement
    const matchWords = slugWords.slice(0, Math.min(4, slugWords.length));

    $('a[href]').each((_, el) => {
      if (reviewUrl) return;
      let href = $(el).attr('href') || '';
      if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
      if (!href.startsWith('https://www.notebookcheck.net')) return;
      href = href.split('?')[0];
      if (!/-review-/i.test(href)) return;
      if (!/\.\d{4,}\.0\.html$/.test(href)) return;

      // The review slug must start with the device name words
      const reviewSlug = (href.split('/').pop() || '').toLowerCase();
      const allMatch = matchWords.every(w => reviewSlug.includes(w));
      if (!allMatch) return;

      reviewUrl = href;
    });
  }

  // Strategy 3: Last resort — scan all links for any review URL whose slug starts
  // with the full library slug prefix (exact match on the device name part)
  if (!reviewUrl) {
    $('a[href]').each((_, el) => {
      if (reviewUrl) return;
      let href = $(el).attr('href') || '';
      if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
      if (!href.startsWith('https://www.notebookcheck.net')) return;
      href = href.split('?')[0];
      if (!/-review-/i.test(href)) return;
      if (!/\.\d{4,}\.0\.html$/.test(href)) return;
      const reviewSlug = (href.split('/').pop() || '').toLowerCase();
      if (reviewSlug.startsWith(librarySlug.slice(0, 10))) {
        reviewUrl = href;
      }
    });
  }

  const result = reviewUrl ?? libraryUrl;
  await rSet(ck, result, RESOLVE_TTL);
  return result;
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
    if (/^(Reviews|Smartphones|Search|Topics|RSS|index|Notebooks|News|Smartphone|Library|Comparison|Chronological)\./i.test(slug)) return;
    if (/-Series\./i.test(slug)) return;

    // The Chronological page labels each entry type in the parent element text.
    // Keep only "(Smartphone)" entries — tablets, notebooks, gaming etc. are skipped.
    const parentText = $(el).parent().text();
    if (!/\(Smartphone\)/i.test(parentText)) return;

    if (seen.has(href.toLowerCase())) return;
    seen.add(href.toLowerCase());

    const title = ($(el).attr('title') || $(el).text().trim() || '').replace(/\s+/g, ' ').trim().slice(0, 200);
    if (title.length < 5) return;

    out.push({ url: href, title });
  });

  return out;
}

// ══════════════════════════════════════════════════════════════════════════════
//  CRAWL — TWO SOURCES
//
//  SOURCE A: crawlReviewsPage(page)
//    NBC Smartphones review listing — internal NBC reviews only (~80 pages).
//    These have "-review-" in their slug and contain full benchmarks/specs/images.
//    Pagination: page 0 = no ns_page param, page 1+ = &ns_page=N
//
//  SOURCE B: crawlChronoPage(page)
//    Chronological listing — all device types including library/external pages.
//    Only adds phones NOT already in the index from Source A.
//    Skips any URL that already has a review entry for the same device slug.
//    No resolveToReviewUrl needed — library URLs are only added as fallback.
// ══════════════════════════════════════════════════════════════════════════════

export interface CrawlPageResult {
  page: number; phonesFound: number; totalUrls: number;
  newUrls: number; done: boolean; nextPage: number | null; durationMs: number;
  source: 'reviews' | 'chrono';
}

function makeEntry(url: string, title: string): IndexEntry {
  return { url, title, brand: extractBrand(title), slug: url.split('/').pop() || '',
    discoveredAt: new Date().toISOString(), status: 'pending', retries: 0 };
}

// ── SOURCE A: NBC Smartphones reviews listing ─────────────────────────────────
export async function crawlReviewsPage(page: number): Promise<CrawlPageResult> {
  const t0 = Date.now();
  const url = page === 0 ? NBC_REVIEWS_BASE : `${NBC_REVIEWS_BASE}&ns_page=${page}`;
  const html = await fetchHtml(url);
  const $ = cheerio.load(html);

  const found: Array<{ url: string; title: string }> = [];
  const seen = new Set<string>();

  $('a[href]').each((_, el) => {
    let href = $(el).attr('href') || '';
    if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
    href = href.split('?')[0];
    if (!href.includes('notebookcheck.net')) return;
    if (!/-review-/i.test(href)) return;          // SOURCE A: review URLs only
    if (!/\.\d{4,}\.0\.html$/.test(href)) return;
    if (seen.has(href.toLowerCase())) return;
    seen.add(href.toLowerCase());

    const title = ($(el).attr('title') || $(el).text().trim() || '')
      .replace(/^\d+%\s*/, '').replace(/\s+/g, ' ').trim().slice(0, 120);
    // Clean review title: strip everything after " - " (article subtitle)
    const cleanTitle = title.split(' - ')[0].split(' review')[0].trim();
    if (cleanTitle.length < 4) return;
    found.push({ url: href, title: cleanTitle });
  });

  // Empty page = no review links at all
  const pageIsEmpty = found.length === 0 && !html.includes('notebookcheck.net');

  const entries = await loadEntries();
  let newUrls = 0;
  for (const { url: u, title } of found) {
    if (!entries[u]) { entries[u] = makeEntry(u, title); newUrls++; }
  }
  if (newUrls > 0) await saveEntries(entries);

  const totalUrls = Object.keys(entries).length;
  await rSet(PROGRESS_KEY, { page, totalUrls, startedAt: new Date().toISOString(), updatedAt: new Date().toISOString() }, LOCK_TTL);

  return { page, phonesFound: found.length, totalUrls, newUrls,
    done: pageIsEmpty, nextPage: page + 1,
    durationMs: Date.now() - t0, source: 'reviews' };
}

// ── SOURCE C: Smartphone listing (phones, resolve library → review URLs) ────────
// Same resolve logic as Source B — every non-review URL is resolved to its
// internal NBC review URL before being stored.
export async function crawlSmartphonePage(page: number): Promise<CrawlPageResult> {
  const t0 = Date.now();
  const url = page === 0 ? NBC_SMARTPHONE_BASE : `${NBC_SMARTPHONE_BASE}&ns_page=${page}`;
  const html = await fetchHtml(url);
  const $ = cheerio.load(html);

  const found: Array<{ url: string; title: string }> = [];
  const seen = new Set<string>();

  $('a[href]').each((_, el) => {
    let href = $(el).attr('href') || '';
    if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
    href = href.split('?')[0];
    if (!href.includes('notebookcheck.net')) return;
    if (!/\.\d{4,}\.0\.html$/.test(href)) return;
    if (seen.has(href.toLowerCase())) return;
    seen.add(href.toLowerCase());

    const title = ($(el).attr('title') || $(el).text().trim() || '')
      .replace(/^\d+%\s*/, '').replace(/\s+/g, ' ').trim().slice(0, 120);
    const cleanTitle = title.split(' - ')[0].split(' review')[0].trim();
    if (cleanTitle.length < 4) return;
    found.push({ url: href, title: cleanTitle });
  });

  const rawLinks = (html.match(/\.notebookcheck\.net\/[^"']+\.\d+\.0\.html/g) || []).length;
  const pageIsEmpty = rawLinks === 0;

  const entries = await loadEntries();

  // Build review prefix set — skip devices already covered by a review URL
  const reviewPrefixes = new Set<string>();
  for (const u of Object.keys(entries)) {
    if (/-review-/i.test(u)) {
      const slug = u.split('/').pop() || '';
      reviewPrefixes.add(slug.toLowerCase().split('-review-')[0]);
    }
  }

  // Filter to only new, uncovered URLs
  const toResolve: Array<{ url: string; title: string }> = [];
  for (const { url: u, title } of found) {
    if (entries[u]) continue;
    const slug = u.split('/').pop() || '';
    const prefix = slug.toLowerCase().replace(/\.\d+\.0\.html$/, '');
    if (reviewPrefixes.has(prefix)) continue;
    toResolve.push({ url: u, title });
  }

  let newUrls = 0;
  for (let i = 0; i < toResolve.length; i += RESOLVE_CONCURRENCY) {
    const batch = toResolve.slice(i, i + RESOLVE_CONCURRENCY);
    await Promise.all(batch.map(async ({ url: libraryUrl, title }) => {
      try {
        const resolvedUrl = /-review-/i.test(libraryUrl)
          ? libraryUrl
          : await resolveToReviewUrl(libraryUrl);
        const finalUrl = resolvedUrl || libraryUrl;
        if (entries[finalUrl]) return;
        entries[finalUrl] = makeEntry(finalUrl, title);
        newUrls++;
      } catch {
        if (!entries[libraryUrl]) { entries[libraryUrl] = makeEntry(libraryUrl, title); newUrls++; }
      }
    }));
  }

  if (newUrls > 0) await saveEntries(entries);
  const totalUrls = Object.keys(entries).length;
  await rSet(PROGRESS_KEY, { page, totalUrls, startedAt: new Date().toISOString(), updatedAt: new Date().toISOString() }, LOCK_TTL);

  return { page, phonesFound: found.length, totalUrls, newUrls,
    done: pageIsEmpty, nextPage: page + 1,
    durationMs: Date.now() - t0, source: 'smartphone' };
}

// ── SOURCE B: Chronological listing (library/external fallback) ───────────────
//
// KEY FIX: every new library URL is immediately resolved to its internal NBC
// review URL (if one exists) before being stored in the index.
// Previously crawlChronoPage stored library URLs raw and relied on a separate
// migrateToReviewUrls step — meaning devices like Pixel 10 Pro XL ended up
// with the minimised library/aggregator page instead of the full review page.
//
// Vercel safety: resolves in parallel batches of RESOLVE_CONCURRENCY.
// Each resolveToReviewUrl does one HTTP fetch (~1-3s). With concurrency=5
// and ~20 phones/page we need ~4-12s — well within the 30s Vercel limit.
const RESOLVE_CONCURRENCY = 5;

export async function crawlChronoPage(page: number): Promise<CrawlPageResult> {
  const t0 = Date.now();
  const chronoUrl = page === 1 ? NBC_CHRONO_BASE : `${NBC_CHRONO_BASE}?&ns_page=${page}`;
  const html = await fetchHtml(chronoUrl);
  const found = extractPhoneUrls(html);

  const rawLinks = (html.match(/\.notebookcheck\.net\/[^"']+\.\d+\.0\.html/g) || []).length;
  const pageIsEmpty = rawLinks === 0;

  const entries = await loadEntries();

  // Build a set of device slug prefixes already covered by a review URL
  const reviewPrefixes = new Set<string>();
  for (const u of Object.keys(entries)) {
    if (/-review-/i.test(u)) {
      const slug = u.split('/').pop() || '';
      reviewPrefixes.add(slug.toLowerCase().split('-review-')[0]);
    }
  }

  // Filter to only new URLs not yet in the index and not already covered by a review
  const toResolve: Array<{ url: string; title: string }> = [];
  for (const { url: u, title } of found) {
    if (entries[u]) continue;
    const slug = u.split('/').pop() || '';
    const prefix = slug.toLowerCase().replace(/\.\d+\.0\.html$/, '');
    if (reviewPrefixes.has(prefix)) continue;
    // Skip if we already have a review URL for the same device slug
    // (e.g. library URL arrives after Source A already indexed the review URL)
    if (/-review-/i.test(u)) {
      // It's already a review URL — store directly
      toResolve.push({ url: u, title });
    } else {
      toResolve.push({ url: u, title });
    }
  }

  let newUrls = 0;

  // Resolve each library URL → internal review URL in parallel batches
  for (let i = 0; i < toResolve.length; i += RESOLVE_CONCURRENCY) {
    const batch = toResolve.slice(i, i + RESOLVE_CONCURRENCY);
    await Promise.all(batch.map(async ({ url: libraryUrl, title }) => {
      try {
        // resolveToReviewUrl fetches the library page and finds the "-review-" link.
        // Returns the internal review URL if found, else the original library URL.
        const resolvedUrl = /-review-/i.test(libraryUrl)
          ? libraryUrl  // already a review URL — no fetch needed
          : await resolveToReviewUrl(libraryUrl);

        const finalUrl = resolvedUrl || libraryUrl;

        // If the resolved URL is already in the index (e.g. Source A already added it), skip
        if (entries[finalUrl]) return;

        // If resolved to a review URL, also check its prefix against existing reviews
        if (finalUrl !== libraryUrl && /-review-/i.test(finalUrl)) {
          const rSlug = finalUrl.split('/').pop() || '';
          const rPrefix = rSlug.toLowerCase().split('-review-')[0];
          // If we already have a different review for the same device, skip
          if ([...Object.keys(entries)].some(u => /-review-/i.test(u) && u !== finalUrl && u.split('/').pop()?.toLowerCase().startsWith(rPrefix))) return;
        }

        entries[finalUrl] = makeEntry(finalUrl, title);
        newUrls++;
      } catch {
        // On resolve failure, fall back to storing the library URL
        if (!entries[libraryUrl]) {
          entries[libraryUrl] = makeEntry(libraryUrl, title);
          newUrls++;
        }
      }
    }));
  }

  if (newUrls > 0) await saveEntries(entries);

  const totalUrls = Object.keys(entries).length;
  await rSet(PROGRESS_KEY, { page, totalUrls, startedAt: new Date().toISOString(), updatedAt: new Date().toISOString() }, LOCK_TTL);

  return { page, phonesFound: found.length, totalUrls, newUrls,
    done: pageIsEmpty, nextPage: pageIsEmpty ? null : page + 1,
    durationMs: Date.now() - t0, source: 'chrono' };
}

// Legacy alias — kept for existing /api/index/crawl-page endpoint compatibility
export async function crawlOnePage(page: number): Promise<CrawlPageResult> {
  return crawlChronoPage(page);
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

  // Deduplicate: for each device title, always prefer the NBC internal review URL
  // (slug contains "-review-") over the library/aggregator URL.
  // This guards against stale library entries coexisting with upgraded review entries.
  const byTitle = new Map<string, IndexEntry>();
  for (const e of Object.values(entries) as IndexEntry[]) {
    const key = e.title.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim();
    const existing = byTitle.get(key);
    const isReview = /-review-/i.test(e.url);
    if (!existing) {
      byTitle.set(key, e);
    } else if (isReview && !/-review-/i.test(existing.url)) {
      byTitle.set(key, e); // upgrade to review URL
    }
  }

  const flat = Array.from(byTitle.values()).map((e: IndexEntry) => ({ url: e.url, title: e.title, slug: e.slug }));
  await rSetPermanent(SEARCH_INDEX_KEY, flat);
}

// Purge stale/junk entries from the index:
//
//  1. JUNK TITLES — entries whose title is longer than 80 chars are article snippets
//     from the old crawl (e.g. "86% Smartphone with superlatives has its eye on the prize...")
//     These are never clean device names and should be deleted unconditionally.
//
//  2. LIBRARY DUPLICATES — library URLs (no "-review-" in slug) whose device slug
//     (the part before the first dot) matches a review URL already in the index.
//     e.g. library:  Samsung-Galaxy-S25-Ultra.975474.0.html
//          review:   Samsung-Galaxy-S25-Ultra-review-The-AI-phone.968346.0.html
//     Both start with "samsung-galaxy-s25-ultra" → library is deleted.
//
// Safe to run multiple times. Rebuilds search index on completion.
export async function purgeLibraryDuplicates(): Promise<{ purged: number; kept: number; reasons: Record<string, number> }> {
  const entries = await loadEntries();
  const reasons: Record<string, number> = { junkTitle: 0, libraryDuplicate: 0 };

  // Build a set of device slug prefixes that have a review URL
  // e.g. "samsung-galaxy-s25-ultra" from "Samsung-Galaxy-S25-Ultra-review-...968346.0.html"
  const reviewPrefixes = new Set<string>();
  for (const url of Object.keys(entries)) {
    if (!/-review-/i.test(url)) continue;
    const slug = url.split('/').pop() || '';
    // Device prefix = everything before "-review-"
    const prefix = slug.toLowerCase().split('-review-')[0];
    if (prefix.length > 3) reviewPrefixes.add(prefix);
  }

  const toDelete: string[] = [];
  for (const [url, e] of Object.entries(entries)) {
    // Only touch non-review URLs — NEVER delete internal review entries
    if (/-review-/i.test(url)) continue;

    // 1. Junk title — library entries with article snippet titles (>80 chars)
    //    e.g. "86% Smartphone with superlatives has its eye on the prize - Realme GT 7 Pro review..."
    if (e.title.length > 80) {
      toDelete.push(url);
      reasons.junkTitle++;
      continue;
    }

    // 2. Library duplicate — library URL whose slug prefix matches a review entry
    //    e.g. "Samsung-Galaxy-S25-Ultra.975474.0.html" → prefix "samsung-galaxy-s25-ultra"
    //    matches review "Samsung-Galaxy-S25-Ultra-review-....968346.0.html"
    const slug = url.split('/').pop() || '';
    const prefix = slug.toLowerCase().replace(/\.\d+\.0\.html$/, '');
    if (reviewPrefixes.has(prefix)) {
      toDelete.push(url);
      reasons.libraryDuplicate++;
    }
  }

  for (const url of toDelete) delete entries[url];

  await saveEntries(entries);
  await rebuildSearchIndex();

  return { purged: toDelete.length, kept: Object.keys(entries).length, reasons };
}

// Normalise the stored title for matching.
// Index titles from the Chronological crawl are already clean: "Vivo X300 Pro", "Samsung Galaxy S26 Ultra".
// We just lowercase and trim. The old snippet-stripping logic was for SearXNG result titles
// which are no longer stored here.
function cleanIndexTitle(raw: string): string {
  // Strip a leading score like "88% " if somehow present (legacy entries)
  return raw.replace(/^\d+%\s*/, '').toLowerCase().trim();
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

// Score an index entry title against a user query.
// Titles in the index are clean: "Vivo X300 Pro", "Samsung Galaxy S26 Ultra".
// The stored URL is always the NBC internal review URL (resolved at crawl time by
// resolveToReviewUrl), so scoring only needs to find the best title match.
// Rules:
//   1. ALL query words must appear in the title (hard reject if any missing)
//   2. Exact match → 10000, title-contains-query → 8000
//   3. Each variant word in the title that the query DIDN'T ask for → -2000 penalty
//      (e.g. query "samsung s25" vs title "Samsung Galaxy S25 Ultra" → -2000 for "ultra")
//   4. Shorter title = small length bonus (more precise match)
function scoreIndexMatch(entryTitle: string, query: string): number {
  const d = cleanIndexTitle(entryTitle);
  const q = query.toLowerCase().trim();
  const qWords = q.split(/\s+/).filter((w: string) => w.length > 1);

  const wordIn = (word: string, text: string) => wordBoundaryRe(word).test(text);

  // Hard reject: any query word missing from the title
  if (!qWords.every((w: string) => wordIn(w, d))) return -1;

  // Exact or contains match — highest confidence
  if (d === q) return 10000;
  if (d.includes(q)) return 8000;

  // Penalise extra variant words in title that query didn't include
  const variants = ['ultra', 'pro', 'plus', 'mini', 'lite', 'fe', 'max', 'edge', 'standard', 'turbo', 'fold', 'flip', 'xl', 'xr', 'se', '5g', '4g', 'go', 'slim', 'zoom', 'compact'];
  let penalty = 0;
  for (const v of variants) {
    if (wordIn(v, d) && !wordIn(v, q)) penalty += 2000;
  }

  // Bonus for shorter title (fewer extra words = closer match)
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

  // Reject if no entry scored above -1 (no word-match at all)
  if (bestScore < 0 || !best) return null;

  // Hard-reject: title has a variant suffix the query didn't ask for.
  // e.g. query "vivo x300" must NOT return "Vivo X300 Pro" — that's a different device.
  // e.g. query "vivo x300 pro" → title "Vivo X300 Pro" has no extra variant → allowed.
  const VARIANT_SUFFIXES = ['ultra', 'pro', 'plus', 'mini', 'lite', 'fe', 'max', 'edge',
    'standard', 'turbo', 'fold', 'flip', 'xl', 'xr', 'se', '5g', '4g', 'go', 'slim', 'zoom', 'compact'];
  const cleanBestTitle = cleanIndexTitle(best.title);
  const titleHasExtraVariant = VARIANT_SUFFIXES.some(
    v => wordBoundaryRe(v).test(cleanBestTitle) && !wordBoundaryRe(v).test(normalized)
  );
  if (titleHasExtraVariant) return null; // wrong variant — let SearXNG handle it

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

// ── RESUMABLE MIGRATION: upgrade library URLs to NBC internal review URLs ────────
// Processes entries in fixed-size batches (default 200). Each call saves progress
// to Redis and returns. Call repeatedly until done=true — safe across Vercel timeouts.
//
// Redis key: nbc:migrate:cursor  — index into the URL list where next call resumes
// Redis key: nbc:migrate:stats   — cumulative upgraded/noReview/errors counts
//
// Call: GET /api/index/migrate-review-urls?batch=200
// Keep calling until the response contains "done": true
//

const MIGRATE_CURSOR_KEY = 'nbc:migrate:cursor';
const MIGRATE_STATS_KEY  = 'nbc:migrate:stats';

export interface MigrateResult {
  total: number; processed: number; remaining: number;
  upgraded: number; alreadyReview: number; noReview: number; errors: number;
  durationMs: number; done: boolean;
}

export async function migrateToReviewUrls(batchSize = 200): Promise<MigrateResult> {
  const t0 = Date.now();
  const entries = await loadEntries();
  // Sort URLs for stable ordering across calls
  const urls = Object.keys(entries).sort();
  const total = urls.length;

  // Load cursor (where we left off) and cumulative stats
  let cursor = 0;
  try { cursor = (await rGet(MIGRATE_CURSOR_KEY) as number) ?? 0; } catch { cursor = 0; }

  let stats = { upgraded: 0, alreadyReview: 0, noReview: 0, errors: 0 };
  try { stats = (await rGet(MIGRATE_STATS_KEY) as typeof stats) ?? stats; } catch { /* fresh start */ }

  // If already finished, return immediately
  if (cursor >= total) {
    return { total, processed: total, remaining: 0, ...stats, durationMs: Date.now() - t0, done: true };
  }

  // Process this batch
  const batch = urls.slice(cursor, cursor + batchSize);
  const CONCURRENCY = 8;

  for (let i = 0; i < batch.length; i += CONCURRENCY) {
    const chunk = batch.slice(i, i + CONCURRENCY);
    await Promise.all(chunk.map(async (oldUrl) => {
      try {
        if (/-review-/i.test(oldUrl)) { stats.alreadyReview++; return; }

        const newUrl = await resolveToReviewUrl(oldUrl);

        if (newUrl === oldUrl) { stats.noReview++; return; }

        // Swap: delete library URL, insert review URL
        if (!entries[newUrl]) {
          entries[newUrl] = { ...entries[oldUrl], url: newUrl, slug: newUrl.split('/').pop() || '' };
        }
        delete entries[oldUrl];
        stats.upgraded++;
      } catch {
        stats.errors++;
      }
    }));
  }

  // Save upgraded entries back to Redis
  await saveEntries(entries);

  // Advance cursor and save stats
  const newCursor = cursor + batch.length;
  const done = newCursor >= total;

  await rSet(MIGRATE_CURSOR_KEY, newCursor, 30 * 24 * 3600);
  await rSet(MIGRATE_STATS_KEY,  stats,     30 * 24 * 3600);

  // On completion: rebuild search index and clear migration state
  if (done) {
    await rebuildSearchIndex();
    await rDel(MIGRATE_CURSOR_KEY);
    await rDel(MIGRATE_STATS_KEY);
  }

  return {
    total, processed: newCursor, remaining: Math.max(0, total - newCursor),
    ...stats, durationMs: Date.now() - t0, done,
  };
}

export async function resetMigration(): Promise<void> {
  await rDel(MIGRATE_CURSOR_KEY);
  await rDel(MIGRATE_STATS_KEY);
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