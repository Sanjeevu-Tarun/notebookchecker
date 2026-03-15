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
const STATS_TTL     = 30 * 24 * 3600; // 30 days — crawl stats only; entries use rSetPermanent
const LOCK_TTL      = 300;

// SOURCE A — NBC Smartphones review listing (internal NBC reviews only, ~80 pages)
// Each entry has "-review-" in its slug and contains full benchmarks/specs/images.
const NBC_REVIEWS_BASE = 'https://www.notebookcheck.net/?&hide_date=1&hide_youtube=1&showHighlightedTags=1&tagArray%5B%5D=10&typeArray%5B%5D=1&id=48';

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
  source?: 'review' | 'library'; // 'review' = from Source A (internal NBC review), 'library' = external
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

const _rax = axios.create({ timeout: 30000 });

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
// Paginated resolve: process BATCH_SIZE library URLs per call.
// Each call does live HTTP fetches — must stay under Vercel 30s limit.
// offset=0 → first batch, offset=N → next batch. Returns done:true when all resolved.
const RESOLVE_BATCH = 15; // 15 parallel fetches × ~3s each = ~5s total, safe under 30s

export async function resolveLibraryUrlsPage(offset: number): Promise<{
  resolved: number; alreadyReview: number; noReview: number;
  total: number; offset: number; done: boolean;
}> {
  const entries = await loadEntries();
  const libraryUrls = Object.keys(entries).filter(u => !/-review[-_.]/i.test(u));  // NEVER include review URLs
  const total = libraryUrls.length;
  const batch = libraryUrls.slice(offset, offset + RESOLVE_BATCH);
  const done = offset + RESOLVE_BATCH >= total;

  let resolved = 0, alreadyReview = 0, noReview = 0;
  const updates: Record<string, IndexEntry> = {};

  await Promise.all(batch.map(async (libraryUrl) => {
    try {
      // Always delete the stale cache key before resolving.
      // The old broken -review- regex may have cached "no review" — nuke it so we refetch fresh.
      const ck = `nbc:review_resolve:${libraryUrl}`;
      await rDel(ck);
      const reviewUrl = await resolveToReviewUrl(libraryUrl);

      if (!reviewUrl || reviewUrl === libraryUrl) {
        noReview++;
        return; // no internal review exists — keep library URL
      }

      alreadyReview++;

      // Review URL already in index from Source A — update its title to the clean
      // library title (e.g. "Vivo X300") then delete the library duplicate
      if (entries[reviewUrl]) {
        const libTitle = entries[libraryUrl]?.title;
        if (libTitle && libTitle.length < 60 && libTitle.length > 3) {
          entries[reviewUrl].title = libTitle;
        }
        delete entries[libraryUrl];
        resolved++;
        return;
      }

      // Add internal review URL using the library entry's clean title (e.g. "Vivo X300")
      // NOT derived from the review slug which may be a descriptive article title.
      const libraryTitle = entries[libraryUrl]?.title || '';
      const slug = reviewUrl.split('/').pop() || '';
      // Use library title if it's clean and short, otherwise extract from slug
      const title = (libraryTitle && libraryTitle.length < 60 && libraryTitle.length > 3)
        ? libraryTitle
        : (() => {
            const raw = slug.split(/-review[-_.]/i)[0].replace(/-/g, ' ').trim();
            return raw.split(' ').slice(-4).map((w: string) =>
              w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
          })();

      updates[reviewUrl] = {
        url: reviewUrl, title,
        brand: extractBrand(title), slug,
        discoveredAt: new Date().toISOString(),
        status: 'pending', retries: 0, source: 'review',
      };
      delete entries[libraryUrl]; // drop the library URL
      resolved++;
    } catch { noReview++; }
  }));

  if (resolved > 0) {
    Object.assign(entries, updates);
    await saveEntries(entries);
    // Rebuild search index immediately so /api/phone can find upgraded URLs right away
    rebuildSearchIndex().catch(() => {});
  }

  return { resolved, alreadyReview, noReview, total, offset: offset + RESOLVE_BATCH, done };
}

// Legacy full-batch version (kept for compatibility — only safe when resolve cache is warm)
export async function recoverDeletedReviewUrls(): Promise<{ recovered: number; alreadyPresent: number; totalCacheKeys: number }> {
  const entries = await loadEntries();
  const libraryUrls = Object.keys(entries).filter(u => !/-review[-_.]/i.test(u));  // NEVER include review URLs
  const totalCacheKeys = libraryUrls.length;
  let recovered = 0, alreadyPresent = 0;

  for (let i = 0; i < libraryUrls.length; i += 5) {
    const batch = libraryUrls.slice(i, i + 5);
    await Promise.all(batch.map(async (libraryUrl) => {
      try {
        const reviewUrl = await resolveToReviewUrl(libraryUrl);
        if (!reviewUrl || reviewUrl === libraryUrl) return;
        if (entries[reviewUrl]) { alreadyPresent++; return; }
        const slug = reviewUrl.split('/').pop() || '';
        const rawTitle = slug.split(/-review[-_.]/i)[0].replace(/-/g, ' ').trim();
        const title = rawTitle.split(' ').map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
        entries[reviewUrl] = { url: reviewUrl, title, brand: extractBrand(title), slug,
          discoveredAt: new Date().toISOString(), status: 'pending', retries: 0 };
        recovered++;
      } catch { /* skip */ }
    }));
  }

  if (recovered > 0) { await saveEntries(entries); await rebuildSearchIndex(); }
  return { recovered, alreadyPresent, totalCacheKeys };
}

const WRITE_LOCK_KEY = `nbc:index:${INDEX_VERSION}:write_lock`;
const WRITE_LOCK_TTL = 15; // 15s — one save takes <2s; this prevents pile-up

// ── ENTRY STORE ───────────────────────────────────────────────────────────────

async function loadEntries(): Promise<Record<string, IndexEntry>> {
  try { return await rGet(ENTRIES_KEY) as Record<string, IndexEntry>; }
  catch { return {}; }
}

async function saveEntries(e: Record<string, IndexEntry>): Promise<void> {
  // Acquire a short-lived write lock to prevent concurrent crawl jobs from
  // clobbering each other's updates (read-modify-write race on the entries blob).
  // Retry up to 6× with 500ms backoff (total wait ≤3s) before giving up.
  let acquired = false;
  for (let attempt = 0; attempt < 6; attempt++) {
    acquired = await rSetNX(WRITE_LOCK_KEY, '1', WRITE_LOCK_TTL);
    if (acquired) break;
    await new Promise(r => setTimeout(r, 500));
  }
  // If we still can't acquire after retries, proceed anyway rather than losing data —
  // the worst case is a write collision, which is the same as the pre-fix behaviour.
  try {
    const json = JSON.stringify(e);
    // Warn at 800KB — Upstash default max is 1MB, this gives headroom
    if (json.length > 800_000) {
      console.warn(`[saveEntries] WARN: entries blob is ${(json.length / 1024).toFixed(0)}KB — approaching Upstash 1MB limit. Consider sharding.`);
    }
    await rSetPermanent(ENTRIES_KEY, e); // rSetPermanent calls JSON.stringify internally
  } catch (err: any) {
    console.error('[saveEntries] FAILED — entries NOT saved:', err?.message ?? err);
    throw err;
  } finally {
    if (acquired) await rDel(WRITE_LOCK_KEY).catch(() => {});
  }
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
    $(sel).each((_: number, el: any) => {
      if (reviewUrl) return;
      let href = $(el).attr('href') || '';
      if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
      if (!href.startsWith('https://www.notebookcheck.net')) return;
      href = href.split('?')[0];
      if (!/-review[-_.]/i.test(href)) return;  // NEVER include review URLs
      if (!/\.\d{4,}\.0\.html$/.test(href)) return;
      if (!isJunkSlug(href.split('/').pop() || '')) reviewUrl = href;
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

    $('a[href]').each((_: number, el: any) => {
      if (reviewUrl) return;
      let href = $(el).attr('href') || '';
      if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
      if (!href.startsWith('https://www.notebookcheck.net')) return;
      href = href.split('?')[0];
      if (!/-review[-_.]/i.test(href)) return;  // NEVER include review URLs
      if (!/\.\d{4,}\.0\.html$/.test(href)) return;

      // The review slug must start with the device name words
      const reviewSlug = (href.split('/').pop() || '').toLowerCase();
      const allMatch = matchWords.every(w => reviewSlug.includes(w));
      if (!allMatch) return;
      if (isJunkSlug(reviewSlug)) return;

      reviewUrl = href;
    });
  }

  // Strategy 3: Last resort — scan all links for any review URL whose slug starts
  // with the full library slug prefix (exact match on the device name part)
  if (!reviewUrl) {
    $('a[href]').each((_: number, el: any) => {
      if (reviewUrl) return;
      let href = $(el).attr('href') || '';
      if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
      if (!href.startsWith('https://www.notebookcheck.net')) return;
      href = href.split('?')[0];
      if (!/-review[-_.]/i.test(href)) return;  // NEVER include review URLs
      if (!/\.\d{4,}\.0\.html$/.test(href)) return;
      const reviewSlug = (href.split('/').pop() || '').toLowerCase();
      if (reviewSlug.startsWith(librarySlug.slice(0, 10)) && !isJunkSlug(reviewSlug)) {
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

// ── JUNK SLUG FILTER ─────────────────────────────────────────────────────────
// Rejects NBC article URLs that are NOT full device reviews:
//   - Comparison articles  e.g. "Honor-Magic8-Pro-vs-Vivo-X300-Pro-photo-comparison"
//   - Camera-only tests    e.g. "Vivo-X300-Pro-camera-test-in-Cyprus"
//   - Hands-on / first-look / unboxing / teardown
//   - Announced / leaked / rumour posts
//   - Camera reviews (not full device review)
//   - Award/best-of roundups
// Applied at crawl time (all three sources) and at purge time (clean up existing junk).
function isJunkSlug(slug: string): boolean {
  const s = slug.toLowerCase();
  return (
    /(-|)vs(-|)/.test(s)            ||  // comparison: "Honor-X-vs-Vivo-Y"
    /photo.?comparison/.test(s)          ||  // photo comparison articles
    /camera.?test/.test(s)               ||  // camera test articles
    /camera.?review/.test(s)             ||  // camera-only review
    /hands?.?on/.test(s)                 ||  // hands-on articles
    /first.?look/.test(s)                ||
    /unboxing/.test(s)                   ||
    /teardown/.test(s)                   ||
    /announced/.test(s)                  ||
    /unveiled/.test(s)                   ||
    /leaked/.test(s)                     ||
    /rumou?r/.test(s)                    ||
    /price.?drop/.test(s)                ||
    /best.?of/.test(s)                   ||
    /roundup/.test(s)                    ||
    /part-[0-9]/.test(s)                 ||  // "camera-test-in-Cyprus-Part-1"
    /showdown/.test(s)                       // "Showdown in London"
  );
}

// Extract clean device name from a review page link.
// For descriptive article titles like "More than just an unusual camera setup – Vivo X300 Pro review"
// we prefer the part after a separator (–, :, |) if it looks like a device name.
// Fallback: last 4 words of the pre-review slug.
// Noise words that appear in review titles but are not part of device names
const TITLE_NOISE = /\b(smartphone|smartphones|mobile|phone|phones|handset|tablet|device|review|test|verdict|hands.on)\b/gi;

function extractDeviceTitle(linkText: string, url: string): string {
  // Try: split on — – | : and take the last segment that looks like a device name
  const parts = linkText.split(/\s*[–—|:]\s*/);
  for (let i = parts.length - 1; i >= 0; i--) {
    const part = parts[i]
      .replace(TITLE_NOISE, '')
      .replace(/\s+/g, ' ').trim();
    if (part.length > 3 && part.length < 50 && !/\b(the|a|an|is|in|for|with|of|to)\b/i.test(part)) {
      return part;
    }
  }
  // Fallback: last 4 meaningful words from pre-review slug
  const slug = url.split('/').pop() || '';
  const pre = slug.split(/-review[-_.]/i)[0].replace(/-/g, ' ').trim();
  const words = pre.split(' ').filter(w => w.length > 1 && !TITLE_NOISE.test(w));
  TITLE_NOISE.lastIndex = 0; // reset regex state
  return words.slice(-4).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ') || linkText.slice(0, 50);
}

export function extractPhoneUrls(html: string): Array<{ url: string; title: string }> {
  const $ = cheerio.load(html);
  const out: Array<{ url: string; title: string }> = [];
  const seen = new Set<string>();

  $('a[href]').each((_: number, el: any) => {
    let href = $(el).attr('href') || '';
    if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
    else if (!href.startsWith('http')) return;
    href = href.split('?')[0];

    if (!href.includes('notebookcheck.net')) return;
    if (!/\.\d{4,}\.0\.html$/.test(href)) return;

    const slug = href.split('/').pop() || '';
    if (/^(Reviews|Smartphones|Search|Topics|RSS|index|Notebooks|News|Smartphone|Library|Comparison|Chronological)\./i.test(slug)) return;
    if (/-Series\./i.test(slug)) return;
    if (isJunkSlug(slug)) return;

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
  source: 'reviews' | 'chrono' | 'smartphone';
}

function makeEntry(url: string, title: string, source: 'review' | 'library' = 'library'): IndexEntry {
  return { url, title, brand: extractBrand(title), slug: url.split('/').pop() || '',
    discoveredAt: new Date().toISOString(), status: 'pending', retries: 0, source };
}

// ── SOURCE A: NBC Smartphones reviews listing ─────────────────────────────────
export async function crawlReviewsPage(page: number): Promise<CrawlPageResult> {
  const t0 = Date.now();
  const url = page === 0 ? NBC_REVIEWS_BASE : `${NBC_REVIEWS_BASE}&ns_page=${page}`;
  const html = await fetchHtml(url);
  const $ = cheerio.load(html);

  const found: Array<{ url: string; title: string }> = [];
  const seen = new Set<string>();

  // Source A page is pre-filtered to phones + internal reviews only.
  // No need to check for -review- in the slug — trust the page content.
  // Still skip junk slugs (comparisons, camera tests, etc.) just in case.
  $('a[href]').each((_: number, el: any) => {
    let href = $(el).attr('href') || '';
    if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
    href = href.split('?')[0];
    if (!href.includes('notebookcheck.net')) return;
    if (!/\.\d{4,}\.0\.html$/.test(href)) return;
    if (seen.has(href.toLowerCase())) return;
    const slug = href.split('/').pop() || '';
    if (isJunkSlug(slug)) return;
    seen.add(href.toLowerCase());

    const title = ($(el).attr('title') || $(el).text().trim() || '')
      .replace(/^\d+%\s*/, '').replace(/\s+/g, ' ').trim().slice(0, 120);
    const cleanTitle = extractDeviceTitle(title, href);
    if (cleanTitle.length < 4) return;
    found.push({ url: href, title: cleanTitle });
  });

  // Empty page = no review links at all
  const pageIsEmpty = found.length === 0 && !html.includes('notebookcheck.net');

  const entries = await loadEntries();
  let newUrls = 0;
  for (const { url: u, title } of found) {
    if (!entries[u]) { entries[u] = makeEntry(u, title, 'review'); newUrls++; }
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

  $('a[href]').each((_: number, el: any) => {
    let href = $(el).attr('href') || '';
    if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
    href = href.split('?')[0];
    if (!href.includes('notebookcheck.net')) return;
    if (!/\.\d{4,}\.0\.html$/.test(href)) return;
    if (seen.has(href.toLowerCase())) return;
    seen.add(href.toLowerCase());

    const title = ($(el).attr('title') || $(el).text().trim() || '')
      .replace(/^\d+%\s*/, '').replace(/\s+/g, ' ').trim().slice(0, 120);
    const cleanTitle = title.split(/\s*[|—–:]\s*/)[0].split(' - ')[0].replace(/\breviews?\b/gi, '').replace(/\s+/g, ' ').trim();
    if (cleanTitle.length < 4) return;
    found.push({ url: href, title: cleanTitle });
  });

  const rawLinks = (html.match(/\.notebookcheck\.net\/[^"']+\.\d+\.0\.html/g) || []).length;
  const pageIsEmpty = rawLinks === 0;

  const entries = await loadEntries();

  // Build review prefix set — skip devices already covered by a review URL
  const reviewPrefixes = new Set<string>();
  for (const u of Object.keys(entries)) {
    if (/-review[-_.]/i.test(u)) {
      const slug = u.split('/').pop() || '';
      reviewPrefixes.add(slug.toLowerCase().split(/-review[-_.]/i)[0]);
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

  // Batch-fetch resolve cache for all new library URLs in one pipeline call
  const libToResolve = toResolve.filter(({ url: u }) => !/-review[-_.]/i.test(u));
  let resolveMapC: Record<string, string> = {};
  if (libToResolve.length > 0) {
    try {
      const { url: rUrl, token: rToken } = rBase();
      const pipeline = libToResolve.map(({ url: u }) => ['GET', `nbc:review_resolve:${u}`]);
      const resp = await _rax.post(`${rUrl}/pipeline`, pipeline,
        { headers: { Authorization: `Bearer ${rToken}`, 'Content-Type': 'application/json' } });
      resp.data.forEach((item: any, i: number) => {
        if (item.result) {
          try { resolveMapC[libToResolve[i].url] = JSON.parse(item.result); } catch { resolveMapC[libToResolve[i].url] = item.result; }
        }
      });
    } catch { /* cache miss */ }
  }

  let newUrls = 0;
  for (const { url: u, title } of toResolve) {
    if (entries[u]) continue;
    let finalUrl = u;
    let finalTitle = title;
    const cached = resolveMapC[u];
    if (cached && cached !== u && /-review[-_.]/i.test(cached)) {
      finalUrl = cached;
      const slug = cached.split('/').pop() || '';
      const raw = slug.split(/-review[-_.]/i)[0].replace(/-/g, ' ').trim();
      finalTitle = raw.split(' ').map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
    }
    if (entries[finalUrl]) continue;
    entries[finalUrl] = makeEntry(finalUrl, finalTitle);
    newUrls++;
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
// Stores library URLs raw — fast, Vercel-safe (no extra HTTP fetches per page).
// After crawling, run /api/index/recover-review-urls to resolve library URLs →
// internal NBC review URLs in bulk (uses Redis-cached resolves, very fast).
export async function crawlChronoPage(page: number): Promise<CrawlPageResult> {
  const t0 = Date.now();
  const chronoUrl = page === 1 ? NBC_CHRONO_BASE : `${NBC_CHRONO_BASE}?&ns_page=${page}`;
  const html = await fetchHtml(chronoUrl);
  const found = extractPhoneUrls(html);

  const rawLinks = (html.match(/\.notebookcheck\.net\/[^"']+\.\d+\.0\.html/g) || []).length;
  const pageIsEmpty = rawLinks === 0;

  const entries = await loadEntries();

  // Titles of devices that already have an internal review in the index.
  // Stops library URLs from being added when the review already exists.
  const reviewedTitles = new Set<string>();
  for (const [u, e] of Object.entries(entries)) {
    if (/-review[-_.]/i.test(u) && !isJunkSlug(u.split('/').pop() || '')) {
      reviewedTitles.add(e.title.toLowerCase().trim());
    }
  }

  // Batch-fetch resolve cache for all NEW library URLs in one pipeline call.
  // This avoids N sequential Redis calls (one per URL) which would timeout on large pages.
  const newFound = found.filter(({ url: u }) => !entries[u]);
  const libraryOnly = newFound.filter(({ url: u }) => !/-review[-_.]/i.test(u));
  let resolveMap: Record<string, string> = {};
  if (libraryOnly.length > 0) {
    try {
      const { url: rUrl, token: rToken } = rBase();
      const pipeline = libraryOnly.map(({ url: u }) =>
        ['GET', `nbc:review_resolve:${u}`]
      );
      const resp = await _rax.post(`${rUrl}/pipeline`, pipeline,
        { headers: { Authorization: `Bearer ${rToken}`, 'Content-Type': 'application/json' } }
      );
      resp.data.forEach((item: any, i: number) => {
        if (item.result) {
          try { resolveMap[libraryOnly[i].url] = JSON.parse(item.result); } catch { resolveMap[libraryOnly[i].url] = item.result; }
        }
      });
    } catch { /* cache miss — resolveMap stays empty, use library URLs as-is */ }
  }

  let newUrls = 0;
  for (const { url: u, title } of newFound) {
    // Skip if review exists with same title
    if (reviewedTitles.has(title.toLowerCase().trim())) continue;

    // Use cached review URL if available
    let finalUrl = u;
    let finalTitle = title;
    const cached = resolveMap[u];
    if (cached && cached !== u && /-review[-_.]/i.test(cached)) {
      finalUrl = cached;
      const rSlug = cached.split('/').pop() || '';
      const raw = rSlug.split(/-review[-_.]/i)[0].replace(/-/g, ' ').trim();
      finalTitle = raw.split(' ').map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
    }

    if (entries[finalUrl]) continue;

    entries[finalUrl] = makeEntry(finalUrl, finalTitle);
    newUrls++;
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
    await rSet(STATS_KEY, stats, STATS_TTL);
    await rDelForce([LOCK_KEY, PROGRESS_KEY]);
    return stats;

  } catch (e: any) {
    await rDelForce([LOCK_KEY, PROGRESS_KEY]);
    const stats: CrawlStats & { nextPage: number | null } = {
      totalPages: pagesRead, totalUrls: lastTotalUrls, newUrls,
      crawlMs: Date.now() - t0, lastCrawledAt: new Date().toISOString(),
      error: e.message, nextPage: null,
    };
    await rSet(STATS_KEY, stats, STATS_TTL);
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

  // Deduplicate: for each device keep only the best entry.
  // Pass 1 — title-key dedup: prefer NBC internal review URL over library URL.
  // Pass 2 — slug-prefix dedup: if two entries share the same device name prefix
  //   (e.g. library "samsung-galaxy-a55-5g" + review "samsung-galaxy-a55-5g-review-…")
  //   the review URL wins even when their stored titles differ (covers the stale-title bug).
  const byTitle  = new Map<string, IndexEntry>();
  const byPrefix = new Map<string, IndexEntry>();

  for (const e of Object.values(entries) as IndexEntry[]) {
    const isReview   = /-review[-_.]/i.test(e.url);
    const titleKey   = e.title.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim();
    const slugPrefix = (e.slug || e.url.split('/').pop() || '')
      .replace(/\.\d+\.0\.html$/, '')
      .toLowerCase()
      .split(/-review[-_.]/i)[0]
      .replace(/-/g, ' ')
      .trim();

    // Title-key dedup
    const exTitle = byTitle.get(titleKey);
    if (!exTitle || (isReview && !/-review[-_.]/i.test(exTitle.url))) {
      byTitle.set(titleKey, e);
    }

    // Slug-prefix dedup (only meaningful slugs ≥ 5 chars)
    if (slugPrefix.length >= 5) {
      const exSlug = byPrefix.get(slugPrefix);
      if (!exSlug || (isReview && !/-review[-_.]/i.test(exSlug.url))) {
        byPrefix.set(slugPrefix, e);
      }
    }
  }

  // Merge both maps — url is the unique key; review entries always beat library entries
  const merged = new Map<string, IndexEntry>();
  for (const e of byTitle.values())  merged.set(e.url, e);
  for (const e of byPrefix.values()) {
    const existing = merged.get(e.url);
    if (!existing) merged.set(e.url, e);
    else if (/-review[-_.]/i.test(e.url) && !/-review[-_.]/i.test(existing.url)) {
      merged.set(e.url, e);
    }
  }

  const flat = Array.from(merged.values()).map((e: IndexEntry) => ({
    url: e.url, title: e.title, slug: e.slug,
  }));
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
  const reasons: Record<string, number> = { hasReviewCounterpart: 0, junkTitle: 0 };

  // Build a set of exact titles that have an internal review URL.
  // Only genuine review URLs (not junk comparisons/camera-tests) contribute.
  // Matching is exact title == exact title (case-insensitive).
  // This is the ONLY safe way — substring/slug matching causes false positives
  // e.g. "fe" substring matches "perfectly", purging Vivo X300 FE when only X300 has a review.
  const reviewTitles = new Set<string>();
  for (const [url, e] of Object.entries(entries)) {
    if (!/-review[-_.]/i.test(url)) continue;  // NEVER include review URLs
    if (isJunkSlug(url.split('/').pop() || '')) continue;
    reviewTitles.add(e.title.toLowerCase().trim());
  }

  const toDelete: string[] = [];
  for (const [url, e] of Object.entries(entries)) {
    const slug = url.split('/').pop() || '';

    // ABSOLUTE SAFETY: never touch any URL that is a genuine internal review.
    // This check runs before everything else — no other rule can override it.
    if (/-review[-_.]/i.test(url)) continue;

    // Rule 0: junk article that slipped through crawl (comparison, camera test, etc.)
    // Note: isJunkSlug only runs on non-review URLs (guard above already skipped reviews).
    if (isJunkSlug(slug)) {
      toDelete.push(url);
      reasons.junkTitle++;
      continue;
    }

    // Rule 1: junk title (article snippet — too long to be a clean device name)
    if (e.title.length > 80) {
      toDelete.push(url);
      reasons.junkTitle++;
      continue;
    }

    // Rule 2: an internal review exists with the EXACT same title → library URL is redundant.
    // "Vivo X300" library purged only if a review entry also has title "Vivo X300".
    // "Vivo X300 FE" is safe — no review with that exact title exists.
    // "Vivo X300 Pro" is safe — no review with that exact title exists (until one is crawled).
    if (reviewTitles.has(e.title.toLowerCase().trim())) {
      toDelete.push(url);
      reasons.hasReviewCounterpart++;
    }
  }

  for (const url of toDelete) delete entries[url];

  await saveEntries(entries);
  await rebuildSearchIndex();

  return { purged: toDelete.length, kept: Object.keys(entries).length, reasons };
}

// Normalise a slug or URL into a searchable token string.
// "Samsung-Galaxy-A55-5G-review-A-lot-of-premium.835803.0.html"
//   → "samsung galaxy a55 5g review a lot of premium"
// Used to match query words against slug tokens when title match fails.
function slugToTokens(slugOrUrl: string): string {
  const slug = slugOrUrl.split('/').pop() || slugOrUrl;
  return slug
    .replace(/\.\d+\.0\.html$/, '')   // strip numeric id + .html
    .replace(/[-_]/g, ' ')             // dashes/underscores → spaces
    .toLowerCase()
    .trim();
}

// Match user query directly against stored titles AND slug/URL.
// Titles in Redis are already clean: "Samsung Galaxy S25 Ultra", "Google Pixel 9 Pro XL".
// However, titles can be wrong/stale (e.g. "Samsung Galaxy A50s" stored for an A55 entry).
// The slug and URL are always authoritative — if the query words all appear in the slug,
// that is a strong signal and should score highly regardless of the stored title.
//
// Strategy (scored, highest wins):
//   TIER 1 (score 10000): all query words found in title AND title has no extra variants
//   TIER 2 (score  8000): all query words found in slug  AND slug has no extra variants
//   TIER 3 (score  5000): all query words found in title, some extra variants present
//   TIER 4 (score  3000): all query words found in slug,  some extra variants present
//
// Within each tier, shorter title wins (more precise match).
export async function searchIndex(q: string, _nq?: string): Promise<{ url: string; title: string } | null> {
  let flat: Array<{ url: string; title: string; slug: string }> = [];
  try {
    flat = await rGet(SEARCH_INDEX_KEY) as any[];
  } catch {
    // Key missing — rebuild from entries
    await rebuildSearchIndex();
    try { flat = await rGet(SEARCH_INDEX_KEY) as any[]; } catch { return null; }
  }
  // If flat is empty, try rebuilding once — entries may have been added since last rebuild
  if (!flat?.length) {
    await rebuildSearchIndex();
    try { flat = await rGet(SEARCH_INDEX_KEY) as any[]; } catch { return null; }
  }
  if (!flat?.length) return null;

  // Tokenize the raw user query — lowercase words, 2+ chars OR single digit
  // Single digits are critical model discriminators: "pixel 8 pro" ≠ "pixel 9 pro".
  // w.length >= 2 alone drops "8", "9", "3", making all "Pixel X Pro" queries identical.
  const rawWords = q.toLowerCase().trim().split(/\s+/).filter(w =>
    w.length >= 2 || /^\d+$/.test(w)
  );
  if (!rawWords.length) return null;

  // Variant suffix list — if title/slug has one of these but query doesn't,
  // the match is penalised (but not hard-rejected, since slug-based match is a fallback)
  const VARIANTS = new Set([
    'ultra','pro','plus','mini','lite','fe','max','edge',
    'standard','turbo','fold','flip','xl','xr','se',
    '5g','4g','go',
  ]);

  const queryWordSet = new Set(rawWords);

  function hasExtraVariant(tokenStr: string): boolean {
    return tokenStr.split(/\s+/).some(tw => VARIANTS.has(tw) && !queryWordSet.has(tw));
  }

  const candidates: Array<{ url: string; title: string; score: number; titleLen: number }> = [];

  for (const entry of flat) {
    const titleTokens = entry.title.toLowerCase();
    const slugTokens  = slugToTokens(entry.slug || entry.url);

    // Use word-boundary matching so "8" does not match inside "18", "80", "X800" etc.
    const wbMatch = (tokens: string, w: string) =>
      new RegExp('(?<![a-z0-9])' + w.replace(/[.*+?^${}()|[\]\]/g, '\\$&') + '(?![a-z0-9])').test(tokens);
    const titleHitsAll = rawWords.every(w => wbMatch(titleTokens, w));
    const slugHitsAll  = rawWords.every(w => wbMatch(slugTokens, w));

    // Neither title nor slug contains all query words — skip entirely
    if (!titleHitsAll && !slugHitsAll) continue;

    let score = 0;

    if (titleHitsAll) {
      score = hasExtraVariant(titleTokens) ? 5000 : 10000;
    }

    if (slugHitsAll) {
      const slugScore = hasExtraVariant(slugTokens) ? 3000 : 8000;
      // Take whichever signal is stronger
      if (slugScore > score) score = slugScore;
    }

    if (score > 0) {
      candidates.push({ url: entry.url, title: entry.title, score, titleLen: entry.title.length });
    }
  }

  if (!candidates.length) return null;

  // Sort: highest score first, then shortest title as tiebreaker
  candidates.sort((a, b) => b.score - a.score || a.titleLen - b.titleLen);
  return { url: candidates[0].url, title: candidates[0].title };
}


// ── SCRAPE INDEXED DEVICE ────────────────────────────────────────────────────
export async function clearScrapeCache(url: string): Promise<void> {
  const { clearDeviceCache } = await import('./notebookcheck');
  await clearDeviceCache(url);
}

export async function scrapeIndexedDevice(url: string, title?: string): Promise<{ success: boolean; data?: any; error?: string; cached?: boolean }> {
  // scrapeNotebookCheckDevice has its own Redis cache (nbc:device:...) — use it directly.
  // Pass `title` as deviceName so image folder matching uses the clean device name
  // rather than deriving it from the URL slug.
  try {
    const { scrapeNotebookCheckDevice } = await import('./notebookcheck');
    const t0 = Date.now();
    const data = await scrapeNotebookCheckDevice(url, title);
    const ms = Date.now() - t0;
    // Detect device-level cache hit by timing.
    // mem cache hit: ~0ms | warm Redis hit: ~50-150ms | cold Redis (TLS): ~150-350ms
    // Live scrape: >1000ms. Use 400ms as a safe threshold covering all cache scenarios.
    const cached = ms < 400;

    // scrapeNotebookCheckDevice initialises pageFound.name to '' — fill it from index title
    if (data && title && (!data.pageFound?.name)) {
      data.pageFound = { name: title, url };
    }

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