import express from 'express';
import cors from 'cors';
import { getNotebookCheckDataFast, scrapeNotebookCheckDevice, debugNBCSearch, clearFullCache } from './src/notebookcheck';
import { getGSMArenaData, searchGSMArena, scrapeGSMArenaDevice } from './src/gsmarena';
import {
  getNotebookCheckProcessor,
  searchNotebookCheckProcessors,
  scrapeProcessorByUrl,
  normalizeProcQuery,
  searchProcViaSearXNG,
  scrapeNotebookCheckProcessor,
  PROC_CACHE_VERSION,
} from './src/notebookcheck_processor';
import {
  crawlSync,
  crawlOnePage,
  crawlReviewsPage,
  crawlSmartphonePage,
  getQueueStats,
  getIndexEntries,
  scrapeIndexedDevice,
  validateIndexUrl,
  getBrandBreakdown,
  resetErrors,
  resetEntry,
  clearIndex,
  getCrawlInProgress,
  getLastCrawlStats,
  getCrawlProgress,
  resetCrawlLock,
  extractPhoneUrls,
  rebuildSearchIndex,
  searchIndex,
  migrateToReviewUrls,
  resetMigration,
  purgeLibraryDuplicates,
  recoverDeletedReviewUrls,
  resolveLibraryUrlsPage,
  type CrawlPageResult,
  type MigrateResult,
} from './src/notebookcheck_index';

const app = express();
app.use(cors());

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { version } = require('./package.json') as { version: string };
app.get('/api/health', (_, res) => res.json({ status: 'ok', version }));

// Shared query validator — rejects missing, non-string, or oversized queries
// Extract the raw ?q= value from the URL BEFORE Express decodes it.
//
// The "+" ambiguity in query strings:
//   - Form-encoded spaces:  "pixel 8 pro" → "pixel+8+pro"  (+ = space)
//   - Device name suffix:   "samsung s25+" → "samsung%20s25+"  (+ = literal plus)
//   - %2B is always literal plus regardless of context
//
// Rule: a "+" that appears at the END of the q value (before "&" or end of string)
// and immediately follows an alphanumeric char is a literal plus (device suffix).
// All other "+" are spaces (standard form encoding).
// This correctly handles:
//   samsung%20s25+   → "samsung s25+"  (browser-encoded with %20 for space)
//   pixel+8+pro      → "pixel 8 pro"   (form-encoded spaces)
//   iphone+16+pro%2B → "iphone 16 pro+" (%2B = explicit literal plus)
//   s25+             → "s25+"           (trailing plus = device suffix)
function rawQ(req: import('express').Request): string | null {
  try {
    const qs = req.originalUrl.split('?')[1] ?? '';
    const match = qs.match(/(?:^|&)q=([^&]*)/);
    if (!match) return null;
    let raw = match[1];
    // Decode percent-encoding first (%2B -> +, %20 -> space, etc.)
    // Replace form-encoded spaces (+) with actual spaces EXCEPT where + follows
    // an alphanumeric (device suffix like s25+) — protect those first.
    raw = raw.replace(/%2B/gi, '+');           // %2B -> literal +
    raw = raw.replace(/([a-zA-Z0-9])\+/g, '$1LITERALPLUS'); // protect device + suffix
    raw = raw.replace(/\+/g, ' ');             // remaining + = form-encoded spaces
    raw = decodeURIComponent(raw);
    raw = raw.replace(/LITERALPLUS/g, '+');    // restore device + suffix
    raw = raw.trim().slice(0, 200);
    return raw || null;
  } catch {
    return null;
  }
}

function validateQ(q: unknown): string | null {
  if (typeof q !== 'string' || !q.trim()) return null;
  return q.trim().slice(0, 200); // cap at 200 chars to prevent giant cache keys
}

// ─────────────────────────────────────────────────────────────────────────────
// /api/phone — NotebookCheck ONLY (fast version, no GSMArena)
// Flow: full-result cache → Redis index → SearXNG fallback
// ─────────────────────────────────────────────────────────────────────────────
app.get('/api/phone', async (req, res) => {
  const q = (rawQ(req) ?? validateQ(req.query.q))?.replace(/[+]/g, ' plus').replace(/\s+/g, ' ').trim() || null;
  const nocache = req.query.nocache === '1';
  if (!q) return res.status(400).json({ success: false, error: '"q" query param is required' });

  try {
    if (nocache) {
      // Clear full-result cache FIRST — otherwise it fires before device cache check
      await clearFullCache(q).catch(() => {});
      const { clearScrapeCache, searchIndex } = await import('./src/notebookcheck_index');
      const best = await searchIndex(q).catch(() => null);
      if (best) await clearScrapeCache(best.url).catch(() => {});
    }

    const result = await getNotebookCheckDataFast(q);
    if (!result) return res.status(404).json({ success: false, error: 'Device not found' });
    const { data, source } = result;
    if ('error' in data) return res.status(502).json({ success: false, source, ...data });
    return res.json({ success: true, source, data });

  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/phone/debug — per-stage timing: index lookup → scrape or SearXNG fallback
app.get('/api/phone/debug', async (req, res) => {
  const q = ((rawQ(req) ?? validateQ(req.query.q) ?? 'samsung s25 ultra')).replace(/[+]/g, ' plus').replace(/\s+/g, ' ').trim();
  const nocache = req.query.nocache === '1';
  const t0 = Date.now();

  try {
    const { searchIndex, scrapeIndexedDevice, clearScrapeCache } = await import('./src/notebookcheck_index');

    // Stage 1: index lookup
    const ti0 = Date.now();
    const indexMatch = await searchIndex(q).catch(() => null);
    const indexSearchMs = Date.now() - ti0;

    let source = 'none';
    let data: any = null;
    let indexScrapeMs = 0;
    let searxngMs = 0;
    let cached = false;

    if (indexMatch) {
      if (nocache) {
        await clearFullCache(q).catch(() => {});
        await clearScrapeCache(indexMatch.url).catch(() => {});
      }
      const ts0 = Date.now();
      const result = await scrapeIndexedDevice(indexMatch.url, indexMatch.title).catch(() => null);
      indexScrapeMs = Date.now() - ts0;
      if (result?.success) {
        data = result.data;
        cached = !!result.cached;
        source = result.cached ? 'device-cache' : 'index';
      }
    }

    // Stage 2: SearXNG fallback only if index missed or scrape failed
    if (!data) {
      const ts0 = Date.now();
      const fallback = await getNotebookCheckDataFast(q).catch(() => null);
      searxngMs = Date.now() - ts0;
      data = fallback?.data ?? null;
      source = fallback?.source ?? 'searxng-fallback';
    }

    return res.json({
      query: q, source, totalMs: Date.now() - t0,
      timing: { indexSearchMs, indexScrapeMs: indexScrapeMs || undefined, searxngMs: searxngMs || undefined },
      cached,
      indexMatch: indexMatch ? { url: indexMatch.url, title: indexMatch.title } : null,
      result: data ? {
        title: data.title, url: data.reviewUrl || data.sourceUrl, rating: data.rating,
        hasBenchmarks: data.benchmarks ? Object.values(data.benchmarks).some((b: any) => b.length > 0) : false,
        hasSpecs: data.specs ? Object.keys(data.specs).length > 2 : false,
        imageCounts: data.images ? {
          device: data.images.device?.length || 0, cameraSamples: data.images.cameraSamples?.length || 0,
          screenshots: data.images.screenshots?.length || 0, charts: data.images.charts?.length || 0,
        } : null,
      } : null,
      error: data ? undefined : 'Not found in index or SearXNG',
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/api/phone/suggestions', async (req, res) => {
  const q = validateQ(req.query.q);
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });
  try {
    const results = await searchGSMArena(q);
    return res.json({ success: true, source: 'gsmarena', data: results });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/api/phone/device', async (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: '"url" required' });
  try {
    const data = await scrapeGSMArenaDevice(url);
    return res.json({ success: true, source: 'gsmarena', data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// Legacy NanoReview endpoints (scraper functions removed — stubs kept for API compat)
app.get('/api/search', async (req, res) => {
  return res.status(410).json({ success: false, error: 'Legacy endpoint removed. Use /api/phone?q=<device>' });
});
app.get('/api/suggestions', async (req, res) => {
  return res.status(410).json({ success: false, error: 'Legacy endpoint removed. Use /api/nbc/suggestions?q=<device>' });
});
app.get('/api/device', async (req, res) => {
  return res.status(410).json({ success: false, error: 'Legacy endpoint removed. Use /api/nbc/device?url=<url>' });
});

// NotebookCheck debug endpoints
app.get('/api/nbc/debug', async (req, res) => {
  const q = (validateQ(req.query.q) ?? 'vivo x300 pro');
  try {
    const result = await debugNBCSearch(q);
    return res.json(result);
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message, code: e.code });
  }
});


// /api/nbc/searxng-debug — full search+scrape pipeline with per-stage timing
app.get('/api/nbc/searxng-debug', async (req, res) => {
  const q = (validateQ(req.query.q) ?? 'oneplus 15');
  const t0 = Date.now();

  try {
    const { searchViaSearXNG, scrapeNotebookCheckDevice, normalizeQuery, resolveSearchResult } =
      await import('./src/notebookcheck');

    const oq = q.trim();
    const nq = normalizeQuery(q);
    const stages: any = {};

    // Stage 1: SearXNG
    const t1 = Date.now();
    const searchResults = await searchViaSearXNG(nq, oq);
    stages.searxng = { ms: Date.now() - t1, count: searchResults.length };

    if (!searchResults.length) {
      const debugLog = (globalThis as any).__searxng_debug || [];
      return res.json({ 
        query: q, 
        stages, 
        totalMs: Date.now() - t0, 
        error: 'SearXNG returned 0 results',
        debug: debugLog 
      });
    }

    const sorted = searchResults.sort((a: any, b: any) => b.score - a.score);
    const searchCk = `nbc:search:fast:debug:${q.toLowerCase().trim()}`;
    const page = resolveSearchResult(searchResults, nq, oq, searchCk);
    stages.resolve = { ms: 0, url: page.url }; // synchronous — no ms cost

    // Stage 2: Scrape
    const t3 = Date.now();
    const deviceData = await scrapeNotebookCheckDevice(page.url, page.name);
    stages.scrape = { ms: Date.now() - t3 };

    return res.json({
      query: q,
      stages,
      totalMs: Date.now() - t0,
      topResult: { url: page.url, title: page.name, score: sorted[0].score },
      allResults: sorted.slice(0, 5).map((r: any) => ({ url: r.url, title: r.title, score: r.score })),
      deviceSpecs: {
        title: deviceData?.title,
        rating: deviceData?.rating,
        soc: deviceData?.soc,
        ram: deviceData?.ram,
        os: deviceData?.os,
        price: deviceData?.price,
        weight: deviceData?.weight,
        benchmarkCounts: {
          cpu: deviceData?.benchmarks?.cpu?.length ?? 0,
          gpu: deviceData?.benchmarks?.gpu?.length ?? 0,
          battery: deviceData?.benchmarks?.battery?.length ?? 0,
        },
      },
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message, totalMs: Date.now() - t0 });
  }
});

app.get('/api/nbc/suggestions', async (req, res) => {
  const q = rawQ(req) ?? validateQ(req.query.q);
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });
  try {
    const { searchNotebookCheck } = await import('./src/notebookcheck');
    const data = await searchNotebookCheck(q);
    return res.json({ success: true, data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/api/nbc/device', async (req, res) => {
  const url = req.query.url as string;
  const name = req.query.name as string | undefined;
  if (!url) return res.status(400).json({ success: false, error: '"url" required' });
  try {
    const data = await scrapeNotebookCheckDevice(url, name);
    return res.json({ success: true, data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/', (_, res) => res.json({
  status: 'ok',
  endpoints: {
    // Phone / Device — Redis index first, SearXNG fallback
    phone:            '/api/phone?q=<device>',
    phoneDebug:       '/api/phone/debug?q=<device>',
    nbcDevice:        '/api/nbc/device?url=<notebookcheck-url>',
    nbcSuggestions:   '/api/nbc/suggestions?q=<device>   2190 SearXNG-only suggestions',
    nbcDebug:         '/api/nbc/debug?q=<device>',
    // Index management
    indexCrawlDebug:  '/api/index/crawl-debug               ← START HERE: verify NBC is reachable',
    indexCrawl:       '/api/index/crawl?maxPages=3           ← crawl N pages',
    indexStatus:      '/api/index/status                     ← crawl progress + coverage',
    indexValidate:    '/api/index/validate?count=10          ← verify URLs are correct',
    indexList:        '/api/index/list?status=pending&limit=20',
    indexScrapeOne:   '/api/index/scrape-one?url=<url>       ← scrape single device',
    indexSearchDebug: '/api/index/search-debug?q=<device>    ← verify index hit/miss',
    indexErrors:      '/api/index/errors',
    indexResetErrors: '/api/index/reset-errors',
    indexBrands:      '/api/index/brands',
    indexRebuild:     '/api/index/rebuild-search             ← rebuild search index',
    // Processor / SoC
    processor:            '/api/processor?q=<chip>',
    processorSuggestions: '/api/processor/suggestions?q=<chip>',
    processorDevice:      '/api/processor/device?url=<notebookcheck-url>',
  }
}));

// ─────────────────────────────────────────────────────────────────────────────
// KEEP-ALIVE: /api/nbc/keepalive pings SearXNG on Render free tier.
// Call it every 10 min via UptimeRobot (free) to prevent cold starts.
// URL: https://your-app.vercel.app/api/nbc/keepalive
// ─────────────────────────────────────────────────────────────────────────────
const SEARXNG_INSTANCE = 'https://searxng-notebookcheck.onrender.com';


// Expose a manual ping trigger endpoint for debugging
// /api/nbc/direct-debug?q=<device>
// Shows exactly what NBC's own search returns — raw HTML links, scoring, timing.
// Use this to verify NBC direct search is working before relying on it.
// Example: /api/nbc/direct-debug?q=vivo+x300
app.get('/api/nbc/direct-debug', async (req, res) => {
  const q = (validateQ(req.query.q) ?? 'vivo x300');
  const t0 = Date.now();

  try {
    const { normalizeQuery } = await import('./src/notebookcheck');
    const axios = (await import('axios')).default;
    const cheerio = await import('cheerio');

    const oq = q.trim();
    const nq = normalizeQuery(q);
    const primaryQuery = nq !== oq ? nq : oq;

    // Hit NBC search directly via POST — NBC uses TYPO3 Indexed Search.
    // GET ?word= is silently ignored by TYPO3; the correct field is
    // tx_indexedsearch_pi2[search][sword] submitted as POST form data.
    const NBC_SEARCH_URL = 'https://www.notebookcheck.net/Search.8222.0.html';
    const postBody = new URLSearchParams({
      'tx_indexedsearch_pi2[search][sword]': primaryQuery,
      'tx_indexedsearch_pi2[action]': 'search',
    }).toString();
    const searchUrl = NBC_SEARCH_URL; // for the response payload (method is POST)
    const fetchMs0 = Date.now();
    let html = '';
    let fetchError = '';
    let httpStatus = 0;

    try {
      const resp = await axios.post(NBC_SEARCH_URL, postBody, {
        headers: {
          'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          'Content-Type':    'application/x-www-form-urlencoded',
          'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer':         'https://www.notebookcheck.net/',
          'Origin':          'https://www.notebookcheck.net',
        },
        timeout: 8000,
      });
      html = typeof resp.data === 'string' ? resp.data : JSON.stringify(resp.data);
      httpStatus = resp.status;
    } catch (e: any) {
      fetchError = e?.message;
      httpStatus = e?.response?.status || 0;
    }

    const fetchMs = Date.now() - fetchMs0;

    // Parse all links from the response
    const allLinks: any[] = [];
    const articleLinks: any[] = [];

    if (html) {
      const $ = cheerio.load(html);
      $('a[href]').each((_: any, el: any) => {
        const href = $(el).attr('href') || '';
        const fullUrl = href.startsWith('http') ? href
          : href.startsWith('/') ? 'https://www.notebookcheck.net' + href : '';
        const text = $(el).text().trim();

        if (!fullUrl.includes('notebookcheck.net')) return;

        // Show ALL NBC links — even ones that fail the article-ID filter
        allLinks.push({ url: fullUrl, text: text.slice(0, 80) });

        // Article links: must have 4+ digit article ID in URL
        if (!/\.\d{4,}\.0\.html/.test(fullUrl)) return;
        if (/[?&](tag|q|word|id)=/.test(fullUrl)) return;
        if (/\/(Topics|Search|Smartphones|RSS|index)\.\d/i.test(fullUrl)) return;
        if (!text || text.length < 3) return;

        // Simple keyword match — does the URL/title contain query words?
        const qWords = primaryQuery.toLowerCase().split(/\s+/).filter((w: string) => w.length > 1);
        const combined = (text + ' ' + fullUrl).toLowerCase();
        const matchCount = qWords.filter((w: string) => combined.includes(w)).length;
        const isReview = /review|smartphone-review/.test(fullUrl);

        articleLinks.push({
          url: fullUrl,
          title: text.slice(0, 120),
          matchedWords: matchCount,
          totalWords: qWords.length,
          isReview,
          wouldBeUsed: matchCount >= Math.ceil(qWords.length / 2),
        });
      });
    }

    articleLinks.sort((a: any, b: any) => (b.matchedWords - a.matchedWords) || (b.isReview ? 1 : -1));

    return res.json({
      query: { original: oq, normalized: nq, primaryUsed: primaryQuery },
      searchUrl,
      searchMethod: 'POST',
      searchBody: postBody,
      httpStatus,
      fetchError: fetchError || null,
      fetchMs,
      totalMs: Date.now() - t0,
      html: {
        length: html.length,
        snippet: html.slice(0, 300),   // first 300 chars — shows if it's the right page
      },
      allNBCLinks: allLinks.slice(0, 20),
      articleLinks: articleLinks.slice(0, 10),
      winner: articleLinks.find((l: any) => l.wouldBeUsed) || null,
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message, totalMs: Date.now() - t0 });
  }
});

app.get('/api/nbc/keepalive', async (_, res) => {
  const t0 = Date.now();
  try {
    const axios = (await import('axios')).default;
    await axios.get(`${SEARXNG_INSTANCE}/healthz`, { timeout: 10000 });
    res.json({ status: 'ok', instance: SEARXNG_INSTANCE, elapsedMs: Date.now() - t0 });
  } catch {
    try {
      const axios = (await import('axios')).default;
      await axios.get(`${SEARXNG_INSTANCE}/search`, { params: { q: 'ping', format: 'json' }, timeout: 10000 });
      res.json({ status: 'ok (fallback)', instance: SEARXNG_INSTANCE, elapsedMs: Date.now() - t0 });
    } catch (e: any) {
      res.status(503).json({ status: 'down', instance: SEARXNG_INSTANCE, error: e.message, elapsedMs: Date.now() - t0 });
    }
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// PROCESSOR ENDPOINTS  — /api/processor
// Completely independent from phone scraping; uses notebookcheck_processor.ts
// ─────────────────────────────────────────────────────────────────────────────

// /api/processor?q=snapdragon+8+elite  — full search + scrape
app.get('/api/processor', async (req, res) => {
  const q = validateQ(req.query.q);
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });
  try {
    const data = await getNotebookCheckProcessor(q);
    if (!data) return res.status(404).json({
      success: false,
      error: 'Processor not found on NotebookCheck',
      query: q,
      hint: 'Try a more specific name, e.g. "Snapdragon 8 Elite" or "Dimensity 9400"',
    });
    if ('error' in data) return res.status(502).json({ success: false, ...data });
    return res.json({ success: true, source: 'notebookcheck', data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/processor/suggestions?q=snapdragon  — search-only, returns ranked list
app.get('/api/processor/suggestions', async (req, res) => {
  const q = validateQ(req.query.q);
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });
  try {
    const data = await searchNotebookCheckProcessors(q);
    return res.json({ success: true, source: 'notebookcheck', data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/processor/device?url=<notebookcheck-url>  — scrape by direct URL
app.get('/api/processor/device', async (req, res) => {
  const url = req.query.url as string;
  const name = req.query.name as string | undefined;
  if (!url) return res.status(400).json({ success: false, error: '"url" required' });
  try {
    const data = await scrapeProcessorByUrl(url, name);
    return res.json({ success: true, source: 'notebookcheck', data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/processor/debug?q=<chip>  — staged timing: normalize → search → scrape
app.get('/api/processor/debug', async (req, res) => {
  const q = (validateQ(req.query.q) ?? 'snapdragon 8 elite');
  const t0 = Date.now();
  const stages: Record<string, any> = {};

  // Stage 0: query normalization (synchronous, should be ~0ms)
  const t_norm = Date.now();
  const oq = q.trim();
  const nq = normalizeProcQuery(q);
  stages.normalize = { ms: Date.now() - t_norm, original: oq, normalized: nq };

  // Stage 1: SearXNG search (main latency culprit — cold start on Render free tier)
  const t_search = Date.now();
  let searchResults: any[] = [];
  try {
    searchResults = await searchProcViaSearXNG(nq, oq);
    stages.search = {
      ms: Date.now() - t_search,
      resultsCount: searchResults.length,
      top5: searchResults
        .sort((a: any, b: any) => b.score - a.score)
        .slice(0, 5)
        .map((r: any) => ({ score: r.score, title: r.title, url: r.url })),
    };
  } catch (e: any) {
    stages.search = { ms: Date.now() - t_search, error: e.message };
    return res.json({ query: q, cacheVersion: PROC_CACHE_VERSION, totalMs: Date.now() - t0, stages });
  }

  if (!searchResults.length) {
    return res.json({ query: q, cacheVersion: PROC_CACHE_VERSION, totalMs: Date.now() - t0, stages, result: null });
  }

  const best = searchResults.sort((a: any, b: any) => b.score - a.score)[0];

  // Stage 2: Page fetch + scrape (second latency source — full HTML download + parse)
  const t_scrape = Date.now();
  let scraped: any = null;
  try {
    scraped = await scrapeNotebookCheckProcessor(best.url, best.title);
    stages.scrape = {
      ms: Date.now() - t_scrape,
      url: best.url,
      name: scraped?.name,
      benchmarkCount: scraped?.benchmarks?.length ?? 0,
      specCount: Object.keys(scraped?.specs ?? {}).filter(k => !k.startsWith('__')).length,
    };
  } catch (e: any) {
    stages.scrape = { ms: Date.now() - t_scrape, url: best.url, error: e.message };
  }

  return res.json({
    query: q,
    cacheVersion: PROC_CACHE_VERSION,
    totalMs: Date.now() - t0,
    stages,
    // Quick summary of where time went
    breakdown: {
      normalizeMs: stages.normalize.ms,
      searchMs:    stages.search?.ms ?? 0,
      scrapeMs:    stages.scrape?.ms ?? 0,
      overheadMs:  (Date.now() - t0) - (stages.normalize.ms + (stages.search?.ms ?? 0) + (stages.scrape?.ms ?? 0)),
    },
  });
});

// /api/processor/search?q=<chip>  — search-only timing (no scrape)
app.get('/api/processor/search', async (req, res) => {
  const q = (validateQ(req.query.q) ?? 'snapdragon 8 elite');
  const t0 = Date.now();
  const nq = normalizeProcQuery(q);
  try {
    const results = await searchProcViaSearXNG(nq, q.trim());
    const sorted = results.sort((a: any, b: any) => b.score - a.score);
    return res.json({
      query: q,
      normalized: nq,
      searchMs: Date.now() - t0,
      count: results.length,
      results: sorted.slice(0, 10).map((r: any) => ({ score: r.score, title: r.title, url: r.url })),
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message, searchMs: Date.now() - t0 });
  }
});

// ═════════════════════════════════════════════════════════════════════════════
// NBC REVIEW INDEX ENDPOINTS — Vercel serverless compatible (all state in Redis)
// ─────────────────────────────────────────────────────────────────────────────
// CRAWL FLOW FOR VERCEL:
//   1. GET /api/index/crawl?maxPages=40           → crawl 40 pages (~60s)
//   2. GET /api/index/crawl?startPage=41&maxPages=40  → next batch
//   3. Repeat until nextPage: null
//   OR use /api/index/crawl-page?page=N for one page at a time
// ═════════════════════════════════════════════════════════════════════════════

// /api/index/crawl — crawl N pages synchronously (state saved to Redis each page)
// ?startPage=1   ?maxPages=40   ?delayMs=600
// Returns nextPage: N if more pages remain, null if complete
app.get('/api/index/crawl', async (req, res) => {
  const startPage = parseInt(req.query.startPage as string || '1');
  const maxPages  = Math.min(parseInt(req.query.maxPages as string || '35'), 35); // 35 pages × ~1.5s ≈ 52s, safe under 60s maxDuration
  const delayMs   = parseInt(req.query.delayMs as string || '600');

  // Always force-delete lock directly via Redis REST before crawling.
  // resetCrawlLock() used broken GET /del — this uses POST /pipeline which actually works.
  try {
    const axios2 = (await import('axios')).default;
    const rUrl   = process.env.UPSTASH_REDIS_REST_URL!;
    const rToken = process.env.UPSTASH_REDIS_REST_TOKEN!;
    await axios2.post(`${rUrl}/pipeline`, [
      ['DEL', 'nbc:index:v3:crawl_lock'],
      ['DEL', 'nbc:index:v3:crawl_progress'],
    ], { headers: { Authorization: `Bearer ${rToken}`, 'Content-Type': 'application/json' } });
  } catch { /* ignore — crawlSync will handle it */ }

  try {
    const stats = await crawlSync(startPage, maxPages, delayMs);
    const queue = await getQueueStats();
    return res.json({ success: true, crawl: stats, queue,
      hint: stats.nextPage ? `More pages available — call ?startPage=${stats.nextPage}&maxPages=${maxPages}` : 'Crawl complete!' });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/crawl-page — crawl exactly one chrono/library page (SOURCE B)
// ?page=1
app.get('/api/index/crawl-page', async (req, res) => {
  const page = parseInt(req.query.page as string || '1');
  try {
    const result = await crawlOnePage(page);
    // Rebuild search index after every page so /api/phone searches stay fast
    rebuildSearchIndex().catch(() => {}); // fire and forget
    return res.json({ success: true, result,
      hint: result.done ? 'No more pages — crawl complete' : `Call ?page=${result.nextPage} for next page` });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/crawl-reviews-page — crawl exactly one NBC reviews listing page (SOURCE A)
// SOURCE A = https://www.notebookcheck.net/Smartphones.155.0.html?p=N
// These have "-review-" in their slug and contain full benchmarks/specs/images.
// ?page=1
app.get('/api/index/crawl-reviews-page', async (req, res) => {
  const page = parseInt(req.query.page as string || '1');
  try {
    const { crawlReviewsPage } = await import('./src/notebookcheck_index');
    const result = await crawlReviewsPage(page);
    rebuildSearchIndex().catch(() => {}); // fire and forget
    return res.json({ success: true, result,
      hint: result.done ? 'No more review pages — SOURCE A complete' : `Call ?page=${result.nextPage} for next page` });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/crawl-smartphone-page — crawl one page of SOURCE C (Smartphone listing)
// ?page=0 (no ns_page) then ?page=1, ?page=2 ...
app.get('/api/index/crawl-smartphone-page', async (req, res) => {
  const page = parseInt(req.query.page as string || '0');
  try {
    const result = await crawlSmartphonePage(page);
    rebuildSearchIndex().catch(() => {});
    return res.json({ success: true, result,
      hint: result.done ? 'No more smartphone pages — SOURCE C complete' : `Call ?page=${result.nextPage} for next page` });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/status — current index state (reads Redis)
app.get('/api/index/status', async (req, res) => {
  try {
    const [stats, queue, brands, inProgress, progress] = await Promise.all([
      getLastCrawlStats(),
      getQueueStats(),
      getBrandBreakdown(),
      getCrawlInProgress(),
      getCrawlProgress(),
    ]);
    return res.json({
      success: true,
      index: { totalUrls: queue.total, lastCrawledAt: stats?.lastCrawledAt ?? null, crawlInProgress: inProgress },
      scrape: { ...queue, bulkActive: false },
      progress: progress ?? null,
      brands: Object.fromEntries(Object.entries(brands).slice(0, 20)),
      lastCrawl: stats,
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/list — browse index with filters
// ?status=pending|done|error|all   ?brand=Samsung   ?search=Pixel   ?page=1   ?limit=50
app.get('/api/index/list', async (req, res) => {
  try {
    const result = await getIndexEntries({
      status: (req.query.status as any) || 'all',
      brand:  req.query.brand  as string | undefined,
      search: req.query.search as string | undefined,
      page:   parseInt(req.query.page  as string || '1'),
      limit:  Math.min(parseInt(req.query.limit as string || '50'), 200),
    });
    return res.json({ success: true, ...result });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/scrape-one — scrape one device by URL (no SearXNG)
// ?url=https://www.notebookcheck.net/...
app.get('/api/index/scrape-one', async (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: '"url" required' });
  try {
    const { getEntry } = await import('./src/notebookcheck_index');
    const entry = await getEntry(url).catch(() => null);
    const result = await scrapeIndexedDevice(url, entry?.title);
    if (!result.success) return res.status(502).json({ success: false, error: result.error, url });
    return res.json({ success: true, source: 'notebookcheck_index', url, data: result.data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/validate — spot-check N random URLs from the index
// ?count=5   ?status=pending
app.get('/api/index/validate', async (req, res) => {
  const count  = Math.min(parseInt(req.query.count as string || '5'), 20);
  const status = (req.query.status as string) || 'pending';
  try {
    const { entries } = await getIndexEntries({ status: status as any, limit: count * 3 });
    const sample = entries.sort(() => Math.random() - 0.5).slice(0, count);
    if (!sample.length) return res.json({ success: true, message: 'No entries match filter', results: [] });
    const results = await Promise.all(sample.map(e => validateIndexUrl(e.url)));
    const valid = results.filter(r => r.valid).length;
    return res.json({ success: true,
      summary: { checked: results.length, valid, invalid: results.length - valid, validPct: `${((valid/results.length)*100).toFixed(1)}%` },
      results });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/validate-url — validate one specific URL
app.get('/api/index/validate-url', async (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: '"url" required' });
  try {
    return res.json({ success: true, result: await validateIndexUrl(url) });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/errors — list error entries
app.get('/api/index/errors', async (req, res) => {
  try {
    const result = await getIndexEntries({ status: 'error', brand: req.query.brand as string,
      limit: Math.min(parseInt(req.query.limit as string || '50'), 200) });
    return res.json({ success: true, total: result.total,
      entries: result.entries.map(e => ({ url: e.url, title: e.title, brand: e.brand, retries: e.retries, errorMsg: e.errorMsg })) });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/reset-errors — reset all errors to pending
app.get('/api/index/reset-errors', async (req, res) => {
  try {
    const count = await resetErrors();
    return res.json({ success: true, resetCount: count, queue: await getQueueStats() });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/migrate-review-urls — resumable batch migration
// Each call processes `batch` entries (default 200) and returns JSON.
// Call repeatedly until response contains "done": true.
// Progress is saved in Redis so Vercel timeouts are safe — just call again.
//
// Usage:
//   GET /api/index/migrate-review-urls          → processes next 200 entries
//   GET /api/index/migrate-review-urls?batch=100 → smaller batches (slower but safer)
//   GET /api/index/migrate-review-urls?reset=1   → restart from beginning
app.get('/api/index/migrate-review-urls', async (req, res) => {
  try {
    if (req.query.reset === '1') {
      await resetMigration();
      return res.json({ success: true, message: 'Migration reset. Call again to start.' });
    }
    const batchSize = Math.min(parseInt(req.query.batch as string || '200'), 500);
    const result = await migrateToReviewUrls(batchSize);
    return res.json({ success: true, ...result });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/purge-library-duplicates — delete library URLs that have a review URL + junk titles
app.get('/api/index/purge-library-duplicates', async (req, res) => {
  try {
    const result = await purgeLibraryDuplicates();
    return res.json({ success: true, ...result,
      message: `Purged ${result.purged} entries (${result.reasons.libraryDuplicate} library dupes, ${result.reasons.junkTitle} junk titles). ${result.kept} entries remain.`
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/resolve-url — debug: show what resolveToReviewUrl returns for a library URL
// ?url=https://www.notebookcheck.net/Google-Pixel-9-Pro-XL.873451.0.html
// Use this to check if a device is getting its internal review URL correctly.
app.get('/api/index/resolve-url', async (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: '"url" required' });
  try {
    const { resolveToReviewUrl } = await import('./src/notebookcheck_index') as any;
    if (typeof resolveToReviewUrl !== 'function') {
      return res.status(500).json({ success: false, error: 'resolveToReviewUrl is not exported — add it to exports in notebookcheck_index.ts' });
    }
    const resolved = await resolveToReviewUrl(url);
    return res.json({
      success: true,
      input: url,
      resolved,
      isReviewUrl: /-review-/i.test(resolved),
      changed: resolved !== url,
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/swap-all — swap ALL library URLs that have a known resolve cache entry.
// Processes in one shot: load entries, apply all cached swaps, save once.
// Much faster than paginated resolve — one load, one save.
app.get('/api/index/swap-all', async (req, res) => {
  try {
    const axios2 = (await import('axios')).default;
    const rUrl2   = process.env.UPSTASH_REDIS_REST_URL!;
    const rToken2 = process.env.UPSTASH_REDIS_REST_TOKEN!;
    const headers = { Authorization: `Bearer ${rToken2}`, 'Content-Type': 'application/json' };

    // Load all entries
    const raw = await axios2.get(`${rUrl2}/get/${encodeURIComponent('nbc:index:v3:entries')}`,
      { headers, timeout: 30000 });
    const entries: Record<string, any> = raw.data?.result ? JSON.parse(raw.data.result) : {};

    // Find all library URLs (no -review- in URL)
    const libraryUrls = Object.keys(entries).filter(u => !/-review[-_.]/i.test(u));

    // Batch-fetch all resolve cache keys in one pipeline
    const pipeline = libraryUrls.map(u => ['GET', `nbc:review_resolve:${u}`]);
    const cacheResp = await axios2.post(`${rUrl2}/pipeline`, pipeline,
      { headers, timeout: 30000 });

    let swapped = 0, skipped = 0;
    for (let i = 0; i < libraryUrls.length; i++) {
      const libUrl = libraryUrls[i];
      const raw2 = cacheResp.data[i]?.result;
      if (!raw2) { skipped++; continue; }

      let reviewUrl: string;
      try { reviewUrl = JSON.parse(raw2); } catch { reviewUrl = raw2; }

      if (!reviewUrl || reviewUrl === libUrl || !/-review[-_.]/i.test(reviewUrl)) {
        skipped++; continue;
      }

      // Use library entry's clean title
      const title = entries[libUrl]?.title || '';
      const slug  = reviewUrl.split('/').pop() || '';

      // Add review entry (or update title if already exists from Source A)
      if (entries[reviewUrl]) {
        if (title && title.length < 60) entries[reviewUrl].title = title;
      } else {
        entries[reviewUrl] = {
          url: reviewUrl, title,
          brand: entries[libUrl]?.brand || '',
          slug, discoveredAt: new Date().toISOString(),
          status: 'pending', retries: 0, source: 'review',
        };
      }

      delete entries[libUrl];
      swapped++;
    }

    if (swapped > 0) {
      // Save in one shot
      await axios2.post(`${rUrl2}/pipeline`,
        [['SET', 'nbc:index:v3:entries', JSON.stringify(entries)]],
        { headers, timeout: 30000 });

      const { rebuildSearchIndex } = await import('./src/notebookcheck_index');
      await rebuildSearchIndex();
    }

    return res.json({ success: true, swapped, skipped, total: libraryUrls.length,
      message: `Swapped ${swapped} library URLs → review URLs. ${skipped} had no cached resolve.` });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/swap-url — directly swap a library URL to its review URL in the index
// ?from=library_url&to=review_url
// Use when resolve-one confirms the swap but the batch isn't persisting it.
app.get('/api/index/swap-url', async (req, res) => {
  const fromUrl = req.query.from as string;
  const toUrl   = req.query.to   as string;
  if (!fromUrl || !toUrl) return res.status(400).json({ success: false, error: '"from" and "to" required' });
  try {
    const axios2 = (await import('axios')).default;
    const rUrl2   = process.env.UPSTASH_REDIS_REST_URL!;
    const rToken2 = process.env.UPSTASH_REDIS_REST_TOKEN!;
    const headers = { Authorization: `Bearer ${rToken2}`, 'Content-Type': 'application/json' };

    // Load entries
    const raw = await axios2.get(`${rUrl2}/get/${encodeURIComponent('nbc:index:v3:entries')}`, { headers });
    const entries: Record<string, any> = raw.data?.result ? JSON.parse(raw.data.result) : {};

    if (!entries[fromUrl]) return res.status(404).json({ success: false, error: 'from URL not in index', fromUrl });

    // Use the library entry's clean title
    const title = entries[fromUrl].title;
    const slug  = toUrl.split('/').pop() || '';

    // Add review URL entry
    entries[toUrl] = {
      url: toUrl, title,
      brand: entries[fromUrl].brand,
      slug, discoveredAt: new Date().toISOString(),
      status: 'pending', retries: 0, source: 'review',
    };

    // Remove library URL
    delete entries[fromUrl];

    // Save back
    await axios2.post(`${rUrl2}/pipeline`,
      [['SET', 'nbc:index:v3:entries', JSON.stringify(entries)]],
      { headers, timeout: 30000 });

    // Rebuild search index
    const { rebuildSearchIndex } = await import('./src/notebookcheck_index');
    await rebuildSearchIndex();

    return res.json({ success: true, swapped: true, from: fromUrl, to: toUrl, title,
      message: `Swapped "${title}" → internal review URL and rebuilt search index` });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/resolve-one — force-resolve a single library URL (bypasses cache)
// ?url=https://www.notebookcheck.net/Vivo-X300-Pro.1178848.0.html
app.get('/api/index/resolve-one', async (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: '"url" required' });
  try {
    const axios2 = (await import('axios')).default;
    const rUrl2  = process.env.UPSTASH_REDIS_REST_URL!;
    const rToken2 = process.env.UPSTASH_REDIS_REST_TOKEN!;
    const headers = { Authorization: `Bearer ${rToken2}`, 'Content-Type': 'application/json' };
    // Wipe cache for this URL
    const ck = `nbc:review_resolve:${url}`;
    await axios2.post(`${rUrl2}/pipeline`, [['DEL', ck]], { headers });
    // Now resolve fresh
    const { resolveToReviewUrl } = await import('./src/notebookcheck_index') as any;
    const resolved = await resolveToReviewUrl(url);
    return res.json({ success: true, input: url, resolved, isReviewUrl: /-review[-_.]/i.test(resolved), changed: resolved !== url });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/resolve-page — resolve one page of library URLs → internal review URLs
// ?offset=0 → first 5 URLs, ?offset=5 → next 5, etc.
// Each call does 5 live NBC fetches (~15s), safely under Vercel 30s limit.
app.get('/api/index/resolve-page', async (req, res) => {
  const offset = parseInt(req.query.offset as string || '0');
  try {
    const result = await resolveLibraryUrlsPage(offset);
    return res.json({ success: true, ...result });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/recover-review-urls — resolve library URLs → internal review URLs
// ?clearcache=1  wipes all stale nbc:review_resolve:* keys so every library URL
//               is re-fetched fresh (needed after fixing the -review. bug)
app.get('/api/index/recover-review-urls', async (req, res) => {
  try {
    if (req.query.clearcache === '1') {
      // Wipe all stale resolve cache keys via Upstash SCAN + DEL
      const axios2 = (await import('axios')).default;
      const rUrl   = process.env.UPSTASH_REDIS_REST_URL!;
      const rToken = process.env.UPSTASH_REDIS_REST_TOKEN!;
      const headers = { Authorization: `Bearer ${rToken}`, 'Content-Type': 'application/json' };
      let cursor = '0'; let deleted = 0;
      do {
        const scan: any = await axios2.post(`${rUrl}/pipeline`,
          [['SCAN', cursor, 'MATCH', 'nbc:review_resolve:*', 'COUNT', '100']],
          { headers });
        const [newCursor, keys] = scan.data[0].result as [string, string[]];
        cursor = newCursor;
        if (keys?.length) {
          await axios2.post(`${rUrl}/pipeline`, keys.map(k => ['DEL', k]), { headers });
          deleted += keys.length;
        }
      } while (cursor !== '0');
    }
    if (req.query.norun === '1') return res.json({ success: true, message: 'Cache cleared' });
    const result = await recoverDeletedReviewUrls();
    return res.json({ success: true, ...result,
      message: `Recovered ${result.recovered} review URLs. ${result.alreadyPresent} already present.`
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});
app.get('/api/index/reset-url', async (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: '"url" required' });
  try {
    const ok = await resetEntry(url);
    return res.json({ success: ok, message: ok ? 'Reset to pending' : 'URL not in index', url });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /migrate — self-running HTML dashboard that loops migrate-review-urls until done
app.get('/migrate', (req, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.send(`<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>NBC Migration — Upgrade to Review URLs</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: monospace; background: #0d1117; color: #e6edf3; padding: 24px; }
    h1 { font-size: 18px; color: #58a6ff; margin-bottom: 20px; }
    #status { font-size: 13px; color: #8b949e; margin-bottom: 16px; }
    #bar-wrap { background: #161b22; border-radius: 6px; overflow: hidden; height: 22px; margin-bottom: 20px; }
    #bar { height: 100%; background: #238636; width: 0%; transition: width 0.4s ease; }
    #stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
    .stat { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 14px; text-align: center; }
    .stat .val { font-size: 28px; font-weight: bold; color: #58a6ff; }
    .stat .lbl { font-size: 11px; color: #8b949e; margin-top: 4px; }
    #upgraded .val { color: #3fb950; }
    #noReview .val  { color: #d29922; }
    #errors .val    { color: #f85149; }
    #log { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px;
           height: 260px; overflow-y: auto; font-size: 11px; line-height: 1.6; }
    .log-line { color: #8b949e; }
    .log-line.ok  { color: #3fb950; }
    .log-line.done { color: #58a6ff; font-weight: bold; }
    .log-line.err  { color: #f85149; }
    #btn { margin-top: 16px; padding: 8px 20px; background: #238636; color: #fff;
           border: none; border-radius: 6px; cursor: pointer; font-size: 13px; }
    #btn:disabled { background: #30363d; color: #8b949e; cursor: not-allowed; }
    #done-banner { display: none; margin-top: 16px; padding: 14px; background: #0d4a23;
                   border: 1px solid #238636; border-radius: 6px; color: #3fb950; font-size: 14px; text-align: center; }
  </style>
</head>
<body>
  <h1>🔄 NBC Migration — Upgrade Library URLs → Internal Review URLs</h1>
  <div id="status">Press Start to begin. Each batch processes 200 entries.</div>
  <div id="bar-wrap"><div id="bar"></div></div>
  <div id="stats">
    <div class="stat"><div class="val" id="v-processed">—</div><div class="lbl">Processed</div></div>
    <div class="stat" id="upgraded"><div class="val" id="v-upgraded">—</div><div class="lbl">Upgraded ✅</div></div>
    <div class="stat" id="noReview"><div class="val" id="v-noReview">—</div><div class="lbl">No Review Yet</div></div>
    <div class="stat" id="errors"><div class="val" id="v-errors">—</div><div class="lbl">Errors</div></div>
  </div>
  <div id="log"></div>
  <button id="btn" onclick="start()">▶ Start Migration</button>
  <div id="done-banner">✅ Migration complete! Search index rebuilt. All review URLs are now stored.</div>

<script>
  let running = false;
  const BATCH = 200;
  const DELAY = 1500; // ms between calls

  function log(msg, cls = '') {
    const el = document.getElementById('log');
    const line = document.createElement('div');
    line.className = 'log-line ' + cls;
    line.textContent = '[' + new Date().toLocaleTimeString() + '] ' + msg;
    el.appendChild(line);
    el.scrollTop = el.scrollHeight;
  }

  function update(r) {
    document.getElementById('v-processed').textContent = r.processed + ' / ' + r.total;
    document.getElementById('v-upgraded').textContent  = r.upgraded;
    document.getElementById('v-noReview').textContent  = r.noReview;
    document.getElementById('v-errors').textContent    = r.errors;
    const pct = r.total > 0 ? (r.processed / r.total * 100).toFixed(1) : 0;
    document.getElementById('bar').style.width = pct + '%';
    document.getElementById('status').textContent =
      'Batch done — ' + pct + '% complete (' + r.remaining + ' remaining)';
  }

  async function runBatch() {
    const resp = await fetch('/api/index/migrate-review-urls?batch=' + BATCH);
    const r = await resp.json();
    if (!r.success) throw new Error(r.error || 'API error');
    update(r);
    log('Batch: +' + r.upgraded + ' upgraded, +' + r.noReview + ' no-review, ' + r.remaining + ' left (' + r.durationMs + 'ms)', 'ok');
    return r.done;
  }

  async function start() {
    if (running) return;
    running = true;
    document.getElementById('btn').disabled = true;
    document.getElementById('btn').textContent = '⏳ Running…';
    log('Migration started…');

    try {
      while (true) {
        const done = await runBatch();
        if (done) {
          log('🧹 Purging stale library duplicates…', 'ok');
          try {
            const pr = await fetch('/api/index/purge-library-duplicates');
            const pj = await pr.json();
            log('🗑 Purged ' + pj.purged + ' library duplicates. ' + pj.kept + ' entries remain.', 'ok');
          } catch(pe) {
            log('Purge warning: ' + pe.message, 'err');
          }
          log('🎉 All done! Search index rebuilt. Only internal review URLs remain.', 'done');
          document.getElementById('status').textContent = '✅ Migration complete!';
          document.getElementById('done-banner').style.display = 'block';
          document.getElementById('btn').textContent = '✅ Done';
          break;
        }
        await new Promise(r => setTimeout(r, DELAY));
      }
    } catch (e) {
      log('Error: ' + e.message, 'err');
      document.getElementById('btn').disabled = false;
      document.getElementById('btn').textContent = '▶ Resume';
      running = false;
    }
  }
</script>
</body>
</html>`);
});

// /recover — live UI for recovering deleted review URLs + purging library duplicates
app.get('/recover', (req, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.send(`<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>NBC Recovery — Full Re-crawl</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: monospace; background: #0d1117; color: #e6edf3; padding: 24px; }
    h1 { font-size: 18px; color: #f85149; margin-bottom: 6px; }
    .subtitle { font-size: 12px; color: #8b949e; margin-bottom: 20px; }
    #status { font-size: 13px; color: #8b949e; margin-bottom: 16px; }
    #bar-wrap { background: #161b22; border-radius: 6px; overflow: hidden; height: 22px; margin-bottom: 20px; }
    #bar { height: 100%; background: #1f6feb; width: 0%; transition: width 0.4s ease; }
    #stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
    .stat { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 14px; text-align: center; }
    .stat .val { font-size: 28px; font-weight: bold; color: #58a6ff; }
    .stat .lbl { font-size: 11px; color: #8b949e; margin-top: 4px; }
    #v-recovered .val { color: #3fb950; }
    #log { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px;
           height: 300px; overflow-y: auto; font-size: 11px; line-height: 1.7; }
    .log-line      { color: #8b949e; }
    .log-line.ok   { color: #3fb950; }
    .log-line.done { color: #58a6ff; font-weight: bold; }
    .log-line.warn { color: #d29922; }
    .log-line.err  { color: #f85149; }
    #btn { margin-top: 16px; padding: 9px 22px; background: #f85149; color: #fff;
           border: none; border-radius: 6px; cursor: pointer; font-size: 13px; font-family: monospace; }
    #btn:disabled { background: #30363d; color: #8b949e; cursor: not-allowed; }
    #done-banner { display: none; margin-top: 16px; padding: 14px; background: #0d2942;
                   border: 1px solid #1f6feb; border-radius: 6px; color: #58a6ff; font-size: 14px; text-align: center; }
    .phase { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 10px; font-weight: bold; margin-right: 6px; }
    .phase-crawl  { background: #1a2a3a; color: #58a6ff; }
    .phase-purge  { background: #3a1f1f; color: #f85149; }
    .phase-done   { background: #1f3a1f; color: #3fb950; }
  </style>
</head>
<body>
  <h1>🚨 NBC Recovery — Full Re-crawl + Purge</h1>
  <p class="subtitle">Re-crawls all Chronological pages → resolves library URLs to review URLs (from cache, fast) → purges dupes</p>
  <div id="status">Press Start. Resolve cache is still alive — crawl will be fast (no live NBC fetches).</div>
  <div id="bar-wrap"><div id="bar"></div></div>

  <div id="stats">
    <div class="stat"><div class="val" id="v-page">—</div><div class="lbl">Page</div></div>
    <div class="stat"><div class="val" id="v-total">—</div><div class="lbl">Total in Index</div></div>
    <div class="stat"><div class="val" id="v-new">—</div><div class="lbl">Re-added ✅</div></div>
    <div class="stat"><div class="val" id="v-purged">—</div><div class="lbl">Dupes Purged 🗑</div></div>
  </div>

  <div id="log"></div>
  <button id="btn" onclick="start()">▶ Start Recovery Re-crawl</button>
  <div id="done-banner">✅ Recovery complete! All internal review URLs restored. Index is clean.</div>

<script>
  let running = false;
  let totalNew = 0;

  function log(msg, cls = '') {
    const el = document.getElementById('log');
    const d = document.createElement('div');
    d.className = 'log-line ' + cls;
    d.innerHTML = '[' + new Date().toLocaleTimeString() + '] ' + msg;
    el.appendChild(d);
    el.scrollTop = el.scrollHeight;
  }

  function setStatus(msg) { document.getElementById('status').textContent = msg; }

  async function crawlPage(page) {
    const r = await fetch('/api/index/crawl-page?page=' + page).then(r => r.json());
    if (!r.success) throw new Error(r.result?.error || 'crawl-page failed');
    return r.result;
  }

  async function start() {
    if (running) return;
    running = true;
    document.getElementById('btn').disabled = true;
    document.getElementById('btn').textContent = '⏳ Re-crawling…';
    log('<span class="phase phase-crawl">CRAWL</span> Starting re-crawl from page 1…');

    try {
      let page = 1;
      let maxPages = 300; // safety ceiling

      while (page <= maxPages) {
        setStatus('Step 1/2 — Crawling page ' + page + '…');
        const r = await crawlPage(page);

        totalNew += (r.newUrls || 0);
        document.getElementById('v-page').textContent    = r.page;
        document.getElementById('v-total').textContent   = r.totalUrls;
        document.getElementById('v-new').textContent     = totalNew;

        // Rough progress — NBC Chronological has ~150 pages
        const pct = Math.min((page / 150) * 100, 99).toFixed(0);
        document.getElementById('bar').style.width = pct + '%';

        log('<span class="phase phase-crawl">CRAWL</span> Page ' + r.page +
            ' — ' + r.phonesFound + ' phones found, ' + r.newUrls + ' re-added' +
            ' (total: ' + r.totalUrls + ')', r.newUrls > 0 ? 'ok' : '');

        if (r.done) {
          log('<span class="phase phase-crawl">CRAWL</span> All pages done. ' + totalNew + ' entries re-added total.', 'ok');
          break;
        }

        page = r.nextPage;
        await new Promise(res => setTimeout(res, 800));
      }

      // ── Purge library dupes now that review URLs are back ─────────────────
      setStatus('Step 2/2 — Purging library duplicates…');
      log('<span class="phase phase-purge">PURGE</span> Removing library URLs that now have a review URL…');
      document.getElementById('bar').style.width = '100%';

      const pr = await fetch('/api/index/purge-library-duplicates').then(r => r.json());
      if (!pr.success) throw new Error(pr.error);

      document.getElementById('v-purged').textContent = pr.purged;
      log('<span class="phase phase-purge">PURGE</span> Purged ' + pr.purged +
          ' dupes (' + pr.reasons.libraryDuplicate + ' library, ' + pr.reasons.junkTitle + ' junk). ' +
          pr.kept + ' clean entries remain.', 'ok');

      // ── Done ──────────────────────────────────────────────────────────────
      setStatus('✅ Done — ' + pr.kept + ' clean entries in index');
      log('<span class="phase phase-done">DONE</span> Recovery complete. Only review URLs + unreviewed library phones remain.', 'done');
      document.getElementById('done-banner').style.display = 'block';
      document.getElementById('btn').textContent = '✅ Done';

    } catch(e) {
      log('❌ Error: ' + e.message, 'err');
      setStatus('Error — check log above');
      document.getElementById('btn').disabled = false;
      document.getElementById('btn').textContent = '▶ Retry';
      running = false;
    }
  }
</script>
</body>
</html>`);
});

// /api/index/brands — brand coverage
app.get('/api/index/brands', async (req, res) => {
  try {
    return res.json({ success: true, brands: await getBrandBreakdown() });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/reset-crawl-lock — unstick a hung crawl lock
app.get('/api/index/reset-crawl-lock', async (req, res) => {
  try {
    await resetCrawlLock();
    return res.json({ success: true, message: 'Crawl lock cleared. Safe to crawl again.' });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/clear — LEGACY: only wipes 4 index keys (kept for compatibility)
app.get('/api/index/clear', async (req, res) => {
  if (req.query.confirm !== 'yes') return res.status(400).json({ success: false, error: 'Add ?confirm=yes' });
  try {
    await clearIndex();
    return res.json({ success: true, message: 'Index cleared from Redis (4 keys only — use /api/redis/flushdb for full wipe)' });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/redis/flushdb — FULL Redis wipe: deletes EVERY key in the database
// Clears device cache, search cache, full-result cache, review_resolve cache,
// index entries, search index, migration state — everything.
// Requires ?confirm=yes to prevent accidental calls.
app.get('/api/redis/flushdb', async (req, res) => {
  if (req.query.confirm !== 'yes') {
    return res.status(400).json({ success: false, error: 'Add ?confirm=yes to confirm full wipe' });
  }
  try {
    const axios2 = (await import('axios')).default;
    const rUrl   = process.env.UPSTASH_REDIS_REST_URL!;
    const rToken = process.env.UPSTASH_REDIS_REST_TOKEN!;
    if (!rUrl || !rToken) throw new Error('UPSTASH_REDIS_REST_URL / TOKEN not set');
    const resp = await axios2.post(
      `${rUrl}/pipeline`,
      [['FLUSHDB']],
      { headers: { Authorization: `Bearer ${rToken}`, 'Content-Type': 'application/json' }, timeout: 15000 }
    );
    return res.json({ success: true, message: 'FLUSHDB — entire Redis database wiped', result: resp.data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/library-debug — verify NBC library URL works and finds phone aggregator pages
app.get('/api/index/library-debug', async (req, res) => {
  const page = parseInt(req.query.page as string || '1');
  const t0 = Date.now();
  try {
    const axios2 = (await import('axios')).default;
    const { extractPhoneUrls } = await import('./src/notebookcheck_index');
    const LIBRARY_BASE = 'https://www.notebookcheck.net/Library.279.0.html';
    const url = page === 1 ? LIBRARY_BASE : `${LIBRARY_BASE}?&ns_page=${page}`;
    const resp = await axios2.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36' },
      timeout: 12000,
    });
    const html = typeof resp.data === 'string' ? resp.data : JSON.stringify(resp.data);
    const phones = extractPhoneUrls(html);
    // Look specifically for aggregator-style URLs (no -review in slug)
    const aggregatorUrls = phones.filter(p => !/-review\.\d+\.0\.html$/i.test(p.url));
    const reviewUrls     = phones.filter(p =>  /-review\.\d+\.0\.html$/i.test(p.url));
    // Show what was filtered out as tablets/laptops for verification
    const cheerio2 = await import('cheerio');
    const $ = cheerio2.load(html);
    const allSlugs: string[] = [];
    $('a[href]').each((_: any, el: any) => {
      const href = ($(el).attr('href') || '').split('?')[0];
      if (href.includes('notebookcheck.net') && /\.\d{4,}\.0\.html$/.test(href)) {
        allSlugs.push(href.split('/').pop() || '');
      }
    });
    const tabletSlugs = allSlugs.filter(s => /[-_]pad[-_.0]|[-_]tab[-_.0]|ipad|galaxy[-_]tab|matepad|mediapad|magicpad/i.test(s));
    const seriesSlugs = allSlugs.filter(s => /-Series\./i.test(s));
    return res.json({
      success: phones.length > 0,
      page, url, fetchMs: Date.now() - t0,
      htmlLength: html.length, httpStatus: resp.status,
      total: phones.length,
      aggregatorPages: aggregatorUrls.length,
      reviewPages: reviewUrls.length,
      tabletsFiltered: tabletSlugs.length,
      seriesFiltered: seriesSlugs.length,
      sampleAggregator: aggregatorUrls.slice(0, 5).map((p: any) => ({ url: p.url, title: p.title.slice(0, 80) })),
      sampleReviews:    reviewUrls.slice(0, 3).map((p: any) => ({ url: p.url, title: p.title.slice(0, 80) })),
      sampleTabletsFiltered: tabletSlugs.slice(0, 3),
      sampleSeriesFiltered: seriesSlugs.slice(0, 3),
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message, fetchMs: Date.now() - t0 });
  }
});

// /api/index/crawl-debug — test connectivity + phone link detection
app.get('/api/index/crawl-debug', async (req, res) => {
  const axios2 = (await import('axios')).default;
  const cheerio2 = await import('cheerio');
  const testUrl = (req.query.url as string) || 'https://www.notebookcheck.net/Smartphones.155.0.html';
  const t0 = Date.now();
  try {
    const resp = await axios2.get(testUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        Referer: 'https://www.notebookcheck.net/' }, timeout: 12000 });
    const html = typeof resp.data === 'string' ? resp.data : JSON.stringify(resp.data);
    const phoneLinks = extractPhoneUrls(html);

    // Check page 2
    let page2: any = null;
    try {
      const r2 = await axios2.get(`${testUrl.split('?')[0]}?&ns_page=2`, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36' }, timeout: 8000 });
      const pl2 = extractPhoneUrls(typeof r2.data === 'string' ? r2.data : JSON.stringify(r2.data));
      page2 = { phoneLinks: pl2.length, sample: pl2.slice(0, 3).map(p => p.url) };
    } catch (e2: any) { page2 = { error: e2.message }; }

    return res.json({
      success: phoneLinks.length > 0,
      diagnosis: phoneLinks.length > 0 ? `Found ${phoneLinks.length} phone links on page 1` : 'No phone links found',
      page1: { url: testUrl, status: 200, fetchMs: Date.now() - t0, htmlLength: html.length, phoneLinks: phoneLinks.length,
        sample: phoneLinks.slice(0, 5).map(p => p.url) },
      page2,
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message, fetchMs: Date.now() - t0 });
  }
});

// /api/debug/redis — check Redis connectivity and env vars
app.get('/api/debug/redis', async (req, res) => {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!url || !token) {
    return res.status(500).json({
      success: false,
      error: 'Missing env vars',
      UPSTASH_REDIS_REST_URL:   url   ? '✅ set' : '❌ MISSING',
      UPSTASH_REDIS_REST_TOKEN: token ? '✅ set' : '❌ MISSING',
    });
  }

  try {
    const axios2 = (await import('axios')).default;
    // Write a test key
    await axios2.post(`${url}/pipeline`,
      [['SET', 'debug:ping', 'pong', 'EX', 30]],
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } });
    // Read it back
    const r = await axios2.get(`${url}/get/debug:ping`,
      { headers: { Authorization: `Bearer ${token}` } });
    const val = r.data?.result;
    return res.json({
      success: val === 'pong',
      redis: val === 'pong' ? '✅ connected' : '❌ read failed',
      UPSTASH_REDIS_REST_URL:   '✅ set',
      UPSTASH_REDIS_REST_TOKEN: '✅ set',
      pingResult: val,
    });
  } catch (e: any) {
    return res.status(500).json({
      success: false,
      error: e.message,
      UPSTASH_REDIS_REST_URL:   '✅ set',
      UPSTASH_REDIS_REST_TOKEN: '✅ set',
      hint: 'URL or token value is wrong — check Upstash dashboard',
    });
  }
});

module.exports = app;

// ── STARTUP WARM-UP ──────────────────────────────────────────────────────────
// Fire-and-forget ping to SearXNG on Render free tier so the cold start
// happens at deploy/restart time, not on the first real user request.
// The 30 s timeout is generous on purpose — we want it to actually wake up.
(async () => {
  try {
    const axiosWarmup = (await import('axios')).default;
    await axiosWarmup.get(`${SEARXNG_INSTANCE}/healthz`, { timeout: 30000 });
  } catch {
    // Silently ignore — best-effort. First real request will retry with 12 s timeout.
  }
})();

// /api/debug/redis-force-unlock — directly delete ALL crawl lock keys from Redis
app.get('/api/debug/redis-force-unlock', async (req, res) => {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return res.status(500).json({ success: false, error: 'Redis env vars missing' });

  try {
    const axios2 = (await import('axios')).default;
    const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };

    // Delete all possible lock key versions (v1, v2, v3)
    const keys = [
      'nbc:index:v1:crawl_lock', 'nbc:index:v1:crawl_progress',
      'nbc:index:v2:crawl_lock', 'nbc:index:v2:crawl_progress',
      'nbc:index:v3:crawl_lock', 'nbc:index:v3:crawl_progress',
    ];
    const pipeline = keys.map(k => ['DEL', k]);
    await axios2.post(`${url}/pipeline`, pipeline, { headers });

    // Verify lock is gone
    const check = await axios2.get(`${url}/get/${encodeURIComponent('nbc:index:v3:crawl_lock')}`, { headers });
    const lockVal = check.data?.result;

    return res.json({
      success: true,
      deleted: keys,
      lockStillExists: lockVal !== null,
      lockValue: lockVal,
      hint: lockVal === null ? '✅ Lock is gone — safe to crawl now' : '❌ Lock still exists',
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/debug/crawl-direct — bypass all lock logic, crawl page 1 directly
app.get('/api/debug/crawl-direct', async (req, res) => {
  try {
    // Step 1: force delete lock directly via Redis REST
    const axios2 = (await import('axios')).default;
    const url   = process.env.UPSTASH_REDIS_REST_URL!;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN!;
    const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };

    await axios2.post(`${url}/pipeline`, [
      ['DEL', 'nbc:index:v3:crawl_lock'],
      ['DEL', 'nbc:index:v3:crawl_progress'],
    ], { headers });

    // Step 2: verify lock gone
    const check = await axios2.get(`${url}/get/nbc:index:v3:crawl_lock`, { headers });
    const lockVal = check.data?.result;

    // Step 3: crawl one page directly
    const result = await crawlOnePage(1);

    return res.json({
      success: true,
      version: 'FIX-v4-CRAWLFIX',
      lockDeletedSuccessfully: lockVal === null,
      lockValueAfterDelete: lockVal,
      crawlResult: result,
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message, stack: e.stack?.slice(0, 500) });
  }
});

// /crawler — self-hosted auto-crawler UI
// /recrawl — two-panel crawl control UI (Source A reviews + Source B chrono)
// Flush Redis → crawl Source A (review listing, ~80 pages) → crawl Source B (chrono library)
// → purge library dupes → rebuild search index
app.get('/recrawl', (_, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>NBC Recrawl</title>
<style>
  :root{--bg:#0d0d12;--surface:#14141c;--surface2:#1a1a24;--border:#252535;--green:#00d97e;--blue:#3b9eff;--red:#ff4d6d;--amber:#ffb627;--text:#d8d8ee;--muted:#5a5a78}
  *{margin:0;padding:0;box-sizing:border-box}
  body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,sans-serif;min-height:100vh;padding:2rem;font-size:14px}
  h1{font-size:1.6rem;font-weight:700;color:#fff;margin-bottom:0.25rem;letter-spacing:-0.02em}
  .sub{font-size:12px;color:var(--muted);margin-bottom:1.75rem}
  .flush-bar{display:flex;align-items:center;gap:10px;padding:14px 18px;background:var(--surface);border:1px solid #3a1520;border-radius:10px;margin-bottom:1.5rem}
  .flush-bar p{flex:1;font-size:12px;color:var(--muted);line-height:1.5}
  .flush-bar strong{display:block;font-size:13px;color:#ff7a8a;font-weight:600;margin-bottom:3px}
  .panels{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:14px}
  .panel{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;display:flex;flex-direction:column;gap:12px}
  .panel-head{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:600;color:#fff}
  .tag{font-size:10px;font-weight:600;padding:2px 8px;border-radius:20px;letter-spacing:0.04em}
  .tag-a{background:#0c2e1a;color:var(--green)}
  .tag-b{background:#0c1e35;color:var(--blue)}
  .tag-c{background:#1a1000;color:var(--amber)}
  .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;background:var(--border)}
  .dot.run{background:var(--green);box-shadow:0 0 6px var(--green);animation:blink 1s ease infinite}
  .dot.done{background:var(--green)}
  .dot.err{background:var(--red)}
  @keyframes blink{0%,100%{opacity:1}50%{opacity:0.25}}
  .metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}
  .metric{background:var(--surface2);border-radius:7px;padding:8px 10px;text-align:center}
  .metric-val{font-size:19px;font-weight:700;color:#fff;font-variant-numeric:tabular-nums}
  .metric-lbl{font-size:10px;color:var(--muted);margin-top:1px;letter-spacing:0.03em}
  .bar-wrap{display:flex;flex-direction:column;gap:4px}
  .bar-track{height:5px;background:var(--border);border-radius:3px;overflow:hidden}
  .bar-fill{height:100%;width:0%;border-radius:3px;transition:width 0.5s ease}
  .bar-fill-a{background:var(--green)}
  .bar-fill-b{background:var(--blue)}
  .bar-fill-flush{background:var(--red)}
  .bar-info{display:flex;justify-content:space-between;font-size:11px;color:var(--muted)}
  .ctl{display:flex;align-items:center;gap:8px}
  .ctl label{font-size:11px;color:var(--muted)}
  .ctl input[type=number]{width:68px;height:30px;background:var(--surface2);border:1px solid var(--border);border-radius:6px;color:#fff;padding:0 8px;font-size:13px;font-family:inherit}
  .btn{height:30px;padding:0 14px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:none;display:inline-flex;align-items:center;gap:5px;letter-spacing:0.02em;white-space:nowrap}
  .btn:disabled{opacity:0.4;cursor:not-allowed}
  .btn-go{background:var(--green);color:#000}
  .btn-go:hover:not(:disabled){background:#00f28a}
  .btn-stop{background:transparent;border:1px solid var(--amber);color:var(--amber)}
  .btn-stop:hover:not(:disabled){background:#1f1800}
  .btn-flush{background:transparent;border:1px solid var(--red);color:var(--red);height:32px;padding:0 16px;font-size:13px}
  .btn-flush:hover:not(:disabled){background:#1f0008}
  .btn-plain{background:var(--surface2);border:1px solid var(--border);color:var(--text);height:32px;padding:0 14px;font-size:12px}
  .btn-plain:hover:not(:disabled){border-color:#555;color:#fff}
  .log{height:130px;overflow-y:auto;background:var(--surface2);border-radius:7px;padding:8px 10px;font-family:'SF Mono',monospace,monospace;font-size:11px;line-height:1.7;color:var(--muted)}
  .log .ok{color:var(--green)}.log .err{color:var(--red)}.log .warn{color:var(--amber)}.log .info{color:var(--text)}
  .footer-bar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 18px}
  .footer-bar p{flex:1;font-size:12px;color:var(--muted)}
  .footer-bar strong{display:block;font-size:12px;color:var(--text);margin-bottom:2px}
  #flushmsg,#postmsg{font-size:11px;color:var(--muted)}
  @media(max-width:900px){.panels{grid-template-columns:1fr}}
</style>
</head>
<body>
<h1>NBC Recrawl</h1>
<p class="sub">Flush Redis → crawl reviews (Source A) → crawl library (Source B) → purge dupes → rebuild index</p>

<div class="flush-bar">
  <div style="flex:1">
    <strong>Step 1 — Wipe Redis cache</strong>
    <p>Deletes all cached device data, the index, search index, and crawl state. Run this first.</p>
    <div class="bar-wrap" style="margin-top:8px">
      <div class="bar-track"><div class="bar-fill bar-fill-flush" id="flushBar"></div></div>
      <div class="bar-info"><span id="flushMsg">idle</span></div>
    </div>
  </div>
  <button class="btn btn-flush" id="btnFlush" onclick="doFlush()">Wipe all cache</button>
</div>

<div class="panels">
  <div class="panel">
    <div class="panel-head">
      <div class="dot" id="dotA"></div>
      Source A — reviews listing
      <span class="tag tag-a">~80 pages</span>
    </div>
    <div class="metrics">
      <div class="metric"><div class="metric-val" id="aPages">0</div><div class="metric-lbl">pages</div></div>
      <div class="metric"><div class="metric-val" id="aTotal">0</div><div class="metric-lbl">total index</div></div>
      <div class="metric"><div class="metric-val" id="aNew">0</div><div class="metric-lbl">new added</div></div>
    </div>
    <div class="bar-wrap">
      <div class="bar-track"><div class="bar-fill bar-fill-a" id="barA"></div></div>
      <div class="bar-info"><span id="labelA">not started</span><span id="pctA"></span></div>
    </div>
    <div class="ctl">
      <label>start page</label>
      <input type="number" id="aStart" value="0" min="0" max="80">
      <button class="btn btn-go" id="btnStartA" onclick="startA()">▶ Start</button>
      <button class="btn btn-stop" id="btnStopA" style="display:none" onclick="stopA()">■ Stop</button>
    </div>
    <div class="log" id="logA"></div>
  </div>

  <div class="panel">
    <div class="panel-head">
      <div class="dot" id="dotB"></div>
      Source B — library / chrono
      <span class="tag tag-b">until empty</span>
    </div>
    <div class="metrics">
      <div class="metric"><div class="metric-val" id="bPages">0</div><div class="metric-lbl">pages</div></div>
      <div class="metric"><div class="metric-val" id="bTotal">0</div><div class="metric-lbl">total index</div></div>
      <div class="metric"><div class="metric-val" id="bNew">0</div><div class="metric-lbl">new added</div></div>
    </div>
    <div class="bar-wrap">
      <div class="bar-track"><div class="bar-fill bar-fill-b" id="barB"></div></div>
      <div class="bar-info"><span id="labelB">not started</span><span id="pctB"></span></div>
    </div>
    <div class="ctl">
      <label>start page</label>
      <input type="number" id="bStart" value="1" min="1">
      <button class="btn btn-go" id="btnStartB" onclick="startB()">▶ Start</button>
      <button class="btn btn-stop" id="btnStopB" style="display:none" onclick="stopB()">■ Stop</button>
    </div>
    <div class="log" id="logB"></div>
  </div>
  <div class="panel">
    <div class="panel-head">
      <div class="dot" id="dotC"></div>
      Source C — smartphone listing
      <span class="tag tag-c">phones only</span>
    </div>
    <div class="metrics">
      <div class="metric"><div class="metric-val" id="cPages">0</div><div class="metric-lbl">pages</div></div>
      <div class="metric"><div class="metric-val" id="cTotal">0</div><div class="metric-lbl">total index</div></div>
      <div class="metric"><div class="metric-val" id="cNew">0</div><div class="metric-lbl">new added</div></div>
    </div>
    <div class="bar-wrap">
      <div class="bar-track"><div class="bar-fill" style="background:var(--amber)" id="barC"></div></div>
      <div class="bar-info"><span id="labelC">not started</span><span id="pctC"></span></div>
    </div>
    <div class="ctl">
      <label>start page</label>
      <input type="number" id="cStart" value="0" min="0">
      <button class="btn btn-go" id="btnStartC" onclick="startC()">▶ Start</button>
      <button class="btn btn-stop" id="btnStopC" style="display:none" onclick="stopC()">■ Stop</button>
    </div>
    <div class="log" id="logC"></div>
  </div>
</div>

<div class="footer-bar">
  <div style="flex:1">
    <strong>Step 3 — Finish up (run in order)</strong>
    <p>For each library phone, opens its page and checks if NBC has an internal review. If found, swaps the URL. Then purge and rebuild.</p>
    <div style="margin-top:8px">
      <div class="bar-track" style="background:var(--border)"><div class="bar-fill" style="background:#9b59b6;width:0%" id="barResolve"></div></div>
      <div style="font-size:11px;color:var(--muted);margin-top:3px;display:flex;justify-content:space-between">
        <span id="resolveLabel">idle</span><span id="resolveStats"></span>
      </div>
    </div>
  </div>
  <button class="btn btn-plain" onclick="doResolve()" id="btnResolve">1. Resolve</button>
  <button class="btn btn-stop" onclick="stopResolve()" id="btnStopResolve" style="display:none">■ Stop</button>
  <button class="btn btn-plain" onclick="doPurge()" id="btnPurge">2. Purge dupes</button>
  <button class="btn btn-plain" onclick="doRebuild()" id="btnRebuild">3. Rebuild index</button>
  <span id="postmsg"></span>
</div>

<script>
const DELAY = 900;
let aStop = false, bStop = false;

function ts() { return new Date().toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'}); }

function logLine(id, msg, cls) {
  const el = document.getElementById(id);
  el.innerHTML += '<div class="' + cls + '">[' + ts() + '] ' + msg + '</div>';
  el.scrollTop = el.scrollHeight;
}

function setDot(id, state) {
  const el = document.getElementById(id);
  el.className = 'dot';
  if (state) el.classList.add(state);
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function doFlush() {
  if (!confirm('Wipe ALL Redis data — device cache, index, everything?')) return;
  const btn = document.getElementById('btnFlush');
  btn.disabled = true;
  document.getElementById('flushMsg').textContent = 'wiping…';
  document.getElementById('flushBar').style.width = '35%';
  try {
    const r = await fetch('/api/redis/flushdb?confirm=yes');
    const d = await r.json();
    document.getElementById('flushBar').style.width = '100%';
    document.getElementById('flushMsg').textContent = 'done — Redis cleared ✓';
    setTimeout(() => {
      document.getElementById('flushBar').style.width = '0%';
      document.getElementById('flushMsg').textContent = 'idle';
      btn.disabled = false;
    }, 3000);
  } catch(e) {
    document.getElementById('flushMsg').textContent = 'error: ' + e.message;
    document.getElementById('flushBar').style.width = '0%';
    btn.disabled = false;
  }
}

async function startA() {
  aStop = false;
  let page = parseInt(document.getElementById('aStart').value) || 1;
  let pages = 0, newTotal = 0, lastIdx = 0, empty = 0;
  const MAXP = 150;
  document.getElementById('btnStartA').disabled = true;
  document.getElementById('btnStopA').style.display = 'inline-flex';
  setDot('dotA', 'run');
  logLine('logA', 'Source A started (reviews listing)…', 'ok');

  while (!aStop && page <= MAXP) {
    document.getElementById('aStart').value = page;
    try {
      const r = await fetch('/api/index/crawl-reviews-page?page=' + page);
      if (!r.ok) { logLine('logA', 'p' + page + ' HTTP ' + r.status, 'err'); await sleep(2500); page++; empty++; if (empty >= 5) { logLine('logA', '5 consecutive errors — stopping.', 'warn'); break; } continue; }
      const d = await r.json();
      if (!d.success) { logLine('logA', 'p' + page + ': ' + (d.error || 'error'), 'err'); await sleep(2500); page++; empty++; if (empty >= 5) { logLine('logA', '5 consecutive errors — stopping.', 'warn'); break; } continue; }
      const res = d.result;
      pages++; newTotal += res.newUrls; lastIdx = res.totalUrls;
      if (res.phonesFound === 0) { empty++; } else { empty = 0; }
      document.getElementById('aPages').textContent = pages;
      document.getElementById('aTotal').textContent = lastIdx;
      document.getElementById('aNew').textContent = newTotal;
      const pct = Math.min(Math.round(page / MAXP * 100), 99);
      document.getElementById('barA').style.width = pct + '%';
      document.getElementById('pctA').textContent = pct + '%';
      document.getElementById('labelA').textContent = 'p' + page + ' — ' + res.phonesFound + ' found, ' + res.newUrls + ' new';
      logLine('logA', 'p' + page + ' → ' + res.phonesFound + ' reviews, ' + res.newUrls + ' new (index: ' + res.totalUrls + ')', res.phonesFound > 0 ? 'ok' : 'info');
      if (res.done) { logLine('logA', 'Source A complete (server says done).', 'ok'); break; }
      if (empty >= 5) { logLine('logA', '5 consecutive empty pages — Source A complete.', 'ok'); break; }
      page++;
      await sleep(DELAY);
    } catch(e) { logLine('logA', 'p' + page + ' error: ' + e.message, 'err'); await sleep(3000); page++; empty++; if (empty >= 5) break; }
  }

  document.getElementById('barA').style.width = '100%';
  document.getElementById('pctA').textContent = aStop ? 'stopped' : '100%';
  document.getElementById('labelA').textContent = aStop ? 'stopped at p' + page : 'done';
  setDot('dotA', aStop ? '' : 'done');
  document.getElementById('btnStartA').disabled = false;
  document.getElementById('btnStartA').textContent = '▶ Resume';
  document.getElementById('btnStopA').style.display = 'none';
}

function stopA() { aStop = true; logLine('logA', 'stop requested…', 'warn'); }

async function startB() {
  bStop = false;
  let page = parseInt(document.getElementById('bStart').value) || 1;
  let pages = 0, newTotal = 0, lastIdx = 0, empty = 0;
  document.getElementById('btnStartB').disabled = true;
  document.getElementById('btnStopB').style.display = 'inline-flex';
  setDot('dotB', 'run');
  logLine('logB', 'Source B started (library / chrono)…', 'ok');

  while (!bStop) {
    document.getElementById('bStart').value = page;
    try {
      const r = await fetch('/api/index/crawl-page?page=' + page);
      if (!r.ok) { logLine('logB', 'p' + page + ' HTTP ' + r.status, 'err'); await sleep(2500); page++; empty++; if (empty >= 3) break; continue; }
      const d = await r.json();
      if (!d.success) { logLine('logB', 'p' + page + ': ' + (d.error || 'error'), 'err'); await sleep(2500); page++; empty++; if (empty >= 3) break; continue; }
      const res = d.result;
      pages++; newTotal += res.newUrls; lastIdx = res.totalUrls;
      document.getElementById('bPages').textContent = pages;
      document.getElementById('bTotal').textContent = lastIdx;
      document.getElementById('bNew').textContent = newTotal;
      const pct = Math.min(Math.round(pages / 300 * 100), 99);
      document.getElementById('barB').style.width = pct + '%';
      document.getElementById('pctB').textContent = 'p' + page;
      document.getElementById('labelB').textContent = 'p' + page + ' — ' + res.phonesFound + ' found, ' + res.newUrls + ' new';
      logLine('logB', 'p' + page + ' → ' + res.phonesFound + ' phones, ' + res.newUrls + ' new (index: ' + res.totalUrls + ')', res.newUrls > 0 ? 'ok' : 'info');
      if (res.done || res.phonesFound === 0) { empty++; if (empty >= 3) { logLine('logB', 'Source B exhausted.', 'ok'); break; } }
      else { empty = 0; }
      page++;
      await sleep(DELAY);
    } catch(e) { logLine('logB', 'p' + page + ' error: ' + e.message, 'err'); await sleep(3000); page++; empty++; if (empty >= 3) break; }
  }

  document.getElementById('barB').style.width = '100%';
  document.getElementById('pctB').textContent = bStop ? 'stopped' : 'done';
  document.getElementById('labelB').textContent = bStop ? 'stopped at p' + page : 'done';
  setDot('dotB', bStop ? '' : 'done');
  document.getElementById('btnStartB').disabled = false;
  document.getElementById('btnStartB').textContent = '▶ Resume';
  document.getElementById('btnStopB').style.display = 'none';
}

function stopB() { bStop = true; logLine('logB', 'stop requested…', 'warn'); }

let cStop = false;

async function startC() {
  cStop = false;
  let page = parseInt(document.getElementById('cStart').value) || 0;
  let pages = 0, newTotal = 0, lastIdx = 0, empty = 0;
  document.getElementById('btnStartC').disabled = true;
  document.getElementById('btnStopC').style.display = 'inline-flex';
  setDot('dotC', 'run');
  logLine('logC', 'Source C started (smartphone listing)…', 'ok');

  while (!cStop) {
    document.getElementById('cStart').value = page;
    try {
      const r = await fetch('/api/index/crawl-smartphone-page?page=' + page);
      if (!r.ok) { logLine('logC', 'p' + page + ' HTTP ' + r.status, 'err'); await sleep(2500); page++; empty++; if (empty >= 5) break; continue; }
      const d = await r.json();
      if (!d.success) { logLine('logC', 'p' + page + ': ' + (d.error || 'error'), 'err'); await sleep(2500); page++; empty++; if (empty >= 5) break; continue; }
      const res = d.result;
      pages++; newTotal += res.newUrls; lastIdx = res.totalUrls;
      if (res.phonesFound === 0) { empty++; } else { empty = 0; }
      document.getElementById('cPages').textContent = pages;
      document.getElementById('cTotal').textContent = lastIdx;
      document.getElementById('cNew').textContent = newTotal;
      const pct = Math.min(Math.round(pages / 150 * 100), 99);
      document.getElementById('barC').style.width = pct + '%';
      document.getElementById('pctC').textContent = 'p' + page;
      document.getElementById('labelC').textContent = 'p' + page + ' — ' + res.phonesFound + ' found, ' + res.newUrls + ' new';
      logLine('logC', 'p' + page + ' → ' + res.phonesFound + ' phones, ' + res.newUrls + ' new (index: ' + res.totalUrls + ')', res.phonesFound > 0 ? 'ok' : 'info');
      if (res.done) { logLine('logC', 'Source C complete (server says done).', 'ok'); break; }
      if (empty >= 5) { logLine('logC', '5 consecutive empty pages — Source C complete.', 'ok'); break; }
      page++;
      await sleep(DELAY);
    } catch(e) { logLine('logC', 'p' + page + ' error: ' + e.message, 'err'); await sleep(3000); page++; empty++; if (empty >= 5) break; }
  }

  document.getElementById('barC').style.width = '100%';
  document.getElementById('pctC').textContent = cStop ? 'stopped' : 'done';
  document.getElementById('labelC').textContent = cStop ? 'stopped at p' + page : 'done';
  setDot('dotC', cStop ? '' : 'done');
  document.getElementById('btnStartC').disabled = false;
  document.getElementById('btnStartC').textContent = '▶ Resume';
  document.getElementById('btnStopC').style.display = 'none';
}

function stopC() { cStop = true; logLine('logC', 'stop requested…', 'warn'); }

let resolveStop = false;
function stopResolve() { resolveStop = true; }

async function doResolve() {
  resolveStop = false;
  const btn = document.getElementById('btnResolve');
  btn.disabled = true;
  document.getElementById('btnStopResolve').style.display = 'inline-flex';
  document.getElementById('postmsg').textContent = '';
  let offset = 0, totalResolved = 0, total = 0;

  // First: clear stale resolve caches so we refetch fresh from NBC
  document.getElementById('resolveLabel').textContent = 'clearing stale cache…';
  try {
    await fetch('/api/index/recover-review-urls?clearcache=1&norun=1');
  } catch(e) {}

  document.getElementById('resolveLabel').textContent = 'resolving…';

  while (!resolveStop) {
    try {
      const r = await fetch('/api/index/resolve-page?offset=' + offset);
      if (!r.ok) { await sleep(2000); continue; }
      const d = await r.json();
      if (!d.success) break;
      totalResolved += d.resolved;
      total = d.total;
      offset = d.offset;
      const pct = total > 0 ? Math.min(Math.round(offset / total * 100), 100) : 0;
      document.getElementById('barResolve').style.width = pct + '%';
      document.getElementById('resolveLabel').textContent = offset + ' / ' + total + ' checked';
      document.getElementById('resolveStats').textContent = totalResolved + ' upgraded to review URL';
      if (d.done) {
        document.getElementById('resolveLabel').textContent = 'done — ' + total + ' checked, ' + totalResolved + ' upgraded. Running purge + rebuild…';
        // Auto-run purge then rebuild
        try {
          const pr = await fetch('/api/index/purge-library-duplicates');
          const pd = await pr.json();
          document.getElementById('postmsg').textContent = 'purged ' + (pd.purged || 0) + ' dupes';
        } catch(e) {}
        try {
          const rr = await fetch('/api/index/rebuild-search');
          await rr.json();
          document.getElementById('resolveLabel').textContent = 'done — ' + total + ' checked, ' + totalResolved + ' upgraded ✓ purged + rebuilt';
        } catch(e) {}
        break;
      }
      await sleep(200);
    } catch(e) { await sleep(2000); }
  }

  btn.disabled = false;
  document.getElementById('btnStopResolve').style.display = 'none';
}

async function doPurge() {
  const btn = document.getElementById('btnPurge');
  btn.disabled = true;
  document.getElementById('postmsg').textContent = 'purging…';
  try {
    const r = await fetch('/api/index/purge-library-duplicates');
    const d = await r.json();
    document.getElementById('postmsg').textContent = 'purged ' + (d.purged || 0) + ', kept ' + (d.kept || 0);
  } catch(e) { document.getElementById('postmsg').textContent = 'error: ' + e.message; }
  btn.disabled = false;
}

async function doRebuild() {
  const btn = document.getElementById('btnRebuild');
  btn.disabled = true;
  document.getElementById('postmsg').textContent = 'rebuilding…';
  try {
    const r = await fetch('/api/index/rebuild-search');
    const d = await r.json();
    document.getElementById('postmsg').textContent = d.message || 'done ✓';
  } catch(e) { document.getElementById('postmsg').textContent = 'error: ' + e.message; }
  btn.disabled = false;
}
</script>
</body>
</html>`);
});

app.get('/crawler', (_, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NBC Crawler</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Syne:wght@700;800&display=swap');
  :root{--bg:#0a0a0f;--surface:#12121a;--border:#1e1e2e;--accent:#00ff87;--accent2:#0ff;--danger:#ff4d6d;--warn:#ffd166;--text:#e0e0f0;--muted:#555570}
  *{margin:0;padding:0;box-sizing:border-box}
  body{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;min-height:100vh;padding:2rem}
  body::before{content:'';position:fixed;inset:0;background:radial-gradient(ellipse at 20% 20%,rgba(0,255,135,.04),transparent 60%),radial-gradient(ellipse at 80% 80%,rgba(0,255,255,.03),transparent 60%);pointer-events:none}
  h1{font-family:'Syne',sans-serif;font-size:clamp(1.8rem,4vw,2.8rem);font-weight:800;background:linear-gradient(90deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:.25rem}
  .subtitle{color:var(--muted);font-size:.75rem;letter-spacing:.1em;text-transform:uppercase;margin-bottom:2rem}
  .controls{display:flex;gap:.75rem;margin-bottom:1.5rem;flex-wrap:wrap;align-items:center}
  .btn{padding:.65rem 1.5rem;border-radius:6px;border:none;font-family:'JetBrains Mono',monospace;font-size:.85rem;font-weight:700;cursor:pointer;transition:all .15s}
  .btn-start{background:var(--accent);color:#000}.btn-start:hover{background:#00e87a}.btn-start:disabled{background:var(--muted);color:var(--bg);cursor:not-allowed}
  .btn-stop{background:transparent;border:1px solid var(--danger);color:var(--danger)}.btn-stop:hover{background:var(--danger);color:#fff}
  .btn-unlock{background:transparent;border:1px solid var(--warn);color:var(--warn);font-size:.75rem;padding:.5rem 1rem}.btn-unlock:hover{background:var(--warn);color:#000}
  .stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:1rem;margin-bottom:1.5rem}
  .stat-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem;transition:border-color .3s}
  .stat-card.active{border-color:var(--accent)}
  .stat-label{font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:.4rem}
  .stat-value{font-size:1.6rem;font-weight:700;font-family:'Syne',sans-serif;color:var(--accent)}
  .stat-value.muted{color:var(--muted)}.stat-value.danger{color:var(--danger)}
  .progress-bar-wrap{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.5rem;margin-bottom:1.5rem}
  .progress-info{display:flex;justify-content:space-between;margin-bottom:.6rem;font-size:.75rem}
  .progress-track{height:6px;background:var(--border);border-radius:99px;overflow:hidden}
  .progress-fill{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));border-radius:99px;transition:width .5s ease;width:0%}
  .log-wrap{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden}
  .log-header{padding:.75rem 1rem;border-bottom:1px solid var(--border);font-size:.7rem;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;display:flex;justify-content:space-between}
  .log-body{height:320px;overflow-y:auto;padding:.75rem 1rem;font-size:.75rem;line-height:1.8}
  .log-body::-webkit-scrollbar{width:4px}.log-body::-webkit-scrollbar-track{background:var(--bg)}.log-body::-webkit-scrollbar-thumb{background:var(--border);border-radius:99px}
  .log-line{display:flex;gap:.75rem}.log-time{color:var(--muted);flex-shrink:0}
  .log-msg.ok{color:var(--accent)}.log-msg.info{color:var(--text)}.log-msg.warn{color:var(--warn)}.log-msg.err{color:var(--danger)}.log-msg.done{color:var(--accent2)}
  .pulse{display:inline-block;width:8px;height:8px;background:var(--accent);border-radius:50%;animation:pulse 1.2s ease-in-out infinite}
  @keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.7)}}
  #statusBadge{font-size:.75rem;color:var(--muted)}
</style>
</head>
<body>
<h1>NBC Crawler</h1>
<p class="subtitle">NotebookCheck Auto-Crawler — one page at a time, Vercel-safe</p>
<div class="controls">
  <button class="btn btn-unlock" onclick="unlock()">🔓 Unlock</button>
  <button class="btn btn-start" id="btnStart" onclick="startCrawl()">▶ Start Crawling</button>
  <button class="btn btn-stop" id="btnStop" onclick="stopCrawl()" style="display:none">■ Stop</button>
  <span id="statusBadge">Idle</span>
  <span id="logIndicator"></span>
</div>
<div class="stats-grid">
  <div class="stat-card" id="cardPages"><div class="stat-label">Pages Crawled</div><div class="stat-value muted" id="statPages">0</div></div>
  <div class="stat-card"><div class="stat-label">Total URLs</div><div class="stat-value muted" id="statUrls">0</div></div>
  <div class="stat-card"><div class="stat-label">New This Run</div><div class="stat-value muted" id="statNew">0</div></div>
  <div class="stat-card"><div class="stat-label">Current Page</div><div class="stat-value muted" id="statPage">—</div></div>
  <div class="stat-card"><div class="stat-label">Errors</div><div class="stat-value muted" id="statErrors">0</div></div>
  <div class="stat-card"><div class="stat-label">Elapsed</div><div class="stat-value muted" id="statElapsed">0s</div></div>
</div>
<div class="progress-bar-wrap">
  <div class="progress-info"><span id="progressLabel">Ready to crawl</span><span id="progressPct">0%</span></div>
  <div class="progress-track"><div class="progress-fill" id="progressFill"></div></div>
</div>
<div class="log-wrap">
  <div class="log-header"><span>Activity Log</span><span id="logIndicator2"></span></div>
  <div class="log-body" id="logBody"></div>
</div>
<script>
  let running=false,currentPage=1,totalPages=0,totalUrls=0,totalNew=0,errors=0,startTime=null,elapsedTimer=null,stopRequested=false;
  const base='';// same origin — no CORS issues
  function log(msg,type='info'){const b=document.getElementById('logBody'),now=new Date().toLocaleTimeString(),l=document.createElement('div');l.className='log-line';l.innerHTML=\`<span class="log-time">\${now}</span><span class="log-msg \${type}">\${msg}</span>\`;b.appendChild(l);b.scrollTop=b.scrollHeight}
  function setStatus(msg,active=false){const s=document.getElementById('statusBadge'),i=document.getElementById('logIndicator2');s.textContent=msg;s.style.color=active?'var(--accent)':'var(--muted)';i.innerHTML=active?'<span class="pulse"></span>':''}
  function updateStats(){document.getElementById('statPages').textContent=totalPages;document.getElementById('statUrls').textContent=totalUrls;document.getElementById('statNew').textContent=totalNew;document.getElementById('statPage').textContent=currentPage;const e=document.getElementById('statErrors');e.textContent=errors;e.className='stat-value '+(errors>0?'danger':'muted');document.getElementById('statPages').className='stat-value '+(totalPages>0?'':'muted');document.getElementById('statUrls').className='stat-value '+(totalUrls>0?'':'muted')}
  async function unlock(){log('Unlocking...','warn');try{const r=await fetch('/api/debug/redis-force-unlock'),d=await r.json();log(d.hint?.includes('✅')?'✅ Lock cleared':'Lock: '+JSON.stringify(d),d.hint?.includes('✅')?'ok':'warn')}catch(e){log('Unlock failed: '+e.message,'err')}}
  async function startCrawl(){if(running)return;running=true;stopRequested=false;startTime=Date.now();currentPage=1;document.getElementById('btnStart').disabled=true;document.getElementById('btnStop').style.display='inline-block';setStatus('Crawling...',true);elapsedTimer=setInterval(()=>{const s=Math.round((Date.now()-startTime)/1000);document.getElementById('statElapsed').textContent=s+'s';document.getElementById('statElapsed').className='stat-value'},1000);try{await fetch('/api/debug/redis-force-unlock');log('🔓 Lock cleared','ok')}catch{}log('Starting crawl...','info');while(!stopRequested){try{document.getElementById('cardPages').classList.add('active');const resp=await fetch(\`/api/index/crawl-page?page=\${currentPage}\`);if(!resp.ok){log(\`Page \${currentPage} HTTP \${resp.status}\`,'err');errors++;updateStats();await new Promise(r=>setTimeout(r,2000));currentPage++;continue}const data=await resp.json();if(!data.success){log(\`Page \${currentPage}: \${data.error||'error'}\`,'err');errors++;updateStats();await new Promise(r=>setTimeout(r,2000));currentPage++;continue}const result=data.result;totalPages++;totalUrls=result.totalUrls;totalNew+=result.newUrls;updateStats();const pct=Math.min(Math.round(currentPage/150*100),99);document.getElementById('progressFill').style.width=pct+'%';document.getElementById('progressPct').textContent=pct+'%';document.getElementById('progressLabel').textContent=\`Page \${currentPage} crawled\`;log(\`Page \${currentPage} → \${result.phonesFound} phones, \${result.newUrls} new (total: \${result.totalUrls})\`,result.newUrls>0?'ok':'info');if(result.done){log(\`🎉 Done! \${totalUrls} URLs indexed across \${totalPages} pages.\`,'done');document.getElementById('progressFill').style.width='100%';document.getElementById('progressPct').textContent='100%';document.getElementById('progressLabel').textContent='Crawl complete!';break}currentPage++;await new Promise(r=>setTimeout(r,800))}catch(e){log(\`Page \${currentPage} failed: \${e.message}\`,'err');errors++;updateStats();await new Promise(r=>setTimeout(r,3000));currentPage++}}if(stopRequested)log(\`⏹ Stopped at page \${currentPage}. \${totalUrls} URLs so far.\`,'warn');running=false;clearInterval(elapsedTimer);document.getElementById('btnStart').disabled=false;document.getElementById('btnStart').textContent='▶ Resume';document.getElementById('btnStop').style.display='none';document.getElementById('cardPages').classList.remove('active');setStatus('Done',false)}
  function stopCrawl(){stopRequested=true;setStatus('Stopping...',false);log('Stop requested...','warn')}
</script>
</body>
</html>`);
});

// /api/index/search-debug — check if a query hits the index
// ?q=vivo+x300  → shows what searchIndex returns (or null if miss)
// Also shows flat index size so you know if rebuild has been run.
app.get('/api/index/search-debug', async (req, res) => {
  const q = (validateQ(req.query.q) ?? '');
  try {
    const { searchIndex, rebuildSearchIndex } = await import('./src/notebookcheck_index');
    const axios2 = (await import('axios')).default;
    const rUrl   = process.env.UPSTASH_REDIS_REST_URL!;
    const rToken = process.env.UPSTASH_REDIS_REST_TOKEN!;
    // Get raw flat index size
    const raw = await axios2.get(`${rUrl}/get/nbc:index:v4:search_index`,
      { headers: { Authorization: `Bearer ${rToken}` } });
    const flatRaw = raw.data?.result;
    const flat = flatRaw ? JSON.parse(flatRaw) : [];
    const indexSize = Array.isArray(flat) ? flat.length : 0;
    const match = q ? await searchIndex(q).catch(() => null) : null;
    return res.json({
      success: true,
      indexSize,
      query: q || '(none)',
      match: match ?? null,
      hint: indexSize === 0 ? 'Index is empty — hit /api/index/rebuild-search first' : `${indexSize} entries in search index`,
    });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/clean-titles — fix dirty titles already stored in Redis
// Strips subtitle after |, —, –, : and removes "review" from stored titles.
// Run once after deploying this fix.
app.get('/api/index/clean-titles', async (req, res) => {
  try {
    const axios2 = (await import('axios')).default;
    const rUrl   = process.env.UPSTASH_REDIS_REST_URL!;
    const rToken = process.env.UPSTASH_REDIS_REST_TOKEN!;
    const headers = { Authorization: `Bearer ${rToken}`, 'Content-Type': 'application/json' };
    const raw = await axios2.get(`${rUrl}/get/nbc:index:v3:entries`, { headers });
    const entries = raw.data?.result ? JSON.parse(raw.data.result) : {};
    let cleaned = 0;
    for (const e of Object.values(entries) as any[]) {
      const orig = e.title as string;
      let fixed = orig
        .split(/\s*[|—–:]\s*/)[0]
        .split(' - ')[0]
        .replace(/reviews?/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
      // Also strip noise words like "smartphone" from device titles
      fixed = fixed.replace(/\b(smartphone|smartphones|mobile phone|handset)\b/gi, '').replace(/\s+/g, ' ').trim();
      if (fixed !== orig && fixed.length >= 4) { e.title = fixed; cleaned++; }
    }
    if (cleaned > 0) {
      await axios2.post(`${rUrl}/pipeline`,
        [['SET', 'nbc:index:v3:entries', JSON.stringify(entries)]],
        { headers });
      const { rebuildSearchIndex } = await import('./src/notebookcheck_index');
      await rebuildSearchIndex();
    }
    return res.json({ success: true, cleaned, message: `Fixed ${cleaned} dirty titles and rebuilt search index` });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/rebuild-search — rebuild the fast search index from entries
app.get('/api/index/rebuild-search', async (req, res) => {
  try {
    await rebuildSearchIndex();
    return res.json({ success: true, message: 'Search index rebuilt — /api/phone searches are now fast' });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});