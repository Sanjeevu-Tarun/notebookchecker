import express from 'express';
import cors from 'cors';
// scraper legacy functions removed
import './src/scraper';
import { getNotebookCheckData, searchNotebookCheck, scrapeNotebookCheckDevice, debugNBCSearch } from './src/notebookcheck';
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
  crawlNBCSmartphoneIndex,
  getQueueStats,
  getIndexEntries,
  scrapeIndexedDevice,
  bulkScrapeAll,
  abortBulkScrape,
  isBulkScrapeActive,
  validateIndexUrl,
  getBrandBreakdown,
  resetErrors,
  resetEntry,
  clearIndex,
  loadIndexFromRedis,
  getCrawlInProgress,
  getLastCrawlStats,
  indexStore,
} from './src/notebookcheck_index';

const app = express();
app.use(cors());

app.get('/api/health', (_, res) => res.json({ status: 'ok' }));

// ─────────────────────────────────────────────────────────────────────────────
// /api/phone — NotebookCheck ONLY (fast version, no GSMArena)
// Uses Brave + SearXNG search, guaranteed <5s response
// ─────────────────────────────────────────────────────────────────────────────
app.get('/api/phone', async (req, res) => {
  const q = req.query.q as string;
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });

  try {
    const { getNotebookCheckDataFast } = await import('./src/notebookcheck');
    const data = await getNotebookCheckDataFast(q);
    
    if (!data) return res.status(404).json({ success: false, error: 'Device not found' });
    return res.json({ success: true, source: 'notebookcheck', data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/phone/debug — full timing: search ms + scrape ms + per-strategy breakdown
app.get('/api/phone/debug', async (req, res) => {
  const q = (req.query.q as string) || 'iphone 17 pro max';
  const t0 = Date.now();

  const { getNotebookCheckDataFast, scrapeNotebookCheckDevice } = await import('./src/notebookcheck');

  // Search with timing
  const ts0 = Date.now();
  const nbcData = await getNotebookCheckDataFast(q).catch(() => null);
  const searchMs = Date.now() - ts0;

  // Scrape timing (already done inside getNotebookCheckDataFast, but we log separately)
  const scrapeResult = nbcData && !('error' in nbcData) ? {
    ok: true,
    scrapeMs: 0, // included in searchMs above
    title: (nbcData as any).title,
    url: (nbcData as any).reviewUrl,
    hasBenchmarks: Object.values((nbcData as any).benchmarks).some((b: any) => b.length > 0),
    hasSpecs: Object.keys((nbcData as any).specs).length > 2,
    imageCounts: {
      device: (nbcData as any).images.device.length,
      cameraSamples: (nbcData as any).images.cameraSamples.length,
      screenshots: (nbcData as any).images.screenshots.length,
      charts: (nbcData as any).images.charts.length,
    },
  } : { ok: false };

  const totalMs = Date.now() - t0;
  const strat = { bestMatch: nbcData && !('error' in nbcData) ? { url: (nbcData as any).reviewUrl, name: (nbcData as any).title } : null, elapsedMs: searchMs };
  const gsma = null;

  res.json({
    query: q,
    totalMs,
    searchMs,
    bestMatch: strat.bestMatch,
    scrape: scrapeResult,
  });
});


app.get('/api/phone/race', async (req, res) => {
  const q = (req.query.q as string) || 'iphone 17 pro max';
  const t0 = Date.now();
  
  try {
    const { getNotebookCheckDataFast } = await import('./src/notebookcheck');
    const data = await getNotebookCheckDataFast(q);
    const elapsedMs = Date.now() - t0;
    
    if (!data) return res.status(404).json({ query: q, winner: 'none', elapsedMs });
    return res.json({ query: q, winner: 'notebookcheck', elapsedMs, title: (data as any)?.title });
  } catch (e: any) {
    return res.status(500).json({ error: e.message, elapsedMs: Date.now() - t0 });
  }
});

app.get('/api/phone/suggestions', async (req, res) => {
  const q = req.query.q as string;
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

// NotebookCheck endpoints
app.get('/api/nbc/search', async (req, res) => {
  const q = req.query.q as string;
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });
  try {
    const data = await getNotebookCheckData(q);
    if (!data) return res.status(404).json({ success: false, error: 'Device not found on NotebookCheck' });
    return res.json({ success: true, source: 'notebookcheck', data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message, stack: e.stack?.slice(0, 500) });
  }
});

app.get('/api/nbc/debug', async (req, res) => {
  const q = (req.query.q as string) || 'vivo x300 pro';
  try {
    const result = await debugNBCSearch(q);
    return res.json(result);
  } catch (e: any) {
    return res.status(500).json({ error: e.message, code: e.code });
  }
});


// /api/nbc/searxng-debug — full search+scrape pipeline with per-stage timing
app.get('/api/nbc/searxng-debug', async (req, res) => {
  const q = (req.query.q as string) || 'oneplus 15';
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
    return res.status(500).json({ error: e.message, totalMs: Date.now() - t0 });
  }
});

// /api/nbc/race — shows which NBC search strategy won (DDG vs SearXNG)
app.get('/api/nbc/race', async (req, res) => {
  const q = (req.query.q as string) || 'vivo x300 pro';
  const t0 = Date.now();
  try {
    const suggestions = await searchNotebookCheck(q);
    const elapsedMs = Date.now() - t0;
    return res.json({ 
      query: q, 
      elapsedMs, 
      count: suggestions.length,
      top3: suggestions.slice(0, 3).map(s => ({ title: s.title, url: s.url, score: s.score }))
    });
  } catch (e: any) {
    return res.status(500).json({ error: e.message, elapsedMs: Date.now() - t0 });
  }
});

app.get('/api/nbc/suggestions', async (req, res) => {
  const q = req.query.q as string;
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });
  try {
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
    // Phone / Device (SearXNG-based)
    phone:            '/api/phone?q=<device>',
    phoneDebug:       '/api/phone/debug?q=<device>',
    nbcDevice:        '/api/nbc/device?url=<notebookcheck-url>',
    nbcSuggestions:   '/api/nbc/suggestions?q=<device>',
    nbcDebug:         '/api/nbc/debug?q=<device>',
    // Index-based scraping (no SearXNG — bulk memory approach)
    indexCrawlDebug:  '/api/index/crawl-debug               ← START HERE: verify NBC is reachable',
    indexCrawl:       '/api/index/crawl?maxPages=3           ← then crawl 3 pages to test',
    indexStatus:      '/api/index/status                     ← check crawl progress + coverage',
    indexValidate:    '/api/index/validate?count=10          ← verify URLs are correct device pages',
    indexList:        '/api/index/list?status=pending&limit=20',
    indexScrapeOne:   '/api/index/scrape-one?url=<url>       ← scrape single device (no SearXNG)',
    indexBulkStart:   '/api/index/bulk-start?concurrency=2   ← scrape everything',
    indexBulkAbort:   '/api/index/bulk-abort',
    indexErrors:      '/api/index/errors',
    indexResetErrors: '/api/index/reset-errors',
    indexBrands:      '/api/index/brands',
    indexReload:      '/api/index/reload                     ← restore from Redis after redeploy',
    // Processor / SoC
    processor:            '/api/processor?q=<chip>',
    processorSuggestions: '/api/processor/suggestions?q=<chip>',
    processorDevice:      '/api/processor/device?url=<notebookcheck-url>',
  }
}));

// ─────────────────────────────────────────────────────────────────────────────
// KEEP-ALIVE: Ping Render SearXNG every 10 minutes to prevent cold starts.
// Render free tier spins down after 15 min of inactivity — this prevents that.
// Uses a lightweight /healthz ping (no search query needed, ~50ms).
// ─────────────────────────────────────────────────────────────────────────────
const SEARXNG_INSTANCE = 'https://searxng-notebookcheck.onrender.com';
const PING_INTERVAL_MS = 10 * 60 * 1000; // 10 minutes

async function pingSearXNG() {
  try {
    const axios = (await import('axios')).default;
    const t0 = Date.now();
    await axios.get(`${SEARXNG_INSTANCE}/healthz`, { timeout: 10000 });
    console.log(`[keep-alive] SearXNG ping OK — ${Date.now() - t0}ms`);
  } catch {
    // Fallback: hit the search endpoint with a minimal query if /healthz not available
    try {
      const axios = (await import('axios')).default;
      await axios.get(`${SEARXNG_INSTANCE}/search`, {
        params: { q: 'ping', format: 'json' },
        timeout: 10000,
      });
      console.log('[keep-alive] SearXNG fallback ping OK');
    } catch (e: any) {
      console.warn('[keep-alive] SearXNG ping failed:', e.message);
    }
  }
}

// Start pinging immediately on boot, then every 10 minutes
pingSearXNG();
setInterval(pingSearXNG, PING_INTERVAL_MS);

// Auto-load the NBC review index from Redis on startup (non-blocking)
// This restores index state after a redeploy so you don't lose scrape progress
setTimeout(() => {
  loadIndexFromRedis().then(count => {
    if (count > 0) console.log(`[index] Loaded ${count} entries from Redis on startup`);
  }).catch(e => console.warn('[index] Redis load failed on startup:', e.message));
}, 2000);

// Warm processor search cache on boot — runs in background, staggered 600ms/chip.
// This pre-populates Redis so the first real user request for popular chips
// skips SearXNG entirely and only pays the scrape cost (~1500ms instead of ~2800ms).
// warmProcCache removed (not exported) — processor cache warms on first request

// Expose a manual ping trigger endpoint for debugging
// /api/nbc/direct-debug?q=<device>
// Shows exactly what NBC's own search returns — raw HTML links, scoring, timing.
// Use this to verify NBC direct search is working before relying on it.
// Example: /api/nbc/direct-debug?q=vivo+x300
app.get('/api/nbc/direct-debug', async (req, res) => {
  const q = (req.query.q as string) || 'vivo x300';
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
    return res.status(500).json({ error: e.message, totalMs: Date.now() - t0 });
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
  const q = req.query.q as string;
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
  const q = req.query.q as string;
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
  const q = (req.query.q as string) || 'snapdragon 8 elite';
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
  const q = (req.query.q as string) || 'snapdragon 8 elite';
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
    return res.status(500).json({ error: e.message, searchMs: Date.now() - t0 });
  }
});

// ═════════════════════════════════════════════════════════════════════════════
// NBC REVIEW INDEX ENDPOINTS  — the "memory game" approach
// ─────────────────────────────────────────────────────────────────────────────
// Flow:
//   1. GET /api/index/crawl          → crawl NBC listing page, build URL index
//   2. GET /api/index/status         → check index + scrape progress
//   3. GET /api/index/validate       → verify crawled URLs resolve correctly
//   4. GET /api/index/bulk-start     → bulk-scrape all indexed URLs (no SearXNG!)
//   5. GET /api/index/list           → browse index with filters
// ═════════════════════════════════════════════════════════════════════════════

// /api/index/crawl — crawl NBC smartphone reviews listing and build URL index
// ?maxPages=N   cap pages (default: all)   ?force=true  force re-crawl
// ?delayMs=N    ms between pages (default: 800)   ?bg=true  background mode
app.get('/api/index/crawl', async (req, res) => {
  const maxPages = parseInt(req.query.maxPages as string || '999');
  const force    = req.query.force === 'true';
  const delayMs  = parseInt(req.query.delayMs as string || '800');
  const bg       = req.query.bg === 'true';

  if (bg) {
    res.json({ status: 'started', message: 'Crawl running in background — check /api/index/status' });
    crawlNBCSmartphoneIndex({ maxPages, forceRecrawl: force, delayMs }).catch(e =>
      console.error('[crawl bg error]', e.message)
    );
    return;
  }
  try {
    const crawlStats = await crawlNBCSmartphoneIndex({ maxPages, forceRecrawl: force, delayMs });
    return res.json({ success: true, crawl: crawlStats, queue: getQueueStats() });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/status — overall index and scrape progress at a glance
app.get('/api/index/status', (req, res) => {
  const stats = getLastCrawlStats();
  return res.json({
    success: true,
    index: { totalUrls: indexStore.size, lastCrawledAt: stats?.lastCrawledAt ?? null, crawlInProgress: getCrawlInProgress() },
    scrape: { ...getQueueStats(), bulkActive: isBulkScrapeActive() },
    brands: Object.fromEntries(Object.entries(getBrandBreakdown()).slice(0, 20)),
    lastCrawl: stats,
  });
});

// /api/index/list — browse index with filtering + pagination
// ?status=pending|done|error|scraping|all   ?brand=Google   ?search=Pixel 10
// ?page=1   ?limit=50
app.get('/api/index/list', (req, res) => {
  const status = (req.query.status as string) || 'all';
  const brand  = req.query.brand  as string | undefined;
  const search = req.query.search as string | undefined;
  const page   = parseInt(req.query.page  as string || '1');
  const limit  = Math.min(parseInt(req.query.limit as string || '50'), 200);
  return res.json({ success: true, ...getIndexEntries({ status: status as any, brand, search, page, limit }) });
});

// /api/index/validate — validate N random index URLs (confirm they hit real device pages)
// ?count=10   ?status=pending
app.get('/api/index/validate', async (req, res) => {
  const count  = Math.min(parseInt(req.query.count  as string || '10'), 50);
  const status = (req.query.status as string) || 'pending';
  const { entries } = getIndexEntries({ status: status as any, limit: count * 3 });
  const sample = entries.sort(() => Math.random() - 0.5).slice(0, count);
  if (!sample.length) return res.json({ success: true, message: 'No entries match filter', results: [] });
  const results = await Promise.all(sample.map(e => validateIndexUrl(e.url)));
  const valid = results.filter(r => r.valid).length;
  return res.json({
    success: true,
    summary: { checked: results.length, valid, invalid: results.length - valid, validPct: `${((valid/results.length)*100).toFixed(1)}%` },
    results,
  });
});

// /api/index/validate-url — validate one specific URL
// ?url=https://www.notebookcheck.net/...
app.get('/api/index/validate-url', async (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: 'url required' });
  try {
    return res.json({ success: true, result: await validateIndexUrl(url) });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/scrape-one — scrape a single indexed URL directly (no SearXNG)
// ?url=https://www.notebookcheck.net/...
app.get('/api/index/scrape-one', async (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: 'url required' });
  if (!indexStore.has(url)) {
    const slug = url.split('/').pop() || '';
    indexStore.set(url, {
      url, title: slug.replace(/\.\d+\.0\.html$/, '').replace(/-/g, ' ').trim(),
      brand: 'Unknown', slug, discoveredAt: new Date().toISOString(), status: 'pending', retries: 0,
    });
  }
  try {
    const result = await scrapeIndexedDevice(url);
    if (!result.success) return res.status(502).json({ success: false, error: result.error, url });
    return res.json({ success: true, source: 'notebookcheck_index', url, data: result.data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/bulk-start — start bulk scraping all pending URLs (async, no SearXNG)
// ?concurrency=2   ?delayMs=1200   ?onlyPending=true
app.get('/api/index/bulk-start', (req, res) => {
  if (isBulkScrapeActive()) {
    return res.status(409).json({ success: false, error: 'Already running — call /api/index/bulk-abort first', queue: getQueueStats() });
  }
  const concurrency = Math.min(parseInt(req.query.concurrency as string || '2'), 5);
  const delayMs     = parseInt(req.query.delayMs as string || '1200');
  const onlyPending = req.query.onlyPending === 'true';
  const q = getQueueStats();
  if (q.pending + q.error === 0) {
    return res.json({ success: false, message: 'Nothing to scrape. Run /api/index/crawl first.', queue: q });
  }
  bulkScrapeAll({ concurrency, delayMs, onlyPending }).then(r => console.log('[bulk] done:', r)).catch(e => console.error('[bulk] error:', e.message));
  return res.json({ success: true, message: `Bulk scrape started — ${q.pending + q.error} URLs queued`, queue: q, config: { concurrency, delayMs, onlyPending }, monitor: '/api/index/status' });
});

// /api/index/bulk-abort — abort the running bulk scrape
app.get('/api/index/bulk-abort', (req, res) => {
  if (!isBulkScrapeActive()) return res.json({ success: false, message: 'No bulk scrape running' });
  abortBulkScrape();
  return res.json({ success: true, message: 'Abort signal sent. Current requests will finish, then scrape stops.' });
});

// /api/index/brands — brand breakdown with scrape coverage
app.get('/api/index/brands', (req, res) => {
  return res.json({ success: true, brands: getBrandBreakdown() });
});

// /api/index/errors — list all failed entries with error messages
// ?limit=50   ?brand=Samsung
app.get('/api/index/errors', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit as string || '50'), 200);
  const brand = req.query.brand as string | undefined;
  const result = getIndexEntries({ status: 'error', brand, limit, page: 1 });
  return res.json({
    success: true, total: result.total,
    entries: result.entries.map(e => ({ url: e.url, title: e.title, brand: e.brand, retries: e.retries, errorMsg: e.errorMsg })),
  });
});

// /api/index/reset-errors — reset all error entries back to pending for retry
app.get('/api/index/reset-errors', (req, res) => {
  const count = resetErrors();
  return res.json({ success: true, resetCount: count, queue: getQueueStats() });
});

// /api/index/reset-url — reset a single URL back to pending
// ?url=https://...
app.get('/api/index/reset-url', (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: 'url required' });
  const ok = resetEntry(url);
  return res.json({ success: ok, message: ok ? 'Entry reset to pending' : 'URL not found in index', url });
});

// /api/index/entry — get index metadata for a specific URL
// ?url=https://...
app.get('/api/index/entry', (req, res) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ success: false, error: 'url required' });
  const entry = indexStore.get(url);
  if (!entry) return res.status(404).json({ success: false, error: 'URL not in index', url });
  return res.json({ success: true, entry });
});

// /api/index/reload — reload index from Redis (after redeploy)
app.get('/api/index/reload', async (req, res) => {
  try {
    const count = await loadIndexFromRedis();
    return res.json({ success: true, loaded: count, queue: getQueueStats(), lastCrawl: getLastCrawlStats() });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

// /api/index/clear — clear the entire index (?confirm=yes required)
app.get('/api/index/clear', (req, res) => {
  if (req.query.confirm !== 'yes') return res.status(400).json({ success: false, error: 'Add ?confirm=yes' });
  if (isBulkScrapeActive()) return res.status(409).json({ success: false, error: 'Cannot clear while bulk scrape is running' });
  clearIndex();
  return res.json({ success: true, message: 'Index cleared' });
});

// /api/index/crawl-debug — fetch the NBC listings page raw and show what links were found
// USE THIS FIRST to verify your server can reach NBC and see the URL format
// ?url=   override the listing URL to test
// ?raw=true   include first 3000 chars of HTML in response
app.get('/api/index/crawl-debug', async (req, res) => {
  const axios = (await import('axios')).default;
  const cheerio = await import('cheerio');

  const urlsToTry = [
    req.query.url as string || '',
    'https://www.notebookcheck.net/Reviews.55.0.html?cat=Smartphones',
    'https://www.notebookcheck.net/Smartphones.1311.0.html',
    'https://www.notebookcheck.net/Reviews.55.0.html',
  ].filter(Boolean);

  const results: any[] = [];

  for (const testUrl of urlsToTry) {
    const t0 = Date.now();
    try {
      const resp = await axios.get(testUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': 'https://www.notebookcheck.net/',
        },
        timeout: 12000,
        maxRedirects: 5,
      });

      const html = typeof resp.data === 'string' ? resp.data : JSON.stringify(resp.data);
      const $ = cheerio.load(html);

      // Extract ALL links matching the NBC article pattern
      const allLinks: string[] = [];
      const reviewLinks: string[] = [];
      $('a[href]').each((_: any, el: any) => {
        let href = $(el).attr('href') || '';
        if (href.startsWith('/')) href = 'https://www.notebookcheck.net' + href;
        href = href.split('?')[0];
        if (!href.includes('notebookcheck.net')) return;
        if (!/\.\d{4,}\.0\.html$/.test(href)) return;
        allLinks.push(href);
        const slug = href.split('/').pop() || '';
        if (/review|smartphone|phone/i.test(slug)) reviewLinks.push(href);
      });

      // Pagination links
      const pageLinks: string[] = [];
      $('a[href]').each((_: any, el: any) => {
        const href = $(el).attr('href') || '';
        if (/page=\d+/.test(href)) pageLinks.push(href);
      });

      results.push({
        url: testUrl,
        status: resp.status,
        fetchMs: Date.now() - t0,
        htmlLength: html.length,
        allArticleLinks: allLinks.length,
        reviewLinks: reviewLinks.length,
        sampleReviewLinks: reviewLinks.slice(0, 5),
        paginationLinks: [...new Set(pageLinks)].slice(0, 5),
        htmlSnippet: req.query.raw === 'true' ? html.slice(0, 3000) : undefined,
      });

      // Stop after first successful URL with results
      if (reviewLinks.length > 0) break;

    } catch (e: any) {
      results.push({
        url: testUrl,
        status: e?.response?.status || 0,
        fetchMs: Date.now() - t0,
        error: e.message,
      });
    }
  }

  const winner = results.find(r => (r.reviewLinks || 0) > 0);
  return res.json({
    success: !!winner,
    diagnosis: winner
      ? `Found ${winner.reviewLinks} review links at ${winner.url}`
      : '❌ No review links found on any URL — check if your server can reach notebookcheck.net',
    winner: winner || null,
    allResults: results,
  });
});

module.exports = app;