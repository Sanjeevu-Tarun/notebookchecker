import express from 'express';
import cors from 'cors';
import { searchAndGetDetails, searchDevice, getDeviceDetails } from './src/scraper';
import { getNotebookCheckData, searchNotebookCheck, scrapeNotebookCheckDevice, debugNBCSearch } from './src/notebookcheck';
import { getGSMArenaData, searchGSMArena, scrapeGSMArenaDevice } from './src/gsmarena';
import {
  getNotebookCheckProcessor,
  searchNotebookCheckProcessors,
  scrapeProcessorByUrl,
  normalizeProcQuery,
  searchProcViaSearXNG,
  scrapeNotebookCheckProcessor,
  warmProcCache,
  PROC_CACHE_VERSION,
} from './src/notebookcheck_processor';

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
    return res.json({ query: q, winner: 'notebookcheck', elapsedMs, title: data?.title });
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

// Legacy NanoReview endpoints
app.get('/api/search', async (req, res) => {
  const q = req.query.q as string;
  const index = parseInt(req.query.index as string || '0');
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });
  try {
    const data = await searchAndGetDetails(q, index);
    if (!data) return res.status(404).json({ success: false, error: 'Device not found' });
    return res.json({ success: true, data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/api/suggestions', async (req, res) => {
  const q = req.query.q as string;
  if (!q) return res.status(400).json({ success: false, error: '"q" required' });
  try {
    const data = await searchDevice(q);
    return res.json({ success: true, data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/api/device', async (req, res) => {
  const { type, slug } = req.query as { type: string; slug: string };
  if (!type || !slug) return res.status(400).json({ success: false, error: '"type" and "slug" required' });
  try {
    const data = await getDeviceDetails(type, slug);
    return res.json({ success: true, data });
  } catch (e: any) {
    return res.status(500).json({ success: false, error: e.message });
  }
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
    const { searchViaSearXNG, scrapeNotebookCheckDevice, normalizeQuery, resolveSearchResult, CACHE_VERSION } =
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
    // Phone / Device
    phone:            '/api/phone?q=<device>',
    phoneDebug:       '/api/phone/debug?q=<device>',
    phoneRace:        '/api/phone/race?q=<device>',
    nbcDevice:        '/api/nbc/device?url=<notebookcheck-url>',
    nbcSuggestions:   '/api/nbc/suggestions?q=<device>',
    nbcDebug:         '/api/nbc/debug?q=<device>',
    nbcSearxngDebug:  '/api/nbc/searxng-debug?q=<device>',
    nbcRace:          '/api/nbc/race?q=<device>',
    // Processor / SoC
    processor:            '/api/processor?q=<chip>',
    processorSuggestions: '/api/processor/suggestions?q=<chip>',
    processorDevice:      '/api/processor/device?url=<notebookcheck-url>',
    processorDebug:       '/api/processor/debug?q=<chip>',
    processorSearch:      '/api/processor/search?q=<chip>',
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

// Warm processor search cache on boot — runs in background, staggered 600ms/chip.
// This pre-populates Redis so the first real user request for popular chips
// skips SearXNG entirely and only pays the scrape cost (~1500ms instead of ~2800ms).
setTimeout(() => warmProcCache().catch(() => {}), 5000); // 5s delay — let server fully boot first

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

module.exports = app;