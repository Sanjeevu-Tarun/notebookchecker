import axios from 'axios';
import * as cheerio from 'cheerio';

// ══════════════════════════════════════════════════════════════════════════════
//  GSMARENA SCRAPER
//
//  Why GSMArena instead of (or alongside) NotebookCheck search:
//  - GSMArena search (res.php3?sSearch=) works from Vercel IPs — no blocking
//  - Direct device pages are predictable: brand_model-ID.php
//  - Search returns exact device matches with IDs we can use directly
//  - Much faster than Jina AI proxy chain (~1-3s vs ~8-15s)
//  - Covers all phones including latest releases
// ══════════════════════════════════════════════════════════════════════════════

// PERF: Shared axios with keep-alive (reuse TCP connections)
const _gsmaHttpsAgent = new (require('https').Agent)({ keepAlive: true, maxSockets: 50 });
const _gsmaAxios = require('axios').create({ httpsAgent: _gsmaHttpsAgent, maxRedirects: 5, decompress: true });

// ── CACHE: mem-first, Redis fallback (same pattern as notebookcheck.ts) ───────
const CACHE_TTL_MS  = 30 * 24 * 60 * 60 * 1000; // 30 days (mem TTL check)
const MEM_CACHE_MAX = 200;

const _memCache = new Map<string, { data: any; time: number }>();

function _memEvict() {
  if (_memCache.size < MEM_CACHE_MAX) return;
  const evictCount = Math.floor(MEM_CACHE_MAX * 0.2);
  const keys = [..._memCache.keys()];
  for (let i = 0; i < evictCount; i++) _memCache.delete(keys[i]);
}
function _memGet(k: string): any | null {
  const h = _memCache.get(k);
  if (!h) return null;
  if (Date.now() - h.time >= CACHE_TTL_MS) { _memCache.delete(k); return null; }
  // LRU: move to end
  _memCache.delete(k);
  _memCache.set(k, h);
  return h.data;
}
function _memSet(k: string, d: any) {
  _memEvict();
  _memCache.set(k, { data: d, time: Date.now() });
}

async function _redisGet(k: string): Promise<any | null> {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  try {
    const resp = await _gsmaAxios.get(`${url}/get/${encodeURIComponent(k)}`, {
      headers: { Authorization: `Bearer ${token}` }, timeout: 20000,
    });
    const val = resp.data?.result;
    return val ? JSON.parse(val) : null;
  } catch { return null; }
}

async function _redisSet(k: string, d: any): Promise<void> {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return;
  try {
    await _gsmaAxios.post(
      `${url}/pipeline`,
      [['SET', k, JSON.stringify(d)]],  // no EX — persist indefinitely
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, timeout: 25000 },
    );
  } catch { /* non-fatal */ }
}

async function getCache(k: string): Promise<any | null> {
  const mem = _memGet(k);
  if (mem !== null) return mem;
  const red = await _redisGet(k);
  if (red !== null) { _memSet(k, red); return red; }
  return null;
}

function setCache(k: string, d: any): void {
  _memSet(k, d);
  _redisSet(k, d).catch(() => { /* non-fatal */ });
}

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
];
function randomUA() { return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]; }

async function fetchGSMA(url: string, timeoutMs = 5000): Promise<string> {
  const { data } = await _gsmaAxios.get(url, {
    headers: {
      'User-Agent': randomUA(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Referer': 'https://www.gsmarena.com/',
    },
    timeout: timeoutMs,
    maxRedirects: 5,
  });
  return typeof data === 'string' ? data : JSON.stringify(data);
}

function norm(s: string) { return s.replace(/\s+/g, ' ').trim(); }

// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// SCORING — strict: ALL query words must appear in device name
// ─────────────────────────────────────────────────────────────────────────────
function normalizeGSMAQuery(q: string): string {
  return q.toLowerCase().trim()
    .replace(/\+/g, ' plus')   // s25+ -> s25 plus
    .replace(/\s+/g, ' ').trim();
}

function scoreMatch(deviceName: string, query: string): number {
  const d = normalizeGSMAQuery(deviceName);
  const q = normalizeGSMAQuery(query);
  const qWords = q.split(/\s+/).filter(w => w.length > 1);

  // ALL words must match — if any word missing, reject entirely
  if (!qWords.every(w => d.includes(w))) return -1;

  // Exact match
  if (d === q) return 10000;
  if (d.includes(q)) return 8000;

  // Penalise extra variant words not in query
  // e.g. query="iphone 15 pro" → "iphone 15 pro max" gets penalised
  const variants = ['ultra', 'pro', 'plus', 'mini', 'lite', 'fe', 'max', 'standard', 'turbo'];
  const lastQWord = qWords[qWords.length - 1];
  let penalty = 0;
  for (const v of variants) {
    if (v !== lastQWord && d.includes(' ' + v) && !q.includes(v)) penalty += 2000;
  }

  // Bonus for shorter name (more specific match)
  const lengthBonus = Math.max(0, 500 - deviceName.length * 5);

  return 5000 - penalty + lengthBonus;
}

// ─────────────────────────────────────────────────────────────────────────────
// SEARCH — HTML search page first (accurate), autocomplete as fallback
// Note: GSMArena autocomplete ranks by popularity not relevance — unreliable
// ─────────────────────────────────────────────────────────────────────────────
export async function searchGSMArena(query: string): Promise<{ name: string; url: string; id: string; score: number }[]> {
  const q = query.replace(/\+/g, ' plus').trim();
  const ck = `gsma:search:v3:${q.toLowerCase().trim()}`;
  query = q;
  const cached = await getCache(ck); if (cached) return cached;

  // Strategy A: HTML search — accurate, returns exact model matches
  try {
    const searchUrl = `https://www.gsmarena.com/res.php3?sSearch=${encodeURIComponent(query)}`;
    const html = await fetchGSMA(searchUrl, 5000);
    const $ = cheerio.load(html);
    const results: { name: string; url: string; id: string; score: number }[] = [];
    const seen = new Set<string>();

    $('a[href]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const match = href.match(/^([a-z0-9_]+-(\d+))\.php$/);
      if (!match || seen.has(href)) return;
      const name = norm($(el).find('strong span, span, strong').first().text() || $(el).text());
      if (!name || name.length < 2) return;
      seen.add(href);
      const score = scoreMatch(name, query);
      if (score < 0) return; // ALL words must match
      results.push({ name, url: `https://www.gsmarena.com/${href}`, id: match[1], score });
    });

    results.sort((a, b) => b.score - a.score);
    if (results.length > 0) {
      const top = results.slice(0, 10);
      setCache(ck, top);
      return top;
    }
  } catch { /* fall through */ }

  // Strategy B: autocomplete JSON — faster but ranks by popularity not relevance
  // Only used as last resort since it returns wrong phones (e.g. newer models)
  try {
    const resp = await axios.get('https://www.gsmarena.com/quicksearch-8f.php', {
      params: { sQuickSearch: query },
      headers: {
        'User-Agent': randomUA(),
        'Accept': 'application/json, text/javascript, */*',
        'Referer': 'https://www.gsmarena.com/',
        'X-Requested-With': 'XMLHttpRequest',
      },
      timeout: 3000,
    });
    const items: any[] = Array.isArray(resp.data) ? resp.data
      : Array.isArray(resp.data?.d) ? resp.data.d : [];

    const results = items
      .map((item: any) => {
        const name = item.name || item.DeviceName || item.title || '';
        const id   = item.id   || item.DeviceId   || item.key   || '';
        if (!name || !id) return null;
        const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
        const score = scoreMatch(name, query);
        if (score < 0) return null; // strict: all words must match
        return { name: norm(name), url: `https://www.gsmarena.com/${slug}-${id}.php`, id: String(id), score };
      })
      .filter((r): r is NonNullable<typeof r> => r !== null)
      .sort((a, b) => b.score - a.score)
      .slice(0, 10);

    if (results.length > 0) {
      setCache(ck, results);
      return results;
    }
  } catch { /* give up */ }

  return [];
}

// ─────────────────────────────────────────────────────────────────────────────
// SCRAPE DEVICE PAGE — get full specs from a GSMArena device page
// ─────────────────────────────────────────────────────────────────────────────
export async function scrapeGSMArenaDevice(url: string): Promise<any> {
  const ck = `gsma:device:v1:${url}`;
  const cached = await getCache(ck); if (cached) return cached;

  let html: string;
  try {
    html = await fetchGSMA(url, 4000);
  } catch (e: any) {
    throw new Error(`Failed to fetch GSMArena device page: ${e.message}`);
  }

  const $ = cheerio.load(html);

  const data: any = {
    title: norm($('h1.specs-phone-name-title').text() || $('h1').first().text()),
    sourceUrl: url,
    source: 'gsmarena',
    rating: '',
    verdict: '',
    pros: [] as string[],
    cons: [] as string[],
    images: [] as string[],
    specs: {} as Record<string, any>,
    // Convenience fields matching NotebookCheck output shape
    soc: '',
    gpu: '',
    os: '',
    ram: '',
    storage_capacity: '',
    releaseDate: '',
    dimensions: '',
    weight: '',
    price: '',
    bluetooth: '',
    wifi: '',
    nfc: '',
    usbVersion: '',
    ipRating: '',
    display: {} as Record<string, string>,
    battery: {} as Record<string, string>,
    cameras: {} as any,
    benchmarks: { gpu: [], cpu: [], memory: [], display: [], battery: [], storage: [], networking: [], thermal: [], other: [] },
  };

  // SPECS TABLE ──────────────────────────────────────────────────────────
  // GSMArena specs are in table#specs-list with tr rows: td.ttl + td.nfo
  $('#specs-list tr').each((_, row) => {
    const $row = $(row);
    const key = norm($row.find('td.ttl').text()).replace(/:$/, '');
    const val = norm($row.find('td.nfo').text());
    if (!key || !val || key === val) return;
    data.specs[key] = val;
  });

  // Section headers (th) give category context — rebuild as nested object
  const specsNested: Record<string, Record<string, string>> = {};
  let currentSection = 'General';
  $('#specs-list tr').each((_, row) => {
    const $row = $(row);
    const th = $row.find('th');
    if (th.length) { currentSection = norm(th.text()); return; }
    const key = norm($row.find('td.ttl').text()).replace(/:$/, '');
    const val = norm($row.find('td.nfo').text());
    if (!key || !val) return;
    if (!specsNested[currentSection]) specsNested[currentSection] = {};
    specsNested[currentSection][key] = val;
  });
  data.specsNested = specsNested;

  // ── CONVENIENCE FIELDS ───────────────────────────────────────────────────
  const launch = specsNested['Launch'] || {};
  const platform = specsNested['Platform'] || {};
  const memory = specsNested['Memory'] || {};
  const display = specsNested['Display'] || {};
  const mainCam = specsNested['Main Camera'] || specsNested['Triple cameras'] || specsNested['Dual cameras'] || specsNested['Single camera'] || {};
  const selfieCam = specsNested['Selfie camera'] || specsNested['Front Camera'] || {};
  const sound = specsNested['Sound'] || {};
  const comms = specsNested['Comms'] || {};
  const features = specsNested['Features'] || {};
  const battery = specsNested['Battery'] || {};
  const misc = specsNested['Misc'] || {};
  const bodySpec = specsNested['Body'] || {};
  const tests = specsNested['Tests'] || {};

  data.releaseDate = launch['Announced'] || launch['Status'] || '';
  data.soc = platform['Chipset'] || '';
  data.gpu = platform['GPU'] || '';
  data.os = platform['OS'] || '';

  // RAM: "8GB RAM, 256GB" → "8GB"
  const memStr = memory['Internal'] || memory['RAM'] || '';
  data.ram = memStr.match(/(\d+\s*GB)\s*RAM/i)?.[1] || memStr.match(/(\d+\s*GB)/i)?.[1] || '';
  data.storage_capacity = memStr.match(/,\s*(\d+\s*GB)/i)?.[1] || '';

  // Dimensions & weight
  data.dimensions = bodySpec['Dimensions'] || '';
  const wStr = bodySpec['Weight'] || '';
  data.weight = wStr.match(/(\d+(?:\.\d+)?)\s*g\b/i)?.[0] || '';

  // Display
  data.display = {
    type: display['Type'] || '',
    size: display['Size'] || '',
    resolution: display['Resolution'] || '',
    protection: display['Protection'] || '',
  };

  // Battery
  data.battery = {
    capacity: battery['Type'] || '',
    charging: battery['Charging'] || '',
  };

  // Connectivity
  data.wifi = comms['WLAN'] || '';
  data.bluetooth = comms['Bluetooth'] || '';
  data.nfc = /yes/i.test(comms['NFC'] || '') ? 'Yes' : (comms['NFC'] || '');
  data.usbVersion = comms['USB'] || '';

  // IP Rating
  const bodyFull = Object.values(bodySpec).join(' ');
  const ipM = bodyFull.match(/\bIP\d{2}[A-Z0-9]*/i);
  data.ipRating = ipM ? ipM[0].toUpperCase() : '';

  // Cameras
  const mainCamStr = Object.values(mainCam).join(' | ');
  const selfieStr = Object.values(selfieCam).join(' | ');
  data.cameras = {
    raw: mainCamStr,
    selfieRaw: selfieStr,
    videoCapabilities: mainCam['Video'] || '',
  };

  // Tests (performance scores if available)
  if (Object.keys(tests).length > 0) {
    for (const [k, v] of Object.entries(tests)) {
      if (v && /\d/.test(v)) {
        data.benchmarks.other.push({ name: k, value: v, unit: '' });
      }
    }
  }

  // ── IMAGES ───────────────────────────────────────────────────────────────
  $('img[src]').each((_, el) => {
    const src = $(el).attr('src') || '';
    if (src.includes('fdn') && (src.endsWith('.jpg') || src.endsWith('.png') || src.endsWith('.webp'))
        && !src.includes('logo') && !src.includes('icon') && !src.includes('banner')) {
      data.images.push(src);
    }
  });
  data.images = [...new Set(data.images)].slice(0, 20);

  // ── PROS/CONS (from review page if available) ─────────────────────────
  // GSMArena puts pros/cons on the review page, not specs page
  // We grab them from the specs page verdict section if present
  $('[class*="pros"] li, .pros li').each((_, el) => {
    const t = norm($(el).text());
    if (t && t.length > 2) data.pros.push(t);
  });
  $('[class*="cons"] li, .cons li').each((_, el) => {
    const t = norm($(el).text());
    if (t && t.length > 2) data.cons.push(t);
  });

  setCache(ck, data);
  return data;
}

// ─────────────────────────────────────────────────────────────────────────────
// SEARCH + SCRAPE — main entry point, total budget ~6s
// ─────────────────────────────────────────────────────────────────────────────
export async function getGSMArenaData(query: string): Promise<any> {
  const ck = `gsma:full:v2:${query.toLowerCase().trim()}`;
  const cached = await getCache(ck); if (cached) return cached;

  // Step 1: search (~0.5-2s with autocomplete)
  const results = await searchGSMArena(query);
  if (!results.length) return null;

  const best = results[0];

  // Step 2: scrape device page with tight timeout
  let details: any;
  try {
    details = await Promise.race([
      scrapeGSMArenaDevice(best.url),
      new Promise((_, reject) => setTimeout(() => reject(new Error('scrape timeout')), 6000)),
    ]);
  } catch {
    // Return search result only if scrape times out — still useful for caller
    return { title: best.name, sourceUrl: best.url, source: 'gsmarena', pageFound: { name: best.name, url: best.url }, reviewUrl: best.url, searchResults: results, specs: {} };
  }

  const result = {
    ...details,
    pageFound: { name: best.name, url: best.url },
    reviewUrl: best.url,
    searchResults: results,
  };
  setCache(ck, result);
  return result;
}