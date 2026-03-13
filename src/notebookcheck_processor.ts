import axios, { AxiosError } from 'axios';
import * as cheerio from 'cheerio';

// ── STRUCTURED LOGGER ────────────────────────────────────────────────────────
type LogLevel = 'debug' | 'info' | 'warn' | 'error';
function log(level: LogLevel, msg: string, meta?: Record<string, unknown>): void {
  if (process.env.NODE_ENV === 'test' && (level === 'debug' || level === 'info')) return;
  const entry = { ts: new Date().toISOString(), level, msg, ...meta };
  (level === 'error' || level === 'warn' ? console.error : console.log)(JSON.stringify(entry));
}

// ══════════════════════════════════════════════════════════════════════════════
//  TYPE DEFINITIONS
// ══════════════════════════════════════════════════════════════════════════════

export interface ProcessorCore {
  /** e.g. "Cortex-X4", "Cortex-A720", "Lion Cove", "Raptor" */
  name: string;
  /** Number of this core type in the cluster */
  count: number;
  /** Base clock in MHz */
  baseClockMHz?: number;
  /** Boost/max clock in MHz */
  boostClockMHz?: number;
  /** L1 cache size (per core or total cluster) */
  l1Cache?: string;
  /** L2 cache size (per core or total cluster) */
  l2Cache?: string;
  /** Whether this is a performance core */
  isPerformanceCore: boolean;
  /** Whether this is an efficiency core */
  isEfficiencyCore: boolean;
}

export interface ProcessorGPU {
  name: string;
  variant?: string;
  coreCount?: string;
  /** Max clock in MHz */
  maxClockMHz?: number;
  /** Graphics API support: OpenGL, Vulkan, Metal, DirectX */
  apis?: string[];
  shaderCount?: string;
  /** Peak compute in TFLOPS or GFLOPS */
  compute?: string;
}

export interface ProcessorNPU {
  name?: string;
  /** TOPS (Tera-Operations Per Second) */
  tops?: string;
  cores?: string;
}

export interface ProcessorMemorySpec {
  /** e.g. "LPDDR5X" */
  type?: string;
  /** Max speed in MHz or MT/s */
  speedMHz?: string;
  /** Max bandwidth in GB/s */
  bandwidthGBs?: string;
  /** Supported channels */
  channels?: string;
  /** Max capacity */
  maxCapacityGB?: string;
}

export interface ProcessorBenchmark {
  name: string;
  /** Average value (primary score) */
  value: string;
  /** Minimum value across tested devices */
  minValue?: string;
  /** Maximum value across tested devices */
  maxValue?: string;
  unit: string;
  /** Category: cpu, gpu, memory, ai, misc */
  category: string;
  /** Percentage rank vs other tested processors (0-100) */
  percentile?: string;
  /** Whether lower is better (e.g. latency benchmarks) */
  smallerIsBetter?: boolean;
  /** Per-device scores from the hidden benchmark detail table */
  deviceScores?: Array<{ model: string; score: string }>;
}

export interface ProcessorConnectivity {
  /** e.g. "Wi-Fi 7 (802.11be)" */
  wifi?: string;
  /** e.g. "Bluetooth 5.4" */
  bluetooth?: string;
  /** e.g. "5G NR, LTE, 4G" */
  cellular?: string;
  /** e.g. "Sub-6 GHz, mmWave" */
  cellularBands?: string;
  /** Max downlink speed in Mbps */
  maxDownlinkMbps?: string;
  /** Max uplink speed in Mbps */
  maxUplinkMbps?: string;
  /** e.g. "USB 3.2 Gen 2" */
  usb?: string;
  /** e.g. "DisplayPort 2.1, HDMI 2.1" */
  display?: string;
  /** GPS, GLONASS, BeiDou, Galileo, NavIC, QZSS */
  gnss?: string;
  /** Whether NFC is integrated */
  nfc?: boolean;
  /** Satellite connectivity */
  satellite?: string;
}

export interface ProcessorMediaCapabilities {
  /** Max video encode resolution/framerate e.g. "8K@30fps, 4K@120fps" */
  videoEncode?: string;
  /** Max video decode resolution/framerate */
  videoDecode?: string;
  /** Supported codecs: H.264, H.265, AV1, VP9 etc. */
  codecs?: string[];
  /** Max camera megapixels supported */
  maxCameraMpx?: string;
  /** Max display resolution supported */
  maxDisplayResolution?: string;
  /** Max display refresh rate */
  maxDisplayRefreshHz?: string;
  /** ISP name/generation */
  isp?: string;
  /** Number of ISP cores */
  ispCores?: string;
  /** HDR support */
  hdr?: string;
}

export interface ProcessorSecurityFeatures {
  /** e.g. "TrustZone", "Secure Enclave", "Titan M3" */
  tee?: string;
  /** Hardware key storage */
  secureEnclave?: string;
  /** Biometric auth support */
  biometricSupport?: string;
  /** Memory tagging extension (MTE) */
  mte?: boolean;
  /** Secure boot support */
  secureBoot?: boolean;
}

export interface ProcessorPowerSpec {
  /** Thermal Design Power in Watts */
  tdpWatts?: string;
  /** Typical / average power in Watts */
  typicalPowerWatts?: string;
  /** Process node e.g. "3nm TSMC N3E", "4nm Samsung" */
  processNode?: string;
  /** Architecture generation e.g. "ARMv9-A", "x86-64" */
  architecture?: string;
  /** Instruction Set Architecture */
  isa?: string;
}

export interface DeviceSupport {
  name: string;
  url: string;
}

export interface NBCProcessorData {
  /** Full processor name e.g. "Qualcomm Snapdragon 8 Elite" */
  name: string;
  /** Subtitle / variant description */
  subtitle: string;
  /** Full NBC page URL */
  sourceUrl: string;
  /** Canonical URL used */
  reviewUrl: string;
  /** Search result that led here */
  pageFound: { name: string; url: string };

  // ── IDENTITY ──────────────────────────────────────────────────────────────
  /** e.g. "Qualcomm", "Apple", "MediaTek", "Samsung", "Google" */
  manufacturer: string;
  /** e.g. "Mobile SoC", "Desktop CPU", "Laptop CPU" */
  category: string;
  /** e.g. "ARM", "x86", "RISC-V" */
  architecture: string;
  /** e.g. "ARMv9-A", "ARMv8-A", "x86-64" */
  isa: string;
  /** Process node e.g. "3nm TSMC N3E" */
  processNode: string;
  /** Die size in mm² */
  dieSizeMm2: string;
  /** Transistor count e.g. "16 billion" */
  transistorCount: string;
  /** Announced / released date */
  announcedDate: string;
  /** Codename */
  codename: string;

  // ── CPU ──────────────────────────────────────────────────────────────────
  /** Total CPU core count */
  totalCores: number;
  /** Total CPU thread count */
  totalThreads: number;
  /** Heterogeneous core clusters (big.LITTLE / DynamIQ / Hybrid) */
  cpuClusters: ProcessorCore[];
  /** Overall base clock (lowest/efficiency floor) */
  baseClockMHz: number;
  /** Max boost clock (highest/prime core) */
  boostClockMHz: number;
  /** L2 cache (total or largest cluster) */
  l2CacheTotal: string;
  /** L3 cache */
  l3Cache: string;
  /** L4 / last-level cache */
  l4Cache: string;
  /** System-level cache (SLC) */
  systemLevelCache: string;

  // ── GPU ──────────────────────────────────────────────────────────────────
  gpu: ProcessorGPU;

  // ── NPU / AI ENGINE ───────────────────────────────────────────────────────
  npu: ProcessorNPU;

  // ── MEMORY ──────────────────────────────────────────────────────────────
  memory: ProcessorMemorySpec;

  // ── CONNECTIVITY ─────────────────────────────────────────────────────────
  connectivity: ProcessorConnectivity;

  // ── MEDIA & DISPLAY ──────────────────────────────────────────────────────
  media: ProcessorMediaCapabilities;

  // ── SECURITY ─────────────────────────────────────────────────────────────
  security: ProcessorSecurityFeatures;

  // ── POWER ────────────────────────────────────────────────────────────────
  power: ProcessorPowerSpec;

  // ── SERIES ───────────────────────────────────────────────────────────────
  /** Processor family/series name e.g. "Qualcomm Snapdragon 8" */
  series: string;
  /** Sibling processors in the same family (from NBC series table) */
  seriesProcessors: Array<{ name: string; url?: string; isCurrent: boolean }>;

  // ── RAW SPEC TABLE ───────────────────────────────────────────────────────
  /** Raw key-value pairs from the NBC spec table — nothing omitted */
  specs: Record<string, string>;

  // ── BENCHMARKS ───────────────────────────────────────────────────────────
  benchmarks: ProcessorBenchmark[];

  // ── DEVICE COMPATIBILITY ─────────────────────────────────────────────────
  /** Devices that use this processor (from NBC "devices tested" section) */
  devicesUsing: DeviceSupport[];

  // ── DESCRIPTION ─────────────────────────────────────────────────────────
  /** NBC editorial description / verdict */
  description: string;
  /** NBC editorial verdict on performance tier */
  performanceTier: string;

  // ── IMAGES ──────────────────────────────────────────────────────────────
  images: string[];
}

export interface NBCProcessorError {
  error: string;
  query: string;
  code?: number;
}

export interface ProcessorSearchResult {
  url: string;
  title: string;
  score: number;
}

// ══════════════════════════════════════════════════════════════════════════════
//  CACHE VERSION — auto-derived from schema field names using DJB2
// ══════════════════════════════════════════════════════════════════════════════
const PROC_CACHE_VERSION = (() => {
  const SCHEMA_FIELDS = [
    'name','subtitle','sourceUrl','reviewUrl','pageFound','manufacturer','category',
    'series','seriesProcessors',
    'architecture','isa','processNode','dieSizeMm2','transistorCount','announcedDate',
    'codename','totalCores','totalThreads','cpuClusters','baseClockMHz','boostClockMHz',
    'l2CacheTotal','l3Cache','l4Cache','systemLevelCache','gpu','npu','memory',
    'connectivity','media','security','power','specs','benchmarks','devicesUsing',
    'description','performanceTier','images',
  ].sort().join(',');
  let h = 5381;
  for (let i = 0; i < SCHEMA_FIELDS.length; i++) {
    h = ((h << 5) + h + SCHEMA_FIELDS.charCodeAt(i)) >>> 0;
  }
  return `proc${h.toString(36)}`;
})();

const PROC_CACHE_TTL     = 72 * 60 * 60 * 1000; // 72h — processor specs rarely change
const PROC_CACHE_TTL_SEC = 72 * 60 * 60;

// ── IN-MEMORY CACHE ──────────────────────────────────────────────────────────
const PROC_MEM_CACHE_MAX = 200;
const procMemCache = new Map<string, { data: unknown; time: number }>();

function procMemEvict(): void {
  if (procMemCache.size < PROC_MEM_CACHE_MAX) return;
  const keys = [...procMemCache.keys()];
  const evictCount = Math.floor(keys.length / 2);
  for (let i = 0; i < evictCount; i++) procMemCache.delete(keys[i]);
}

function procMemGet(k: string): unknown | null {
  const h = procMemCache.get(k);
  if (!h) return null;
  if (Date.now() - h.time >= PROC_CACHE_TTL) { procMemCache.delete(k); return null; }
  procMemCache.delete(k);
  procMemCache.set(k, h);
  return h.data;
}

function procMemSet(k: string, d: unknown): void {
  procMemEvict();
  procMemCache.set(k, { data: d, time: Date.now() });
}

// ── REDIS HELPERS ────────────────────────────────────────────────────────────
import * as http from 'http';
import * as https from 'https';
const _procHttpAgent  = new (require('http').Agent)({ keepAlive: true, maxSockets: 50, maxFreeSockets: 10 });
const _procHttpsAgent = new (require('https').Agent)({ keepAlive: true, maxSockets: 50, maxFreeSockets: 10 });
const procAxios = axios.create({ httpAgent: _procHttpAgent, httpsAgent: _procHttpsAgent, maxRedirects: 3, decompress: true });

async function procRedisGet(k: string): Promise<unknown | null> {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  try {
    const resp = await procAxios.get(`${url}/get/${encodeURIComponent(k)}`, {
      headers: { Authorization: `Bearer ${token}` }, timeout: 800,
    });
    const val = resp.data?.result;
    return val ? JSON.parse(val) : null;
  } catch (e) { log('warn', 'proc.redis.get failed', { key: k, err: (e as Error).message }); return null; }
}

async function procRedisSet(k: string, d: unknown): Promise<void> {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return;
  try {
    await procAxios.post(
      `${url}/pipeline`,
      [['SET', k, JSON.stringify(d), 'EX', PROC_CACHE_TTL_SEC]],
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, timeout: 800 },
    );
  } catch (e) { log('warn', 'proc.redis.set failed', { key: k, err: (e as Error).message }); }
}

async function procGetCache(k: string): Promise<unknown | null> {
  const mem = procMemGet(k);
  if (mem !== null) return mem;
  const red = await procRedisGet(k);
  if (red !== null) { procMemSet(k, red); return red; }
  return null;
}

async function procGetCacheAs<T>(k: string): Promise<T | null> {
  const v = await procGetCache(k);
  return v !== null ? (v as T) : null;
}

function procSetCache(k: string, d: unknown): void {
  procMemSet(k, d);
  procRedisSet(k, d).catch((e) => log('warn', 'proc.redis.set async failed', { key: k, err: (e as Error).message }));
}

// ── CIRCUIT BREAKER ──────────────────────────────────────────────────────────
const PROC_CIRCUIT_FAIL_THRESHOLD = 3;
const PROC_CIRCUIT_COOLDOWN_MS    = 5 * 60 * 1000;
interface CircuitState { fails: number; cooldownUntil: number; }
const procCircuitBreakers = new Map<string, CircuitState>();

function procCircuitIsOpen(host: string): boolean {
  const s = procCircuitBreakers.get(host);
  if (!s) return false;
  if (s.cooldownUntil > Date.now()) return true;
  procCircuitBreakers.delete(host);
  return false;
}
function procCircuitRecordFailure(host: string): void {
  const s = procCircuitBreakers.get(host) ?? { fails: 0, cooldownUntil: 0 };
  s.fails++;
  if (s.fails >= PROC_CIRCUIT_FAIL_THRESHOLD) {
    s.cooldownUntil = Date.now() + PROC_CIRCUIT_COOLDOWN_MS;
    log('warn', 'proc.circuit.open', { host, until: new Date(s.cooldownUntil).toISOString() });
  }
  procCircuitBreakers.set(host, s);
}
function procCircuitRecordSuccess(host: string): void { procCircuitBreakers.delete(host); }

// ── UTILITY ──────────────────────────────────────────────────────────────────
const PROC_USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];
function procRandomUA() { return PROC_USER_AGENTS[Math.floor(Math.random() * PROC_USER_AGENTS.length)]; }

function normStr(s: string): string {
  return s.replace(/\u00a0/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
}

async function procFetchUrl(url: string, timeoutMs = 6000, signal?: AbortSignal): Promise<string> {
  for (let i = 0; i <= 1; i++) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    const ctrl = new AbortController();
    const onAbort = () => ctrl.abort();
    signal?.addEventListener('abort', onAbort, { once: true });
    try {
      const { data } = await procAxios.get(url, {
        headers: {
          'User-Agent': procRandomUA(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'gzip, deflate, br',
          'Referer': 'https://www.notebookcheck.net/',
        },
        timeout: timeoutMs,
        maxRedirects: 3,
        decompress: true,
        signal: ctrl.signal,
      });
      return typeof data === 'string' ? data : JSON.stringify(data);
    } catch (e: unknown) {
      const isLast  = i === 1;
      const status  = (e as AxiosError)?.response?.status;
      if ((e as Error).name === 'AbortError') throw e;
      if (status && status >= 400 && status < 500) throw e;
      if (isLast) throw e;
      await new Promise(res => setTimeout(res, 400));
    } finally {
      ctrl.abort();
      signal?.removeEventListener('abort', onAbort);
    }
  }
  throw new Error('procFetchUrl: exhausted retries');
}

function cleanCell($: cheerio.CheerioAPI, el: import('domhandler').Element): string {
  const outer = $.html(el) || '';
  const stripped = outer
    .replace(/<(style|script|noscript)[^>]*>[\s\S]*?<\/\1>/gi, '')
    .replace(/<[^>]+>/g, ' ');
  return normStr(stripped);
}

// ══════════════════════════════════════════════════════════════════════════════
//  PROCESSOR SEARCH — SearXNG (reuses the same instance as device search)
// ══════════════════════════════════════════════════════════════════════════════

/** Processor-specific brand/alias normalisation */
const PROC_ALIASES: Array<[string, string]> = [
  ['snapdragon 8s gen', 'Qualcomm Snapdragon 8s Gen'],
  ['snapdragon 8 gen', 'Qualcomm Snapdragon 8 Gen'],
  ['snapdragon 7s gen', 'Qualcomm Snapdragon 7s Gen'],
  ['snapdragon 7 gen', 'Qualcomm Snapdragon 7 Gen'],
  ['snapdragon 6 gen', 'Qualcomm Snapdragon 6 Gen'],
  ['snapdragon 4 gen', 'Qualcomm Snapdragon 4 Gen'],
  ['snapdragon 8 elite', 'Qualcomm Snapdragon 8 Elite'],
  ['snapdragon 7s elite', 'Qualcomm Snapdragon 7s Elite'],
  ['snapdragon',         'Qualcomm Snapdragon'],
  ['dimensity 9',       'MediaTek Dimensity 9'],
  ['dimensity 8',       'MediaTek Dimensity 8'],
  ['dimensity 7',       'MediaTek Dimensity 7'],
  ['dimensity 6',       'MediaTek Dimensity 6'],
  ['dimensity',         'MediaTek Dimensity'],
  ['helio g',           'MediaTek Helio G'],
  ['helio p',           'MediaTek Helio P'],
  ['helio',             'MediaTek Helio'],
  ['exynos 2',         'Samsung Exynos 2'],
  ['exynos 1',         'Samsung Exynos 1'],
  ['exynos',           'Samsung Exynos'],
  ['kirin 9',          'Huawei Kirin 9'],
  ['kirin',            'Huawei Kirin'],
  ['apple m',          'Apple M'],
  ['apple a',          'Apple A'],
  ['google tensor',    'Google Tensor'],
  ['tensor g',         'Google Tensor G'],
  ['unisoc t',         'Unisoc T'],
  ['unisoc s',         'Unisoc S'],
  ['unisoc',           'Unisoc'],
];

export function normalizeProcQuery(query: string): string {
  let q = query.toLowerCase().trim();
  for (const [alias, replacement] of PROC_ALIASES) {
    const rep = replacement.toLowerCase();
    if (q === alias || q.startsWith(alias + ' ') || q.startsWith(alias + '\t')) {
      q = rep + q.slice(alias.length);
      break;
    }
    const esc = alias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    q = q.replace(new RegExp('\\b' + esc + '\\b', 'gi'), rep);
  }
  return q.trim();
}

/**
 * Score a NBC search result for processor relevance.
 * NBC processor pages follow the URL pattern:
 *   /SoC-Name-Processor-Benchmarks-and-Specs.NNNNN.0.html
 */
function scoreProcCandidate(title: string, url: string, nq: string, oq: string): number {
  const q = nq.toLowerCase();
  const t = title.toLowerCase();
  const u = url.toLowerCase();

  if (!u.includes('notebookcheck.net')) return -1;
  if (/[?&](tag|q|word)=/.test(u)) return -1;
  if (/\/(topics|search|rss-feed|index)\.\d/i.test(u)) return -1;

  // Processor/SoC pages contain "Benchmarks-and-Specs" or "Processor" in the URL
  const isProcUrl = /processor-benchmarks-and-specs|soc.*benchmarks|cpu.*benchmarks/i.test(u)
    || /-Processor-|Benchmarks-and-Specs/i.test(u);

  // Reject if the URL is clearly a phone review page
  if (/smartphone-review|phone-review|tablet-review/i.test(u)) return -1;

  const qWords = q.split(/\s+/).filter(w => w.length > 0);
  const urlSlug = (u.split('/').pop() || '').replace(/\.\d+\.\d+\.html$/, '').replace(/-/g, ' ').toLowerCase();
  const combined = t + ' ' + urlSlug;

  // All query words must appear somewhere in title or URL slug
  if (!qWords.every(w => combined.includes(w))) return -1;

  let score = 400;

  if (isProcUrl)                                           score += 1500;
  if (combined.includes(q))                               score += 2000;
  if (u.includes(q.replace(/\s+/g, '-')))                 score += 1500;
  if (/benchmarks.and.specs/i.test(u))                    score += 1000;
  if (/processor/i.test(u))                               score += 500;
  if (t.includes('benchmarks') || t.includes('specs'))    score += 400;

  // Penalise comparisons and news
  if (t.includes(' vs ') || t.includes(' vs. '))            score -= 1200;
  if (t.includes('comparison') || t.includes('compared'))   score -= 1000;
  if (t.includes('leak') || t.includes('rumor'))            score -= 800;
  if (t.includes('announced') || t.includes('unveiled'))    score -= 600;
  if (t.includes('external reviews') || u.includes('external-reviews')) score -= 2500;

  return score;
}

/** SearXNG search targeting NotebookCheck processor/SoC spec pages */
export async function searchProcViaSearXNG(nq: string, oq: string, signal?: AbortSignal): Promise<ProcessorSearchResult[]> {
  const seen = new Set<string>();
  const all: ProcessorSearchResult[] = [];
  const base = 'https://searxng-notebookcheck.onrender.com';

  if (procCircuitIsOpen(base)) return [];

  interface ExternalItem { url?: string; href?: string; title?: string; }

  // ── SINGLE QUERY STRATEGY ──────────────────────────────────────────────────
  // Previously fired 2 parallel queries (oq + nq) against duckduckgo+bing.
  // Problems:
  //   1. Bing blocks Render IPs → times out at 4.5s → causes 10s+ total latency
  //   2. DuckDuckGo rate-limits shared IPs → random null results
  //   3. 2 parallel queries × 2 engines = 4 concurrent upstream requests from
  //      a single Render free-tier container → CPU contention, slow responses
  //
  // Fix: single query, google engine only (set in settings.yml), 3s timeout.
  // Google on Render responds in <2s consistently. One fast hit beats four
  // slow unreliable ones.
  //
  // We pick the best of nq (normalized) vs oq (original): use nq if it differs
  // from oq (it has expanded brand prefixes like "Qualcomm Snapdragon"), otherwise
  // just oq. One network call total.
  const searchQuery = nq !== oq ? nq : oq;

  const doSearch = async (q: string): Promise<ExternalItem[]> => {
    const resp = await procAxios.get(`${base}/search`, {
      params: {
        q: `site:notebookcheck.net ${q} processor benchmarks specs`,
        format: 'json',
        // No engines override — let settings.yml decide (Google only).
        // Hardcoding engines here bypassed the settings.yml config and forced
        // duckduckgo+bing even after we fixed settings.yml.
        categories: 'general',
      },
      headers: { 'User-Agent': procRandomUA(), 'Accept': 'application/json' },
      timeout: 3000,   // 3s — Google responds in <2s; if it hasn't by 3s it won't
      signal,
    });
    return (resp.data?.results || []) as ExternalItem[];
  };

  // ── FALLBACK QUERY ─────────────────────────────────────────────────────────
  // If the primary query returns no usable results, try the original query.
  // This handles cases where brand normalization makes the query worse
  // (e.g. a misspelled brand that NBC actually indexes as-is).
  const tryQuery = async (q: string): Promise<ExternalItem[]> => {
    try {
      return await doSearch(q);
    } catch (e) {
      if ((e as Error).name === 'AbortError') throw e;
      return [];
    }
  };

  try {
    let items = await tryQuery(searchQuery);

    // If primary returned nothing AND nq !== oq, try oq as fallback
    if (items.length === 0 && nq !== oq) {
      log('debug', 'proc.searxng.fallback', { primary: searchQuery, fallback: oq });
      items = await tryQuery(oq);
    }

    procCircuitRecordSuccess(base);

    for (const item of items) {
      const url   = (item.url || '').trim();
      const title = (item.title || '').trim();
      if (!url.includes('notebookcheck.net') || seen.has(url)) continue;

      // URL filter — accept NBC pages that look like processor spec pages.
      // Previously this was too strict: required BOTH a 4-digit ID AND
      // "benchmarks-and-specs" in URL. NBC SoC pages sometimes lack one.
      // Now: accept if EITHER condition matches, or if the URL slug contains
      // "processor" + "benchmark" (covers newer NBC URL formats).
      const urlLower = url.toLowerCase();
      const isNbcSpecPage =
        /\.\d{4,}\.\d+\.html/.test(url) ||
        /benchmarks-and-specs/i.test(url) ||
        (/processor/i.test(url) && /benchmark/i.test(url)) ||
        /soc.*spec|spec.*soc/i.test(urlLower);
      if (!isNbcSpecPage) continue;

      const sc = scoreProcCandidate(title || url, url, nq, oq);
      if (sc < 0) continue;
      seen.add(url);
      all.push({ url, title: title || url, score: sc });
    }
  } catch (e) {
    if ((e as Error).name !== 'AbortError') {
      procCircuitRecordFailure(base);
      log('debug', 'proc.searxng.failed', { err: (e as Error).message });
    }
  }
  return all;
}

/** Pick best candidate from multiple search result arrays */
function procPickBest(results: ProcessorSearchResult[]): ProcessorSearchResult | null {
  if (!results.length) return null;
  return [...results].sort((a, b) => b.score - a.score)[0];
}

async function searchProcessor(query: string): Promise<{ name: string; url: string } | null> {
  const ck  = `proc:search:${PROC_CACHE_VERSION}:${query.toLowerCase().trim()}`;
  const oq  = query.trim();
  const nq  = normalizeProcQuery(query);

  // ── CHECK CACHE FIRST ──────────────────────────────────────────────────────
  // Previously used Promise.all([cache, searxng]) — this always fired SearXNG
  // even on cache hits, wasting a full network round-trip every time.
  // Now: cache check first (mem = sync, redis = ~5ms), SearXNG only on miss.
  const cached = await procGetCacheAs<{ name: string; url: string }>(ck);
  if (cached) return cached;

  const results = await searchProcViaSearXNG(nq, oq);
  if (!results.length) return null;

  const best = procPickBest(results);
  if (!best) return null;

  const r = { name: best.title || best.url, url: best.url };
  procSetCache(ck, r);
  return r;
}

// ══════════════════════════════════════════════════════════════════════════════
//  PROCESSOR PAGE SCRAPER
//
//  NotebookCheck processor pages (e.g. /Qualcomm-Snapdragon-8-Elite-Processor-
//  Benchmarks-and-Specs.NNNNN.0.html) have a consistent HTML structure:
//
//  1. #content > .specs_table  — main spec table with labelled rows
//  2. .r_compare_bars tables   — benchmark comparison bars (same as device pages)
//  3. .nbc_related_devices     — linked devices that use this processor
//  4. article description text in .text_column paragraphs
//  5. Images in <figure> elements
//
//  Spec table rows follow the pattern:
//    <tr><td class="nc-specs__label">Label</td>
//        <td class="nc-specs__value">Value</td></tr>
// ══════════════════════════════════════════════════════════════════════════════

export async function scrapeNotebookCheckProcessor(
  pageUrl: string,
  procName?: string,
  signal?: AbortSignal,
): Promise<NBCProcessorData> {
  const ck = `proc:device:${PROC_CACHE_VERSION}:${pageUrl}`;
  const cached = await procGetCacheAs<NBCProcessorData>(ck);
  if (cached) return cached;

  const html = await procFetchUrl(pageUrl, 6000, signal);
  const $ = cheerio.load(html);
  $('script, style, noscript').remove();

  // Lazy body text (expensive on large pages — compute once on demand)
  let _bodyText: string | null = null;
  function getBodyText(): string {
    if (_bodyText === null) _bodyText = normStr($('body').text());
    return _bodyText;
  }
  const bodyText = {
    match:    (rx: RegExp) => getBodyText().match(rx),
    includes: (s: string)  => getBodyText().includes(s),
    slice:    (a: number, b?: number) => getBodyText().slice(a, b),
  };

  // ── EXTRACT RAW SPEC TABLE ──────────────────────────────────────────────────
  // NBC processor pages use table.contenttable with alternating gpu-even/gpu-odd rows.
  // Each spec row: <tr class="gpu-even|gpu-odd"><td class="caption">LABEL</td><td>VALUE</td></tr>
  // The "Series" row is special: a colspan=2 td containing a nested table of sibling processors.
  const rawSpecs: Record<string, string> = {};

  // Primary extraction: NBC processor spec table (table.contenttable)
  $('table.contenttable tr.gpu-even, table.contenttable tr.gpu-odd').each((_, row) => {
    const $row = $(row);
    // Skip the series row (it has colspan=2 and contains a sub-table)
    if ($row.find('td[colspan]').length) return;

    const caption = normStr($row.find('td.caption').text()).replace(/:$/, '').trim();
    // Value is the td that is NOT .caption
    const $valCell = $row.find('td:not(.caption)').first();
    const value = cleanCell($, $valCell[0] as import('domhandler').Element).trim();

    if (caption && value && caption.length < 100 && value.length < 600 && !value.includes('{')) {
      rawSpecs[caption] = value;
    }
  });

  // Fallback Layout A — modern class-based spec rows (non-NBC sites)
  if (Object.keys(rawSpecs).length === 0) {
    $('.nc-specs__row, .nbc-specs__row, [class*="specs__row"], [class*="spec-row"]').each((_, row) => {
      const label = normStr($(row).find('[class*="label"], [class*="key"], dt').first().text());
      const value = normStr($(row).find('[class*="value"], [class*="data"], dd').first().text());
      if (label && value && label.length < 80 && value.length < 600 && !value.includes('{')) {
        rawSpecs[label] = value;
      }
    });
  }

  // Fallback Layout B — classic table rows
  if (Object.keys(rawSpecs).length === 0) {
    $('table tr').each((_, row) => {
      const cells = $(row).find('td, th');
      if (cells.length < 2) return;
      const label = cleanCell($, cells.eq(0)[0]).replace(/:$/, '').trim();
      const value = cleanCell($, cells.eq(1)[0]).trim();
      if (label && value && label.length < 80 && value.length < 600
          && !rawSpecs[label] && !value.includes('{') && !value.startsWith(':')) {
        rawSpecs[label] = value;
      }
    });
  }

  // Fallback Layout C — dl/dt/dd lists
  $('dl').each((_, dl) => {
    $(dl).find('dt').each((_, dt) => {
      const label = normStr($(dt).text()).replace(/:$/, '').trim();
      const value = normStr($(dt).next('dd').text()).trim();
      if (label && value && !rawSpecs[label] && label.length < 80 && value.length < 600) {
        rawSpecs[label] = value;
      }
    });
  });

  // Generic span/div labelled pairs — only as last resort when nothing else found
  if (Object.keys(rawSpecs).length === 0) {
    $('td, th, dt, span[class*="label"], div[class*="label"]').each((_, el) => {
      const label = normStr($(el).text()).replace(/:$/, '').trim();
      if (!label || label.length > 80 || label.includes('\n')) return;
      const $sib = $(el).next('td, dd, span[class*="value"], div[class*="value"]');
      if ($sib.length) {
        const val = cleanCell($, $sib[0] as import('domhandler').Element);
        if (val && val.length > 1 && val.length < 600 && !rawSpecs[label] && !val.includes('{')) {
          rawSpecs[label] = val;
        }
      }
    });
  }

  // Helper: find spec value by regex-matching its label
  function findSpec(labelRx: RegExp): string {
    for (const [k, v] of Object.entries(rawSpecs)) {
      if (labelRx.test(k)) return v;
    }
    return '';
  }

  // Helper: find spec value from body text with fallback regex
  function findInBody(rx: RegExp, group = 1): string {
    const m = html.match(rx) || bodyText.match(rx);
    return m ? (m[group] || '').trim() : '';
  }

  // NBC often embeds connectivity/memory/ISP/NPU info in a single "Features" field.
  // Hoist this early so all sections can reference it.
  const featuresRaw = findSpec(/^Features?/i) || '';

  // ── TITLE / IDENTITY ──────────────────────────────────────────────────────
  const rawTitle = normStr($('h1').first().text()) || normStr($('title').text().split('|')[0]);
  const rawSubtitle = normStr($('h2').first().text());

  // ── MANUFACTURER ─────────────────────────────────────────────────────────
  function detectManufacturer(name: string): string {
    const n = name.toLowerCase();
    if (/qualcomm|snapdragon/.test(n)) return 'Qualcomm';
    if (/mediatek|dimensity|helio/.test(n)) return 'MediaTek';
    if (/samsung|exynos/.test(n)) return 'Samsung';
    if (/apple/.test(n)) return 'Apple';
    if (/google|tensor/.test(n)) return 'Google';
    if (/huawei|kirin/.test(n)) return 'Huawei';
    if (/unisoc/.test(n)) return 'Unisoc';
    if (/intel/.test(n)) return 'Intel';
    if (/amd/.test(n)) return 'AMD';
    if (/nvidia/.test(n)) return 'NVIDIA';
    if (/arm/.test(n)) return 'ARM';
    return findSpec(/manufacturer|brand|company/i)
      || findInBody(/manufacturer[:\s]+([A-Z][a-zA-Z\s]+)/i);
  }

  const manufacturer = detectManufacturer(rawTitle || procName || '');

  // ── SERIES / SIBLINGS ─────────────────────────────────────────────────────
  // NBC renders the "Series" row as a colspan=2 cell containing a sub-table
  // with sibling processor links. The current processor is shown in bold (no link).
  let series = findSpec(/^series|^family/i) || '';
  const seriesProcessors: Array<{ name: string; url?: string; isCurrent: boolean }> = [];

  $('table.contenttable tr.gpu-even td[colspan], table.contenttable tr.gpu-odd td[colspan]').each((_, td) => {
    const $td = $(td);
    $td.find('table tr').each((_, row) => {
      const $row = $(row);
      const $link = $row.find('td:first-child a');
      const $bold = $row.find('td:first-child b');

      if ($link.length) {
        // Linked sibling — not current
        const name = normStr($link.text()).replace(/[«»◄►▶]/g, '').trim();
        const href = $link.attr('href') || '';
        const url = href.startsWith('http') ? href
          : href.startsWith('/') ? 'https://www.notebookcheck.net' + href : undefined;
        if (name) seriesProcessors.push({ name, url, isCurrent: false });
      } else if ($bold.length) {
        // Bold = current processor
        const name = normStr($bold.text()).replace(/[«»◄►▶]/g, '').trim();
        if (name) seriesProcessors.push({ name, isCurrent: true });
      } else {
        // Check if this row has the « marker (NBC marks current with «)
        const cellText = normStr($row.find('td:first-child').text());
        const isCurrent = cellText.includes('«') || cellText.includes('◄');
        const name = cellText.replace(/[«»◄►▶]/g, '').trim();
        if (name && name.length > 2) seriesProcessors.push({ name, isCurrent });
      }
    });
  });

  // Extract series name from the caption label of the series row
  if (!series) {
    $('table.contenttable tr').each((_, row) => {
      const $row = $(row);
      if ($row.find('td[colspan]').length) {
        // The previous row or the caption td typically labels this as "Series"
        const prevCaption = $row.prev('tr').find('td.caption').text().trim();
        if (/series|family/i.test(prevCaption)) {
          // Extract series name from first sibling processor that matches the current proc
          const currentEntry = seriesProcessors.find(s => s.isCurrent);
          if (currentEntry) {
            // Series name = current processor name stripped of specific gen number
            const m = currentEntry.name.match(/^(.+?)\s+(?:Gen\s*\d+|\d+\s*Gen|\d{4})/i);
            series = m ? m[1].trim() : '';
          }
        }
      }
    });
  }

  // Fallback series name from spec table
  if (!series) {
    series = findSpec(/^series|^family/i) || '';
  }

  // ── CATEGORY (SoC, Mobile CPU, Desktop CPU, etc.) ───────────────────────
  const rawCategory = findSpec(/^category|^type|^class/i)
    || findSpec(/processor type/i);
  let category = rawCategory;
  if (!category) {
    const n = (rawTitle + ' ' + rawSubtitle).toLowerCase();
    if (/mobile|smartphone|tablet/.test(n)) category = 'Mobile SoC';
    else if (/desktop/.test(n)) category = 'Desktop CPU';
    else if (/laptop|notebook/.test(n)) category = 'Laptop CPU';
    else if (/server/.test(n)) category = 'Server CPU';
    else category = 'Processor';
  }

  // ── PROCESS NODE ─────────────────────────────────────────────────────────
  const processNode =
    findSpec(/^process|^lithography|^manufacturing|^fab(?:rication)?|^node/i)
    || findInBody(/(?:process|lithography|manufactured(?:\s+on)?|built\s+on|fabricated\s+on)[:\s]+([^\n,;.]{3,60}nm[^\n,;.]{0,40})/i);

  // ── ARCHITECTURE & ISA ───────────────────────────────────────────────────
  const architectureRaw = findSpec(/^architecture|^instruction\s*set|^ISA/i)
    || findSpec(/CPU Architecture/i);
  const isaRaw = findSpec(/^ISA|instruction\s*set/i) || architectureRaw;

  let architecture = architectureRaw;
  let isa = isaRaw;

  // Detect from title/body if not in spec table
  if (!architecture) {
    const n = (rawTitle + ' ' + getBodyText().slice(0, 3000)).toLowerCase();
    if (/armv9/i.test(n))      { architecture = 'ARMv9'; isa = 'ARMv9-A'; }
    else if (/armv8/i.test(n)) { architecture = 'ARMv8'; isa = 'ARMv8-A'; }
    else if (/x86-64|amd64/i.test(n)) { architecture = 'x86'; isa = 'x86-64'; }
    else if (/risc-v/i.test(n)){ architecture = 'RISC-V'; isa = 'RISC-V'; }
    else if (/arm/i.test(n))   { architecture = 'ARM'; isa = 'ARM'; }
  }

  // ── DIE SIZE & TRANSISTORS ───────────────────────────────────────────────
  // NBC uses "Die Size" as the spec label; value may contain "mm 2" (space before 2)
  const dieSizeRaw = findSpec(/^die\s*size|^die\s*area/i)
    || findInBody(/die\s*(?:size|area)[:\s]+([0-9.]+)\s*mm[²2\s]/i)
    || findInBody(/([0-9.]+)\s*mm[²2]\s*die/i);
  // Normalize "126.2 mm 2" → "126.2 mm²"
  const dieSizeMm2 = dieSizeRaw
    ? dieSizeRaw.replace(/mm\s*2\b/g, 'mm²').replace(/mm²\s*2/g, 'mm²').trim()
    : '';

  const transistorCount =
    findSpec(/^transistors?|transistor\s*count/i)
    || findInBody(/([0-9.]+\s*(?:billion|million))\s*transistors?/i)
    || findInBody(/transistors?[:\s]+([0-9.,]+\s*(?:billion|million|B|M))/i);

  // ── ANNOUNCED DATE ────────────────────────────────────────────────────────
  const announcedDate =
    findSpec(/^announcement\s*date|^announced|^released?|^launch(?:ed)?|^introduced/i)
    || findInBody(/(?:announced|introduced|launched|released)[:\s]+([A-Z][a-z]+\s+\d{4}|\d{4})/i)
    || normStr($('time[datetime]').first().attr('datetime') || $('time[datetime]').first().text() || '');

  // ── CODENAME ─────────────────────────────────────────────────────────────
  const codename =
    findSpec(/^codename|code\s*name/i)
    // NBC spec table sometimes has codename under CPU name field
    || findSpec(/^CPU\s*(?:Name|Model)/i)
    // Qualcomm model numbers appear in description: "SM8850-AC", "SM8650", etc.
    || findInBody(/\b(SM\d{4,}(?:-[A-Z0-9]+)?)\b/i)
    // Generic codename patterns
    || findInBody(/codename[:\s"']+([A-Za-z][A-Za-z0-9\s\-]+)/i);

  // ══════════════════════════════════════════════════════════════════════════
  //  CPU CORES — parse heterogeneous cluster configurations
  //
  //  NBC formats CPU cluster info as:
  //    "1x Cortex-X4 @ 4.32 GHz + 5x Cortex-A720 @ 3.53 GHz + 2x Cortex-A520 @ 3.15 GHz"
  //    "2x Lion Cove @ 3.6 GHz + 16x Skymont @ 2.8 GHz"
  //    "8x Cortex-A53 @ 2.0 GHz"
  //    "4+4 cores, 4x Cortex-X1 + 4x Cortex-A55"
  // ══════════════════════════════════════════════════════════════════════════

  // Find the CPU cluster / core specification string
  // NBC processor pages use "Number of Cores / Threads" as the label, and the value
  // contains the cluster config after the count: "8 / 8 2 x 4.6 GHz Oryon Gen 3 + ..."
  const cpuRaw = (() => {
    // Try the NBC-specific label first
    const nbcField = findSpec(/^Number\s+of\s+Cores?\s*\/\s*Threads?/i);
    if (nbcField) {
      // Strip the leading "N / N " core/thread count, keep the cluster description
      return nbcField.replace(/^\d+\s*\/\s*\d+\s*/, '').trim();
    }
    return findSpec(/^CPU|^Cores?|^Processor Cores?|^CPU Cluster|^Core Configuration/i)
      || findSpec(/^Cluster\s*1|^Performance\s*Cores?/i)
      || '';
  })();

  const cpuClusters: ProcessorCore[] = [];
  let totalCores = 0;
  let baseClockMHz = 0;
  let boostClockMHz = 0;

  // Parse cluster string — matches patterns like "1x Cortex-X4 @ 4320 MHz"
  // or "2x Lion Cove @ 3.6 GHz" or "8x Cortex-A55 @ 2.0 GHz"
  // NBC sometimes omits "+" between clusters: "2 x 4.6 GHz Oryon Gen 3 6 x 3.6 GHz Oryon Gen 3"
  function parseCpuClusters(raw: string): void {
    // Normalize: insert "+" before each "N x" group that isn't at the start
    // This handles both "A + B" and "A B" formats
    const normalized = raw
      .replace(/\s+(\d+)\s*[xX×]\s*/g, (m, n, offset) => offset === 0 ? m : ` + ${n} x `)
      .trim();

    // Split on "+" separating different cluster types
    const segments = normalized.split(/\s*\+\s*/);

    // Regex to match: COUNT x CORE_NAME @ FREQ UNIT  (standard)
    const segRx = /(\d+)\s*x\s*([A-Za-z][A-Za-z0-9\s\-_.]+?)\s*(?:@|at|,)\s*([\d.]+)\s*(GHz|MHz)/gi;

    // Also try: COUNT x CORE_NAME @ FREQ (alt punctuation)
    const altRx  = /(\d+)\s*[xX×]\s*([A-Za-z][A-Za-z0-9\s\-_.]+?)[\s,]*(?:@|at)\s*([\d.]+)\s*(GHz|MHz)/gi;

    // NBC-specific reversed format: COUNT x FREQ UNIT CORE_NAME
    // e.g. "2 x 4.6 GHz Qualcomm Oryon Gen 3"
    const revRx = /(\d+)\s*[xX×]\s*([\d.]+)\s*(GHz|MHz)\s+([A-Za-z][A-Za-z0-9\s\-_.]{2,40})/gi;

    for (const seg of segments) {
      segRx.lastIndex = 0;
      altRx.lastIndex = 0;
      revRx.lastIndex = 0;

      // Try reversed NBC format first: "2 x 4.6 GHz Qualcomm Oryon Gen 3"
      const revM = revRx.exec(seg);
      if (revM) {
        const count   = parseInt(revM[1]);
        const freq    = parseFloat(revM[2]);
        const unit    = revM[3].toLowerCase();
        const name    = revM[4].trim();
        const freqMHz = unit === 'ghz' ? Math.round(freq * 1000) : Math.round(freq);
        totalCores += count;
        if (freqMHz > boostClockMHz) boostClockMHz = freqMHz;
        if (baseClockMHz === 0 || freqMHz < baseClockMHz) baseClockMHz = freqMHz;
        const isPerfCore = /X[1-9]|Prime|Oryon|Lion\s*Cove|Raptor|Firestorm|Avalanche|\bBIG\b|Cortex-A[789]\d\d/i.test(name);
        const isEffCore  = /A5[0-9]|Skymont|Sawtooth|Icestorm|Blizzard|Efficiency|little|small/i.test(name);
        cpuClusters.push({ name, count, baseClockMHz: freqMHz, boostClockMHz: freqMHz, isPerformanceCore: isPerfCore, isEfficiencyCore: isEffCore });
        continue;
      }

      let m = segRx.exec(seg) || altRx.exec(seg);
      if (!m) {
        // Try without frequency: "Nx CORE_NAME"
        const noFreqRx = /(\d+)\s*[xX×]\s*([A-Za-z][A-Za-z0-9\s\-_.]{2,30})/;
        const mNF = seg.match(noFreqRx);
        if (mNF) {
          const count = parseInt(mNF[1]);
          const name  = mNF[2].trim();
          totalCores += count;
          const isPerfCore = /X[1-9]|Lion|Cortex-A[789]\d\d|Prime|Performance|Big|Large/i.test(name);
          const isEffCore  = /A5[0-9]|Skymont|Efficiency|Little|Small/i.test(name);
          cpuClusters.push({ name, count, isPerformanceCore: isPerfCore, isEfficiencyCore: isEffCore });
        }
        continue;
      }

      while (m) {
        const count  = parseInt(m[1]);
        const name   = m[2].trim();
        const freq   = parseFloat(m[3]);
        const unit   = m[4].toLowerCase();
        const freqMHz = unit === 'ghz' ? Math.round(freq * 1000) : Math.round(freq);

        totalCores += count;

        // Classify core type heuristically
        const isPerfCore = /X[1-9]|Prime|Oryon|Lion\s*Cove|Raptor|Firestorm|Avalanche|\bBIG\b|Cortex-A[789]\d\d/i.test(name);
        const isEffCore  = /A5[0-9]|Skymont|Sawtooth|Icestorm|Blizzard|Efficiency|little|small/i.test(name);

        // Track global clocks
        if (freqMHz > boostClockMHz) boostClockMHz = freqMHz;
        if (baseClockMHz === 0 || freqMHz < baseClockMHz) baseClockMHz = freqMHz;

        // Find per-cluster L1/L2 cache info
        const l1Match = seg.match(/L1[:\s]+([0-9]+\s*(?:KB|MB))/i);
        const l2Match = seg.match(/L2[:\s]+([0-9]+\s*(?:KB|MB))/i);

        cpuClusters.push({
          name,
          count,
          baseClockMHz: freqMHz, // NBC only reports one clock per cluster
          boostClockMHz: freqMHz,
          l1Cache: l1Match ? l1Match[1] : undefined,
          l2Cache: l2Match ? l2Match[1] : undefined,
          isPerformanceCore: isPerfCore,
          isEfficiencyCore:  isEffCore,
        });

        segRx.lastIndex = m.index + m[0].length;
        m = segRx.exec(seg);
      }
    }
  }

  parseCpuClusters(cpuRaw);

  // Fallback: total cores from dedicated spec field
  if (totalCores === 0) {
    const coreField = findSpec(/^(?:number\s+of\s+)?cores?|^total\s+cores?/i);
    const cm = coreField.match(/(\d+)/);
    if (cm) totalCores = parseInt(cm[1]);
    // Also try body text: "octa-core", "deca-core"
    if (!totalCores) {
      const coreWords: Record<string, number> = {
        dual: 2, quad: 4, hexa: 6, octa: 8, deca: 10, dodeca: 12,
      };
      const cwM = findInBody(/(dual|quad|hexa|octa|deca|dodeca)[\s-]core/i);
      if (cwM) totalCores = coreWords[cwM.toLowerCase()] || 0;
    }
  }

  // Boost/base clock fallback from spec table
  // NBC uses "Clock Rate": "3620 - 4600 MHz" or "1800 - 2840 MHz"
  if (!boostClockMHz || !baseClockMHz) {
    const clockRateField = findSpec(/^clock\s*rate|^clock\s*speed|^frequency/i)
      || findSpec(/^(?:max|boost|turbo|maximum)\s*(?:clock|frequency|speed)/i);
    if (clockRateField) {
      // "3620 - 4600 MHz" → base=3620, boost=4600
      const rangeM = clockRateField.match(/([\d.]+)\s*[-–]\s*([\d.]+)\s*(GHz|MHz)/i);
      if (rangeM) {
        const unit = rangeM[3].toLowerCase();
        const lo = parseFloat(rangeM[1]);
        const hi = parseFloat(rangeM[2]);
        const toMHz = (v: number) => unit === 'ghz' ? Math.round(v * 1000) : Math.round(v);
        if (!baseClockMHz) baseClockMHz = toMHz(lo);
        if (!boostClockMHz) boostClockMHz = toMHz(hi);
      } else {
        // Single value
        const singleM = clockRateField.match(/([\d.]+)\s*(GHz|MHz)/i);
        if (singleM) {
          const f = parseFloat(singleM[1]);
          const mhz = singleM[2].toLowerCase() === 'ghz' ? Math.round(f * 1000) : Math.round(f);
          if (!boostClockMHz) boostClockMHz = mhz;
        }
      }
    }
  }

  // Total threads — often same as cores for modern ARM; x86 may have HT
  // NBC "Number of Cores / Threads" field: "8 / 8 2 x 4.6 GHz ..."
  const threadField = findSpec(/^Number\s+of\s+Cores?\s*\/\s*Threads?/i)
    || findSpec(/^threads?|^(?:total\s+)?threads?/i);
  let totalThreads = totalCores;
  const tm = threadField.match(/\d+\s*\/\s*(\d+)/);  // "8 / 8" → capture second
  if (tm) totalThreads = parseInt(tm[1]);
  else {
    const tm2 = threadField.match(/(\d+)/);
    if (tm2) totalThreads = parseInt(tm2[1]);
  }

  // Also fix totalCores from "N / N" if parseCpuClusters didn't find it
  if (totalCores === 0) {
    const coreThreadField = findSpec(/^Number\s+of\s+Cores?\s*\/\s*Threads?/i);
    const ctM = coreThreadField.match(/^(\d+)\s*\//);
    if (ctM) totalCores = parseInt(ctM[1]);
  }

  // ── CACHE ────────────────────────────────────────────────────────────────
  // NBC uses "Level 2 Cache", "Level 3 Cache" as spec labels
  const l2CacheTotal = findSpec(/^Level\s*2\s*Cache|^L2\s*Cache(?:\s*\(total\))?|^L2$/i)
    || findInBody(/L2\s*(?:cache)?[:\s]+([0-9.]+\s*(?:MB|KB))/i);
  const l3Cache      = findSpec(/^Level\s*3\s*Cache|^L3\s*Cache|^L3$/i)
    || findInBody(/L3\s*(?:cache)?[:\s]+([0-9.]+\s*(?:MB|KB))/i);
  const l4Cache      = findSpec(/^Level\s*4\s*Cache|^L4\s*Cache|^L4$/i)
    || findInBody(/L4\s*(?:cache)?[:\s]+([0-9.]+\s*(?:MB|KB))/i);
  const systemLevelCache = findSpec(/^System[-\s]?Level\s*Cache|^SLC|^LLC$/i)
    || findInBody(/(?:System[\s-]Level\s*Cache|SLC)[:\s]+([0-9.]+\s*(?:MB|KB))/i);

  // ══════════════════════════════════════════════════════════════════════════
  //  GPU — extract name, variant, core count, clocks, APIs
  // ══════════════════════════════════════════════════════════════════════════
  const gpuRaw = findSpec(/^GPU|^Graphics|^Integrated\s*GPU/i)
    || findSpec(/^Graphics\s*Adapter|^Graphics\s*Card/i) || '';

  const gpu: ProcessorGPU = { name: gpuRaw.split(/[,@(]/)[0].trim() };

  // Parse GPU name — NBC formats:
  //   "Adreno 830"
  //   "Immortalis-G925 MC12"
  //   "Apple GPU (38-core)"
  //   "Qualcomm Adreno 840 ( - 1200 MHz)"  ← clock range with leading dash
  // Strip trailing clock/core annotations to get the clean name
  const gpuNameM = gpuRaw.match(/^([A-Za-z][A-Za-z0-9\s\-]+?)\s*(?:MC\d+|\([\s\-\d]|@|,|$)/i);
  if (gpuNameM) gpu.name = gpuNameM[1].trim();

  // GPU core count — "MC12", "(12-core)", "12 cores", "38 cores"
  const gpuCoreM = gpuRaw.match(/MC(\d+)|(\d+)[\s-]?(?:cores?|CUs?)\b/i)
    || gpuRaw.match(/\((\d+)[\s-]?core/i);
  if (gpuCoreM) gpu.coreCount = (gpuCoreM[1] || gpuCoreM[2]) + ' cores';

  // GPU max clock — handles "@1200MHz", "( - 1200 MHz)", "(up to 1200 MHz)"
  const gpuClockM = gpuRaw.match(/@\s*([\d.]+)\s*(GHz|MHz)/i)
    || gpuRaw.match(/\(\s*(?:up\s*to\s*|-\s*)?([\d.]+)\s*(GHz|MHz)\s*\)/i)
    || findInBody(/GPU[^.]{0,40}@\s*([\d.]+)\s*(GHz|MHz)/i);
  if (gpuClockM) {
    const f    = parseFloat(gpuClockM[1]);
    const unit = gpuClockM[2].toLowerCase();
    gpu.maxClockMHz = unit === 'ghz' ? Math.round(f * 1000) : Math.round(f);
  }

  // GPU shader/ALU count
  const shaderM = findSpec(/^shader|^ALU|^CUDA\s*cores?|^Stream\s*Processors?/i)
    || findInBody(/(\d+)\s*(?:shader|ALU|CUDA|stream\s*processor)/i);
  if (shaderM) gpu.shaderCount = shaderM.match(/(\d+)/)?.[1] ? shaderM : '';

  // GPU compute (TFLOPS / GFLOPS)
  const computeM = findSpec(/^(?:peak\s*)?compute|TFLOPS|GFLOPS/i)
    || findInBody(/([0-9.]+)\s*(?:TFLOPS|GFLOPS)/i);
  if (computeM) gpu.compute = computeM;

  // GPU APIs
  const gpuApiRaw = findSpec(/^API|^(?:Supported\s*)?APIs?|^Graphics\s*APIs?/i)
    || gpuRaw + ' ' + getBodyText().slice(0, 5000);
  const gpuApis: string[] = [];
  for (const api of ['OpenGL ES 3.2', 'OpenGL 4.6', 'Vulkan 1.3', 'Vulkan 1.1', 'DirectX 12', 'DirectX 11', 'Metal', 'OpenCL 3.0', 'OpenCL 2.0']) {
    if (new RegExp(api.replace(/\s/g, '\\s*'), 'i').test(gpuApiRaw)) gpuApis.push(api);
  }
  if (gpuApis.length) gpu.apis = gpuApis;

  // ══════════════════════════════════════════════════════════════════════════
  //  NPU / AI ENGINE
  // ══════════════════════════════════════════════════════════════════════════
  const npuRaw   = findSpec(/^NPU|^AI\s*Engine|^Neural\s*(?:Engine|Processor)|^Machine\s*Learning/i) || '';
  // NBC uses "Chip AI": "26 TOPS INT8" for the AI/NPU spec
  const npuAiRaw = findSpec(/^Chip\s*AI|^AI\s*Chip/i) || '';
  const npuTops  = findSpec(/^AI\s*Performance|^(?:NPU\s*)?TOPS|^AI\s*TOPS/i)
    || npuAiRaw.match(/([\d.]+)\s*TOPS/i)?.[0]
    || findInBody(/(\d+(?:\.\d+)?)\s*TOPS/i);
  const npuCores = findSpec(/^NPU\s*(?:Cores?|Count)/i)
    || npuRaw.match(/(\d+)[\s-]core/i)?.[1] || '';

  const npu: ProcessorNPU = {};
  if (npuRaw)   npu.name  = npuRaw.split(/[,@(]/)[0].trim();
  // Extract Hexagon DSP name from Features if no dedicated NPU field
  if (!npu.name) {
    const hexM = featuresRaw.match(/Hexagon[\s\w\d]*/i);
    if (hexM) npu.name = hexM[0].trim();
  }
  // Extract TOPS value
  if (npuTops) {
    const topsStr = Array.isArray(npuTops) ? (npuTops[0] || '') : String(npuTops);
    const topsNum = topsStr.match(/[\d.]+/)?.[0];
    if (topsNum) npu.tops = topsNum;
  }
  // Also try extracting TOPS from the Chip AI field directly
  if (!npu.tops && npuAiRaw) {
    const m = npuAiRaw.match(/([\d.]+)\s*TOPS/i);
    if (m) npu.tops = m[1];
  }
  if (npuCores) npu.cores = npuCores.toString();

  // ══════════════════════════════════════════════════════════════════════════
  //  MEMORY
  // ══════════════════════════════════════════════════════════════════════════
  const memRaw     = findSpec(/^Memory|^RAM\s*Type|^DRAM/i) || '';
  const memBwRaw   = findSpec(/^Memory\s*Bandwidth|^Bandwidth/i) || '';
  const memSpeedRaw= findSpec(/^Memory\s*(?:Speed|Frequency|Clock)/i)
    || findSpec(/^LPDDR\d+[Xx]?\s*Speed/i) || '';

  const memory: ProcessorMemorySpec = {};

  // Memory type: LPDDR5X, LPDDR5, LPDDR4X, etc. — check Features field too
  const memTypeSrc = memRaw + ' ' + featuresRaw + ' ' + findSpec(/^RAM|^Memory\s*Standard/i);
  const memTypeM = memTypeSrc.match(/LPDDR[45][Xx]?|DDR[45][Xx]?|LPDDR3|LPCAMM/i);
  if (memTypeM) memory.type = memTypeM[0].toUpperCase();

  // Memory speed — from spec field or Features: "LPDDR5x 4800" → 4800 MHz
  // Also handles "LPDDR5-6400" format
  const memSpdSrc = memSpeedRaw || memRaw || featuresRaw;
  const memSpdM = memSpdSrc.match(/LPDDR\d+[Xx]?[-\s](\d{3,5})/i)   // "LPDDR5-6400" or "LPDDR5x 4800"
    || memSpdSrc.match(/([\d,]+)\s*(MT\/s|Mbps|MHz)/i)
    || findInBody(/memory\s*(?:speed|frequency|clock)[:\s]+([\d,]+)\s*(MT\/s|Mbps|MHz)/i);
  if (memSpdM) {
    // LPDDR pattern gives raw MT/s number; generic pattern gives number + unit
    const spd = memSpdM[1].replace(/,/g, '');
    const unit = memSpdM[2] || 'MHz';
    memory.speedMHz = spd + ' ' + unit;
  }

  // Memory bandwidth
  const memBwM = (memBwRaw || '').match(/([\d.]+)\s*GB\/s/i)
    || findInBody(/(?:bandwidth|memory\s*BW)[:\s]+([\d.]+)\s*GB\/s/i);
  if (memBwM) memory.bandwidthGBs = memBwM[1] + ' GB/s';

  // Memory channels
  const memChM = findSpec(/^(?:Memory\s*)?Channels?/i)
    || findInBody(/(\d+)[\s-]?channel\s*memory/i);
  const mChNum = memChM.match?.(/(\d+)/);
  if (mChNum) memory.channels = mChNum[1] + '-channel';

  // Max memory capacity
  const memCapM = findSpec(/^Max(?:imum)?\s*(?:Memory|RAM)(?:\s*Size)?/i)
    || findInBody(/(?:up to|max)\s+(\d+)\s*GB\s*(?:RAM|LPDDR|memory)/i);
  const mCapNum = memCapM.match?.(/(\d+)\s*GB/i);
  if (mCapNum) memory.maxCapacityGB = mCapNum[1] + ' GB';

  // ══════════════════════════════════════════════════════════════════════════
  //  CONNECTIVITY
  // ══════════════════════════════════════════════════════════════════════════
  const connectivity: ProcessorConnectivity = {};

  const wifiRaw   = findSpec(/^Wi-?Fi|^WLAN|^Wireless\s*LAN/i) || '';
  const btRaw     = findSpec(/^Bluetooth/i) || '';
  const celRaw    = findSpec(/^Cellular|^Mobile\s*(?:Network|Modem)|^Modem/i) || '';
  const usbRaw    = findSpec(/^USB/i) || '';
  const dispRaw   = findSpec(/^Display\s*(?:Interface|Output)|^Video\s*Output/i) || '';
  const gnssRaw   = findSpec(/^GNSS|^GPS|^Positioning/i) || '';
  const dlRaw     = findSpec(/^(?:Max\s*)?Downlink|^LTE\s*(?:DL|Download)/i) || '';
  const ulRaw     = findSpec(/^(?:Max\s*)?Uplink|^LTE\s*(?:UL|Upload)/i) || '';
  const satRaw    = findSpec(/^Satellite/i) || '';

  // Parse connectivity tokens from the Features field when dedicated fields are missing
  // e.g. "Adreno GPU, Spectra ISP, Hexagon, X80 5G Modem, Wi-Fi 7, LPDDR5x 4800"
  //      "FastConnect 6900 WiFi, LPDDR5-6400 / LPDDR4X-4266 MHz Memory Controller"
  const wifiEffective  = wifiRaw  || (featuresRaw.match(/Wi-?Fi\s*([\w.]+)/i)?.[0] ?? '')
    || (featuresRaw.match(/FastConnect\s*[\w\d]+\s*WiFi/i)?.[0] ?? '');
  const celEffective   = celRaw   || (featuresRaw.match(/X\d+\s*5G\s*Modem|4G\s*LTE|5G\s*NR/i)?.[0] ?? '');
  const modemToken     = featuresRaw.match(/X\d+\s*5G|Snapdragon\s*X\d+/i)?.[0] ?? '';

  if (wifiEffective)  connectivity.wifi      = wifiEffective;
  if (btRaw)          connectivity.bluetooth = btRaw;
  if (celEffective)   connectivity.cellular  = celEffective;
  else if (modemToken) connectivity.cellular = modemToken;
  if (usbRaw)    connectivity.usb       = usbRaw;
  if (dispRaw)   connectivity.display   = dispRaw;
  if (gnssRaw)   connectivity.gnss      = gnssRaw;
  if (satRaw)    connectivity.satellite = satRaw;

  // NFC — often embedded
  if (/\bNFC\b/i.test(wifiRaw + ' ' + celRaw + ' ' + getBodyText().slice(0, 3000))) {
    connectivity.nfc = true;
  }

  // Downlink / uplink speeds
  const dlM = (dlRaw || celRaw).match(/([\d,]+(?:\.\d+)?)\s*Mbps|Gbps/i)
    || findInBody(/downlink[:\s]+([\d,]+(?:\.\d+)?)\s*Mbps/i);
  if (dlM) connectivity.maxDownlinkMbps = dlM[1];

  const ulM = (ulRaw || celRaw).match(/upload[:\s]+([\d,]+(?:\.\d+)?)\s*Mbps/i)
    || findInBody(/uplink[:\s]+([\d,]+(?:\.\d+)?)\s*Mbps/i);
  if (ulM) connectivity.maxUplinkMbps = ulM[1];

  // Cellular bands (sub-6, mmWave) — check Features field too
  const celBandRaw = findSpec(/^(?:5G\s*)?(?:Bands?|Spectrum|Frequency\s*Bands?)/i) || '';
  const celBandSrc = celBandRaw + ' ' + celEffective + ' ' + featuresRaw + ' ' + getBodyText().slice(0, 3000);
  if (celBandRaw) connectivity.cellularBands = celBandRaw;
  else if (/mmwave|millimeter/i.test(celBandSrc)) {
    connectivity.cellularBands = 'Sub-6 GHz, mmWave';
  } else if (/sub.6|sub6/i.test(celBandSrc)) {
    connectivity.cellularBands = 'Sub-6 GHz';
  } else if (/5G/i.test(celBandSrc)) {
    connectivity.cellularBands = '5G';
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  MEDIA & DISPLAY CAPABILITIES
  // ══════════════════════════════════════════════════════════════════════════
  const media: ProcessorMediaCapabilities = {};

  const videoEncRaw   = findSpec(/^(?:Max\s*)?Video\s*(?:Encode|Encoding|Capture)|^Encode/i) || '';
  const videoDecRaw   = findSpec(/^(?:Max\s*)?Video\s*(?:Decode|Decoding|Playback)|^Decode/i) || '';
  const ispRaw        = findSpec(/^ISP|^Image\s*Signal\s*Processor/i)
    // NBC puts ISP in Features: "Spectra 580 ISP" or "Spectra ISP"
    || featuresRaw.match(/Spectra[\s\w\d]*/i)?.[0]
    || featuresRaw.match(/\w+\s+ISP/i)?.[0]
    || '';
  const maxDispRaw    = findSpec(/^(?:Max\s*)?Display\s*(?:Resolution|Output)/i) || '';
  const maxCamRaw     = findSpec(/^(?:Max\s*)?(?:Camera|Single\s*Camera)\s*(?:Resolution|Megapixel)/i) || '';
  const hdrRaw        = findSpec(/^HDR\s*Support|^HDR$/i) || '';

  if (videoEncRaw) media.videoEncode = videoEncRaw;
  if (videoDecRaw) media.videoDecode = videoDecRaw;
  if (ispRaw)      media.isp         = ispRaw.split(/[,@]/)[0].trim();
  if (maxDispRaw)  media.maxDisplayResolution = maxDispRaw;
  if (hdrRaw)      media.hdr         = hdrRaw;

  // ISP core count
  const ispCoreM = ispRaw.match(/(\d+)[\s-]?(?:ISP|core)/i)
    || findInBody(/(\d+)[\s-]?ISP\s*cores?/i);
  if (ispCoreM) media.ispCores = ispCoreM[1];

  // Max camera MP
  const camMpM = (maxCamRaw || findSpec(/^Camera/i)).match(/([\d,]+)\s*MP/i)
    || findInBody(/(?:up to|max)\s+([\d,]+)\s*MP\s*(?:camera|sensor)/i);
  if (camMpM) media.maxCameraMpx = camMpM[1] + ' MP';

  // Max display refresh
  const dispHzM = (maxDispRaw || '').match(/(\d+)\s*Hz/i)
    || findInBody(/(?:display|refresh)[^\n.]{0,30}(\d{2,4})\s*Hz/i);
  if (dispHzM) media.maxDisplayRefreshHz = dispHzM[1] + ' Hz';

  // Supported codecs
  const codecSrc = videoDecRaw + ' ' + videoEncRaw + ' ' + getBodyText().slice(0, 8000);
  const codecList: string[] = [];
  for (const codec of ['AV1', 'H.266', 'H.265', 'HEVC', 'H.264', 'AVC', 'VP9', 'VP8', 'MPEG-4', 'MPEG-2', 'ProRes', 'JPEG XL']) {
    if (new RegExp('\\b' + codec.replace('.', '\\.') + '\\b', 'i').test(codecSrc)) codecList.push(codec);
  }
  if (codecList.length) media.codecs = [...new Set(codecList)];

  // ══════════════════════════════════════════════════════════════════════════
  //  SECURITY
  // ══════════════════════════════════════════════════════════════════════════
  const security: ProcessorSecurityFeatures = {};

  const teeRaw  = findSpec(/^(?:TEE|Trusted\s*Execution|Security\s*Engine|Secure\s*Zone)/i) || '';
  const secRaw  = findSpec(/^Security|^Secure\s*(?:Enclave|Element|Processor)/i) || '';
  const bioRaw  = findSpec(/^(?:Biometric|Fingerprint|Face\s*(?:ID|Detection))\s*(?:Support|Authentication)?/i) || '';

  if (teeRaw || secRaw) security.tee = teeRaw || secRaw;
  if (secRaw.toLowerCase().includes('enclave')) security.secureEnclave = secRaw;
  if (bioRaw) security.biometricSupport = bioRaw;

  const secBodySrc = secRaw + ' ' + teeRaw + ' ' + getBodyText().slice(0, 5000);
  security.mte         = /\bMTE\b|memory\s*tagging/i.test(secBodySrc);
  security.secureBoot  = /secure\s*boot/i.test(secBodySrc);

  // ══════════════════════════════════════════════════════════════════════════
  //  POWER / TDP
  // ══════════════════════════════════════════════════════════════════════════
  const power: ProcessorPowerSpec = {
    processNode,
    architecture,
    isa,
  };

  const tdpRaw = findSpec(/^TDP|^Thermal\s*Design\s*(?:Power|Point)|^Max\s*(?:Power|TDP)/i)
    || findInBody(/TDP[:\s]+([\d.]+)\s*W/i);
  const tdpM = tdpRaw.match(/([\d.]+)\s*W/i);
  if (tdpM) power.tdpWatts = tdpM[1] + ' W';

  const typPwrRaw = findSpec(/^Typical\s*Power|^Average\s*Power|^Power\s*Consumption/i)
    || findInBody(/typical\s*power[:\s]+([\d.]+)\s*W/i);
  const typPwrM = typPwrRaw.match(/([\d.]+)\s*W/i);
  if (typPwrM) power.typicalPowerWatts = typPwrM[1] + ' W';

  // ══════════════════════════════════════════════════════════════════════════
  //  BENCHMARKS — NBC processor pages render benchmarks as div.gpubench_div
  //
  //  Structure:
  //    <div class="gpubench_div b{benchId}_s{settingId}">
  //      <div class="gpubench_benchmark"><b>NAME</b> - SUB-TEST</div>
  //      <div class="paintAB_legend">min: X  avg: Y  median: Z (P%)  max: W unit</div>
  //      <div id="bench_details_N" style="display:none">
  //        <table> ... per-device score rows ... </table>
  //      </div>
  //      <span id="get_benchmark_chart_data_N" style="display:none">
  //        {"min_value":"X","avg_value":"Y","max_value":"W","unit":"u","smallerisbetter":"0",...}
  //      </span>
  //    </div>
  // ══════════════════════════════════════════════════════════════════════════
  const benchmarks: ProcessorBenchmark[] = [];
  const benchSeen = new Set<string>();

  // Helper to classify benchmark category
  function catProcBench(name: string): string {
    const n = name.toLowerCase();
    if (/geekbench|cinebench|passmark|kraken|jetstream|speedometer|pcmark|crossmark|single.core|multi.core|cpu\s*\d|perf\s*score|ai\s*benchmark\s*cpu/i.test(n)) return 'cpu';
    if (/gfxbench|3dmark|manhattan|aztec|t-rex|wild\s*life|gpu|graphics|offscreen|onscreen|vulkan|opengl|basemark|steel\s*nomad/i.test(n)) return 'gpu';
    if (/memory\s*bandwidth|androbench|cpdt|sequential|random\s*(read|write)|mb\/s|gb\/s|storage/i.test(n)) return 'memory';
    if (/ai\s*benchmark|npu|tops|neural|machine\s*learning/i.test(n)) return 'ai';
    if (/antutu/i.test(n)) return 'cpu';
    return 'misc';
  }

  // Pass 1: NBC gpubench_div — the primary benchmark display on processor pages
  $('div.gpubench_div').each((_, div) => {
    const $div = $(div);

    // Benchmark name from the gpubench_benchmark div
    const benchNameRaw = normStr($div.find('div.gpubench_benchmark').text()).trim();
    if (!benchNameRaw) return;

    // Parse "BENCHMARK_NAME - SUB_TEST" format
    // The <b> tag contains the benchmark family, the text after " - " is the sub-test
    const $benchDiv = $div.find('div.gpubench_benchmark');
    const benchFamily = normStr($benchDiv.find('b').first().text()).trim();
    const fullDivText = normStr($benchDiv.text()).trim();
    // Remove the bold part to get the sub-test
    let subTest = '';
    if (benchFamily && fullDivText.includes(' - ')) {
      subTest = fullDivText.replace(benchFamily, '').replace(/^\s*-\s*/, '').trim();
    }
    const benchName = subTest && subTest !== benchFamily ? subTest : benchFamily || benchNameRaw;

    // Prefer JSON data (most reliable source for avg/min/max)
    let avgValue = '';
    let minValue = '';
    let maxValue = '';
    let unit = '';
    let smallerIsBetter = false;

    const $jsonSpan = $div.find('[id^="get_benchmark_chart_data_"]');
    if ($jsonSpan.length) {
      try {
        const jsonText = $jsonSpan.text().trim();
        if (jsonText) {
          const data = JSON.parse(jsonText) as Record<string, string>;
          avgValue = data.avg_value || '';
          minValue = data.min_value || '';
          maxValue = data.max_value || '';
          unit = data.unit || '';
          smallerIsBetter = data.smallerisbetter === '1';
        }
      } catch {
        // fall through to legend parsing
      }
    }

    // Fallback: parse paintAB_legend text
    // Format: "min: 3498  avg: 3672  median: 3679.5 (85%)  max: 3831 points"
    if (!avgValue) {
      const legendText = $div.find('div.paintAB_legend').text();
      const minM   = legendText.match(/min:\s*([\d.,]+)/i);
      const avgM   = legendText.match(/avg:\s*([\d.,]+)/i);
      const maxM   = legendText.match(/max:\s*([\d.,]+)\s*([a-zA-Z/%]+)/i);
      if (minM) minValue = minM[1];
      if (avgM) avgValue = avgM[1];
      if (maxM) { maxValue = maxM[1]; if (!unit) unit = maxM[2]; }
    }

    if (!avgValue && !minValue && !maxValue) return;
    const primaryValue = avgValue || minValue || maxValue;

    // Percentile from legend: "median: 3679.5 (85%)"
    let percentile: string | undefined;
    const legendText2 = $div.find('div.paintAB_legend').text();
    const pctM = legendText2.match(/\((\d+(?:\.\d+)?)%\)/);
    if (pctM) percentile = pctM[1];

    // Per-device scores from hidden bench_details table
    const deviceScores: Array<{ model: string; score: string }> = [];
    $div.find('[id^="bench_details_"] table tr').each((_, row) => {
      const $row = $(row);
      // Skip header row
      if ($row.find('th').length) return;
      const model = normStr($row.find('td:first-child a').text()).trim()
        || normStr($row.find('td:first-child').text()).trim();
      const score = normStr($row.find('td:last-child').text()).trim();
      if (model && score && /\d/.test(score)) {
        deviceScores.push({ model, score });
      }
    });

    const key = `${benchName}::${primaryValue}`;
    if (benchSeen.has(key)) return;
    benchSeen.add(key);

    const entry: ProcessorBenchmark = {
      name: benchName,
      value: primaryValue,
      unit,
      category: catProcBench(benchName),
    };
    if (minValue && minValue !== primaryValue) entry.minValue = minValue;
    if (maxValue && maxValue !== primaryValue) entry.maxValue = maxValue;
    if (avgValue) entry.value = avgValue; // always prefer avg as primary
    if (percentile) entry.percentile = percentile;
    if (smallerIsBetter) entry.smallerIsBetter = true;
    if (deviceScores.length) entry.deviceScores = deviceScores;

    benchmarks.push(entry);
  });

  // Pass 2: fallback r_compare_bars tables (older NBC processor pages)
  if (benchmarks.length === 0) {
    $('table[class*="r_compare_bars"]').each((_, table) => {
      const $t = $(table);
      const benchName = normStr($t.find('td.prog_header').first().text());
      if (!benchName) return;

      let subTest = '';
      $t.find('tr').each((_, row) => {
        const $row = $(row);

        const settingsCell = $row.find('td.settings_header');
        if (settingsCell.length) {
          const hasDeviceLink = settingsCell.find('a').length > 0
            && settingsCell.find('span.r_compare_bars_specs').length > 0;
          if (!hasDeviceLink) {
            subTest = normStr(settingsCell.text()).trim();
          }
          return;
        }

        if (!($row.attr('class') || '').includes('referencespecs')) return;

        const fullName = subTest ? `${benchName} / ${subTest}` : benchName;

        let value = '';
        $row.find('span[class*="r_compare_bars_number"]').each((_, span) => {
          if (!value) value = normStr($(span).text()).trim();
        });
        if (!value) {
          const barText = normStr($row.find('td.bar').text()).replace(/[+\-−]\s*\d+\s*%/g, '').trim();
          const numM = barText.match(/^([\d,.]+)/);
          if (numM) value = numM[1];
        }
        if (!value || !/\d/.test(value)) return;

        const barText = $row.find('td.bar').text();
        const unitM = barText.match(/[\d)\s](Points?|fps|ms\b|MB\/s|GB\/s|MBit\/s|%|\bh\b|min\b|MHz|GHz|TOPS|ops\/s)/i);
        const unit = unitM ? unitM[1] : '';

        let percentile: string | undefined;
        const pctM = barText.match(/(\d+(?:\.\d+)?)\s*%/);
        if (pctM) percentile = pctM[1];

        const key = `${fullName}::${value}`;
        if (benchSeen.has(key)) return;
        benchSeen.add(key);

        const entry: ProcessorBenchmark = { name: fullName, value, unit, category: catProcBench(fullName) };
        if (percentile) entry.percentile = percentile;
        benchmarks.push(entry);
      });
    });
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  DEVICES USING THIS PROCESSOR
  //  NBC shows a "Devices tested" section linking to phone reviews
  // ══════════════════════════════════════════════════════════════════════════
  const devicesUsing: DeviceSupport[] = [];
  const devSeen = new Set<string>();

  // Strategy 1: NBC's related/tested devices section
  $('[class*="related"], [class*="devices"], [id*="devices"], [class*="tested"]').each((_, section) => {
    $(section).find('a[href]').each((_, el) => {
      const href  = $(el).attr('href') || '';
      const title = normStr($(el).text());
      if (!href || !title || title.length < 3 || title.length > 150) return;

      const fullUrl = href.startsWith('http') ? href
        : href.startsWith('/') ? 'https://www.notebookcheck.net' + href : '';
      if (!fullUrl || devSeen.has(fullUrl)) return;

      // Only accept NBC smartphone/laptop review pages
      if (!/notebookcheck\.net/.test(fullUrl)) return;
      if (!/\.\d{4,}\.\d+\.html/i.test(fullUrl)) return;
      // Skip other processor pages
      if (/processor-benchmarks|soc.*benchmarks/i.test(fullUrl)) return;

      devSeen.add(fullUrl);
      devicesUsing.push({ name: title, url: fullUrl });
    });
  });

  // Strategy 2: links in the main article text that point to NBC review pages
  if (devicesUsing.length < 3) {
    $('a[href]').each((_, el) => {
      if (devicesUsing.length >= 40) return false;
      const href  = $(el).attr('href') || '';
      const title = normStr($(el).text());
      if (!href || !title || title.length < 4 || title.length > 120) return;
      const fullUrl = href.startsWith('http') ? href
        : href.startsWith('/') ? 'https://www.notebookcheck.net' + href : '';
      if (!fullUrl || devSeen.has(fullUrl)) return;
      if (!/notebookcheck\.net/.test(fullUrl)) return;
      if (!/\.\d{4,}\.\d+\.html/i.test(fullUrl)) return;
      if (/processor-benchmarks|soc.*benchmarks/i.test(fullUrl)) return;
      if (/(smartphone|phone|tablet|laptop)[\s-]review/i.test(fullUrl)) {
        devSeen.add(fullUrl);
        devicesUsing.push({ name: title, url: fullUrl });
      }
    });
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  DESCRIPTION & PERFORMANCE TIER
  // ══════════════════════════════════════════════════════════════════════════
  let description = '';
  let performanceTier = '';

  // First meaningful paragraph in the editorial section
  $('p').each((_, el) => {
    const t = normStr($(el).text());
    if (t.length > 100 && !description && !t.toLowerCase().includes('cookie') && !t.includes('{')) {
      description = t.slice(0, 1000);
    }
  });

  // Performance tier — NBC often writes "flagship", "high-end", "mid-range", "entry-level"
  const tierSrc = description + ' ' + getBodyText().slice(0, 4000);
  if (/\bflagship\b/i.test(tierSrc))              performanceTier = 'Flagship';
  else if (/\bhigh[\s-]end\b|\btop[\s-]end\b/i.test(tierSrc)) performanceTier = 'Flagship';
  else if (/\bpremium\b/i.test(tierSrc))          performanceTier = 'Premium';
  else if (/\bupper.?mid[\s-]range\b/i.test(tierSrc)) performanceTier = 'Upper Mid-Range';
  else if (/\bmid[\s-]range\b/i.test(tierSrc))    performanceTier = 'Mid-Range';
  else if (/\bupper[\s-]entry\b/i.test(tierSrc))  performanceTier = 'Upper Entry-Level';
  else if (/\bentry[\s-]level\b/i.test(tierSrc))  performanceTier = 'Entry-Level';
  else if (/\bbudget\b/i.test(tierSrc))            performanceTier = 'Budget';

  // ══════════════════════════════════════════════════════════════════════════
  //  IMAGES
  //  NBC processor pages show processor images as lightbox links:
  //    <a href="/fileadmin/..." data-fancybox="device_img" class="lightbox">
  //      <picture><source srcset="...1x, ...2x"><img src="..."></picture>
  //    </a>
  //  og:image meta tag also reliably contains the main processor image.
  // ══════════════════════════════════════════════════════════════════════════
  const images: string[] = [];
  const imgSeen = new Set<string>();

  function addProcImage(rawUrl: string): void {
    if (!rawUrl) return;
    const url = rawUrl.startsWith('http') ? rawUrl
      : rawUrl.startsWith('/') ? 'https://www.notebookcheck.net' + rawUrl
      : 'https://www.notebookcheck.net/' + rawUrl;
    if (!/\.(jpe?g|png|webp|gif)(\?.*)?$/i.test(url)) return;
    if (!/\/fileadmin\//i.test(url)) return;
    if (/\/templates\/|\/awards?\/|\/png_rating\/|\/svg\//i.test(url)) return;
    if (/clear\.gif|spacer\.|pixel\.(gif|png)/i.test(url)) return;
    const key = url.toLowerCase();
    if (imgSeen.has(key) || images.length >= 15) return;
    imgSeen.add(key);
    images.push(url);
  }

  // Primary: NBC lightbox links (data-fancybox="device_img") — full-resolution href
  $('a.lightbox[data-fancybox="device_img"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    addProcImage(href);
  });

  // Also capture 2x srcset images from the same lightbox elements
  $('a.lightbox[data-fancybox="device_img"] source').each((_, el) => {
    const srcset = $(el).attr('srcset') || '';
    // srcset format: "url1 1x, url2 2x"
    srcset.split(',').forEach(part => {
      const urlPart = part.trim().split(/\s+/)[0];
      if (urlPart) addProcImage(urlPart);
    });
  });

  // og:image fallback (usually the main processor photo)
  const ogImg = $('meta[property="og:image"]').attr('content') || '';
  addProcImage(ogImg);

  // Fallback: <figure> elements
  if (images.length === 0) {
    $('figure').each((_, fig) => {
      const href = $(fig).find('a[href]').first().attr('href') || '';
      if (href) addProcImage(href);
    });
  }

  // Fallback: any <a href> pointing to fileadmin images
  if (images.length === 0) {
    $('a[href]').each((_, el) => {
      const href = $(el).attr('href') || '';
      addProcImage(href);
    });
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  ASSEMBLE FINAL DATA OBJECT
  // ══════════════════════════════════════════════════════════════════════════
  const result: NBCProcessorData = {
    name:             rawTitle || procName || '',
    subtitle:         rawSubtitle,
    sourceUrl:        pageUrl,
    reviewUrl:        pageUrl,
    pageFound:        { name: procName || rawTitle || '', url: pageUrl },

    manufacturer,
    category,
    series,
    seriesProcessors,
    architecture,
    isa,
    processNode,
    dieSizeMm2,
    transistorCount,
    announcedDate,
    codename,

    totalCores,
    totalThreads,
    cpuClusters,
    baseClockMHz,
    boostClockMHz,
    l2CacheTotal,
    l3Cache,
    l4Cache,
    systemLevelCache,

    gpu,
    npu,
    memory,
    connectivity,
    media,
    security,
    power,

    specs:        rawSpecs,
    benchmarks,
    devicesUsing,
    description,
    performanceTier,
    images,
  };

  procSetCache(ck, result);
  return result;
}

// ══════════════════════════════════════════════════════════════════════════════
//  PUBLIC API
// ══════════════════════════════════════════════════════════════════════════════

/** Full processor data: search + scrape pipeline */
export async function getNotebookCheckProcessor(query: string): Promise<NBCProcessorData | NBCProcessorError | null> {
  const ck = `proc:full:${PROC_CACHE_VERSION}:${query.toLowerCase().trim()}`;
  const cached = await procGetCacheAs<NBCProcessorData | NBCProcessorError>(ck);
  if (cached) return cached;

  const page = await searchProcessor(query);
  if (!page) return null;

  try {
    const details = await scrapeNotebookCheckProcessor(page.url, page.name);
    const result: NBCProcessorData = { ...details, pageFound: { name: page.name, url: page.url }, reviewUrl: page.url };
    procSetCache(ck, result);
    return result;
  } catch (e: any) {
    const err: NBCProcessorError = { error: e?.message ?? String(e), query, code: e?.response?.status };
    return err;
  }
}

/** Search-only — returns ranked list of matching NBC processor pages */
export async function searchNotebookCheckProcessors(query: string): Promise<ProcessorSearchResult[]> {
  const ck = `proc:suggestions:${PROC_CACHE_VERSION}:${query.toLowerCase().trim()}`;
  const oq = query.trim();
  const nq = normalizeProcQuery(query);

  // Cache first — same fix as searchProcessor()
  const cached = await procGetCacheAs<ProcessorSearchResult[]>(ck);
  if (cached) return cached;

  const results = await searchProcViaSearXNG(nq, oq);
  if (!results.length) return [];

  const sorted = results.sort((a, b) => b.score - a.score);
  procSetCache(ck, sorted);
  return sorted;
}

/** Scrape a known NBC processor page URL directly */
export async function scrapeProcessorByUrl(url: string, name?: string): Promise<NBCProcessorData> {
  return scrapeNotebookCheckProcessor(url, name);
}

/** Export cache version for external use (e.g. cache invalidation) */
export { PROC_CACHE_VERSION };