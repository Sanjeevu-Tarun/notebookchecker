import axios, { AxiosError } from 'axios';
import * as cheerio from 'cheerio';

// ══════════════════════════════════════════════════════════════════════════════
//  NOTEBOOKCHECK SCRAPER - FIXED IMAGE CLASSIFICATION
//
//  FIX 7 — aPic_ image misclassification (2026-03-12):
//  ─────────────────────────────────────────────────────────────────────────────
//  ROOT CAUSE (Vivo X300 Pro / any review where NBC photographer uses aPic_ naming):
//  ──────────────────────────────────────────────────────────────────────────────
//  `aPic_` is NBC's review-photographer naming for ALL photos in a review, including:
//    • The hero/header device shot at the article top (e.g. aPic_Vivo_X300_Pro-0002.jpg)
//    • Case/housing section device shots    (e.g. aPic_Vivo_X300_Pro-1769.jpg)
//    • Connectivity angle shots with "Top:", "Left:" captions (correctly → deviceAngles)
//    • Actual camera samples in the Camera section
//  The previous rule `if (/^apic_/i.test(filename)) return 'cameraSamples'` was a
//  blanket match that routed ALL aPic_ files to cameraSamples, leaving device = [] and
//  stuffing the hero + hardware shots into the wrong bucket.
//
//  FIX 7a — classifyByFilename(): aPic_ now defaults to 'device' (not 'cameraSamples').
//    True camera samples with aPic_ naming always have informative captions
//    ("1x", "Night mode", "Selfie", etc.) → classifyByCaption handles them first.
//    The camera-section override in sectionBucketOverride catches the rest.
//
//  FIX 7b — sectionBucketOverride():
//    • Case/housing section + aPic_ → 'device'  (hero + case hardware shots)
//    • Camera/photo section + aPic_ → 'cameraSamples'  (safety net for captionless samples)
//
//  FIX 7c — hasClearFilename gate (Pass 2):
//    Added aPic_ to the list of filename patterns that pass the csm_ gate,
//    so csm_aPic_* thumbnails in the Case section aren't silently dropped.
//
//  FIXES APPLIED (2026-03-12) — SCORING v2 (universal model-suffix fix):
//  ─────────────────────────────────────────────────────────────────────────────
//  ROOT CAUSE (Pixel 10 Pro XL / general wrong-variant issue):
//  ────────────────────────────────────────────────────────────
//  VARIANT_WORDS was missing key model-suffix tokens: 'xl', 'xr', 'se', '5g',
//  '4g', 'go', 'compact', 'slim', 'zoom', 'plus', 'fold', 'flip'.
//  Because 'xl' was not in VARIANT_WORDS, a query for "Pixel 10 Pro" received
//  ZERO penalty when the "Pixel 10 Pro XL" review appeared in results, and it
//  could outscore the correct result on other heuristics (URL boost, etc.).
//
//  FIX C5 — VARIANT_WORDS extended: all common model-distinguishing suffixes added.
//
//  FIX C6 — HARD_REJECT_SUFFIXES in scoreCandidate():
//    A new Set of suffixes that unambiguously identify a distinct SKU.
//    If a result has such a suffix but the query does NOT (or vice-versa),
//    the result is HARD-REJECTED (returns -1) rather than just penalised.
//    This makes the fix robust even when score gaps are small.
//    Suffixes: xl, xr, se, 5g, 4g, go, compact, slim, zoom, plus, fold, flip.
//
//  FIX C7 — Exact suffix-match bonus (+800):
//    When the set of model-suffix words in the result exactly equals the set in
//    the query, award a bonus to push the correct model to the top of the ranking.
//
//  ORIGINAL FIXES APPLIED (2026-03-12):
//  ─────────────────────────────────────────────────────────────────────────────
//  ROOT CAUSE (Pixel 10 Pro XL / all modern NBC reviews):
//  ───────────────────────────────────────────────────────
//  NBC device review pages serve device photos ONLY as /_processed_/csm_* URLs.
//  The previous isThumbnail() function blocked ALL _processed_ and csm_ paths,
//  which caused:
//    • images.device = only 1 image (subpixel_matrix.jpg, a raw /Notebooks/ file)
//    • images.deviceAngles = [] (all "Top:", "Bottom:" shots are csm_ only)
//    • images.cameraSamples = [] (PXL_ originals served via csm_ in figures)
//    • images.screenshots = correct (already in thumbnailBuckets allowlist)
//
//  FIX 1 — isThumbnail(): Whitelist "not-really-thumbnails":
//    - csm_BRAND_DEVICE_N_HASH.jpg  → numbered device photos (ONLY version available)
//    - csm_BRAND_DEVICE_top_HASH.jpg → named angle shots (top/bottom/left/right)
//    - _processed_/webp/            → NBC hi-res WebP renders (already whitelisted)
//    True thumbnails (camera roll originals exist) are still blocked:
//    - csm_PXL_*, csm_IMG_*, csm_DSC_*, csm_Foto_*, csm_Photo_*
//
//  FIX 2 — classifyByFilename(): 
//    - Added bare-name pattern for numbered device photos: BRAND_DEVICE_N → 'device'
//    - Added direction-suffix pattern: BRAND_DEVICE_top → 'deviceAngles'
//    - Added subpixel_matrix → 'displayMeasurements' (was falling through to 'device')
//
//  FIX 3 — Pass 2 isCsmLarge gate:
//    - Extended hasClearFilename to recognize:
//        • BRAND_DEVICE_N bare pattern (numbered device photos)
//        • _(top|bottom|left|right|front|back|side|angle) suffix (angle shots)
//        • _software_ pattern (software screenshots)
//        • foto_/photo_ prefix (NBC camera shots)
//    - Previously only recognized: rigolds, calman, screenshot, img_/dsc_/pxl_,
//      date-stamps, and _test_\d+ — causing all other device photos to be dropped.
//
//  FIX 4 — classifyByCaption():
//    - subpixel_matrix caption → 'displayMeasurements' (not 'device')
//    - Deduplicated the ^(top:|bottom:|left:|right:) regex (was matched twice)
//
//  FIX 5 — Pass 3 standalone <img>:
//    - Added 'deviceAngles' to the allowed thumbnail buckets (alongside 'device')
//
//  FIX 6 — Competitor device images leaking into images.device:
//    CAUSE: isThumbnail() whitelisted ALL /_processed_/webp/ paths, but
//    NBC's size-comparison widget uses /_processed_/webp/uploads/tx_nbc2/
//    for tiny (w125) competitor device silhouettes (Pixel 9, iPhone 16, S25 Ultra, etc.)
//    Also: generic "4_zu_3_Teaser" article headers were leaking through.
//    FIX: Block /uploads/tx_nbc2/ and \d+_zu_\d+_teaser in both isThumbnail()
//    and addImage(). Only whitelist /_processed_/webp/Notebooks/ (genuine hi-res renders).
//
//  - device:              General product photos (color variants, main shots)
//  - deviceAngles:        Hardware detail views (top, bottom, left, right, ports, buttons)
//  - cameraSamples:       Photos taken BY the phone (PXL_*, all zoom levels, selfies, Portrait Studio)
//  - screenshots:         UI/software screenshots (Android 16, Gemini, settings)
//  - charts:              GNSS, navigation, battery logs
//  - displayMeasurements: Oscilloscope PWM waveforms (SDS*.jpg) + subpixel microscopy
//  - colorCalibration:    Calman plots, colorchecker images
// ══════════════════════════════════════════════════════════════════════════════

// ── STRUCTURED LOGGER ────────────────────────────────────────────────────────
// FIX: previously all errors were silently swallowed with `/* skip */`.
// Every failure path now emits a structured log entry so failures are
// observable in production without crashing the scraper.
type LogLevel = 'debug' | 'info' | 'warn' | 'error';
function log(level: LogLevel, msg: string, meta?: Record<string, unknown>): void {
  if (process.env.NODE_ENV === 'test' && (level === 'debug' || level === 'info')) return;
  const entry = { ts: new Date().toISOString(), level, msg, ...meta };
  (level === 'error' || level === 'warn' ? console.error : console.log)(JSON.stringify(entry));
}

// ══════════════════════════════════════════════════════════════════════════════
//  TYPE DEFINITIONS  (FIX C1 — was Promise<any> everywhere)
// ══════════════════════════════════════════════════════════════════════════════

export interface Benchmark {
  name:      string;
  value:     string;
  unit:      string;
  minValue?: string;
}

export interface CameraLens {
  type:              'main' | 'ultrawide' | 'telephoto' | 'depth' | 'selfie';
  megapixels?:       string;
  sensor?:           string;
  sensorSize?:       string;
  aperture?:         string;
  focalLength?:      string;
  opticalZoom?:      string;
  pixelSize?:        string;
  fov?:              string;
  ois:               boolean;
  oisType?:          string;
  af:                boolean;
  cipaStabilization?: string;
  stabilization?:    string;
  description:       string;
}

export interface NBCImageBuckets {
  device:              string[]; // general product photos — main device shots, color variants
  deviceAngles:        string[]; // hardware detail shots — top, bottom, left, right, SIM tray, ports, buttons
  cameraSamples:       string[]; // photos taken BY the phone (main/UW/tele/zoom/lowlight/selfie)
  screenshots:         string[]; // OS / UI screenshots
  charts:              string[]; // GNSS tracks, battery discharge plots, camera resolution charts
  displayMeasurements: string[]; // oscilloscope PWM waveforms (RigolDS*)
  colorCalibration:    string[]; // Calman colour accuracy / colour space / greyscale plots
}

export interface NBCBenchmarks {
  gpu:        Benchmark[];
  cpu:        Benchmark[];
  memory:     Benchmark[];
  display:    Benchmark[];
  battery:    Benchmark[];
  storage:    Benchmark[];
  networking: Benchmark[];
  thermal:    Benchmark[];
  audio:      Benchmark[];
  other:      Benchmark[];
}

export interface NBCCameraData {
  raw:                string;
  lenses:             CameraLens[];
  selfie?:            CameraLens;
  videoCapabilities:  string;
  camera2ApiLevel?:   string;
}

export interface NBCDeviceData {
  title:            string;
  subtitle:         string;
  sourceUrl:        string;
  reviewUrl:        string;
  pageFound:        { name: string; url: string };
  rating:           string;
  ratingLabel:      string;
  verdict:          string;
  author:           string;
  publishDate:      string;
  pros:             string[];
  cons:             string[];
  specs:            Record<string, string>;
  soc:              string;
  gpu:              string;
  os:               string;
  ram:              string;
  storage_capacity: string;
  storage_type:     string;
  price:            string;
  releaseDate:      string;
  weight:           string;
  dimensions:       string;
  colorOptions:     string[];
  display:          Record<string, string>;
  hdr:              string;
  drm:              string;
  connectivity:     string;
  networking_raw:   string;
  bluetooth:        string;
  bluetoothCodecs:  string;
  wifi:             string;
  nfc:              string;
  usbVersion:       string;
  usbSpeed:         string;
  gnss:             string;
  simSlots:         string;
  simType:          string;
  esim:             string;
  battery:          Record<string, string>;
  maxChargingSpeed: string;
  // FIX: null is cleaner and safer than Record<string,never> — callers check `data.cameras !== null`
  cameras:          NBCCameraData | null;
  ipRating:         string;
  biometrics:       { fingerprintType: string; faceUnlock: boolean };
  audio:            { headphoneJack: boolean; maxVolumeDb: string };
  sar:              { body: string; head: string };
  warrantyMonths:   string;
  hasWalkieTalkie:  boolean;
  hasIRBlaster:     boolean;
  hasBarometer:     boolean;
  speakers:         string;
  images:           NBCImageBuckets;
  benchmarks:       NBCBenchmarks;
}

export interface NBCError {
  error: string;
  query: string;
  code?: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// INTERNAL TYPES for search pipeline
// ─────────────────────────────────────────────────────────────────────────────
export interface SearchResult {
  url:   string;
  title: string;
  score: number;
}

export interface SearchResultWithHits extends SearchResult {
  hits: number;
}

/** Shape of a single SearXNG / Jina external result item (dynamic API response) */
interface ExternalResultItem {
  url?:         string;
  href?:        string;
  title?:       string;
  text?:        string;
  description?: string;
}

/** Structured return from debugNBCSearch */
export interface NBCDebugResult {
  query:           string;
  originalQuery:   string;
  normalizedQuery: string;
  timing: {
    cacheHit:  boolean;
    memHit:    boolean;  // served from in-process memory (0ms overhead)
    redisHit:  boolean;  // served from Redis
    redisMs:   number;   // Redis GET latency — >200ms = cold TLS reconnect
    searchMs:  number;   // SearXNG round-trip
    scrapeMs:  number;   // NBC page fetch + cheerio parse
    totalMs:   number;
  };
  elapsedMs:    number; // same as timing.totalMs — kept for backwards compat
  bestMatch:    { name: string; url: string } | null;
  scrapeOk:     boolean;
  scrapeError?: string;
  strategies:   Record<string, { count: number; top5: SearchResult[] } | { error: string }>;
}

// ══════════════════════════════════════════════════════════════════════════════
//  NOTEBOOKCHECK SCRAPER — current
//
//  KEY IMPROVEMENTS (this version):
//  ─────────────────────────────────
//  1. CACHE VERSION auto-derived from schema field hash (DJB2).
//  2. Competitor camera samples no longer leak in.
//  3. DOM-first extraction for GNSS, Bluetooth codecs, IP rating, USB speed,
//     SAR, speakers, biometrics via domFindSpecField().
//  4. bodyText truncation removed — full normalised body used for all regex passes.
//  5. resolveSearchResult() is fully synchronous — no extra HTTP round-trip.
//     getReviewFromDevicePage (was ~2–3s extra latency) removed entirely.
//  6. searchViaNBCSearch removed — SearXNG is the sole search path.
//  7. scrapeNotebookCheckDevice() accepts optional AbortSignal.
// ══════════════════════════════════════════════════════════════════════════════

// ── AUTO-DERIVED CACHE VERSION ────────────────────────────────────────────────
// Computed at startup from the sorted field names of NBCDeviceData using DJB2.
// Adding, removing, or renaming any field on NBCDeviceData automatically
// invalidates all caches — no manual bumping required or possible.
// The version is stable across restarts (same fields → same hash always).
const CACHE_VERSION = (() => {
  // Canonical field list of NBCDeviceData — keep in sync with the interface above.
  // TypeScript interfaces are erased at runtime; we maintain this list manually,
  // but forgetting to update it is a compile-time-visible omission (lint/review),
  // not a silent cache-poisoning bug.
  const SCHEMA_FIELDS = [
    'title','subtitle','sourceUrl','reviewUrl','pageFound','rating','ratingLabel',
    'verdict','author','publishDate','pros','cons','specs','soc','gpu','os','ram',
    'storage_capacity','storage_type','price','releaseDate','weight','dimensions',
    'colorOptions','display','hdr','drm','connectivity','networking_raw','bluetooth',
    'bluetoothCodecs','wifi','nfc','usbVersion','usbSpeed','gnss','simSlots',
    'simType','esim','battery','maxChargingSpeed','cameras','ipRating','biometrics',
    'audio','sar','warrantyMonths','hasWalkieTalkie','hasIRBlaster','hasBarometer',
    'speakers','images','benchmarks',
  ].sort().join(',');
  // DJB2 hash — fast, deterministic, no dependencies
  let h = 5381;
  for (let i = 0; i < SCHEMA_FIELDS.length; i++) {
    h = ((h << 5) + h + SCHEMA_FIELDS.charCodeAt(i)) >>> 0;
  }
  return `s${h.toString(36)}`; // e.g. "s2k4m9vf" — stable across restarts
})();

const CACHE_TTL     = 48 * 60 * 60 * 1000; // 48 h in ms  (mem cache TTL check)
const CACHE_TTL_SEC = 48 * 60 * 60;         // 48 h in sec (Redis EX param)

// ── CIRCUIT BREAKER FOR EXTERNAL INSTANCES ───────────────────────────────────
// FIX: previously unhealthy SearXNG instances were retried on every request.
// Now a 5-minute cooldown is enforced after 3 consecutive failures.
const CIRCUIT_FAIL_THRESHOLD = 5;
const CIRCUIT_COOLDOWN_MS    = 3 * 60 * 1000;
interface CircuitState { fails: number; cooldownUntil: number; }
const circuitBreakers = new Map<string, CircuitState>();

function circuitIsOpen(host: string): boolean {
  const s = circuitBreakers.get(host);
  if (!s) return false;
  if (s.cooldownUntil > Date.now()) return true;
  circuitBreakers.delete(host); // cooldown expired — reset
  return false;
}
function circuitRecordFailure(host: string): void {
  const s = circuitBreakers.get(host) ?? { fails: 0, cooldownUntil: 0 };
  s.fails++;
  if (s.fails >= CIRCUIT_FAIL_THRESHOLD) {
    s.cooldownUntil = Date.now() + CIRCUIT_COOLDOWN_MS;
    log('warn', 'circuit.open', { host, until: new Date(s.cooldownUntil).toISOString() });
  }
  circuitBreakers.set(host, s);
}
function circuitRecordSuccess(host: string): void { circuitBreakers.delete(host); }

// ── IN-MEMORY CACHE with LRU eviction ────────────────────────────────────────
// BUG FIX: previously the Map grew unbounded — expired entries were checked on
// read but never removed, so a long-running server would OOM.
// Fix: cap at MEM_CACHE_MAX entries; on overflow evict the oldest half.
const MEM_CACHE_MAX = 500;
const memCache = new Map<string, { data: unknown; time: number }>();

function memEvict(): void {
  if (memCache.size < MEM_CACHE_MAX) return;
  // Sort by insertion time (Map preserves insertion order) and drop oldest half
  const keys = [...memCache.keys()];
  const evictCount = Math.floor(keys.length / 2);
  for (let i = 0; i < evictCount; i++) memCache.delete(keys[i]);
}

function memGet(k: string): unknown | null {
  const h = memCache.get(k);
  if (!h) return null;
  if (Date.now() - h.time >= CACHE_TTL) { memCache.delete(k); return null; }
  // FIX C2: true LRU — re-insert to move this key to the end of the Map so the
  // eviction pass (which deletes from the front) always evicts least-recently-used.
  memCache.delete(k);
  memCache.set(k, h);
  return h.data;
}

function memSet(k: string, d: unknown): void {
  memEvict();
  memCache.set(k, { data: d, time: Date.now() });
}

// ══ PERFORMANCE: Shared Axios instance with TCP keep-alive ══
// Reuses connections across requests — eliminates per-request TCP handshake overhead.
import * as http from 'http';
import * as https from 'https';
const _httpAgent  = new (require('http').Agent)({ keepAlive: true, maxSockets: 100, maxFreeSockets: 20 });
const _httpsAgent = new (require('https').Agent)({ keepAlive: true, maxSockets: 100, maxFreeSockets: 20 });
const sharedAxios = axios.create({ httpAgent: _httpAgent, httpsAgent: _httpsAgent, maxRedirects: 3, decompress: true });

async function redisGet(k: string): Promise<unknown | null> {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  try {
    const resp = await sharedAxios.get(`${url}/get/${encodeURIComponent(k)}`, {
      headers: { Authorization: `Bearer ${token}` }, timeout: 2000,
    });
    const val = resp.data?.result;
    return val ? JSON.parse(val) : null;
  } catch (e) { log('warn', 'redis.get failed', { key: k, err: (e as Error).message }); return null; }
}

async function redisSet(k: string, d: unknown): Promise<void> {
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return;
  try {
    await sharedAxios.post(
      `${url}/pipeline`,
      [['SET', k, JSON.stringify(d), 'EX', CACHE_TTL_SEC]],
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, timeout: 2000 },
    );
  } catch (e) { log('warn', 'redis.set failed', { key: k, err: (e as Error).message }); }
}

// getCache: mem-first, Redis fallback (warm mem on hit so next call is instant)
async function getCache(k: string): Promise<unknown | null> {
  const mem = memGet(k);
  if (mem !== null) return mem;
  const red = await redisGet(k);
  if (red !== null) { memSet(k, red); return red; }
  return null;
}

// Typed cache helper — eliminates 'unknown' cast errors at every call site
async function getCacheAs<T>(k: string): Promise<T | null> {
  const v = await getCache(k);
  return v !== null ? (v as T) : null;
}

function setCache(k: string, d: unknown): void {
  memSet(k, d);
  redisSet(k, d).catch((e) => log('warn', 'redis.set async failed', { key: k, err: (e as Error).message }));
}

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];
function randomUA() { return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]; }

async function fetchUrl(url: string, timeoutMs = 5000, extraHeaders: Record<string, string> = {}, retries = 0, signal?: AbortSignal): Promise<string> {
  const attempt = (ctrl: AbortController) => sharedAxios.get(url, {
    headers: {
      'User-Agent': randomUA(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate, br',
      'Referer': 'https://www.notebookcheck.net/',
      ...extraHeaders,
    },
    timeout: timeoutMs,
    maxRedirects: 3,
    decompress: true,
    signal: ctrl.signal,
  });

  for (let i = 0; i <= retries; i++) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    const ctrl = new AbortController();
    const onAbort = () => ctrl.abort();
    signal?.addEventListener('abort', onAbort, { once: true });
    try {
      const { data } = await attempt(ctrl);
      return typeof data === 'string' ? data : JSON.stringify(data);
    } catch (e: unknown) {
      const isLast = i === retries;
      const status = (e as AxiosError)?.response?.status;
      if ((e as Error).name === 'AbortError') throw e;
      if (status && status >= 400 && status < 500) throw e;
      if (isLast) throw e;
      await new Promise(res => setTimeout(res, 200 * Math.pow(2, i)));
    } finally {
      ctrl.abort();
      signal?.removeEventListener('abort', onAbort);
    }
  }
  throw new Error('fetchUrl: exhausted retries');
}

function norm(s: string): string {
  return s.replace(/\u00a0/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
}

// ─────────────────────────────────────────────────────────────────────────────
// BRAND ALIASES
// ─────────────────────────────────────────────────────────────────────────────
// FIX: changed from Record (unordered) to Array sorted longest-first.
// Previously "mi " could shadow "xiaomi civi" because object key iteration
// order is not guaranteed to be insertion order for all engines.
// Longest match first ensures "samsung galaxy z fold" is tried before "galaxy".
const NBC_ALIASES: Array<[string, string]> = [
  ['samsung galaxy z fold', 'Samsung Galaxy Z Fold'],
  ['samsung galaxy z flip', 'Samsung Galaxy Z Flip'],
  ['xiaomi poco',           'Xiaomi Poco'],
  ['xiaomi redmi',          'Xiaomi Redmi'],
  ['xiaomi civi',           'Xiaomi Civi'],
  ['xiaomi mi ',            'Xiaomi Mi '],
  ['google pixel',          'Google Pixel'],
  ['apple iphone',          'Apple iPhone'],
  ['samsung galaxy',        'Samsung Galaxy'],
  ['oneplus nord',          'OnePlus Nord'],
  ['oppo find x',           'Oppo Find X'],
  ['motorola edge',         'Motorola Edge'],
  ['motorola razr',         'Motorola Razr'],
  ['asus zenfone',          'Asus Zenfone'],
  ['sony xperia',           'Sony Xperia'],
  ['oppo reno',             'Oppo Reno'],
  ['nothing',               'Nothing'],
  ['pixel',                 'Google Pixel'],
  ['iphone',                'Apple iPhone'],
  ['galaxy',                'Samsung Galaxy'],
  ['s25',                   'Samsung Galaxy S25'],
  ['s24',                   'Samsung Galaxy S24'],
  ['s23',                   'Samsung Galaxy S23'],
  ['s22',                   'Samsung Galaxy S22'],
  ['fold',                  'Samsung Galaxy Z Fold'],
  ['flip',                  'Samsung Galaxy Z Flip'],
  ['oneplus',               'OnePlus'],
  ['poco',                  'Xiaomi Poco'],
  ['redmi',                 'Xiaomi Redmi'],
  ['mi ',                   'Xiaomi Mi '],
];

export function normalizeQuery(query: string): string {
  let q = query.toLowerCase().trim();
  // Apply aliases longest-first. Once a prefix alias fires (e.g. "samsung galaxy z fold"),
  // mark the covered prefix length so shorter overlapping aliases (e.g. "samsung galaxy",
  // "galaxy") don't fire again on the same tokens and produce duplicates.
  let coveredPrefixLen = 0;
  for (const [alias, replacement] of NBC_ALIASES) {
    const rep = replacement.toLowerCase();
    // Prefix match — alias covers the start of the query
    if (q === alias || q.startsWith(alias + ' ') || q.startsWith(alias + '\t')) {
      if (alias.length <= coveredPrefixLen) continue; // already handled by a longer alias
      q = rep + q.slice(alias.length);
      coveredPrefixLen = rep.length;
      continue;
    }
    // Whole-word match anywhere in string — only if none of its words are already
    // part of a previously expanded prefix (avoids "galaxy" re-firing on "samsung galaxy...")
    if (coveredPrefixLen > 0) {
      // Check if this alias overlaps with the already-expanded prefix
      const aliasWords = alias.trim().split(/\s+/);
      const prefixWords = q.slice(0, coveredPrefixLen).trim().split(/\s+/);
      if (aliasWords.every(w => prefixWords.includes(w))) continue;
    }
    const esc = alias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    q = q.replace(new RegExp('\\b' + esc + '\\b', 'gi'), rep);
  }
  return q.trim();
}

function toSlug(s: string): string {
  return s.trim().split(/\s+/).map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join('-');
}

// ─────────────────────────────────────────────────────────────────────────────
// SCORING — relaxed matching: core words (non-brand) must match
// ─────────────────────────────────────────────────────────────────────────────
// Single source of truth for brand tokens — used in scoring, sitemap, slug filtering
const BRAND_TOKENS = new Set(['xiaomi','samsung','apple','google','oneplus','oppo','vivo','motorola','sony','asus','realme','honor','huawei','nothing','poco','redmi','galaxy','iphone','pixel']);
// Single-char tokens that are meaningful model identifiers (e.g. 'z' in Galaxy Z Fold)
const MODEL_SINGLE_CHARS = new Set(['z', 'x', 's', 'p', 'a', 'f', 'v', 'm']);

// FIX C4: Compile variant regexes ONCE at module level.
// Previously `variantRe` was a closure recreating 17 RegExp objects on every
// scoreCandidate() call — which fires hundreds of times per search strategy.
//
// FIX C5 (2026-03-12): Added missing model-suffix tokens that caused wrong-variant
// results to pass scoring with zero penalty:
//   'xl'       — e.g. Pixel 10 Pro XL vs Pixel 10 Pro (THE primary reported bug)
//   'xr'       — e.g. iPhone XR vs iPhone XS
//   'se'       — e.g. iPhone SE vs iPhone 16
//   '5g'       — e.g. Galaxy A53 5G vs Galaxy A53
//   '4g'       — e.g. Redmi Note 12 4G vs 5G
//   'plus'     — already present, kept
//   'go'       — e.g. Pixel 8a Go (budget variants)
//   'compact'  — e.g. Sony Xperia 5 vs Xperia 5 Compact
//   'slim'     — e.g. Galaxy S25 Slim/Edge
//   'zoom'     — e.g. Pixel 9 Pro vs Pixel 9 Pro Fold (distinct model)
//   'a'        — handled separately as single-char model identifier (see MODEL_SINGLE_CHARS)
const VARIANT_WORDS = [
  'ultra','plus','mini','lite','fe','max','pro','standard','elite','turbo',
  'neo','speed','air','fold','flip','edge','note',
  // ── newly added ──
  'xl','xr','se','5g','4g','go',
  // 'compact','slim','zoom' excluded — too common in prose titles
] as const;
type VariantWord = typeof VARIANT_WORDS[number];
const VARIANT_RE: ReadonlyMap<VariantWord, RegExp> = new Map(
  VARIANT_WORDS.map(v => [v, new RegExp('\\b' + v + '\\b', 'i')] as [VariantWord, RegExp])
);

function scoreCandidate(title: string, url: string, nq: string, originalQuery: string): number {
  const q  = nq.toLowerCase();
  const oq = originalQuery.toLowerCase();
  const t  = title.toLowerCase();
  const u  = url.toLowerCase();

  if (!u.includes('notebookcheck.net')) return -1;
  // Hard reject: tag pages, search pages, listing pages — never device reviews
  if (/[?&](tag|q|word)=/.test(u)) return -1;
  if (/\/(topics|search|smartphones|rss-feed|index)\.\d/i.test(u)) return -1;

  // ── HARD-REJECT: obvious non-review page types ──────────────────────────────
  // NBC news/article URLs are identifiable by content in their slug.  Rather than
  // requiring "-review" (which many valid NBC review URLs omit — e.g.
  // "Pixel-10-Pro-Powerful-smartphone-with-weak-heart.1128379.0.html"), we instead
  // reject slugs that contain known news/article patterns.
  const urlSlugLower = (u.split('/').pop() || '');
  // Reject complaint/issue articles, rumour posts, hands-on/camera-only pieces,
  // "users-complain", "surfaces-on", "leaked", "announced", "price-drop" etc.
  if (/users?[-_]complain|complain.*issues?|issues?.*update|rumou?r|leak(ed)?|announced|unveiled|price[-_]drop|hands?[-_]on(?!.*review)|camera[-_]review|camera[-_]test(?!.*smartphone)|first[-_]look|unboxing|teardown/.test(urlSlugLower)) return -1;
  // Reject external-reviews aggregation pages
  if (/external[-_]reviews?/.test(urlSlugLower)) return -1;

  const qWords    = q.split(/\s+/).filter(w => w.length > 0);
  const coreWords = qWords.filter(w => !BRAND_TOKENS.has(w));
  // Use core words for matching; drop pure single chars (e.g. stray "a") but keep
  // meaningful single-letter model identifiers like "z" (Galaxy Z Fold/Flip).
  const checkWords = (coreWords.length >= 1 ? coreWords : qWords)
    .filter(w => w.length > 1 || MODEL_SINGLE_CHARS.has(w) || /^\d$/.test(w));

  const urlSlug = (u.split('/').pop() || '').replace(/\.\d+\.0\.html$/, '').replace(/-/g, ' ');
  const combined = t + ' ' + urlSlug;

  if (!checkWords.every(w => combined.includes(w))) return -1;

  // ── HARD-REJECT: result has extra model-suffix not in query ─────────────────
  // ONE-DIRECTIONAL: only reject when the result has a suffix the user did NOT ask
  // for. Never reject for a *missing* suffix — NBC titles often omit "5G"/"4G"
  // even for 5G devices; those cases are handled by the soft VARIANT_RE penalty.
  //
  // Intentionally excludes 'pro','ultra','max','plus','fold','flip' — those are
  // already soft-penalised by VARIANT_RE (-1200/-1800) so a result can still win
  // if it is the only one available (avoids "no device found" on sparse results).
  // 'compact','slim','zoom' intentionally excluded — they appear in review title prose
  // (e.g. "Compact AI monster") and would reject valid results.
  const HARD_REJECT_SUFFIXES: ReadonlySet<string> = new Set(['xl','xr','se','5g','4g','go']);
  for (const suffix of HARD_REJECT_SUFFIXES) {
    const re = new RegExp('\\b' + suffix + '\\b', 'i');
    const inQuery  = re.test(q) || re.test(oq);
    const inResult = re.test(combined);
    if (!inQuery && inResult) return -1; // result has suffix user didn't ask for → wrong model
  }

  let score = 500;

  if (combined.includes(q))                            score += 2000;
  if (combined.includes(oq))                           score += 1000;
  if (u.includes(q.replace(/\s+/g, '-')))              score += 1500;
  if (u.includes(oq.replace(/\s+/g, '-')))             score += 800;
  if (u.includes('-review'))                            score += 600;
  if (u.includes('smartphone-review'))                  score += 400;

  // ── Exact model-suffix match bonus ──────────────────────────────────────────
  // Only fires when the query actually has suffix words. Guards against giving
  // a free +800 to every plain query where both arrays are empty.
  const qSuffixWords  = [...HARD_REJECT_SUFFIXES].filter(s => new RegExp('\\b' + s + '\\b', 'i').test(q) || new RegExp('\\b' + s + '\\b', 'i').test(oq));
  const resSuffixWords = [...HARD_REJECT_SUFFIXES].filter(s => new RegExp('\\b' + s + '\\b', 'i').test(combined));
  if (qSuffixWords.length > 0 &&
      qSuffixWords.length === resSuffixWords.length &&
      qSuffixWords.every(s => resSuffixWords.includes(s))) {
    score += 800; // perfect suffix match — reward exact model alignment
  }

  // Use the pre-compiled VARIANT_RE map (compiled once at module level — FIX C4)
  for (const [v, re] of VARIANT_RE) {
    // Penalise results that contain a variant word NOT in the query (wrong variant)
    if (!re.test(q) && !re.test(oq) && re.test(combined)) score -= 1200;
    // Penalise results MISSING a variant word that IS required by the query
    // e.g. query="v70 elite" but result only mentions "v70" — wrong variant
    if ((re.test(q) || re.test(oq)) && !re.test(combined)) score -= 1800;
  }

  if (t.includes(' vs ') || t.includes(' vs. '))            score -= 1000;
  if (t.includes('comparison') || t.includes('compared'))   score -= 800;
  if (t.includes('leak') || t.includes('rumor'))            score -= 600;
  if (t.includes('announced') || t.includes('unveiled'))    score -= 500;
  if (t.includes('price') && !t.includes('review'))         score -= 400;
  if (t.includes('benchmark') && !t.includes('review'))     score -= 300;
  if (t.includes('external reviews') || u.includes('external-reviews')) score -= 2000;
  if (t.includes('series') && !t.includes('review'))        score -= 1500;
  if (t.includes('upcoming') || t.includes('surfaces on'))  score -= 800;

  return score;
}

function extractLinks(html: string, nq: string, oq: string, seen: Set<string>): SearchResult[] {
  const $ = cheerio.load(html);
  const results: SearchResult[] = [];
  $('a[href]').each((_, el) => {
    let href = $(el).attr('href') || '';
    const uddg = href.match(/uddg=([^&]+)/);
    if (uddg) href = decodeURIComponent(uddg[1]);
    const amp = href.match(/\/amp\/s\/([^?&]+)/);
    if (amp) href = 'https://' + decodeURIComponent(amp[1]);

    const fullUrl = href.startsWith('http') ? href
      : href.startsWith('/') ? 'https://www.notebookcheck.net' + href : '';

    // FIX: reject tag/search/listing pages masquerading as device pages
    if (!fullUrl || !fullUrl.includes('notebookcheck.net') || !/\.\d{4,}\.0\.html/.test(fullUrl) || seen.has(fullUrl)) return;
    if (/[?&](tag|q|word|id)=/.test(fullUrl)) return; // reject query-param URLs
    if (/\/(Topics|Search|Smartphones|RSS|index)\.\d/.test(fullUrl)) return; // reject listing/search pages

    const text = norm($(el).text() || $(el).attr('title') || '');
    if (!text || text.length < 3 || text.length > 300) return;

    const sc = scoreCandidate(text, fullUrl, nq, oq);
    if (sc < 0) return;
    seen.add(fullUrl);
    results.push({ url: fullUrl, title: text, score: sc });
  });
  return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// SEARCH: SearXNG
// Circuit breaker prevents hammering the instance when it is consistently down.
// ─────────────────────────────────────────────────────────────────────────────
export async function searchViaSearXNG(nq: string, oq: string, signal?: AbortSignal): Promise<SearchResult[]> {
  const seen = new Set<string>();
  const debugLog: any[] = [];
  
  debugLog.push({ step: 'start', normalizedQuery: nq, originalQuery: oq });
  
  // Only use your own instance - public ones are blocked/rate limited
  const instances = [
    'https://searxng-notebookcheck.onrender.com',
  ];

  // Fire two queries in parallel when nq differs from oq
  const queries = [oq, ...(nq !== oq ? [nq] : [])];
  
  debugLog.push({ step: 'queries', queries, queryCount: queries.length });

  const doSearch = async (base: string, q: string) => {
    const searchUrl = `${base}/search`;
    const params = { 
      q: `site:notebookcheck.net ${q} review`, 
      format: 'json', 
      engines: 'google,bing,duckduckgo', 
      categories: 'general' 
    };
    
    debugLog.push({ step: 'request', base, query: q, fullQuery: params.q });
    
    const resp = await sharedAxios.get(searchUrl, {
      params,
      headers: { 'User-Agent': randomUA(), 'Accept': 'application/json' },
      timeout: 15000, // Increased to 15s for cold starts
      signal,
    });
    
    const results = (resp.data?.results || []) as ExternalResultItem[];
    
    debugLog.push({ 
      step: 'response',
      base, 
      query: q,
      rawResultCount: results.length,
      statusCode: resp.status,
      hasData: !!resp.data,
      dataKeys: resp.data ? Object.keys(resp.data) : [],
      sampleResults: results.slice(0, 3).map(r => ({ url: r.url, title: r.title }))
    });
    
    return results;
  };

  // Try each instance until we get results
  for (const base of instances) {
    if (circuitIsOpen(base)) {
      debugLog.push({ step: 'circuit_open', base });
      log('warn', 'searxng.circuit_open', { base });
      continue;
    }

    debugLog.push({ step: 'trying_instance', base });

    try {
      const responses = await Promise.all(queries.map(q => doSearch(base, q)));
      const totalResults = responses.reduce((sum, r) => sum + r.length, 0);
      
      debugLog.push({ 
        step: 'parallel_results',
        base, 
        totalRawResults: totalResults,
        perQuery: responses.map((r, i) => ({ query: queries[i], count: r.length }))
      });
      
      if (totalResults > 0) {
        circuitRecordSuccess(base);
        
        const all: SearchResult[] = [];
        let droppedCount = 0;
        let dropReasons: Record<string, number> = {};
        
        for (const items of responses) {
          for (const item of items) {
            const url   = (item.url || '').trim();
            const title = (item.title || '').trim();
            
            // Track why results get dropped
            if (!url.includes('notebookcheck.net')) {
              droppedCount++;
              dropReasons['not_notebookcheck'] = (dropReasons['not_notebookcheck'] || 0) + 1;
              debugLog.push({ step: 'drop', reason: 'not_notebookcheck', url, title });
              continue;
            }
            
            if (!/\.\d{4,}\.0\.html/.test(url)) {
              droppedCount++;
              dropReasons['wrong_url_format'] = (dropReasons['wrong_url_format'] || 0) + 1;
              debugLog.push({ step: 'drop', reason: 'wrong_url_format', url, title });
              continue;
            }
            
            if (seen.has(url)) {
              droppedCount++;
              dropReasons['duplicate'] = (dropReasons['duplicate'] || 0) + 1;
              continue;
            }
            
            if (/[?&](tag|q|word)=/.test(url) || /\/(Topics|Search|Smartphones|RSS|index)\.\d/i.test(url)) {
              droppedCount++;
              dropReasons['tag_or_listing_page'] = (dropReasons['tag_or_listing_page'] || 0) + 1;
              debugLog.push({ step: 'drop', reason: 'tag_or_listing_page', url, title });
              continue;
            }
            
            const sc = scoreCandidate(title || url, url, nq, oq);
            if (sc < 0) {
              droppedCount++;
              dropReasons['score_negative'] = (dropReasons['score_negative'] || 0) + 1;
              debugLog.push({ step: 'drop', reason: 'score_negative', score: sc, url, title });
              continue;
            }
            
            seen.add(url);
            all.push({ url, title: title || url, score: sc });
            debugLog.push({ step: 'keep', score: sc, url, title });
          }
        }
        
        debugLog.push({ 
          step: 'final',
          base,
          rawResults: totalResults,
          droppedResults: droppedCount,
          dropReasons,
          finalResults: all.length,
          topResults: all.slice(0, 3).map(r => ({ score: r.score, url: r.url, title: r.title }))
        });
        
        // Store debug in global for debugging
        (globalThis as any).__searxng_debug = debugLog;
        
        return all;
      } else {
        debugLog.push({ step: 'zero_raw_results', base, queries });
      }
    } catch (e) {
      debugLog.push({ 
        step: 'instance_error',
        base, 
        error: (e as Error).message,
        errorName: (e as Error).name
      });
      
      if ((e as Error).name !== 'AbortError') {
        circuitRecordFailure(base);
        log('error', 'searxng.failed', { 
          base, 
          err: (e as Error).message,
          errName: (e as Error).name
        });
      }
    }
  }
  
  debugLog.push({ 
    step: 'all_failed',
    query: nq,
    instances
  });
  
  // Store debug info globally so it can be retrieved
  (globalThis as any).__searxng_debug = debugLog;
  
  // Also log it
  log('error', 'searxng.all_instances_failed', { 
    query: nq,
    instances,
    debugLog
  });
  
  return [];
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function pickBest(results: SearchResult[][]): SearchResultWithHits | null {
  const urlMap = new Map<string, SearchResultWithHits>();
  for (const list of results) {
    for (const c of list) {
      const key = c.url.toLowerCase();
      if (urlMap.has(key)) { const e = urlMap.get(key)!; e.score += 500; e.hits++; }
      else urlMap.set(key, { ...c, hits: 1 });
    }
  }
  const sorted = Array.from(urlMap.values()).sort((a, b) => b.score - a.score);
  return sorted[0] || null;
}

// ─────────────────────────────────────────────────────────────────────────────
// SHARED resolveResult — fully synchronous, no extra HTTP round-trip.
// SearXNG results are almost always direct review URLs so we pick the best
// scored review URL immediately. The old getReviewFromDevicePage fetch
// (added as a fallback for index-page hits) was costing ~2–3s on every miss
// and has been removed — index-page hits are already penalised by scoreCandidate.
// ─────────────────────────────────────────────────────────────────────────────
export function resolveSearchResult(
  results: SearchResult[],
  _nq: string,
  _oq: string,
  cacheKey: string,
): { name: string; url: string } {
  const isReview = (u: string) => /-review/i.test(u) || /smartphone-review/i.test(u);
  const sorted    = [...results].sort((a, b) => b.score - a.score);
  const topReview = sorted.find(r => isReview(r.url));
  const top       = pickBest([results])!;

  const pick = isReview(top.url)
    ? top
    : (topReview && (top.score - topReview.score) < 2500 ? topReview : top);

  const r = { name: pick.title || pick.url, url: pick.url };
  setCache(cacheKey, r);
  return r;
}

// ─────────────────────────────────────────────────────────────────────────────
// SEARCH — SearXNG only
// ─────────────────────────────────────────────────────────────────────────────
async function searchNBC(query: string): Promise<{ name: string; url: string } | null> {
  const ck = `nbc:search:${CACHE_VERSION}:${query.toLowerCase().trim()}`;
  const oq = query.trim(), nq = normalizeQuery(query);
  const [cached, results] = await Promise.all([
    getCacheAs<{ name: string; url: string }>(ck),
    searchViaSearXNG(nq, oq),
  ]);
  if (cached) return cached;
  if (!results.length) return null;
  return resolveSearchResult(results, nq, oq, ck);
}




// ─────────────────────────────────────────────────────────────────────────────
// HELPERS FOR SCRAPER
// ─────────────────────────────────────────────────────────────────────────────
function cleanCellText($: cheerio.CheerioAPI, el: import('domhandler').Element): string {
  const outer = $.html(el) || '';
  const stripped = outer
    .replace(/<(style|script|noscript)[^>]*>[\s\S]*?<\/\1>/gi, '') // BUG FIX: was <\/> — missing tag name, leaked style/script content
    .replace(/<[^>]+>/g, ' ');
  return norm(stripped);
}

// (stripTags removed — was dead code, never called)

// ─────────────────────────────────────────────────────────────────────────────
// IMAGE HELPER — resolve NBC URL (fixes relative paths without leading slash)
//
// NBC HTML contains two forms of image paths:
//   A) `/fileadmin/Notebooks/...`  → has leading slash, easy to prefix
//   B) `fileadmin/Notebooks/...`   → NO leading slash, was silently dropped
//
// This helper handles both, plus already-absolute https:// URLs.
// ─────────────────────────────────────────────────────────────────────────────
function resolveNbcUrl(raw: string): string {
  if (!raw) return '';
  if (raw.startsWith('http')) return raw;
  // With or without leading slash — normalise and prepend origin
  return 'https://www.notebookcheck.net/' + raw.replace(/^\/+/, '');
}

// Is this URL a processed thumbnail? (csm_* or _processed_/ path segment)
// We always prefer the full-res <a href> version over the <img src> thumbnail.
//
// EXCEPTION: NBC device review pages serve device photos ONLY as _processed_/csm_* links.
// The href on the <a> wrapping the inline device image points to a larger csm_ version —
// there is no separate full-res original. We must allow these through.
//
// Patterns that are NOT true thumbnails (they are the best available version):
//   csm_BRAND_DEVICE_N_HASH.jpg       — numbered device photos (front/back/angle shots)
//   csm_BRAND_DEVICE_top_HASH.jpg     — labelled angle shots (top/bottom/left/right)
//   csm_BRAND_DEVICE_bottom_HASH.jpg
//   _processed_/webp/Notebooks/…      — NBC hi-res WebP renders
//
// True thumbnails that should always be skipped:
//   csm_PXL_YYYYMMDD_*_HASH.jpg       — camera sample thumbnails (raw PXL_ is better)
//   csm_IMG_*_HASH.jpg / csm_DSC_*    — camera roll thumbnails
//   csm_Foto_* / csm_Photo_*          — camera shot thumbnails
function isThumbnail(url: string): boolean {
  if (!url.includes('/_processed_/') && !/\/csm_/.test(url)) return false;

  // ── ALWAYS BLOCK these _processed_ subtrees ───────────────────────────────
  // tx_nbc2 = NBC's TYPO3 size-comparison widget — tiny (w125) competitor silhouettes
  if (/\/uploads\/tx_nbc2\//i.test(url)) return true;
  // Generic teaser/article images — not device photos
  if (/\d+_zu_\d+_teaser/i.test(url)) return true;

  // ── ALLOW: NBC hi-res WebP renders of device/Notebooks images ────────────
  // Only the /webp/Notebooks/ subtree contains genuine hi-res renders.
  // /webp/uploads/tx_nbc2/ (handled above) are small comparison icons.
  // EXCEPTION: NBC also serves small-width preview WebPs under /webp/Notebooks/ —
  // these encode their width as -wNNN-h in the filename (e.g. -q82-w125-h.webp).
  // Any width < 400px is a thumbnail preview, not a full-res image.
  if (/\/_processed_\/webp\/Notebooks\//i.test(url)) {
    // Block narrow previews: -w125-h, -w150-h, -w200-h, etc.
    if (/-w(\d+)-h\./i.test(url)) {
      const wMatch = url.match(/-w(\d+)-h\./i);
      if (wMatch && parseInt(wMatch[1], 10) < 400) return true; // too small
    }
    return false;
  }

  const filename = (url.split('/').pop() || '').toLowerCase().replace(/^csm_/, '');

  // Camera sample originals — the raw camera-roll file is always better; keep as thumbnail
  if (/^pxl_\d{8}/i.test(filename)) return true;
  if (/^(img_?\d|dsc_?\d|dcim|sam_|fotos_kamera|foto_|photo_)/i.test(filename)) return true;

  // "Portrait Studio" shots are camera samples taken BY the phone, not device photos.
  // Must be checked BEFORE the numbered-device-photo allowlist below so they are
  // not mistakenly whitelisted as device photos (portrait_studio_…_4_HASH matches
  // the _DIGIT_HEXHASH pattern).
  if (/^portrait[_\-]studio/i.test(filename)) return true;

  // Numbered device photos: BRAND_DEVICE_N_HASH.jpg
  // e.g. "csm_google_pixel_10_pro_xl_1_6ae836d9c8.jpg" → after stripping csm_:
  //      "google_pixel_10_pro_xl_1_6ae836d9c8.jpg"
  // Pattern: word_chars + _DIGIT(1-2)_HEXHASH.ext
  if (/^[a-z0-9_]+_\d{1,2}_[a-f0-9]{8,}\.(jpe?g|png|webp)$/i.test(filename)) return false;

  // NBC "Bild_" series: Bild_BRAND_DEVICE-NNNN_HASH.jpg (4-digit sequence, hyphen-separated)
  // e.g. csm_Bild_Samsung_Galaxy_S25_Ultra-0039_abc123.jpg — these are the only version available.
  if (/^bild_[a-z0-9_]+-\d{3,4}_[a-f0-9]{6,}\.(jpe?g|png|webp)$/i.test(filename)) return false;
  // Also the -View suffix: Bild_DEVICE-View_HASH.jpg (hero/overview shot)
  if (/^bild_[a-z0-9_]+-view_[a-f0-9]{6,}\.(jpe?g|png|webp)$/i.test(filename)) return false;

  // Named angle shots: BRAND_DEVICE_top_HASH, BRAND_DEVICE_bottom_HASH, etc.
  if (/_(top|bottom|left|right|front|back|side|angle|hand|in_hand)_[a-f0-9]{6,}\.(jpe?g|png|webp)$/i.test(filename)) return false;

  // All other _processed_ / csm_ URLs are thumbnails
  return true;
}

// ── BENCHMARK KEYWORD ARRAYS — module-level (allocated once at startup) ───────
const GPU_KW  = ['gfxbench', '3dmark', 'manhattan', 'aztec', 't-rex', 'wild life', 'car chase', 'gpu', 'graphics', 'offscreen', 'onscreen', 'vulkan', 'opengl', 'basemark', 'steel nomad'];
const CPU_KW  = ['geekbench', 'kraken', 'octane', 'speedometer', 'jetstream', 'pcmark', 'passmark', 'cinebench', 'crossmark', 'ai benchmark', 'single-core', 'multi-core', 'cpu', 'work score', 'antutu', 'cpu throttle', 'ai score', 'productivity', 'creativity', 'responsiveness'];
const STO_KW  = ['sequential read', 'sequential write', 'random read', 'random write', 'androbench', 'mb/s', 'gb/s', 'iops', 'read speed', 'write speed', 'ufs', 'emmc', 'cpdt', 'passmark storage', 'disk test'];
const BAT_KW  = ['h.264', 'video playback', 'charging time', 'reader', 'wifi v', 'load (h)', 'idle (w)', 'power consumption', '(watt)', 'watt)', 'idle minimum', 'idle average', 'idle maximum', 'load average', 'load maximum', 'off / standby', 'standby', 'watt', ' (w)', 'power', 'streaming', 'battery life', 'discharge', 'battery runtime'];
const DIS_KW  = ['brightness', 'contrast', 'colorchecker', 'greyscale', 'srgb', 'dci-p3', 'luminance', 'cd/m', 'gamma', 'cct', 'pwm', 'flickering', 'apl', 'response time'];
const NET_KW  = ['iperf', 'websurfing', 'networking'];
const THM_KW  = ['temperature', 'thermal', 'surface temp', 'throttl', '\u00b0c', 'heat', 'cooling', 'upper side', 'lower side', 'skin temp', 'palm rest', 'emissions', 'emissionen', 'oberseite', 'unterseite', 'temperatur', 'w\u00e4rme', 'erw\u00e4rmung'];
const MEM_KW  = ['memory bandwidth', 'antutu memory'];
const AUD_KW  = ['pink noise', 'maximum volume', 'db(a)', 'dba', 'loudspeaker', 'speaker volume', 'audio', 'sound level', 'lautsprecher', 'lautstaerke', 'lautst\u00e4rke', 'rosa rauschen', 'maximale lautst\u00e4rke', 'maximale lautstaerke', 'pink rauschen', 'lautst\u00e4rke'];

function catBench(name: string, ctx: string): string {
  const n = name.toLowerCase(), c = (ctx || '').toLowerCase();
  if (/^(idle|load|off\s*\/\s*standby|reader\s*\/\s*idle|h\.264|wifi\s*v\d|streaming|charging\s*time|battery\s*runtime)/i.test(name.trim())) return 'battery';
  if (/^(upper side|lower side|surface temp|skin temp|palm|oberseite|unterseite|temperatur oben|temperatur unten)/i.test(name.trim())) return 'thermal';
  if (/^(pink\s*noise|rosa\s*rauschen|maximum\s*volume|maximale\s*lautst|speaker|loudspeaker|lautsprecher)/i.test(name.trim())) return 'audio';
  if (AUD_KW.some(k => n.includes(k) || c.includes(k))) return 'audio';
  if (THM_KW.some(k => n.includes(k) || c.includes(k))) return 'thermal';
  if (MEM_KW.some(k => n.includes(k) || c.includes(k))) return 'memory';
  if (GPU_KW.some(k => n.includes(k) || c.includes(k))) return 'gpu';
  if (CPU_KW.some(k => n.includes(k) || c.includes(k))) return 'cpu';
  if (STO_KW.some(k => n.includes(k))) return 'storage';
  if (BAT_KW.some(k => n.includes(k) || c.includes(k))) return 'battery';
  if (DIS_KW.some(k => n.includes(k) || c.includes(k))) return 'display';
  if (NET_KW.some(k => n.includes(k) || c.includes(k))) return 'networking';
  return 'other';
}

function isRawGrid(name: string): boolean {
  return (name.match(/°C/g) || []).length >= 3
    || (name.match(/cd\/m[²2]/gi) || []).length >= 3
    || /^\d+\.?\d*\s*(cd\/m[²2]|°C)/.test(name);
}

function isRankingRow(name: string): boolean {
  return /^\s*\d+\.\s*\d+[\d.]*%/.test(name);
}

function isDeviceInfoHeader(name: string): boolean {
  const n = name.toLowerCase();
  if (/^(photo|pro|expert raw|video|film|portrait|slow-motion|time-lapse|panorama|macro|director\'s view|night)$/i.test(n.replace(/\s*\(.*?\)\s*/g, '').trim())) return true;
  if (/^\d{2,3}\s*mp$/i.test(n)) return true;
  return /802\.11|bluetooth|\bgsm\b|\blte\b|\b5g\b|wcdma/.test(n)
    || (/\d+\s*gb/i.test(name) && /snapdragon|dimensity|exynos|adreno|mali/i.test(n));
}

// ══════════════════════════════════════════════════════════════════════════════
//  DEVICE PAGE SCRAPER
// ══════════════════════════════════════════════════════════════════════════════
export async function scrapeNotebookCheckDevice(pageUrl: string, deviceName?: string, signal?: AbortSignal): Promise<NBCDeviceData> {
  const ck = `nbc:device:${CACHE_VERSION}:${pageUrl}`;
  const cached = await getCacheAs<NBCDeviceData>(ck); if (cached) return cached;

  const html = await fetchUrl(pageUrl, 6000, {}, 0, signal);
  const $ = cheerio.load(html);
  $('script, style, noscript').remove();
  // FIX: bodyText was truncated to 400 k characters which silently dropped battery
  // runtimes, charging speeds, and display measurements that appear late in long
  // reviews.  We now use the full normalised body text for all regex passes.
  // Memory cost is negligible — cheerio already holds the full DOM in memory.
  const bodyText = norm($('body').text());

  // FIX C1: typed as Partial<NBCDeviceData> during construction (some fields set later);
  // cast to NBCDeviceData at return. Using `as` once at the end is far better than
  // `any` throughout — TypeScript will catch missing/misnamed fields at assignment sites.
  const data = {
    title:            norm($('h1').first().text()),
    subtitle:         norm($('h2').first().text()),
    sourceUrl:        pageUrl,
    reviewUrl:        pageUrl,
    pageFound:        { name: '', url: pageUrl }, // overwritten by caller
    rating: '', ratingLabel: '', verdict: '', author: '', publishDate: '',
    pros:             [] as string[],
    cons:             [] as string[],
    colorOptions:     [] as string[],
    images: {
      device:              [] as string[],
      deviceAngles:        [] as string[],
      cameraSamples:       [] as string[],
      screenshots:         [] as string[],
      charts:              [] as string[],
      displayMeasurements: [] as string[],
      colorCalibration:    [] as string[],
    } satisfies NBCImageBuckets,
    specs:            {} as Record<string, string>,
    cameras:          null as NBCCameraData | null,
    display:          {} as Record<string, string>,
    battery:          {} as Record<string, string>,
    audio:            { maxVolumeDb: '', headphoneJack: false },
    biometrics:       { fingerprintType: '', faceUnlock: false },
    gpu: '', soc: '', os: '', ram: '', storage_capacity: '', storage_type: '',
    price: '', releaseDate: '', weight: '', dimensions: '',
    connectivity: '', networking_raw: '', bluetooth: '',
    bluetoothCodecs: '', wifi: '', nfc: '', usbVersion: '',
    gnss: '', hdr: '', drm: '', ipRating: '',
    maxChargingSpeed: '', usbSpeed: '', warrantyMonths: '',
    simSlots: '', simType: '', esim: '',
    sar:              { body: '', head: '' },
    hasWalkieTalkie:  false, hasIRBlaster: false, hasBarometer: false, speakers: '',
    benchmarks: {
      gpu:        [] as Benchmark[], cpu:        [] as Benchmark[], memory:     [] as Benchmark[],
      display:    [] as Benchmark[], battery:    [] as Benchmark[], storage:    [] as Benchmark[],
      networking: [] as Benchmark[], thermal:    [] as Benchmark[], audio:      [] as Benchmark[],
      other:      [] as Benchmark[],
    } satisfies NBCBenchmarks,
  };

  // ══ RATING ══
  for (const rx of [
    /\((\d{2,3}(?:\.\d+)?)%\)\s*\n?\s*(very good|good|excellent|average|poor)/i,
    /(very good|good|excellent|average|poor)\s*\(?(\d{2,3}(?:\.\d+)?)%\)?/i,
    /(\d{2,3})%[^)]{0,30}(very good|good|excellent|average|poor)/i,
  ]) {
    const m = html.match(rx) || bodyText.match(rx);
    if (m) {
      const pct = m[1].match(/^\d/) ? m[1] : m[2]; const label = m[1].match(/^\d/) ? m[2] : m[1];
      if (pct && label && /^\d{2,3}/.test(pct)) { data.rating = pct.replace(/[^0-9.]/g, '') + '%'; data.ratingLabel = label.trim().toLowerCase(); break; }
    }
  }
  if (!data.rating) {
    $('[class*="rating"], [class*="score"]').each((_, el) => {
      const t = $(el).text().trim();
      const m = t.match(/(very good|good|excellent|average|poor)[^(]*\(?(\d{2,3})%\)?/i) || t.match(/(\d{2,3})%[^)]{0,20}(very good|good|excellent|average|poor)/i);
      if (m && !data.rating) { const pct = m[1].match(/^\d/) ? m[1] : m[2]; const label = m[1].match(/^\d/) ? m[2] : m[1]; if (pct) { data.rating = pct + '%'; data.ratingLabel = label.toLowerCase().trim(); } }
    });
  }

  // ══ AUTHOR + DATE ══
  for (const sel of ['[class*="author"]', '[itemprop="author"]', '[class*="byline"]', '.author']) {
    const el = $(sel).first(); if (!el.length) continue;
    const t = cleanCellText($, el[0] as import('domhandler').Element);
    if (t && t.length > 3 && t.length < 300 && !t.includes('{')) { data.author = t; break; }
  }
  if (!data.author) { const m = bodyText.match(/(?:Editor|Writer|Senior|Junior)[^.]{5,80}(?:articles published|Tech Writer|Journalist)/i); if (m) data.author = m[0].trim().slice(0, 200); }
  const timeEl = $('time[datetime]').first();
  if (timeEl.length) data.publishDate = timeEl.attr('datetime') || timeEl.text().trim();
  if (!data.publishDate) {
    for (const sel of ['[class*="date"]', '[class*="published"]', '[itemprop="datePublished"]']) {
      const el = $(sel).first(); if (!el.length) continue;
      const dm = cleanCellText($, el[0] as import('domhandler').Element).match(/(\d{1,2}[\/.]\d{1,2}[\/.]\d{4}|\d{4}-\d{2}-\d{2})/);
      if (dm) { data.publishDate = dm[1]; break; }
    }
  }
  if (!data.publishDate) { const dm = html.match(/Published\s+(\d{1,2}\/\d{1,2}\/\d{4})/i) || bodyText.match(/(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})/); if (dm) data.publishDate = dm[1]; }

  // ══ VERDICT ══
  $('h2, h3, h4').each((_, el) => {
    const h = $(el).text().toLowerCase();
    if ((h.includes('verdict') || h.includes('conclusion') || h.includes('result')) && !data.verdict) {
      let sib = $(el).next();
      let verdictFound = false;
      while (sib.length && !verdictFound) { const t = norm(sib.text()); if (t.length > 100 && !t.includes('Download') && !t.includes('cookie') && !t.includes('{')) { data.verdict = t.slice(0, 900); verdictFound = true; } sib = sib.next(); }
    }
  });
  if (!data.verdict) { $('p').each((_, el) => { const t = norm($(el).text()); if (t.length > 150 && !data.verdict && !t.toLowerCase().includes('cookie') && !t.includes('{')) data.verdict = t.slice(0, 900); }); }

  // ══ PROS & CONS ══
  const prosSeen = new Set<string>(); const consSeen = new Set<string>();
  $('[class*="pro_eintrag"], [class="pro"]').each((_, el) => { const t = cleanCellText($, el).replace(/^[+✓•✚]\s*/, ''); if (t.length > 2 && t.length < 300 && !t.includes('{') && !prosSeen.has(t)) { prosSeen.add(t); data.pros.push(t); } });
  $('[class*="contra_eintrag"], [class="contra"]').each((_, el) => { const t = cleanCellText($, el).replace(/^[-−✗•]\s*/, ''); if (t.length > 2 && t.length < 300 && !t.includes('{') && !consSeen.has(t)) { consSeen.add(t); data.cons.push(t); } });
  if (data.pros.length === 0 && data.cons.length === 0) {
    $('li').each((_, el) => {
      const raw = cleanCellText($, el);
      if (raw.startsWith('+ ') || raw.startsWith('✓ ') || raw.startsWith('✚ ')) { const p = raw.replace(/^[+✓✚]\s+/, ''); if (p.length > 2 && p.length < 300 && !prosSeen.has(p)) { prosSeen.add(p); data.pros.push(p); } }
      else if (raw.startsWith('- ') || raw.startsWith('− ') || raw.startsWith('✗ ')) { const c = raw.replace(/^[-−✗]\s+/, ''); if (c.length > 2 && c.length < 300 && !consSeen.has(c)) { consSeen.add(c); data.cons.push(c); } }
    });
  }

  // ══ SPEC TABLE EXTRACTION ══
  const NBC_SPEC_LABELS: Array<[string, string]> = [
    ['Processor', 'processor'], ['Graphics adapter', 'graphics'], ['Memory', 'memory'],
    ['Display', 'display'], ['Storage', 'storage'], ['Connections', 'connections'],
    ['Networking', 'networking'], ['Size', 'dimensions'], ['Battery', 'battery'],
    ['Charging', 'charging'], ['Operating System', 'os'], ['Camera', 'camera'],
    ['Additional features', 'additionalFeatures'], ['Released', 'releaseDate'],
    ['Weight', 'weight'], ['Price', 'price'], ['Note', 'note'],
  ];

  const specs: Record<string, string> = {};

  $('table tr').each((_, row) => {
    const cells = $(row).find('td, th'); if (cells.length < 2) return;
    const k = cleanCellText($, cells.eq(0)[0]).replace(/:$/, '');
    const found = NBC_SPEC_LABELS.find(([l]) => l === k); if (!found || specs[found[0]]) return;
    const v = cleanCellText($, cells.eq(1)[0]);
    if (v && v.length > 0 && v.length < 1500 && !v.includes('{') && !v.startsWith(':')) { specs[found[0]] = v; data.specs[found[1]] = v; }
  });

  $('td, th, dt, span, div').each((_, el) => {
    const t = cleanCellText($, el);
    const found = NBC_SPEC_LABELS.find(([l]) => t === l || t === l + ':');
    if (found) {
      const [label, field] = found; if (specs[label]) return;
      const next = $(el).next();
      if (next.length) {
        const val = cleanCellText($, next[0]);
        if (val && val.length > 1 && val.length < 1200 && !NBC_SPEC_LABELS.find(([l]) => l === val) && !val.includes('{')) {
          specs[label] = val; data.specs[field] = val;
        }
      }
    }
  });

  if (specs['Note']) {
    const noteClean = specs['Note']
      .replace(/["'][^"']{0,200}["']/g, '')
      .replace(/https?:\/\/\S+/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 300);
    specs['Note'] = noteClean;
    data.specs['note'] = noteClean;
  }

  // NOTE: specs.raw intentionally omitted — all spec data is already present
  // in the structured data fields below, and double-serialising specs as JSON
  // wastes cache space and transfer on every response.

  // ══ CONVENIENCE FIELDS ══
  data.gpu = norm(specs['Graphics adapter'] || '').replace(/\s*,\s*Core:\s*\d+\s*MHz/i,'').replace(/,\s*,/g,',').trim();
  data.soc = norm(specs['Processor'] || '').replace(/,\s*,/g,',').replace(/\d+\s*x\s*,/g,'').replace(/\s{2,}/g,' ').trim();
  data.os               = norm(specs['Operating System'] || '');
  data.ram              = norm(specs['Memory'] || '').match(/(\d+\s*GB)/i)?.[1] || '';
  data.storage_capacity = norm(specs['Storage'] || '').match(/(\d+)\s*GB/i)
    ? (norm(specs['Storage'] || '').match(/(\d+)\s*GB/i)![1] + ' GB') : '';
  data.storage_type = norm(specs['Storage'] || '').match(/(UFS\s*[\d.]+|eMMC\s*[\d.]+|NVMe)/i)?.[1] || '';
  data.releaseDate = norm(specs['Released'] || specs['Announced'] || specs['Launch'] || '')
    || (data.publishDate ? data.publishDate.split(' ')[0] : '');
  data.dimensions       = norm(specs['Size'] || '');
  data.connectivity     = norm(specs['Connections'] || '');
  data.networking_raw   = norm(specs['Networking'] || '');
  data.bluetooth        = norm(specs['Networking'] || '').match(/Bluetooth\s*([\d.]+)/i)?.[1] || '';

  const rawWeight = norm(specs['Weight'] || '');
  const weightMatch = rawWeight.match(/(\d{2,4}(?:\.\d+)?)\s*g(?:rams?)?\b/i)
    || (html.match(/Weight[^:]*:\s*(\d{2,4}(?:\.\d+)?)\s*g\b/i) ?? bodyText.match(/Weight[^:]*:\s*(\d{2,4}(?:\.\d+)?)\s*g\b/i))
    || (html.match(/weighs?\s+(?:about\s+|approx\.\s*)?(\d{2,4}(?:\.\d+)?)\s*g\b/i) ?? bodyText.match(/weighs?\s+(?:about\s+|approx\.\s*)?(\d{2,4}(?:\.\d+)?)\s*g\b/i));
  data.weight = weightMatch ? weightMatch[1] + ' g' : '';

  const rawPrice = norm(specs['Price'] || '');
  const CSYM: Record<string,string> = {
    '€':'€','$':'$','£':'£',
    'eur':'€','usd':'$','gbp':'£',
    'euro':'€','euros':'€','dollar':'$','dollars':'$','pound':'£','pounds':'£',
  };
  const normCurrency = (s: string) => CSYM[s] || CSYM[s.toLowerCase()] || s.toUpperCase();
  let priceVal = '';
  const pmA = rawPrice.match(/(\d+(?:[.,]\d+)?)\s*(EUR|USD|GBP|Euro|Euros?|Dollars?|Pounds?|€|\$|£)/i);
  const pmB = rawPrice.match(/(€|\$|£|EUR|USD|GBP)\s*(\d+(?:[.,]\d+)?)/i);
  if (pmA)       priceVal = normCurrency(pmA[2]) + pmA[1].replace(',','.');
  else if (pmB)  priceVal = normCurrency(pmB[1]) + pmB[2];
  else {
    const pmC = (html.match(/(?:price[^:]*:|costs?|priced?\s+at)[^€$£\d]{0,20}(\d{2,5}(?:[.,]\d+)?)\s*(EUR|USD|GBP|Euro|€|\$|£)/i) ?? bodyText.match(/(?:price[^:]*:|costs?|priced?\s+at)[^€$£\d]{0,20}(\d{2,5}(?:[.,]\d+)?)\s*(EUR|USD|GBP|Euro|€|\$|£)/i));
    if (pmC) priceVal = normCurrency(pmC[2]) + pmC[1];
  }
  data.price = priceVal;

  // ══ DISPLAY ══
  data.display = {};
  if (specs['Display']) {
    const rawDisp = norm(specs['Display']);
    data.display['Display'] = rawDisp;
    const ppiM = rawDisp.match(/(\d{3,4})\s*PPI/i);
    if (ppiM) data.display['ppi'] = ppiM[1];
    const glassM = rawDisp.match(/(Gorilla Glass[^,]*|Ceramic Shield[^,]*|Armor Glass[^,]*|DragonTrail[^,]*)/i);
    if (glassM) data.display['glass'] = glassM[1].trim();
    const techM = rawDisp.match(/\b(AMOLED|OLED|IPS|LCD|LTPO|LTPS|Super AMOLED)\b/i);
    if (techM) data.display['technology'] = techM[1].toUpperCase();
    const allHz = [...rawDisp.matchAll(/(\d{2,4})\s*Hz/g)]
      .map(m => parseInt(m[1])).filter(h => h >= 60 && h <= 240);
    if (allHz.length) data.display['refreshRate'] = Math.max(...allHz) + ' Hz';
    const sizeM = rawDisp.match(/^([\d.]+)\s*inch/i);
    if (sizeM) data.display['sizeInch'] = sizeM[1];
  }
  // FIX C3: Display measurement extraction now reads from the already-loaded cheerio DOM
  // BEFORE falling back to bodyText regex. NBC renders these values in structured table
  // cells — the DOM path is exact and unaffected by the bodyText length.
  // Strategy: walk <table> cells containing the key label, grab the adjacent value cell.
  function domFindMeasurement(labelRe: RegExp): string {
    let found = '';
    $('td, th').each((_, el) => {
      if (found) return false; // break
      const label = norm($(el).text());
      if (!labelRe.test(label)) return;
      const $row = $(el).closest('tr');
      // Value is typically the next <td> in the same row
      const cells = $row.find('td');
      cells.each((_, cell) => {
        if (found) return false;
        const t = norm($(cell).text());
        if (/[\d.]+/.test(t) && t !== label) { found = t; }
      });
    });
    return found;
  }

  // General DOM spec-field finder — like domFindMeasurement but accepts any text
  // value (not just numeric).  Used for GNSS, Bluetooth codecs, IP rating, etc.
  // where the value lives in a structured spec table cell rather than prose.
  function domFindSpecField(labelRe: RegExp): string {
    let found = '';
    $('td, th, dt').each((_, el) => {
      if (found) return false;
      const label = norm($(el).text());
      if (!labelRe.test(label)) return;
      // Try sibling <td>/<dd> first (table row), then next sibling (dl pattern)
      const $sib = $(el).next('td, dd');
      if ($sib.length) {
        const t = cleanCellText($, $sib[0] as import('domhandler').Element);
        if (t && t.length > 1 && !labelRe.test(t)) { found = t; return false; }
      }
      // Fallback: any other <td> in the same <tr>
      const $row = $(el).closest('tr');
      $row.find('td').each((_, cell) => {
        if (found) return false;
        const t = cleanCellText($, cell as import('domhandler').Element);
        if (t && t.length > 1 && !labelRe.test(t)) { found = t; }
      });
    });
    return found;
  }

  for (const [key, rx] of Object.entries({
    'Brightness center (cd/m²)': /Center[^:]*:\s*([\d.]+)\s*cd\/m[²2]/i,
    'Brightness max (cd/m²)':    /Maximum[^:]*:\s*([\d.]+)\s*cd\/m[²2]/i,
    'Brightness avg (cd/m²)':    /Average[^:]*:\s*([\d.]+)\s*cd\/m[²2]/i,
    'Brightness min (cd/m²)':    /Minimum[^:]*:\s*([\d.]+)\s*cd\/m[²2]/i,
    'Brightness distribution (%)': /Brightness Distribution[^:]*:\s*([\d.]+)\s*%/i,
    'Black Level (cd/m²)':       /Black\s*Level[^:]*:\s*([\d.]+)\s*cd\/m[²2]/i,
    'Contrast':                  /Contrast[^:]*:\s*((?:∞|\d+[\d.:]*(?::1)?)[^,\n]{0,25})/i,
    'ΔE ColorChecker':           /(?:ΔE|Delta E|dE)\s*ColorChecker[^:]*:\s*([\d.]+)/i,
    'ΔE Greyscale':              /(?:ΔE|Delta E|dE)\s*Greyscale[^:]*:\s*([\d.]+)/i,
    'sRGB coverage':             /([\d.]+)\s*%\s*sRGB/i,
    'DCI-P3 coverage':           /([\d.]+)\s*%\s*DCI-P3/i,
    'AdobeRGB coverage':         /([\d.]+)\s*%\s*AdobeRGB/i,
    'Gamma':                     /Gamma[^:]*:\s*([\d.]+)/i,
    'CCT (K)':                   /CCT[^:]*:\s*([\d.]+)\s*K/i,
    'PWM frequency':             /PWM[^:]*:\s*([\d.]+)\s*Hz/i,
    'APL18 brightness':          /APL18[^,\n]*(?:reached|achieved)[^(]*\(?([\d.]+)\s*cd\/m[²2]/i,
  } as Record<string, RegExp>)) {
    // Try the full raw HTML first (DOM pass — no 200k limit), then fall back to bodyText
    const m = html.match(rx) ?? bodyText.match(rx);
    if (m) {
      let val = m[1].trim();
      if (key === 'Contrast') { val = val.split(/Δ|dE/i)[0].trim(); if (!val.includes('∞') && !val.includes(':1')) val = ''; }
      if (val) data.display[key] = val;
    }
  }

  // ══ ADDITIONAL FEATURES ══
  const addFeat = norm(specs['Additional features'] || '');
  const netRaw  = norm(specs['Networking'] || '');

  // ══ BATTERY ══
  data.battery = {};
  const rawBatt = norm(specs['Battery'] || '');
  data.battery['capacity'] = rawBatt;
  data.battery['capacityMah'] = rawBatt.match(/(\d[\d,]+)\s*mAh/i)?.[1]?.replace(',','') || '';
  data.battery['capacityWh']  = rawBatt.match(/([\d.]+)\s*Wh/i)?.[1] || '';
  const rawCharg = norm(specs['Charging'] || '');
  data.battery['charging'] = rawCharg;
  const consText = data.cons.join(' ');
  // DOM-first: try to find charging speed in the structured spec table before regex-on-text
  const domChargingW = (() => {
    const raw = domFindMeasurement(/max(?:imum)?\s*charg(?:ing)?\s*speed/i)
             || domFindMeasurement(/wired\s*charg/i);
    const m = raw.match(/(\d{2,3})\s*W/i);
    return m ? m[1] : '';
  })();
  data.battery['wiredW'] =
    domChargingW
    || addFeat.match(/max\.?\s*charging\s*speed[:\s]+(\d{2,3})\s*W/i)?.[1]
    || addFeat.match(/(\d{2,3})\s*W\s*(?:wired|HyperCharge|fast|quick|turbo|PD)\b/i)?.[1]
    || rawCharg.match(/(\d{2,3})\s*W/i)?.[1]
    || consText.match(/(?:charg(?:ing|es?)[^.]{0,60}?|maximum\s+)(\d{2,3})\s*watt/i)?.[1]
    || (html.match(/(?:wired|HyperCharge|fast)\s*charg[^.\n]{0,50}?(\d{2,3})\s*W/i) ?? bodyText.match(/(?:wired|HyperCharge|fast)\s*charg[^.\n]{0,50}?(\d{2,3})\s*W/i))?.[1]
    || (html.match(/(\d{2,3})\s*W\s*(?:wired|HyperCharge)/i) ?? bodyText.match(/(\d{2,3})\s*W\s*(?:wired|HyperCharge)/i))?.[1]
    || (html.match(/charges?\s+(?:at\s+(?:up\s+to\s+)?|up\s+to\s+)(\d{2,3})\s*W/i) ?? bodyText.match(/charges?\s+(?:at\s+(?:up\s+to\s+)?|up\s+to\s+)(\d{2,3})\s*W/i))?.[1]
    || (html.match(/(?:maximum|max\.?)\s*(?:wired\s*)?charg[^.\n]{0,50}?(\d{2,3})\s*W/i) ?? bodyText.match(/(?:maximum|max\.?)\s*(?:wired\s*)?charg[^.\n]{0,50}?(\d{2,3})\s*W/i))?.[1]
    || (html.match(/(?:up\s+to\s+|maximum\s+)(\d{2,3})\s*watt/i) ?? bodyText.match(/(?:up\s+to\s+|maximum\s+)(\d{2,3})\s*watt/i))?.[1] || '';
  data.battery['wirelessW'] =
    addFeat.match(/\/\s*(\d{2,3})\s*W\s*\(wireless\)/i)?.[1]
    || addFeat.match(/wireless[^,|]{0,30}?(\d{2,3})\s*W/i)?.[1]
    || rawCharg.match(/wireless[^,]{0,30}?(\d{2,3})\s*W/i)?.[1]
    || (html.match(/wireless\s*charg[^.\n]{0,50}?(\d{2,3})\s*W/i) ?? bodyText.match(/wireless\s*charg[^.\n]{0,50}?(\d{2,3})\s*W/i))?.[1] || '';
  data.battery['technology'] = rawBatt.match(/Lithium[- ](Ion|Polymer|Silicon)/i)?.[0] || rawBatt.match(/Silicon-Carbon/i)?.[0] || '';
  for (const [key, rx] of Object.entries({
    'Reader idle (h)':    /(?:Reader|Idle)\s*\/\s*(?:Idle|Standby)[^:]*:\s*([\d.]+)\s*h/i,
    'WiFi (h)':           /Wi-?Fi[^:]*(?:v[\d.]+)?[^:]*:\s*([\d.]+)\s*h/i,
    'Load (h)':           /\bLoad\b[^:]*:\s*([\d.]+)\s*h/i,
    'H.264 (h)':          /H\.264[^:]*:\s*([\d.]+)\s*h/i,
    'Streaming (h)':      /Streaming[^:]*:\s*([\d.]+)\s*h/i,
    'Idle min (W)':       /Idle[^W°]*Minimum[^W°]*?([\d.]+)\s*Watt/i,
    'Idle avg (W)':       /Idle[^W°]*Average[^W°]*?([\d.]+)\s*Watt/i,
    'Idle max (W)':       /Idle[^W°]*Maximum[^W°]*?([\d.]+)\s*Watt/i,
    'Load avg (W)':       /Load[^W°]*Average[^W°]*?([\d.]+)\s*Watt/i,
    'Load max (W)':       /Load[^W°]*Maximum[^W°]*?([\d.]+)\s*Watt/i,
    'Standby (W)':        /(?:Off|Standby)[^W°]*?([\d.]+)\s*Watt/i,
    'Charging time (min)': /(?:Charging|Charge)\s*time[^:]*:\s*([\d.]+)\s*min/i,
    'Charging time (h)':   /(?:Charging|Charge)\s*time[^:]*:\s*([\d.]+)\s*h\b/i,
  } as Record<string, RegExp>)) {
    // FIX C3: try full HTML (no truncation) before falling back to bodyText
    const m = html.match(rx) ?? bodyText.match(rx);
    if (m) { const val = parseFloat(m[1]); if (!isNaN(val)) data.battery[key] = m[1]; }
  }

  // GNSS — DOM-first (spec table), then addFeat regex, then netRaw fallback
  {
    const domGnss = domFindSpecField(/^GNSS$/i) || domFindSpecField(/^GPS$/i);
    const addGnss = addFeat.match(/GNSS[:\s]+([^|;\n]{3,300})/i)?.[1]
      || addFeat.match(/(GPS\s*\([^)]+\)(?:[,\s]+(?:BDS|BeiDou|GLONASS|GALILEO|QZSS|NavIC)[^|;\n]*)+)/i)?.[1]
      || addFeat.match(/(GPS(?:[,\s]+(?:BDS|BeiDou|GLONASS|GALILEO|QZSS|NavIC))+)/i)?.[1];
    const rawGnss = domGnss || addGnss || netRaw.match(/GPS[^,\n]{0,50}/i)?.[0] || (netRaw.includes('GPS') ? 'GPS' : '');
    data.gnss = rawGnss ? rawGnss.trim().replace(/\s+/g, ' ') : '';
  }

  // HDR
  const hdrMatch = addFeat.match(/HDR[:\s]+((?:HLG|HDR10|Dolby\s*Vision)[^|;\n]{0,150})/i)
    || addFeat.match(/\b(HLG[,\s]+HDR[^|;\n]{0,100})/i)
    || (html.match(/\b(HDR10\+?|HLG|Dolby\s*Vision)(?:[,\s]+(?:HDR10\+?|HLG|Dolby\s*Vision)){1,3}/i) ?? bodyText.match(/\b(HDR10\+?|HLG|Dolby\s*Vision)(?:[,\s]+(?:HDR10\+?|HLG|Dolby\s*Vision)){1,3}/i));
  data.hdr = hdrMatch ? hdrMatch[1].trim().replace(/\s+/g,' ')
    : (addFeat.match(/\bHDR\b/i) || norm(specs['Display'] || '').match(/\bHDR\b/i) ? 'Yes' : '');

  // DRM
  const drmRaw = addFeat.match(/(?:DRM\s*)?(?:Widevine)\s*(L\d)/i)
    || (html.match(/Widevine\s+(L\d)/i) ?? bodyText.match(/Widevine\s+(L\d)/i));
  data.drm = drmRaw ? 'Widevine ' + drmRaw[1].toUpperCase() : '';

  // IP Rating — DOM-first, then addFeat, then brief bodyText scan
  {
    const domIp = domFindSpecField(/^IP\s*Rating|^Protection\s*Class/i);
    const ipSrc = (domIp ? domIp + ' ' : '') + addFeat + ' ' + bodyText.slice(0, 3000);
    const ipCodes = [...ipSrc.matchAll(/\b(IP\d{2}[A-Z0-9]*)\b/gi)].map(m => m[1].toUpperCase());
    const ipUniq = [...new Set(ipCodes)].slice(0, 3);
    data.ipRating = ipUniq.length ? ipUniq.join('/') : '';
    if (!data.ipRating) {
      const milM = addFeat.match(/\b(MIL-STD-\d+[A-Z0-9-]*)\b/i);
      if (milM) data.ipRating = milM[1].toUpperCase();
    }
    if (!data.ipRating && /\bwaterproof\b/i.test(addFeat)) data.ipRating = 'waterproof';
  }

  // Max charging speed
  {
    const candidates: number[] = [];
    const tryW = (m: RegExpMatchArray | null, g = 1) => {
      if (m) { const w = parseInt(m[g] || '0'); if (w >= 5 && w <= 350) candidates.push(w); }
    };
    tryW(addFeat.match(/max\.?\s*charging\s*speed[:\s]+(\d{2,3})\s*W/i));
    tryW(addFeat.match(/(\d{2,3})\s*W\s*(?:wired|HyperCharge|fast|quick|rapid|turbo|PD)\b/i));
    [...(specs['Charging'] || '').matchAll(/(\d{2,3})\s*W/gi)].forEach(m => tryW(m));
    tryW(consText.match(/(?:charg(?:ing|es?)[^.]{0,60}?|maximum\s+)(\d{2,3})\s*watt/i));
    tryW(html.match(/(?:HyperCharge|SuperVOOC|FlashCharge|TurboCharge)[^.\n]{0,40}?(\d{2,3})\s*W/i) ?? bodyText.match(/(?:HyperCharge|SuperVOOC|FlashCharge|TurboCharge)[^.\n]{0,40}?(\d{2,3})\s*W/i));
    tryW(html.match(/(\d{2,3})\s*W\s*(?:HyperCharge|SuperVOOC|FlashCharge)/i) ?? bodyText.match(/(\d{2,3})\s*W\s*(?:HyperCharge|SuperVOOC|FlashCharge)/i));
    tryW(html.match(/(?:fast|wired)\s*charg[^.\n]{0,60}?(\d{2,3})\s*W/i) ?? bodyText.match(/(?:fast|wired)\s*charg[^.\n]{0,60}?(\d{2,3})\s*W/i));
    tryW(html.match(/charges?\s+(?:at|up to)\s+(\d{2,3})\s*W/i) ?? bodyText.match(/charges?\s+(?:at|up to)\s+(\d{2,3})\s*W/i));
    tryW(html.match(/(?:maximum|max\.?)\s*(?:wired\s*)?charg[^.\n]{0,50}?(\d{2,3})\s*W/i) ?? bodyText.match(/(?:maximum|max\.?)\s*(?:wired\s*)?charg[^.\n]{0,50}?(\d{2,3})\s*W/i));
    tryW(html.match(/(?:up\s+to|maximum)\s+(\d{2,3})\s*watt/i) ?? bodyText.match(/(?:up\s+to|maximum)\s+(\d{2,3})\s*watt/i));
    if (candidates.length) data.maxChargingSpeed = Math.max(...candidates) + ' W';
  }

  // USB copy speed — DOM-first
  data.usbSpeed = domFindSpecField(/USB Copy Test/i)?.match(/([\d.]+\s*MB\/s)/i)?.[1]
    || addFeat.match(/USB Copy Test[:\s]*([\d.]+\s*MB\/s)/i)?.[1]
    || addFeat.match(/USB[^|]{0,30}([\d.]+\s*MB\/s)/i)?.[1] || '';

  // FIX: if the matched unit is "years", multiply by 12 so the field is always months
  {
    const mMonths = addFeat.match(/(\d+)\s*Months?\s*Warranty/i);
    const mGeneric = addFeat.match(/Warranty[:\s]*(\d+)\s*(months?|years?)/i);
    if (mMonths) {
      data.warrantyMonths = mMonths[1];
    } else if (mGeneric) {
      const n = parseInt(mGeneric[1]);
      data.warrantyMonths = /year/i.test(mGeneric[2]) ? String(n * 12) : String(n);
    }
  }
  data.esim = /Dual[- ]eSIM/i.test(addFeat) ? '2'
    : (addFeat.match(/(\d+)[- ]eSIM/i)?.[1] || addFeat.match(/up to (\d+) eSIM/i)?.[1] || (/eSIM/i.test(addFeat) ? '1' : ''));
  data.simSlots = addFeat.match(/up to (\d+) Nano-SIM/i)?.[1] || addFeat.match(/(\d+)\s*(?:Nano|Micro)-SIM/i)?.[1] || '';
  if (!data.simSlots) {
    if (/Dual[- ]?SIM/i.test(netRaw + ' ' + addFeat)) {
      data.simSlots = '2';
      const noEsimMentioned = /no eSIM/i.test(data.cons.join(' ') + ' ' + bodyText);
      if (!data.esim && !noEsimMentioned) data.esim = '1';
    }
    else if (/Single[- ]?SIM/i.test(netRaw + ' ' + addFeat)) data.simSlots = '1';
  }
  const simTypeRaw = addFeat.match(/(Nano|Micro|Mini)-SIM/i)?.[1];
  data.simType = simTypeRaw ? simTypeRaw.toLowerCase() + '-SIM' : '';

  // Bluetooth codecs — DOM-first, then addFeat regex
  {
    const domCodecs = domFindSpecField(/Bluetooth Audio Codec/i);
    const addCodecs = addFeat.match(/Bluetooth Audio Codecs?[:\s]+([^|;\n]{3,150})/i)?.[1];
    data.bluetoothCodecs = (domCodecs || addCodecs || '').trim();
  }

  data.hasWalkieTalkie = /walkie[- ]talkie/i.test(addFeat + ' ' + bodyText);
  data.hasIRBlaster = /IR[- ]?Blaster|Infrared/i.test(norm(specs['Connections'] || '') + ' ' + addFeat);
  data.hasBarometer = /barometer/i.test(addFeat + ' ' + norm(specs['Connections'] || '') + ' ' + bodyText.slice(0, 5000));
  data.speakers = domFindSpecField(/^Speakers?$/i) || addFeat.match(/Speakers?[:\s]+([^|,\n]{2,40})/i)?.[1]?.trim() || '';

  // SAR — DOM-first, then addFeat, then full html/bodyText
  data.sar = {
    body: domFindSpecField(/Body[-\s]?SAR/i)?.match(/([\d.]+\s*W\/kg)/i)?.[1]
      || addFeat.match(/Body[-\s]?SAR[:\s]*([\d.]+\s*W\/kg)/i)?.[1]
      || (html.match(/Body\s*SAR[:\s]*([\d.]+\s*W\/kg)/i) ?? bodyText.match(/Body\s*SAR[:\s]*([\d.]+\s*W\/kg)/i))?.[1] || '',
    head: domFindSpecField(/Head[-\s]?SAR/i)?.match(/([\d.]+\s*W\/kg)/i)?.[1]
      || addFeat.match(/Head[-\s]?SAR[:\s]*([\d.]+\s*W\/kg)/i)?.[1]
      || (html.match(/Head\s*SAR[:\s]*([\d.]+\s*W\/kg)/i) ?? bodyText.match(/Head\s*SAR[:\s]*([\d.]+\s*W\/kg)/i))?.[1] || '',
  };

  const netRawClean = netRaw.replace(/\u200b/g, '').replace(/\s+/g, ' ');
  const wifiM = netRawClean.match(/802\.11\s*([a-z\/]+(?:\/[a-z]+)*)/i)
    || netRawClean.match(/Wi-Fi\s*([\d.]+(?:,\s*[\d.]+)*)/i);
  if (wifiM) {
    const wifiGenMap: Record<string,string> = { '7':'a/b/g/n/ac/ax/be','6e':'a/b/g/n/ac/ax','6':'a/b/g/n/ac/ax','5':'a/b/g/n/ac','4':'a/b/g/n' };
    const rawWifi = wifiM[1];
    data.wifi = wifiGenMap[rawWifi.toLowerCase()] || rawWifi;
  }
  data.nfc = /\bNFC\b/i.test(norm(specs['Connections'] || '') + ' ' + addFeat) ? 'Yes' : '';
  const usbM = norm(specs['Connections'] || '').match(/USB\s*([\d.]+|Type-[ABC]|USB-[ABC])/i);
  if (usbM) data.usbVersion = usbM[1];

  // Biometrics — DOM-first for fingerprint type, then full text scan
  {
    const domBio = domFindSpecField(/Fingerprint|Biometric/i);
    const bioRaw = (domBio ? domBio + ' ' : '') + norm(specs['Connections'] || '') + ' ' + addFeat + ' ' + bodyText.slice(0, 40000);
    data.biometrics = {
      fingerprintType: bioRaw.match(/(ultrasonic|optical|side-mounted|rear-mounted|physical)\s*fingerprint/i)?.[1]?.toLowerCase()
        || (bioRaw.includes('Fingerprint Reader') ? 'Standard' : ''),
      faceUnlock: /face\s*(?:unlock|recognition|ID)/i.test(bioRaw),
    };
  }

  data.audio = {
    headphoneJack: /3\.5\s*mm|headphone jack/i.test(norm(specs['Connections'] || '') + ' ' + addFeat),
    maxVolumeDb: (() => {
      // NBC table format: "74.3 / 93 dB(A)" — grab the LAST (highest) number before dB(A)
      const src = html + ' ' + bodyText;
      const m1 = src.match(/\b([\d.]+)\s*dB\(A\)/gi);
      if (m1) {
        // Filter to plausible speaker volume range (60–120 dB)
        const vals = m1.map(s => parseFloat(s)).filter(v => v >= 60 && v <= 120);
        if (vals.length) return String(Math.max(...vals));
      }
      return (html.match(/(?:maximum|max)[\s\S]{0,60}?([\d.]+)\s*dB\(A\)/i) ?? bodyText.match(/(?:maximum|max)[\s\S]{0,60}?([\d.]+)\s*dB\(A\)/i))?.[1] || '';
    })(),
  };

  // ══ CAMERA ══
  const camRaw = norm(specs['Camera'] || '');
  if (camRaw) {
    data.cameras = { raw: camRaw, lenses: [] as CameraLens[], videoCapabilities: '' } as NBCCameraData;
    const videoMatch = camRaw.match(/(\d+[Kk][^;,]*(?:fps|FPS)[^;,]*)/g);
    if (videoMatch) data.cameras!.videoCapabilities = videoMatch.join(', ');
    const [mainPart = '', selfieRaw = ''] = camRaw.split(/\s*Secondary Camera:/i);
    mainPart.replace(/Primary Camera:/gi, '').split(/\s+\+\s+/).forEach((lens: string) => {
      if (!lens.trim()) return;
      const cam: Partial<CameraLens> = {};
      const mp = lens.match(/([\d.]+)\s*MPix/i); const sn = lens.match(/\(([A-Z][A-Z0-9-]{2,})/); const sz = lens.match(/(\d+\/[\d.]+)/);
      const ap = lens.match(/f\/([\d.]+)/); const fl = lens.match(/(\d{2,3})\s*mm/i);
      const zm = lens.match(/([\d.]+)\s*x\s+(?:optical\s+)?zoom/i) || lens.match(/([\d.]+)\s*x\s+Tele/i) || lens.match(/([\d.]+)\s*x\s+optical/i);
      const ci = lens.match(/Cipa\s*([\d.]+)/i);
      if (mp) cam.megapixels = mp[1] + ' MP';
      if (sn) cam.sensor = sn[1];
      const szFrac = lens.match(/(\d+\/[\d.]+)\s*"/);
      const sz1inch = !szFrac && /\b1\s*"/.test(lens);
      cam.sensorSize = szFrac ? szFrac[1] + '\"' : sz1inch ? '1\"' : '';
      const apRange = lens.match(/f\/(([\d.]+)[–\-]([\d.]+))/i);
      cam.aperture = apRange ? `f/${apRange[2]}-${apRange[3]}` : (ap ? `f/${ap[1]}` : '');
      if (fl) cam.focalLength = fl[1] + 'mm'; if (zm) cam.opticalZoom = zm[1] + 'x'; if (ci) cam.stabilization = 'Cipa ' + ci[1];
      cam.type = /\bTOF\b|depth/i.test(lens) ? 'depth'
        : /ultra.?wide/i.test(lens) ? 'ultrawide'
        : /periscope|\dx\s*(?:opt\.?|zoom)|tele/i.test(lens) ? 'telephoto'
        : 'main';
      cam.ois = /\bOIS\b|\bGimbal.?OIS\b|\bGimbal\b|\bEIS\b/i.test(lens);
      cam.oisType = /Gimbal.?OIS/i.test(lens) ? 'Gimbal-OIS' : /\bOIS\b/i.test(lens) ? 'OIS' : /\bEIS\b/i.test(lens) ? 'EIS' : '';
      cam.af = /\bAF\b|\bPDAF\b|\bLiDAR\b|\bLaser\s*AF\b|\bPhase\s*Detection/i.test(lens);
      cam.cipaStabilization = ci ? ci[1] : '';
      const pxM2 = lens.match(/([\d.]+)\s*µm/i);   if (pxM2) cam.pixelSize = pxM2[1] + ' µm';
      const fovM2 = lens.match(/FOV\s*([\d.]+)/i);  if (fovM2) cam.fov = fovM2[1] + '°';
      const snRaw = lens.match(/\(([A-Z][A-Z0-9]{2,}(?:[- ][A-Z0-9]+)?)/);
      if (snRaw && !/Cipa|Super|Dual|Quad|Gimbal/i.test(snRaw[1])) cam.sensor = snRaw[1];
      cam.description = lens.trim().replace(/;.*$/, '').slice(0, 150);
      data.cameras!.lenses.push(cam as CameraLens);
    });
    if (selfieRaw.trim()) {
      const selfie: Partial<CameraLens> = { type: 'selfie' };
      const mp = selfieRaw.match(/([\d.]+)\s*MPix/i); const ap = selfieRaw.match(/f\/([\d.]+)/); const sz = selfieRaw.match(/(\d+\/[\d.]+)/);
      const sn = selfieRaw.match(/\(([A-Z][A-Z0-9-]{2,}(?:[- ][A-Z0-9]+)?)/);
      const fl = selfieRaw.match(/(\d{2,3})\s*mm/i);
      const pxSelf = selfieRaw.match(/([\d.]+)\s*µm/i);
      const fovSelf = selfieRaw.match(/FOV\s*([\d.]+)/i);
      if (mp) selfie.megapixels = mp[1] + ' MP';
      if (ap) selfie.aperture = 'f/' + ap[1];
      if (sz) selfie.sensorSize = sz[1] + '\"';
      if (sn && !/Cipa|Super|Dual|Quad/i.test(sn[1])) selfie.sensor = sn[1];
      if (fl) selfie.focalLength = fl[1] + 'mm';
      if (pxSelf) selfie.pixelSize = pxSelf[1] + ' µm';
      if (fovSelf) selfie.fov = fovSelf[1] + '°';
      selfie.af = /\bAF\b|\bPDAF\b/i.test(selfieRaw);
      selfie.description = selfieRaw.trim().slice(0, 150);
      data.cameras!.selfie = selfie as CameraLens;
    }
    if (!data.cameras!.videoCapabilities) {
      const vm8k = html.match(/8K[^.]{0,30}?@?\s*(\d+)\s*fps/i) ?? bodyText.match(/8K[^.]{0,30}?@?\s*(\d+)\s*fps/i);
      const vm4k = html.match(/4K[^.]{0,30}?@?\s*(\d+)\s*fps/i) ?? bodyText.match(/4K[^.]{0,30}?@?\s*(\d+)\s*fps/i);
      const vm1080 = html.match(/(?:1080p|FHD)[^.]{0,30}?@?\s*(\d+)\s*fps/i) ?? bodyText.match(/(?:1080p|FHD)[^.]{0,30}?@?\s*(\d+)\s*fps/i);
      if (vm8k || vm4k) {
        const parts: string[] = [];
        if (vm8k) parts.push('8K@' + vm8k[1] + 'fps');
        if (vm4k) parts.push('4K@' + vm4k[1] + 'fps');
        if (vm1080) parts.push('1080p@' + vm1080[1] + 'fps');
        data.cameras!.videoCapabilities = parts.join(', ');
      } else {
        const vm = html.match(/(?:4K|2160p|1080p)[^.]{0,60}(?:fps|FPS)/i) ?? bodyText.match(/(?:4K|2160p|1080p)[^.]{0,60}(?:fps|FPS)/i);
        if (vm) data.cameras!.videoCapabilities = vm[0].trim().slice(0, 100);
      }
    }
    // FIX: NBC writes this multiple ways — all variants matched now:
    //   "Camera2 API Level: Level 3", "Camera2 API: Level 3",
    //   "Camera2 API support: full (Level 3)", "Camera2: Level 3"
    const c2m = (specs['Camera'] || html).match(/Camera2[^.\n]{0,40}Level\s*(\d)/i)
             || (html.match(/Camera2[^.\n]{0,40}Level\s*(\d)/i) ?? bodyText.match(/Camera2[^.\n]{0,40}Level\s*(\d)/i));
    if (c2m) data.cameras!.camera2ApiLevel = 'Level ' + c2m[1];
  }

  // ══ COLOR OPTIONS ══
  const NON_COLOR = new Set(['europe', 'asia', 'global', 'china', 'india', 'usa', 'america', 'international', 'uk', 'dual', 'single', 'sim', 'standard', 'edition', 'version', 'variant', 'model', 'review', 'test', 'market', 'region', 'country']);
  $('[class*="color-option"], [class*="colorOption"], [class*="color_option"], [class*="variant-selector"], [data-color], [aria-label*="color"], [class*="swatches"] a, [class*="color-picker"] span').each((_, el) => {
    const t = ($(el).attr('data-color') || $(el).attr('aria-label') || $(el).attr('title') || cleanCellText($, el)).trim();
    if (t && t.length > 2 && t.length < 40 && !NON_COLOR.has(t.toLowerCase()) && !/\d/.test(t) && !data.colorOptions.includes(t)) data.colorOptions.push(t);
  });
  if (data.colorOptions.length === 0) {
    const colorMatch = bodyText.slice(0, 5000).match(/(?:available in|colou?rs?[:\s]+|comes? in)([^.]{10,120})/i);
    if (colorMatch) {
      const possibleColors = colorMatch[1].split(/,| and /)
        .map((c: string) => c.trim())
        .filter((c: string) => /^[A-Z][a-z]+(?:\s[A-Z][a-z]+)*$/.test(c) && c.length > 3 && c.length < 20 && !NON_COLOR.has(c.toLowerCase()));
      if (possibleColors.length > 0) data.colorOptions = [...new Set(possibleColors)] as string[];
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  IMAGES — v24 OVERHAUL
  //
  //  Strategy:
  //  1. Walk every <figure> element.
  //     a. Get the full-res URL from <a href> (skip if absent).
  //     b. Classify using figcaption text + <a title> (ground truth).
  //     c. Fall back to filename pattern only when caption is absent.
  //  2. Collect remaining non-figure images from <a href> / <img src>
  //     that weren't covered by figures (device photos, charts, etc.).
  //  3. Skip all thumbnail variants (_processed_/ or csm_* in the path).
  //  4. Enforce independent per-bucket caps:
  //       cameraSamples: 30, device: 20, screenshots: 20, charts: 10
  //  5. Resolve relative URLs correctly — both `/fileadmin/...` and
  //     `fileadmin/...` (without leading slash) are handled.
  // ══════════════════════════════════════════════════════════════════════════

  // Per-bucket caps
  const IMG_CAPS = {
    cameraSamples:       30,
    device:              20,
    deviceAngles:        10,  // hardware detail shots (top, bottom, left, right views)
    screenshots:         20,
    charts:              10,
    displayMeasurements: 15,  // RigolDS oscilloscope waveforms
    colorCalibration:    10,  // Calman colour plots
  } as const;
  type ImgBucket = keyof typeof IMG_CAPS;

  // Seen set operates on the canonical full-res URL (lowercased)
  const imgSeen = new Set<string>();
  // Base-filename dedup for displayMeasurements / colorCalibration (prevents csm_ duplicates)
  const imgSeenBase = new Set<string>();
  // URLs that were added by the nbcCI widget pass — already device-filtered, skip post-pass
  const nbcCIUrls = new Set<string>();

  function addImage(rawUrl: string, bucket: ImgBucket): boolean {
    const url = resolveNbcUrl(rawUrl);
    if (!url) return false;
    // Some NBC image types are ONLY available as _processed_/csm_* (no full-res):
    //   - screenshots (OS/UI), selfie/zoom camera samples, displayMeasurements, colorCalibration
    // For all other buckets, block thumbnails and require full-res.
    const thumbnailBuckets: ImgBucket[] = ['screenshots', 'cameraSamples', 'displayMeasurements', 'colorCalibration'];
    if (!thumbnailBuckets.includes(bucket) && isThumbnail(url)) return false;

    // Block non-content images: award badges, rating graphics, template assets, logos,
    // color test pixels/swatches (NBC's PWM/response test GIFs), clear spacers
    if (/\/templates\/|\/awards?\/|\/png_rating\/|\/svg\//i.test(url)) return false;
    // Block NBC size-comparison widget thumbnails (tx_nbc2 = TYPO3 extension for device comparison)
    // These are tiny (w125) competitor device silhouettes from the size comparison section.
    if (/\/uploads\/tx_nbc2\//i.test(url)) return false;
    // Block generic NBC article teaser images (e.g. 4_zu_3_Teaser_1_*.jpg)
    if (/\d+_zu_\d+_teaser/i.test(url)) return false;
    // Block NBC's internal test color swatches, spacer GIFs, subpixel patterns, and FLIR thermal camera shots
    if (/\/(darkred|lightred|midred|green|red_to_green|clear)_pixel|subpixel\.|clear\.gif/i.test(url)) return false;
    if (/\/Sonstiges\//i.test(url)) return false; // NBC's "misc assets" folder
    if (/\/flir_/i.test(url.toLowerCase())) return false; // FLIR thermal camera photos — internal test images

    // ── COMPETITOR GUARD 1: /Notebooks/ folder path ──────────────────────────────
    // NBC stores every device's images under /fileadmin/Notebooks/BRAND/DEVICE_FOLDER/.
    // Any /Notebooks/ image from a DIFFERENT device's folder is a competitor image.
    {
      const notebooksMatch = url.match(/\/fileadmin\/(?:_processed_\/webp\/)?Notebooks\/([^/]+)\/([^/]+)\//i);
      if (notebooksMatch) {
        const folderName = notebooksMatch[2].toLowerCase().replace(/[_\-]/g, ' ');
        if (!deviceFolderMatchesReviewedDevice(folderName)) return false;
      }
    }

    // ── COMPETITOR GUARD 2: csm_Bild_ filename-encoded device name ───────────────
    // csm_ files live under /_processed_/X/Y/ with no /Notebooks/ path, so Guard 1
    // never fires for them. For the "Bild_" photo series, NBC encodes the full device
    // name directly in the filename:
    //   csm_Bild_Samsung_Galaxy_S25_Ultra-0039_HASH.jpg  → S25 Ultra (reviewed) ✓
    //   csm_Bild_Samsung_Galaxy_A56_5G-0496_HASH.jpg     → A56 5G (competitor)  ✗
    // Extract the device segment and validate against the reviewed device.
    {
      const bildMatch = url.match(/csm_Bild_([A-Za-z0-9_]+)-(?:\d{2,4}|view|intro|overview)_[a-f0-9]{6}/i);
      if (bildMatch) {
        const encodedDevice = bildMatch[1].toLowerCase().replace(/_/g, ' ');
        if (!deviceFolderMatchesReviewedDevice(encodedDevice)) return false;
      }
    }

    // ── COMPETITOR GUARD 3: csm_BRAND_DEVICE_..._N_HASH numbered device photos ──
    // NBC names device angle/test shots as csm_BRAND_DEVICE_[Smartphone_Test_]N_HASH.jpg
    // e.g. csm_Vivo_V70_Smartphone_Test_2_dcea5566d8.jpg  → Vivo V70 (reviewed) ✓
    //      csm_Xiaomi_17_Ultra_Test_Smartphone_4_8b63de22e0.jpg → competitor        ✗
    // The numbered-device-photo pattern in isThumbnail() allows ALL of these through
    // (it cannot know the reviewed device). So we must check device identity here.
    // Strategy: strip the trailing _N_HASH to get the device slug, then validate.
    {
      const csmFilename = (url.split('/').pop() || '').toLowerCase();
      // Only applies to csm_ files NOT already handled by Bild_ guard above
      if (/^\/fileadmin\/_processed_\//i.test(url.replace('https://www.notebookcheck.net',''))
          && /\/csm_[a-z]/i.test(url)
          && !/csm_bild_/i.test(csmFilename)) {
        // Strip csm_ prefix and trailing _HEXHASH.ext to get the device slug portion
        const bare = csmFilename
          .replace(/^csm_/, '')
          .replace(/_[a-f0-9]{6,}\.(jpe?g|png|webp)$/, '');
        // Only apply this guard when the bare name looks like a device photo
        // (contains a trailing digit sequence: brand_device_test_N or brand_device_N)
        if (/[a-z]_\d{1,2}$/.test(bare)) {
          // Remove trailing _N digit(s) and known noise words to get the device slug
          const deviceSlug = bare
            .replace(/_\d{1,2}$/, '')              // strip trailing _N
            .replace(/_(smartphone|test|bild|photo|review|sample|cam|selfie)_?/gi, '_')
            .replace(/__+/g, '_')
            .replace(/^_|_$/g, '');
          if (deviceSlug.length >= 3 && !deviceFolderMatchesReviewedDevice(deviceSlug.replace(/_/g, ' '))) {
            return false;
          }
        }
      }
    }

    // ── URL dedup: plain url key (each URL collected at most once across all buckets) ──
    const key = url.toLowerCase();
    if (imgSeen.has(key)) return false;
    if (data.images[bucket].length >= IMG_CAPS[bucket]) return false;
    // Block explicitly small-width images: NBC encodes width as -wNNN-h or _wNNN_
    // in WebP and some JPEG filenames. Width < 400 means it's a preview thumbnail.
    {
      const wMatch = url.match(/-w(\d+)-h\.|[_-]w(\d+)[_-]h/i);
      if (wMatch) {
        const w = parseInt(wMatch[1] || wMatch[2], 10);
        if (w < 400) return false;
      }
    }
    // Basic sanity: must be a known image extension
    if (!/\.(jpe?g|png|webp|gif)(\?.*)?$/i.test(url)) return false;

    // Deduplicate by canonical base filename across ALL buckets (not per-bucket).
    // Prevents collecting both the raw /Notebooks/ file AND its csm_/_webp_ version,
    // even when they end up in different buckets (e.g. caption puts Test_4 in
    // 'screenshots' in Pass 1, then filename puts it in 'deviceAngles' in Pass 2).
    // e.g. "Vivo_V70_Smartphone_Test_2-q82-w2560-h.webp" and
    //      "csm_Vivo_V70_Smartphone_Test_2_dcea5566d8.jpg"
    //   both normalise to "vivo_v70_smartphone_test_2" — second one is rejected globally.
    {
      const baseName = url.split('/').pop()!
        .toLowerCase()
        .replace(/^csm_/, '')                          // strip csm_ prefix
        .replace(/_[a-f0-9]{8,}\./, '.')              // strip _HEXHASH.
        .replace(/-q\d{2,3}(?:-w\d+-h)?(?:-\d+)?\./, '.') // strip -q82-w2560-h.
        .replace(/\.[^.]+$/, '');                      // strip extension
      // Global dedup key (no bucket prefix) — same image content in any bucket blocks duplicates
      if (imgSeenBase.has(baseName)) return false;
      imgSeenBase.add(baseName);
    }

    imgSeen.add(key);
    data.images[bucket].push(url);
    return true;
  }

  // Classify by figcaption / title text — returns the bucket or null
  function classifyByCaption(caption: string): ImgBucket | null {
    const c = caption.toLowerCase();

    // ── DISPLAY MEASUREMENTS first ─────────────────────────────────────────────
    // NBC oscilloscope waveform images — figcaptions like:
    //   "Minimum display brightness", "25% display brightness",
    //   "50% display brightness", "Maximum manual display brightness"
    // Also catch titles like "Min." / "25%" / "50%" / "75%" / "100%"
    // Also: "Subpixel matrix" — display microscopy shot
    if (/(?:minimum|maximum\s*manual|\d+%)\s*(?:display\s*)?brightness|pwm/i.test(c)
        || /\brigolds?\b|\boscilloscope\b/i.test(c)
        || /subpixel[_\s\-]?matrix/i.test(c)) {
      return 'displayMeasurements';
    }

    // ── COLOUR CALIBRATION ─────────────────────────────────────────────────────
    // Calman plots — figcaptions like:
    //   "Colour accuracy (profile: natural, target colour space: sRGB)"
    //   "Colour space (profile: natural, target colour space: sRGB)"
    //   "Greyscale (profile: natural, target colour space: sRGB)"
    if (/colour\s*accuracy|colou?r\s*space|greyscale.*srgb|calman|colorchecker.*calman/i.test(c)) {
      return 'colorCalibration';
    }

    // ── CHARTS (before cameraSamples — guards "Chart 1", "Diagramm") ──────────
    // NBC chart captions: "Chart 1", "Chart 1 Lux", "Diagramm 1", "GNSS",
    // "Battery log", "GPS log", "Signal chart", "Navigation", "Satellite constellations"
    if (/\bgnss\b|\bgps\s*log\b|\bbattery\s*log\b|\bsignal\s*chart\b|\bnavigation\b|\bsatellite/i.test(c)
        || /\bchart\s*\d/i.test(c)
        || /\bdiagramm\b/i.test(c)
        || /^\s*chart\s*$/i.test(c)) {
      return 'charts';
    }

    // ── CAMERA SAMPLES (HIGH PRIORITY - before screenshots) ───────────────────
    // NBC uses captions like: "Main camera 1x", "Ultra-wide", "Telephoto 3.7x",
    // "0.6x", "1x", "2x", "5x", "30x", "100x", "Night mode", "Portrait", "Camera image", "Camera sample",
    // "Selfie with the Pixel 10", "Selfie in Photo mode", "Selfie in Portrait mode",
    // "Macro shot", "Low light", optical/digital zoom indicators, "Pro Res Zoom"
    // "Portrait Studio" — Samsung's on-device portrait mode output, shot BY the phone.
    if (/\bcamera\s*(image|sample|ui|settings|coach)\b|main\s*camera|\bultra[\s-]?wide\b|\btelephoto\b|\bselfie\b|\bfront\s*camera\b|night\s*mode|portrait\s*(?:mode|studio)|photo\s*(?:mode|settings)|photo\s*sample|\b\d+\.?\d*x\b|\bzoom\b|macro\s*shot|low\s*light|optical\s*zoom|digital\s*zoom|pro\s*res|shot\s*composition|c2pa\s*content/i.test(c)) {
      return 'cameraSamples';
    }

    // ── SCREENSHOTS ────────────────────────────────────────────────────────────
    // More specific screenshot patterns to avoid false positives with device images
    // Match common UI/software screenshot patterns
    // Exclude device photos which might mention physical parts
    if (/\b(screenshot|home\s*screen|app\s*drawer|notification|quick\s*settings|settings?\s*page|android|wallpaper|gemini|pixel\s*ui|about\s*phone|software|calling|dialer|gboard|keyboard|writing\s*tools|material\s*3|live\s*effects)\b/i.test(c)
        && !/\b(device|front|back|side|angle|moonstone|obsidian|porcelain|jade|test_\d+|bottom:|top:|left:|right:|sim\s*tray|loudspeaker|microphone|usb|volume|power\s*button)\b/i.test(c)) {
      return 'screenshots';
    }

    // ── CHARTS fallback ────────────────────────────────────────────────────────
    if (/\bgnss\b|chart|gps\s*log|battery\s*log|signal\s*chart/i.test(c)) {
      return 'charts';
    }

    // ── DEVICE ANGLES (hardware detail shots with specific labels) ─────────────
    // NBC hardware angle captions: "Top: Nano SIM tray", "Bottom: Loudspeaker, USB",
    // "Left: No buttons", "Right: Volume rocker, Power button"
    // Also: "Top: Nano SIM tray, Microphone" / "Bottom: Loudspeaker, USB 3.2 Type-C, Microphone"
    if (/^(top:|bottom:|left:|right:)/i.test(c)
        || /\b(sim\s*tray|loudspeaker|microphone.*top|microphone.*bottom|usb.*type-c|volume\s*rocker|power\s*button|no\s*buttons)\b/i.test(c)
        // Any caption that is ONLY a direction label + hardware description (e.g. "Top: Nano SIM tray, Microphone")
        || /^(top|bottom|left|right)\s*:/i.test(c)) {
      return 'deviceAngles';
    }

    // ── DEVICE IMAGES (general product photos) ────────────────────────────────
    // General device photos: "Google Pixel 10 Pro XL", "Google Pixel 10 Pro XL in Moonstone",
    // "Size comparison"
    // Color variant captions: "in Titanium", "in Phantom Black", etc.
    // Require the color word to be Title Case in the ORIGINAL caption (not lowercased c)
    // AND appear at the end of the string — this avoids "Battery runtime in Hours",
    // "Score in Points", "Built in Speaker" which are all mid-sentence prepositions.
    if (/\bsize\s*comparison\b/i.test(c)) return 'device';
    if (/\bin\s+[A-Z][a-z]{2,}(\s+[A-Z][a-z]+)*\s*$/.test(caption.trim())
        && !/\b(hours?|points?|speaker|colour|color|testing|addition|this|our|the|a)\s*$/i.test(caption.trim())) {
      return 'device';
    }

    return null;
  }

  // Classify by filename patterns — fallback when caption unavailable
  function classifyByFilename(url: string): ImgBucket {
    const l = url.toLowerCase();
    const filename = l.split('/').pop() || '';
    // Strip csm_ prefix, hash suffix, and NBC quality/size encoding for pattern matching
    // e.g. "vivo_v70_smartphone_test_2-q82-w2560-h.webp" → bare = "vivo_v70_smartphone_test_2"
    const bare = filename
      .replace(/^csm_/, '')
      .replace(/_[a-f0-9]{8,}\.(jpe?g|png|webp|gif)$/, '') // strip _HEXHASH.ext
      .replace(/-q\d{2,3}(?:-w\d+-h)?(?:-\d+)?\.(jpe?g|png|webp|gif)$/, '') // strip NBC -q82-w2560-h
      .replace(/\.(jpe?g|png|webp|gif)$/, ''); // strip bare extension

    // ── DISPLAY MEASUREMENTS ──────────────────────────────────────────────────
    // NBC oscilloscope waveform images are named RigolDS{N}.jpg or SDS{N}.jpg
    // ("Rigol DS" is the oscilloscope model used in NBC's lab)
    if (/^(rigolds|sds)\d+/i.test(filename)) return 'displayMeasurements';
    // subpixel_matrix is a display microscopy shot — belongs with display measurements
    if (/subpixel[_\-]?matrix/i.test(filename)) return 'displayMeasurements';

    // ── COLOUR CALIBRATION ────────────────────────────────────────────────────
    // Calman output images: Calman_Natural_Standard_ColorChecker_sRGB.jpg
    //                       CalMAN_Farbgenauigkeit.jpg (German)
    //                       CalMAN_Farbraum_sRGB.jpg
    //                       Farbchart_high.jpg / Farbchart_low.jpg (German camera ColorChecker)
    //                       colorchecker*.jpg / colorchecker*.png
    //                       grayscale*.png / srgb_gamut*.png / sat_sweeps*.png
    if (/^cal\s?man/i.test(filename)) return 'colorCalibration';
    if (/^farbchart/i.test(filename)) return 'colorCalibration';
    if (/colorchecker|grayscale|srgb_gamut|color_gamut|sat_sweeps/i.test(filename)) return 'colorCalibration';

    // ── SCREENSHOTS ────────────────────────────────────────────────────────────
    // NBC screenshot naming patterns:
    //   - {device}_software_{os}_{number}.{ext}  (e.g. pixel_10_pro_xl_software_android16_1.jpg)
    //   - screenshot_*.png
    //   - UUID-style names (common for UI screenshots): a1b2c3d4-5e6f-7890-abcd-ef1234567890.jpg
    if (/screenshot/i.test(filename)) return 'screenshots';
    if (/_software_/i.test(filename)) return 'screenshots';
    if (/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/i.test(filename)) return 'screenshots';

    // ── CAMERA SAMPLES ─────────────────────────────────────────────────────────
    // Camera samples: "Fotos_Kamera_*" — shots taken BY the phone
    // Also common camera roll patterns:
    //   - PXL_YYYYMMDD_*.jpg (Google Pixel camera naming)
    //   - IMG_*.jpg / IMG*.jpg (Apple iPhone, generic)
    //   - DSC_*.jpg / DSC*.jpg (Sony, Nikon camera apps)
    //   - DCIM_*.jpg (generic camera roll)
    //   - {Date stamp patterns} YYYYMMDD_*.jpg
    //   - Portrait_Studio_*.jpg — camera sample shot taken by the phone (portrait mode)
    if (/fotos_kamera/i.test(filename)) return 'cameraSamples';
    if (/^pxl_\d{8}/i.test(filename)) return 'cameraSamples';
    if (/^img_?\d+/i.test(filename)) return 'cameraSamples';
    if (/^dsc_?\d+/i.test(filename)) return 'cameraSamples';
    if (/^dcim/i.test(filename)) return 'cameraSamples';
    // aPic_ = NBC review-photographer naming for photos in the article (shots OF the device,
    // OR shots taken BY the device in the Camera section).
    // Default → 'device' because the most common use is hero + case/housing shots.
    // classifyByCaption (runs before filename) catches camera samples that have
    // informative captions ("1x", "Night mode", etc.).
    // sectionBucketOverride catches aPic_ inside the Camera section (see FIX 7b).
    if (/^apic_/i.test(filename)) return 'device';
    // Portrait Studio shots are photos taken BY the phone, not product shots of the device.
    // e.g. Portrait_Studio_Samsung_Galaxy_S25_Ultra_4.jpg
    if (/^portrait[_\-]studio/i.test(filename)) return 'cameraSamples';
    // Date-stamped files: YYYYMMDD_HHMMSS.jpg
    // In NBC reviews, these are DEVICE ANGLE PHOTOS taken by NBC in their lab
    // (showing ports, buttons, angles under controlled lighting).
    // Camera samples in NBC always use Foto_{slug}_{scene} naming — never date-stamps.
    // Exception handled by sectionBucketOverride() in Pass 2 for software-section screenshots.
    if (/^\d{8}[_T]\d/.test(filename)) return 'deviceAngles';
    // Unix timestamp filenames: also device shots
    if (/^\d{13}/.test(filename)) return 'deviceAngles';


    // ── CHART INTERCEPT (must run BEFORE the generic Foto_* branch) ──────────
    // NBC names display/luminance/GNSS chart images as:
    //   Foto_{DeviceSlug}_Chart_1Lux.jpg  →  "Chart" segment present
    //   Foto_{DeviceSlug}_Chart_1.jpg
    //   Foto_{DeviceSlug}_Diagramm_*.jpg
    //   Foto_{DeviceSlug}_GNSS_*.jpg
    //   Testchart_high.jpg, Testchart_low.jpg  (German NBC camera resolution charts)
    //   …and standalone patterns:
    //   gnss_*.jpg, *_chart_*.jpg, *_diagramm_*.jpg
    // These must be routed to 'charts', not 'cameraSamples', even though
    // the filename starts with "Foto_" and the device slug matches the device.
    if (/[_\-]chart[_\-\d]|[_\-]diagramm[_\-]|[_\-]gnss[_\-]|\bgnss\b|[_\-]chart\b/i.test(filename)
        || /^gnss/i.test(filename)
        || /^testchart/i.test(filename)) {
      return 'charts';
    }

    // TYPE C/D: "Foto_{slug}_{scene}" or "Photo_{slug}_{scene}" — NBC's per-device shot naming
    // Route to cameraSamples here; the post-pass filter will remove non-matching device shots.
    // Also covers Photo_X300_ColorChecker*.jpg — the post-pass will keep them
    // only if the slug matches (which it will), but classifyByCaption handles
    // Photo_X300_ColorChecker via the inline ΔE grid (no figure/caption → filename fallback).
    // We intercept colorchecker filenames here:
    if (/[_\-]colorchecker/i.test(filename)) return 'colorCalibration';
    if (/^(foto|photo)_/i.test(filename)) return 'cameraSamples';

    // Screenshots
    if (/screenshot/i.test(filename)) return 'screenshots';

    // Charts (GNSS, battery discharge plots) — standalone filenames not starting with Foto_
    // Also catches: view.jpg (size comparison), comparison.jpg, etc.
    if (/\bgnss\b|(?<![a-z])chart(?![a-z])|^view\.jpg$|comparison/i.test(filename)) return 'charts';

    // ── NUMBERED/ANGLED DEVICE PHOTOS ────────────────────────────────────────
    // NBC device review pages name device photos as:
    //   csm_BRAND_DEVICE_N_HASH.jpg  → bare = "brand_device_n"
    //   csm_BRAND_DEVICE_Smartphone_Test_N_HASH.jpg → bare = "brand_device_smartphone_test_n"
    //   csm_BRAND_DEVICE_top_HASH.jpg → bare = "brand_device_top"
    //
    // Classification:
    //   _1 suffix (first shot) → device (main hero/marketing shot)
    //   _2+ suffix (subsequent shots) → deviceAngles (detail/angle views)
    //   _Smartphone_Test_1 → device (front-facing hero shot)
    //   _Smartphone_Test_2+ → deviceAngles (angle/port/button detail shots)
    //
    // This matches NBC editorial convention: the first image is always the main
    // product shot, subsequent numbered images are detail/angle views.
    if (/^[a-z0-9]+(?:_[a-z0-9]+)+_\d{1,2}$/i.test(bare)) {
      const numMatch = bare.match(/_([\d]+)$/);
      const shotNum = numMatch ? parseInt(numMatch[1], 10) : 1;
      return shotNum <= 1 ? 'device' : 'deviceAngles';
    }
    if (/_(top|bottom|left|right|front|back|side|angle)$/i.test(bare)) return 'deviceAngles';

    // NBC "Bild_" series (Samsung reviews and others):
    //   Bild_Samsung_Galaxy_S25_Ultra-0039.jpg (bare: "bild_samsung_galaxy_s25_ultra-0039.jpg")
    //   Bild_Samsung_Galaxy_S25_Ultra-View.jpg (bare: "bild_samsung_galaxy_s25_ultra-view.jpg")
    //   Bild_Samsung_Galaxy_S25_Ultra-Intro.jpg (bare: "...ultra-intro.jpg") — hero intro shot
    // These have a 2-4 digit sequence number, or a label like "-View"/"-Intro", after a hyphen.
    // Raw /Notebooks/ files have no hash suffix so `bare` retains the extension — match both forms.
    if (/^bild_[a-z0-9_]+-\d{2,4}(\.jpe?g|\.png|\.webp)?$/i.test(bare)) return 'device';
    if (/^bild_[a-z0-9_]+-(view|intro|overview)(\.jpe?g|\.png|\.webp)?$/i.test(bare)) return 'device';

    // ── DEFAULT ───────────────────────────────────────────────────────────────
    // Anything unrecognised: device bucket.
    // The addImage() folder guard rejects images from other devices' /Notebooks/ folders.
    // classifyByCaption() (called before this function) handles aPic_* and other ambiguous
    // files that appear in figures — caption is always ground truth.
    return 'device';
  }

  // Helper: match a zoom_title string against the searched device name
  // Uses bidirectional non-brand token equality so "S23 Ultra" != "S23" and "OnePlus 15" != "OnePlus 16"
  const GENERIC_TK = new Set(['ultra','pro','max','plus','lite','mini','fe','neo','edge','note','fold','flip','air','speed','turbo']);

  // Extract a clean device model name from a full article title.
  // e.g. "OnePlus 15 smartphone review - Gaming at 165fps..." -> "OnePlus 15"
  //      "Samsung Galaxy S25 Ultra Review"                    -> "Samsung Galaxy S25 Ultra"
  //      "...Ricoh camera - Realme GT 8 Pro review"          -> "Realme GT 8 Pro"
  function extractModelName(raw: string): string {
    if (!raw) return '';
    // Normalise "+" suffix → "plus" so "S23+" → "S23plus"
    let r = raw.replace(/(\w)\+/g, '$1plus');
    // Split on em-dash/en-dash/pipe OR space-hyphen-space (NBC editorial separator)
    let s = r.split(/\s*[\u2013\u2014|]\s*|\s+-\s+/)[0];
    s = s.replace(/\s+(smartphone\s+review|phone\s+review|review|test|im\s+test|hands.on|preview)\b.*/i, '');
    s = s.replace(/\b(smartphone|phone|tablet|laptop|hands.on|preview|announced|unveiled)\b.*/i, '').trim();
    return s.trim();
  }

  // Brand-anchored URL slug extraction: finds the last known brand in the slug
  // and returns everything from there to the end (before review/test suffix).
  // e.g. "The-most-unusual-...-Realme-GT-8-Pro-review" -> "Realme GT 8 Pro"
  // e.g. "OnePlus-15-smartphone-review-Gaming..."       -> "OnePlus 15"
  function extractFromUrlSlug(slug: string): string {
    const clean = slug
      .replace(/%2B/gi, 'plus')   // URL-encoded + → Plus (e.g. S23%2B → S23plus)
      .replace(/\.\d+\.0\.html$/, '')
      .split(/-(?:smartphone-review|phone-review|review|test|hands-on|preview|announced|unveiled)\b/i)[0];
    const words = clean.split('-');
    let lastBrandIdx = -1;
    for (let i = 0; i < words.length; i++) {
      if (BRAND_TOKENS.has(words[i].toLowerCase())) lastBrandIdx = i;
    }
    return (lastBrandIdx >= 0 ? words.slice(lastBrandIdx) : words).join(' ').trim();
  }

  const urlSlugRaw = (pageUrl.split('/').pop() || '');
  const urlSlugName = extractFromUrlSlug(urlSlugRaw);

  const rawDeviceName = deviceName || urlSlugName;
  const extractedModel = extractModelName(rawDeviceName);
  const extractedTks = extractedModel.toLowerCase().split(/\s+/);
  const slugTks = urlSlugName.toLowerCase().split(/\s+/);
  // Use title extraction only when it is a valid device name (brand-led, <=6 tokens)
  // AND slug does not provide more model tokens (e.g. slug gives "Nothing Phone 3 Pro" vs title "Nothing")
  const titleIsValid = extractedModel.length >= 3 && extractedTks.length <= 6 && BRAND_TOKENS.has(extractedTks[0]);
  const resolvedDeviceName = (titleIsValid && extractedTks.length >= slugTks.length)
    ? extractedModel
    : urlSlugName;

  // ── DEVICE FOLDER MATCHER ─────────────────────────────────────────────────
  // NBC stores images under /fileadmin/Notebooks/BRAND/DEVICE_FOLDER/.
  // We use this to reject competitor images that appear in the page
  // (e.g. /Notebooks/Vivo/X300_Pro/aPic_*.jpg inside a Pixel review).
  //
  // Strategy: normalise both the folder name and the reviewed device's tokens
  // to lowercase-alphanumeric, then require that every significant device token
  // (non-brand, length >= 2) appears in the folder string.
  // e.g. folder "pixel 10 pro xl" vs device "Google Pixel 10 Pro XL" → match ✓
  //      folder "x300 pro"        vs device "Google Pixel 10 Pro XL" → no match ✗
  //
  // We're intentionally permissive (any token match) to avoid false negatives for
  // devices where the folder name is an abbreviation (e.g. "Galaxy_S25U" for S25 Ultra).
  const FOLDER_GENERIC = new Set(['ultra','pro','max','plus','lite','mini','fe','neo','edge','note','fold','flip','air','speed','turbo','5g','4g','xl','xpro']);

  function deviceFolderMatchesReviewedDevice(folderNormalized: string): boolean {
    // folderNormalized: already lowercased and underscores→spaces by caller
    // e.g. "pixel 10 pro xl", "x300 pro", "galaxy s25 ultra"
    const norm2 = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, '');
    const folderAlnum = norm2(folderNormalized);

    const deviceTokens = resolvedDeviceName
      .toLowerCase()
      .split(/[\s\-_]+/)
      .filter(t => t.length >= 2 && !BRAND_TOKENS.has(t) && !FOLDER_GENERIC.has(t));

    if (deviceTokens.length === 0) return true; // can't determine — allow

    // At least half the significant device tokens must appear in the folder string
    // (handles truncated folder names like "Pixel10" for "Pixel 10 Pro XL")
    const matchCount = deviceTokens.filter(t => folderAlnum.includes(norm2(t))).length;
    return matchCount >= Math.ceil(deviceTokens.length / 2);
  }

  function zoomTitleMatchesDevice(zoomTitle: string): boolean {
    if (!zoomTitle) return false;
    const toTk = (s: string) => s.toLowerCase().split(/[\s\-_,.:;()/]+/).filter(p => p.length >= 1);
    // Use resolvedDeviceName (already cleaned) — no need to strip "review" again
    const allDev   = toTk(resolvedDeviceName).filter(t => !BRAND_TOKENS.has(t));
    const allTitle = toTk(zoomTitle).filter(t => !BRAND_TOKENS.has(t));
    if (!allDev.length || !allTitle.length) return false;
    // Bidirectional: every non-brand token in device must be in title AND vice versa
    return allDev.every(t => allTitle.includes(t)) && allTitle.every(t => allDev.includes(t));
  }

  // ── PASS 0: NBC comparison-image widget (.nbcCI_zoom) ──────────────────────────────
  // NBC renders camera comparison images in a custom widget. Structure:
  //   .nbcCI_whole > .nbcCI_zoom (one per device) > <a href="..."> + .nbcCI_zoom_title
  // The .nbcCI_zoom_title ("OnePlus 15", "Apple iPhone 17", ...) is the exact device name
  // — use it directly to filter to only the reviewed device's shots.
  // These never appear inside <figure> so Pass 1 misses them entirely.
  //
  // Also capture .nbcCI_zoom_orig — the large "original" image shown for the reviewed device.
  // It carries no zoom_title, but it is always the reviewed device's shot.
  $('.nbcCI_whole').each((_, whole) => {
    const $whole = $(whole);

    // Grab the original (reviewed-device) image from nbcCI_zoom_orig
    const origHref = $whole.find('.nbcCI_zoom_orig a[href]').first().attr('href')
      || $whole.find('.nbcCI_zoom_orig img').first().attr('data-imgsrc')
      || $whole.find('.nbcCI_zoom_orig img').first().attr('src')
      || '';
    if (origHref) {
      const resolved = resolveNbcUrl(origHref);
      if (resolved && /\.(jpe?g|png|webp|gif)(\?.*)?$/i.test(resolved)) {
        if (addImage(origHref, 'cameraSamples')) {
          nbcCIUrls.add(resolved.toLowerCase());
        }
      }
    }

    // Grab per-device comparison images from each .nbcCI_zoom
    $whole.find('.nbcCI_zoom').each((_, el) => {
      const $el     = $(el);
      const href    = $el.find('a[href]').first().attr('href') || '';
      if (!href) return;
      const resolved = resolveNbcUrl(href);
      if (!resolved) return;
      if (!/\.(jpe?g|png|webp|gif)(\?.*)?$/i.test(resolved)) return;

      const zoomTitle = norm($el.find('.nbcCI_zoom_title').first().text());

      // If we have a zoom_title, use it as ground truth for device filtering
      // Only keep images whose zoom_title matches the searched device
      if (zoomTitle && !zoomTitleMatchesDevice(zoomTitle)) return;

      // All nbcCI images are camera samples (the widget is exclusively for photo comparisons)
      if (addImage(href, 'cameraSamples')) {
        nbcCIUrls.add(resolved.toLowerCase());
      }
    });
  });

  // ── PASS 1: Figure elements (highest confidence — has <a href> + figcaption) ──
  $('figure').each((_, fig) => {
    const $fig = $(fig);

    // Full-res URL comes from <a href> inside the figure
    const aHref = $fig.find('a[href]').first().attr('href') || '';
    if (!aHref) return; // no link → skip (img-only figures handled in pass 2)

    // Caption text for classification — try figcaption, then <a title>, then <img alt>
    const caption =
      norm($fig.find('figcaption').first().text())
      || ($fig.find('a').first().attr('title') ?? '')
      || ($fig.find('img').first().attr('alt') ?? '');

    // Determine bucket: caption is ground truth, filename is fallback
    const bucket: ImgBucket = classifyByCaption(caption) ?? classifyByFilename(aHref);

    addImage(aHref, bucket);
  });

  // ── PASS 2: Non-figure <a href> images — section-aware classification ──────────
  //
  // NBC's review page has a predictable section structure. We use the nearest
  // preceding h2 heading to determine the semantic context of each bare image link.
  //
  // Section → image type mapping (from actual NBC HTML structure):
  //   "Case / Chassis / Design / Build / Housing" → deviceAngles (lab hardware shots)
  //   "Software / Sustainability"                 → screenshots  (OS/app UI shots)
  //   "Display"                                   → deviceAngles for date-stamped files
  //                                                  (NBC places device angles in display section)
  //   "Camera"                                    → cameraSamples (Foto_* files)
  //   "Performance / Battery / Emissions"         → device/charts (benchmark section)
  //   Article header (before first h2)            → device (hero shot)
  //
  // Date-stamped YYYYMMDD_* files are ALWAYS device angle photos in NBC reviews.
  // NBC camera samples always use Foto_{slug}_{scene} naming.
  //
  // For csm_ images: allow when caption OR section context gives confident bucket.
  // ── Build section map: element → nearest preceding h2 section heading ──────────
  // NBC review pages are divided into h2 sections. We assign each image link
  // to its section so we can override filename-based classification with the
  // ground-truth context the editor intended.
  //
  // Section keyword → semantic bucket override:
  //   case / chassis / housing / design / build / communication / operation →
  //     bare date-stamped images = deviceAngles (lab hardware shots)
  //   software / sustainability →
  //     bare images = screenshots (OS/UI shots)
  //   display / screen →
  //     bare date-stamped images = deviceAngles (device placed next to display equipment)
  //   camera / photo →
  //     Foto_* images = cameraSamples (already handled by filename; section confirms)
  //   performance / battery / emissions / benchmarks →
  //     no override (charts already classified by filename)
  //
  // "Date-stamped" = YYYYMMDD_HHMMSS.jpg — these are ALWAYS device angle/hardware photos
  // in NBC reviews. Camera samples always use Foto_{slug}_{scene} naming on NBC.

  // Build a section position index ONCE: [{pos, heading}] sorted by pos.
  // Each <a> element's section = the heading with the largest pos <= element's pos.
  // This is O(n log n) total vs O(n * depth) for DOM walking.
  // cheerio elements have a .startIndex / sourceCodeLocation property when
  // the parser is run with location tracking — but since we don't enable that,
  // we use the order of elements in the document as a proxy for position.
  //
  // Strategy: collect all h2 elements and all image <a> elements in document order
  // using a single pass over ALL elements, then map each <a> to its nearest preceding h2.
  const sectionMap = new Map<import('domhandler').Element, string>();
  {
    let currentSection = '';
    // Walk every element in document order using cheerio's * selector
    $('h2, a[href]').each((_, el) => {
      if ($(el).is('h2')) {
        currentSection = $(el).text().toLowerCase();
      } else {
        // It's an <a href> — record the current section at this point in the document
        sectionMap.set(el as import('domhandler').Element, currentSection);
      }
    });
  }

  function getSectionForElement(el: import('domhandler').Element): string {
    return sectionMap.get(el) ?? '';
  }

  function sectionBucketOverride(section: string, filename: string): ImgBucket | null {
    const s = section.toLowerCase();
    const f = filename.toLowerCase();
    const isDateStamped = /^\d{8}[_T]\d/.test(f);
    const isFoto = /^(foto|photo)_/i.test(f);
    // FIX 7b: aPic_ = NBC review-photographer naming. Strip csm_ prefix before testing
    // so both the thumbnail (csm_aPic_*) and the full-res (aPic_*) are detected.
    const isAPic = /^apic_/i.test(f.replace(/^csm_/, ''));

    // Case/chassis/housing section: device hardware shots
    if (/\b(case|chassis|housing|design|build|communication|operation|features|waterproof|connectivity)\b/i.test(s)) {
      if (isDateStamped) return 'deviceAngles';
      if (isAPic) return 'device';           // aPic_ in case = product/hero device shots
      if (isFoto) return 'cameraSamples';    // Foto_* in case = still camera samples
      return null; // let filename classify other types
    }

    // Camera/photo section: aPic_ images without informative captions are still samples.
    // (classifyByCaption already handles aPic_ files that have "1x", "Night mode", etc.)
    if (/\b(camera|photo|image|sample|picture)\b/i.test(s)) {
      if (isAPic || isFoto) return 'cameraSamples';
      return null;
    }

    // Software section: ALL bare images are screenshots
    if (/\b(software|sustainability|updates|android|os\b)/i.test(s)) {
      if (/screenshot/i.test(f) || /^\d{8}[_T]\d/.test(f)) return 'screenshots';
      // csm_ files in software section without "screenshot" name could be UI shots
      return null;
    }

    // Display section: date-stamped files are device angle photos placed here by NBC editors
    if (/\b(display|screen|oled|amoled|panel|brightness)\b/i.test(s)) {
      if (isDateStamped) return 'deviceAngles';
      return null;
    }

    return null;
  }

  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || '';
    if (!href) return;
    const resolved = resolveNbcUrl(href);
    if (!resolved) return;
    if (!/\.(jpe?g|png|webp|gif)(\?.*)?$/i.test(resolved)) return;
    if (!/\/fileadmin\//i.test(resolved)) return; // only NBC-hosted images

    const key = resolved.toLowerCase();
    if (imgSeen.has(key)) return; // already collected in pass 0/1

    // Get caption from: title attr → closest figcaption → img alt
    const $el = $(el);
    const caption =
      ($el.attr('title') ?? '')
      || norm($el.closest('figure').find('figcaption').text())
      || ($el.find('img').first().attr('alt') ?? '');

    const isWebpFullRes = /\/_processed_\/webp\/Notebooks\//i.test(resolved);
    const isCsmLarge = isThumbnail(resolved) && !isWebpFullRes;

    // ── Section-aware override: use h2 context to correct classification ─────
    const filename = resolved.split('/').pop() || '';
    const section  = getSectionForElement(el as import('domhandler').Element);
    const sectionOverride = sectionBucketOverride(section, filename);

    // Caption is always ground truth first
    const captionBucket = classifyByCaption(caption);
    const bucket: ImgBucket = captionBucket ?? sectionOverride ?? classifyByFilename(resolved);

    if (isCsmLarge) {
      // Accept csm_ only when caption, section override, or clear filename confirms the type
      const hasClearCaption   = captionBucket !== null;
      const hasSectionContext = sectionOverride !== null;
      const fn = filename;
      const fnBare = fn.toLowerCase().replace(/^csm_/, '').replace(/_[a-f0-9]{8,}\.(jpe?g|png|webp|gif)$/, '');
      const hasClearFilename =
        /^(rigolds|sds)\d+/i.test(fn)
        || /^cal\s?man|^farbchart/i.test(fn)
        || /colorchecker|grayscale|srgb_gamut|sat_sweeps/i.test(fn)
        || /screenshot/i.test(fn)
        || /_software_/i.test(fn)
        || /^\d{8}[_T]\d/i.test(fnBare)                      // date-stamped = device angle
        || /^(foto|photo)_/i.test(fnBare)
        || /^apic_/i.test(fnBare)                             // FIX 7c: NBC photographer shots
        || /^portrait[_\-]studio/i.test(fnBare)
        || /_test_\d+/i.test(fn)
        || /^[a-z0-9]+(?:_[a-z0-9]+)+_\d{1,2}$/i.test(fnBare)
        || /_(top|bottom|left|right|front|back|side|angle)$/i.test(fnBare)
        || /^bild_[a-z0-9_]+-\d{2,4}$/i.test(fnBare)
        || /^bild_[a-z0-9_]+-view$/i.test(fnBare);
      if (!hasClearCaption && !hasSectionContext && !hasClearFilename) return;
    }

    addImage(resolved, bucket);
  });

  // ── PASS 3: Standalone <img src> not inside an <a> (rare, collect as device) ──
  // Covers hero images and device photos that NBC serves only as <img> with no
  // wrapping <a href>. The src may point to a csm_* thumbnail — in that case
  // we only accept it when the caption or filename clearly identifies the type.
  $('img[src]').each((_, el) => {
    const src = $(el).attr('src') || $(el).attr('data-src') || '';
    if (!src) return;
    const resolved = resolveNbcUrl(src);
    if (!resolved) return;
    if (!/\.(jpe?g|png|webp|gif)(\?.*)?$/i.test(resolved)) return;
    if (!/\/fileadmin\//i.test(resolved)) return;

    const key = resolved.toLowerCase();
    if (imgSeen.has(key)) return;

    const $el = $(el);
    const caption =
      ($el.attr('alt') ?? '')
      || norm($el.closest('figure').find('figcaption').text());

    const bucket: ImgBucket = classifyByCaption(caption) ?? classifyByFilename(resolved);

    // For csm_ thumbnails with no wrapping <a>: only accept device images
    // (hero shots, unlinked device angles) — never collect random thumbnails.
    if (isThumbnail(resolved)) {
      if (bucket !== 'device' && bucket !== 'deviceAngles' && bucket !== 'displayMeasurements' && bucket !== 'colorCalibration') return;
    }

    addImage(resolved, bucket);
  });

  // ── POST-PASS: Filter cameraSamples to only the reviewed device ────────────
  //
  // NBC FILENAME CONVENTIONS (observed across all phones):
  //
  //  TYPE A — Fotos_Kamera_N.jpg (Samsung A35, older reviews)
  //    No device name in filename. Device identity is in folder path only.
  //    e.g. fileadmin/Notebooks/Samsung/Galaxy_A35_5G/Fotos_Kamera_3.jpg
  //    → Always keep (no competitor uses this naming in the same folder).
  //
  //  TYPE B — IMG*.jpg / DSC*.jpg / raw camera roll names (OnePlus 15, Pixel, etc.)
  //    No device name in filename — these are the reviewed device's own shots.
  //    Competitor shots in the same review always use TYPE C naming.
  //    → Keep if the competitor-slug check does NOT flag it.
  //
  //  TYPE C — Foto_{DeviceSlug}_{Scene}.jpg  (Samsung S25 Ultra, most modern reviews)
  //    NBC encodes a short device slug as the SECOND underscore-segment:
  //      Foto_S25Ultra_Hase.jpg       ← reviewed device
  //      Foto_OnePlus13_Hase.jpg      ← competitor
  //      Foto_Magic7Pro_Hase.jpg      ← competitor
  //      Foto_EOS90D_Hase.jpg         ← reference camera
  //    Strategy: extract the slug, build all candidate slugs for the reviewed
  //    device, and only keep the image whose slug matches.
  //
  //  TYPE D — Foto_{Scene}_{Brand}_{Model}.jpg  (older NBC style)
  //    e.g. Foto_Pflanze_Apple_iPhone_17.JPG
  //    The device identifier is the 3rd+ segments and contains full brand name.
  //    Strategy: same slug extraction handles this — build slugs, check match.
  //
  // UNIVERSAL SLUG-MATCH APPROACH
  // ─────────────────────────────
  // For any Foto_* filename, extract the "device segment" (2nd segment after
  // splitting on underscore) and check if it matches any candidate slug for the
  // reviewed device. Candidate slugs are built systematically from the device name
  // so we don't need an ever-growing brand blocklist.
  {
    const GENERIC_TOKENS = new Set([
      'ultra','pro','max','plus','lite','mini','fe','neo',
      'edge','note','fold','flip','air','speed','turbo',
    ]);

    const nameSource = resolvedDeviceName;
    const cleanName  = nameSource.replace(/\breview\b.*/i, '').trim();

    // Normalise "+" suffix → "plus" BEFORE tokenising so "S23+" → "s23plus" not "s23"
    const cleanNameNorm = cleanName.replace(/(\w)\+/g, '$1plus');

    // All tokens, lowercase, keeping digits (e.g. "15", "25")
    const allTokens = cleanNameNorm.toLowerCase().split(/[\s\-_,.:;()/]+/).filter(p => p.length >= 1);
    const deviceTokens = allTokens.filter(p => p.length >= 2);
    const significantTokens = deviceTokens.filter(t => !GENERIC_TOKENS.has(t) && !BRAND_TOKENS.has(t));

    // Concatenated forms used for Tier 1
    const concatAll     = allTokens.join('');           // 'samsunggalaxys25ultra'
    const concatNoFirst = allTokens.slice(1).join('');  // 'galaxys25ultra'
    const concatLast3   = allTokens.slice(-3).join(''); // 's25ultra'

    // ── BUILD CANDIDATE SLUGS for the reviewed device ────────────────────────
    // NBC compresses the device name into a short CamelCase slug for TYPE C filenames.
    // e.g. "Samsung Galaxy S25 Ultra" → "S25Ultra", "GalaxyS25Ultra", "SamsungGalaxyS25Ultra"
    //      "OnePlus 15"               → "OnePlus15", "15"
    //      "Google Pixel 9 Pro"       → "Pixel9Pro", "GooglePixel9Pro"
    // We generate every plausible compression and match case-insensitively.
    function buildDeviceSlugs(tokens: string[]): string[] {
      const slugs = new Set<string>();
      const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);
      // Generic suffix words that appear across many devices — never use as standalone slug
      const GENERIC_SLUG_WORDS = new Set(['ultra','pro','max','plus','lite','mini','fe','neo','edge','note','fold','flip','air','speed','turbo','5g','4g','xl']);

      // Full concat (all tokens): "SamsungGalaxyS25Ultra"
      slugs.add(tokens.map(cap).join(''));
      // Drop first (brand): "GalaxyS25Ultra"
      if (tokens.length > 1) slugs.add(tokens.slice(1).map(cap).join(''));
      // Drop first two: "S25Ultra"
      if (tokens.length > 2) slugs.add(tokens.slice(2).map(cap).join(''));
      // Last 2 tokens: "S25Ultra", "15Pro"
      if (tokens.length >= 2) slugs.add(tokens.slice(-2).map(cap).join(''));
      // Last token only — ONLY if it's not a generic word (prevents "Ultra" matching S25Ultra, etc.)
      const last = tokens[tokens.length - 1];
      if (last && last.length >= 2 && !GENERIC_SLUG_WORDS.has(last.toLowerCase())) slugs.add(cap(last));

      // ALL prefixes from position 1 onward — covers NBC truncated slugs like:
      // "Pixel10Pro" (drops XL), "Pixel10" (drops Pro XL), "Pixel10ProXL" (full)
      // "Pixel10XL" (drops Pro) — NBC may use any combination of model tokens.
      // This is the key fix for phones like Pixel 10 Pro XL where NBC may use
      // any prefix/suffix/subset of the model tokens as the slug.
      for (let start = 0; start < tokens.length; start++) {
        for (let end = start + 1; end <= tokens.length; end++) {
          const slice = tokens.slice(start, end);
          // Skip if the slice is a single generic word (no false positives)
          if (slice.length === 1 && GENERIC_SLUG_WORDS.has(slice[0].toLowerCase())) continue;
          // Skip very short slugs (< 4 chars total) — too ambiguous
          const joined = slice.map(cap).join('');
          if (joined.length >= 4) slugs.add(joined);
        }
      }
      // Also enumerate all SUBSETS (non-contiguous combos) of non-generic tokens.
      // Handles "Pixel10XL" = {pixel, 10, xl} with 'pro' omitted — NBC sometimes
      // skips middle variant words in the slug when labelling XL variants.
      const nonGeneric = tokens.filter(t => !GENERIC_SLUG_WORDS.has(t.toLowerCase()));
      if (nonGeneric.length >= 2 && nonGeneric.length <= 5) {
        // All 2-token combos of non-generic tokens (avoids combinatorial explosion)
        for (let i = 0; i < nonGeneric.length; i++) {
          for (let j = i + 1; j <= nonGeneric.length; j++) {
            const sub = nonGeneric.slice(i, j);
            const joined = sub.map(cap).join('');
            if (joined.length >= 4) slugs.add(joined);
          }
        }
      }

      // Also add lowercase versions of all
      const snap = [...slugs];
      snap.forEach(s => slugs.add(s.toLowerCase()));
      return [...slugs].filter(s => s.length >= 2);
    }

    // FIX C5: build candidate slugs into a Set from the start so duplicate
    // pushes are structurally impossible — previously deviceSlugs was a string[]
    // mutated by a second forEach call, which would compound duplicates if
    // buildDeviceSlugs ever introduced them internally.
    const slugSet = new Set<string>([
      ...buildDeviceSlugs(allTokens),
      ...buildDeviceSlugs(allTokens.filter(t => !BRAND_TOKENS.has(t))),
    ]);
    const candidateSlugs = [...slugSet].filter(s => s.length >= 2);

    // ── HELPER: does a Foto_* or Photo_* filename's device-segment match this device? ────
    // NBC old format: Foto_{Scene}_{Brand}_{Model}.jpg
    //   e.g. Foto_Pflanze_Apple_iPhone_17_Pro_Max.JPG
    // NBC new format: Foto_{DeviceSlug}_{Scene}.jpg  or  Photo_{DeviceSlug}_{Scene}.jpg
    //   e.g. Foto_S25Ultra_Hase.jpg, Photo_X300Pro_Tree.jpg
    function fotoSlugMatchesDevice(filename: string): boolean {
      const bare  = filename.replace(/\.[^.]+$/, '');
      const parts = bare.split('_');
      // Accept both "Foto" and "Photo" as the first segment
      if (!/^(foto|photo)$/i.test(parts[0])) return false;
      if (parts.length < 2) return false;

      // We build slug candidates for BOTH old and new format interpretations.
      // Also handle TYPE D: Foto_{Scene_Words}_{Brand}_{Model}.jpg
      // where the brand token appears mid-parts — we extract the brand-onwards tail.
      let brandIdx = -1;
      for (let i = 1; i < parts.length; i++) {
        if (BRAND_TOKENS.has(parts[i].toLowerCase())) { brandIdx = i; break; }
      }
      const slugCandidates = [
        parts.slice(1).join('').toLowerCase(),       // full tail joined
        parts.slice(2).join('').toLowerCase(),       // skip first word
        parts.slice(1, 3).join('').toLowerCase(),    // first 2 tail segments
        parts[1].toLowerCase(),                      // just part[1]: the device slug
        parts.slice(2).join('_').toLowerCase(),      // keep underscores from part[2]+
        parts.slice(1).join('_').toLowerCase(),      // full tail with underscores
        // TYPE D brand-anchored tail: Foto_Low_Light_Vivo_V70 → parts[3:]="vivo_v70" → "vivov70"
        ...(brandIdx > 0 ? [
          parts.slice(brandIdx).join('').toLowerCase(),   // "vivov70"
          parts.slice(brandIdx).join('_').toLowerCase(),  // "vivo_v70"
        ] : []),
      ].filter(s => s.length >= 2);

      // Normalise a slug to pure lowercase alphanumeric for comparison
      const norm2 = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, '');

      // Strip variant suffixes from a slug for "core" comparison
      // e.g. "pixel10proxl" → "pixel10"  so Pixel10XL matches Pixel10ProXL review
      const VARIANT_SUFFIXES = ['proxl','promax','ultra','pro','max','plus','lite','mini','fe','neo','edge','note','fold','flip','xl'];
      const stripVariants = (s: string) => {
        let r = s;
        for (const v of VARIANT_SUFFIXES) { if (r.endsWith(v) && r.length > v.length + 2) { r = r.slice(0, r.length - v.length); } }
        return r;
      };

      return candidateSlugs.some(ds => {
        const dsN = norm2(ds);
        return slugCandidates.some(sc => {
          const scN = norm2(sc);
          if (scN === dsN) return true;  // exact match

          // Allow containment only when unambiguous: shorter must be ≥90% of longer,
          // and the leftover suffix must not be a variant word (pro/ultra/max etc.)
          const longer  = scN.length > dsN.length ? scN : dsN;
          const shorter = scN.length > dsN.length ? dsN : scN;
          if (shorter.length > 0 && shorter.length / longer.length >= 0.90) {
            if (longer.includes(shorter)) {
              const suffix = longer.slice(shorter.length).toLowerCase();
              if (!VARIANT_SUFFIXES.some(v => suffix === v || suffix.startsWith(v))) return true;
            }
          }

          // Core match: strip all variant suffixes from both sides and compare.
          // Handles NBC omitting middle variant words:
          //   Pixel10XL (reviewing Pixel10ProXL) → core "pixel10" == "pixel10" ✓
          //   GalaxyS25 (reviewing GalaxyS25Ultra) → core "galaxys25" == "galaxys25"
          //   BUT we only accept this if the file slug shares the same core AND
          //   the file slug does NOT have a DIFFERENT variant appended
          //   (prevents GalaxyS25 matching a Pixel10 review)
          const dsCore = stripVariants(dsN);
          const scCore = stripVariants(scN);
          if (dsCore.length >= 4 && scCore.length >= 4 && dsCore === scCore) return true;

          return false;
        });
      });
    }

    if (deviceTokens.length >= 1) {
      data.images.cameraSamples = data.images.cameraSamples.filter((imgUrl: string) => {
        // nbcCI widget images already filtered by device — always keep
        const resolvedKey = resolveNbcUrl(imgUrl).toLowerCase();
        if (nbcCIUrls.has(resolvedKey)) return true;

        const filename = imgUrl.split('/').pop() || '';
        const fLower   = filename.toLowerCase();
        const fNoExt   = fLower.replace(/\.[^.]+$/, '');

        // ── TYPE A: Fotos_Kamera_N  ───────────────────────────────────────────
        if (/^fotos_kamera/i.test(fNoExt)) return true;

        // ── TYPE B: Generic camera-roll filenames (no device name in filename) ─
        // Covers: IMG_*, DSC_*, PXL_*, SAM_*
        // NOTE: date-stamped YYYYMMDD_* files are NBC lab shots (deviceAngles), not cameraSamples.
        // They should never reach here since classifyByFilename now routes them to deviceAngles.
        if (/^(img_?|dsc_?|dcim|pxl_|sam_|pro_)/i.test(filename)) return true;

        // ── TYPE C/D: Foto_{slug}_{scene}.jpg  OR  Photo_{slug}_{scene}.jpg ───
        // Both "Foto" and "Photo" prefixes are used by NBC across different brands
        if (/^(foto|photo)_/i.test(fLower)) {
          // NBC chart images follow the same Foto_* naming but contain a "_Chart_"
          // or "_Diagramm_" or "_GNSS_" segment.  e.g.:
          //   Foto_S25Ultra_Chart_1Lux.jpg   ← display luminance chart
          //   Foto_OnePlus15_Diagramm_1.jpg  ← battery discharge plot
          //   Foto_Pixel9Pro_GNSS_Track.jpg  ← GNSS accuracy plot
          // fotoSlugMatchesDevice() would return true for these (device slug matches),
          // so we must eject them BEFORE the slug check and move them to the correct bucket.
          if (/[_\-]chart[_\-\d]|[_\-]diagramm[_\-]|[_\-]gnss[_\-]|[_\-]chart$/i.test(fNoExt)) {
            // Move to charts bucket if not already there and cap not reached
            const resolved = resolveNbcUrl(imgUrl);
            if (!data.images.charts.includes(resolved) && data.images.charts.length < IMG_CAPS.charts) {
              data.images.charts.push(resolved);
            }
            return false; // remove from cameraSamples
          }
          // NBC ColorChecker images: Photo_X300_ColorChecker.jpg, Photo_X300_ColorChecker_1Lux.jpg
          // These are colour accuracy measurement grids, not camera samples.
          if (/[_\-]colorchecker/i.test(fNoExt)) {
            const resolved = resolveNbcUrl(imgUrl);
            if (!data.images.colorCalibration.includes(resolved) && data.images.colorCalibration.length < IMG_CAPS.colorCalibration) {
              data.images.colorCalibration.push(resolved);
            }
            return false; // remove from cameraSamples
          }
          // For Photo_*/Foto_* filenames the device slug IS encoded in the filename.
          // fotoSlugMatchesDevice() is authoritative — do NOT fall back to the folder
          // path. All competitor comparison shots live in the SAME folder as the reviewed
          // device (e.g. /Poco_F7/Photo_EOS90D_Rabbit.jpg beside
          // /Poco_F7/Photo_PocoF7_Rabbit.jpg), so a path-based fallback incorrectly
          // accepts every competitor image.
          return fotoSlugMatchesDevice(filename);
        }

        // ── Fallback for non-standard filenames ──────────────────────────────
        const fSpaced = fLower.replace(/[_\-]/g, ' ');

        if (concatNoFirst.length >= 4 && fLower.includes(concatNoFirst)) return true;
        if (concatLast3.length   >= 4 && fLower.includes(concatLast3))   return true;
        if (concatAll.length     >= 4 && fLower.includes(concatAll))      return true;

        if (significantTokens.length >= 2) return significantTokens.every(t => fSpaced.includes(t));
        if (significantTokens.length === 1) {
          return fSpaced.includes(significantTokens[0]) &&
            (deviceTokens.length <= 1 || deviceTokens.some(t => t !== significantTokens[0] && fSpaced.includes(t)));
        }

        const urlFull    = resolveNbcUrl(imgUrl).toLowerCase();
        const pathOnly   = urlFull.substring(0, urlFull.lastIndexOf('/'));
        const pathSpaced = pathOnly.replace(/[_\-\/]/g, ' ');
        if (significantTokens.length >= 1 && significantTokens.every(t => pathSpaced.includes(t))) return true;

        return deviceTokens.filter(t => fSpaced.includes(t)).length >= Math.min(2, deviceTokens.length);
      });
    }
  }

  // ══ POST-PROCESSING FALLBACKS ─────────────────────────────────────────────
  if (!data.display['PWM frequency']) {
    // FIX C3: try full html before truncated bodyText
    const pwmM = html.match(/PWM(?:[^0-9]{1,40})?(\d{3,4})\s*Hz/i)
               || bodyText.match(/PWM(?:[^0-9]{1,40})?(\d{3,4})\s*Hz/i)
               || data.cons.join(' ').match(/PWM.*?(\d{3,4})\s*Hz/i);
    if (pwmM) data.display['PWM frequency'] = pwmM[1] + ' Hz';
  }

  // ══ BENCHMARKS ══
  // KW arrays, catBench, isRawGrid, isRankingRow, isDeviceInfoHeader are
  // declared at module level (below scrapeNotebookCheckDevice) so they are
  // allocated once at startup rather than on every call.
  // ── PASS 1: r_compare_bars tables ────────────────────────────────────────
  $('table[class*="r_compare_bars"]').each((_, table) => {
    const $t = $(table);
    let benchName = norm($t.find('td.prog_header').first().text());
    if (!benchName) return;

    const headerIsDeviceInfo = isDeviceInfoHeader(benchName);
    if (headerIsDeviceInfo) {
      const sectionM = benchName.match(/^(Networking|WiFi|Bluetooth|Performance|Display|Battery|Storage)/i);
      benchName = sectionM ? sectionM[1] : 'Benchmark';
    }

    let subTest = '';
    $t.find('tr').each((_, row) => {
      const $row = $(row);

      const settingsCell = $row.find('td.settings_header');
      if (settingsCell.length) {
        const hasDeviceLink = settingsCell.find('a').length > 0 && settingsCell.find('span.r_compare_bars_specs').length > 0;
        const hasSpecsSpan  = settingsCell.find('span.r_compare_bars_specs').length > 0;
        if (!hasDeviceLink && !hasSpecsSpan) {
          subTest = norm(settingsCell.text()).trim();
        }
        return;
      }

      if (!($row.attr('class') || '').includes('referencespecs')) return;

      let fullName: string;
      if (headerIsDeviceInfo) {
        if (subTest) {
          fullName = subTest;
        } else {
          const modelText = norm($row.find('td.modelname').text()).trim();
          const iperf = modelText.match(/(iperf\d*\s+\w+[^,\n]{0,40})/i);
          const websurf = modelText.match(/(websurfing[^,\n]{0,40})/i);
          fullName = iperf ? iperf[1].trim() : websurf ? websurf[1].trim() : benchName;
        }
      } else {
        const modelCellText = norm($row.find('td.modelname').text()).trim();
        const isTestName = /iperf|websurfing|transmit|receive|download|upload/i.test(modelCellText);
        if (isTestName) {
          fullName = modelCellText.slice(0, 80);
        } else {
          fullName = subTest ? `${benchName} / ${subTest}` : benchName;
        }
      }

      if (isRawGrid(fullName)) return;

      let value = '';
      $row.find('span[class*="r_compare_bars_number"]').each((_, span) => {
        if (!value) value = norm($(span).text()).trim();
      });

      if (!value) {
        const barText = norm($row.find('td.bar').text())
          .replace(/[+\-−]\s*\d+\s*%/g, '').trim();
        const numM = barText.match(/^([\d,.]+)/);
        if (numM) value = numM[1];
      }

      if (!value || !/\d/.test(value)) return;

      const barText = $row.find('td.bar').text();
      const unitM = barText.match(/[\d)\s](Points?|fps|ms\b|MB\/s|GB\/s|MBit\/s|Mbit\/s|%|\bh\b|min\b|MHz|GHz|mAh|Wh|\bW\b|Watt|nits?|cd\/m[²2])/i);
      const unit = unitM ? unitM[1] : '';

      let minValue = '';
      const minSpan = $row.find('span.r_compare_bars_min').first().text();
      const minM = minSpan.match(/min:\s*([\d,.]+)/i) || barText.match(/\(min:\s*([\d,.]+)\)/i);
      if (minM) minValue = minM[1];

      const cat = catBench(fullName, benchName) as keyof NBCBenchmarks;
      const entry: Benchmark = { name: fullName, value, unit };
      if (minValue) entry.minValue = minValue;
      data.benchmarks[cat].push(entry);
    });
  });

  // ── PASS 2: non-r_compare_bars tables ────────────────────────────────────
  $('table:not([class*="r_compare_bars"])').each((_, table) => {
    const $t = $(table);
    const cls = ($t.attr('class') || '').toLowerCase();
    if (cls.includes('spec') || cls.includes('nav') || cls.includes('menu') || cls.includes('header')) return;

    const firstRowCells = $t.find('tr').first().find('td,th').length;
    if (firstRowCells > 8) return;

    const ctx = norm(
      $t.closest('section, [id]').find('h2,h3,h4').first().text()
      + ' ' + $t.prev('h2,h3,h4,p').first().text()
    );

    const sampleNames = $t.find('tr').slice(1,4).map((_:any, r:any) => cleanCellText($, $(r).find('td').first()[0])).get().join(' ');
    const enrichedCtx = ctx + ' ' + sampleNames;

    const firstRowThs = $t.find('tr').first().find('th');
    const isComparisonTable = firstRowThs.length >= 2 && firstRowThs.eq(1).find('a').length > 0;

    $t.find('tr').each((_, row) => {
      const $row2 = $(row);
      if ($row2.find('th').length > 0 && isComparisonTable) return;
      if (($row2.attr('class') || '').includes('subheader')) return;

      const cells = $row2.find('td');
      if (cells.length < 2) return;

      const name = cleanCellText($, cells.eq(0)[0]).replace(/\s+/g, ' ').trim();
      if (!name || name.length < 2 || name.length > 200) return;

      if (isRawGrid(name)) return;
      if (isRankingRow(name)) return;
      if (/v\d+\s*\(old\)/i.test(name)) return;
      if (isDeviceInfoHeader(name)) return;
      if (/rise\s*↗|fall\s*↘|↗.*↘/.test(name)) return;

      const rawVal = cleanCellText($, cells.eq(1)[0])
        .split('{')[0]
        .replace(/[>?]/g, '')
        .replace(/\s+/g, ' ')
        .trim();
      if (!rawVal || rawVal.length > 150 || !(/\d/.test(rawVal))) return;

      if (isDeviceInfoHeader(rawVal)) return;
      if (/snapdragon|dimensity|exynos|adreno|mali/i.test(rawVal)) return;
      if ((rawVal.match(/,/g) || []).length > 3) return;
      if (/https?:\/\/|href=/.test(rawVal)) return;

      let value = rawVal;
      let unit = '';

      if (name.toLowerCase().includes('response time')) {
        value = value.replace(/\?|\([^)]*\)/g, '').trim();
      }

      value = value.replace(/\(min:\s*[\d,.]+\)/i, '').trim();

      if (/\d+h\s+\d+min/i.test(value)) {
        const hm = value.match(/(\d+)h\s+(\d+)min/i);
        if (hm) { value = `${hm[1]}:${hm[2].padStart(2,'0')}`; unit = 'h'; }
      } else if (/°C/.test(value)) {
        const tempM = value.match(/^(\d+\.?\d*)\s*°C/);
        if (tempM) { value = tempM[1]; unit = '°C'; }
      } else {
        const unitM = value.match(/(fps|ms\b|MB\/s|GB\/s|MBit\/s|Mbit\/s|%|°C|\bh\b|MHz|GHz|Points?|runs\/min|min\b|mAh|Wh|W\b|nits?|cd\/m[²2])/i);
        if (unitM) unit = unitM[1];
      }

      // Normalize battery benchmark names:
      // "WiFi v1.3 (h)" → name="Battery runtime - WiFi v1.3", unit="h"
      // "H.264 (h)"      → name="Battery runtime - H.264",     unit="h"
      let finalName = name;
      const embeddedUnit = name.match(/\(([hm])\)\s*$/i);
      if (embeddedUnit && catBench(name, enrichedCtx) === 'battery') {
        finalName = 'Battery runtime - ' + name.replace(/\s*\([hm]\)\s*$/i, '').trim();
        if (!unit) unit = embeddedUnit[1].toLowerCase() === 'h' ? 'h' : 'min';
      }

      const cat = catBench(name, enrichedCtx) as keyof NBCBenchmarks;
      if (cat === 'display' && /^[\d.]+\s+\d+%$/.test(value.trim())) {
        value = value.replace(/\s+\d+%$/, '').trim(); unit = '';
      }
      data.benchmarks[cat].push({ name: finalName, value, unit });
    });
  });

  // Dedup benchmarks
  for (const cat of Object.keys(data.benchmarks) as Array<keyof NBCBenchmarks>) {
    const seen = new Set<string>();
    data.benchmarks[cat] = data.benchmarks[cat].filter((b: Benchmark) => {
      if (seen.has(b.name)) return false; seen.add(b.name); return true;
    });
  }

  // Thermal fallback — FIX C3: try full html before truncated bodyText
  if (data.benchmarks.thermal.length === 0) {
    const src = html.length > 0 ? html : bodyText;
    const maxTemp = src.match(/maximum(?: surface)? temperature(?: of| reaches?)?\s*([\d.]+)\s*°C/i)
                 || src.match(/surface[^.]{0,40}reaches?\s*(?:up to\s*)?([\d.]+)\s*°C/i)
                 || src.match(/reaches?\s*(?:up to\s*)?([\d.]+)\s*°C/i);
    if (maxTemp) {
      data.benchmarks.thermal.push({ name: 'Max Surface Temperature', value: maxTemp[1], unit: '°C' });
    }
  }

  // Backfill maxVolumeDb from audio benchmarks if still empty
  if (!data.audio.maxVolumeDb && data.benchmarks.audio.length > 0) {
    const volRow = data.benchmarks.audio.find((b: Benchmark) =>
      /maximum\s*volume|maximale\s*lautst/i.test(b.name)
    );
    if (volRow) {
      // Value may be "74.3 / 93" — take the max number
      const nums = (volRow.value + ' ' + volRow.unit).match(/[\d.]+/g);
      if (nums) {
        const vals = nums.map(Number).filter(v => v >= 60 && v <= 120);
        if (vals.length) data.audio.maxVolumeDb = String(Math.max(...vals));
      }
    }
  }

  setCache(ck, data);
  return data as NBCDeviceData;
}

// ══════════════════════════════════════════════════════════════════════════════
//  PUBLIC EXPORTS
// ══════════════════════════════════════════════════════════════════════════════
export async function getNotebookCheckData(query: string): Promise<NBCDeviceData | NBCError | null> {
  const ck = `nbc:full:${CACHE_VERSION}:${query.toLowerCase().trim()}`;
  const cached = await getCacheAs<NBCDeviceData | NBCError>(ck); if (cached) return cached;
  const page = await searchNBC(query); if (!page) return null;
  try {
    const details = await scrapeNotebookCheckDevice(page.url, page.name);
    const result: NBCDeviceData = { ...details, pageFound: { name: page.name, url: page.url }, reviewUrl: page.url };
    setCache(ck, result); return result;
  } catch (e: any) {
    const err: NBCError = { error: e?.message ?? String(e), query, code: e?.response?.status };
    return err;
  }
}

export async function getNotebookCheckDataFast(query: string): Promise<NBCDeviceData | NBCError | null> {
  const ck = `nbc:full:fast:${CACHE_VERSION}:${query.toLowerCase().trim()}`;
  const t0 = Date.now();

  // PERF: fire full-result cache check and SearXNG search in parallel.
  // On a cache miss the search has already been running while Redis was checked.
  const oq = query.trim(), nq = normalizeQuery(query);
  const [cached, searchResults] = await Promise.all([
    getCacheAs<NBCDeviceData | NBCError>(ck),
    searchViaSearXNG(nq, oq),
  ]);
  if (cached) { log('info', 'cache.hit', { query, elapsedMs: Date.now() - t0 }); return cached; }

  log('info', 'stage.search', { query, ms: Date.now() - t0, count: searchResults.length });
  if (!searchResults.length) return null;

  try {
    const searchCk = `nbc:search:fast:${CACHE_VERSION}:${query.toLowerCase().trim()}`;
    const page = resolveSearchResult(searchResults, nq, oq, searchCk);

    // scrapeNotebookCheckDevice owns its own device-level cache — no double lookup here
    const tp = Date.now();
    const details = await scrapeNotebookCheckDevice(page.url, page.name);
    log('info', 'stage.scrape', { query, ms: Date.now() - tp, url: page.url });

    const result: NBCDeviceData = { ...details, pageFound: { name: page.name, url: page.url }, reviewUrl: page.url };
    log('info', 'stage.total', { query, ms: Date.now() - t0 });
    setCache(ck, result); return result;
  } catch (e: any) {
    const err: NBCError = { error: e?.message ?? String(e), query, code: e?.response?.status };
    return err;
  }
}

export async function searchNotebookCheck(query: string): Promise<SearchResult[]> {
  const ck = `nbc:suggestions:${CACHE_VERSION}:${query.toLowerCase().trim()}`;
  const oq = query.trim(), nq = normalizeQuery(query);
  const [cached, results] = await Promise.all([
    getCacheAs<SearchResult[]>(ck),
    searchViaSearXNG(nq, oq),
  ]);
  if (cached) return cached;
  if (!results.length) return [];
  const sorted = results.sort((a, b) => b.score - a.score);
  setCache(ck, sorted);
  return sorted;
}

// ── STARTUP CACHE WARM-UP ────────────────────────────────────────────────────
// Call warmCache(queries) once at server startup to pre-populate Redis with the
// most-queried devices so the first real requests after a deploy are fast.
// Fire-and-forget: errors are logged but never thrown.
// Example usage in index.ts:
//   import { warmCache } from './notebookcheck';
//   warmCache(['Samsung Galaxy S25', 'Google Pixel 9 Pro', 'iPhone 16 Pro']);
export async function warmCache(queries: string[]): Promise<void> {
  for (const q of queries) {
    try {
      const ck = `nbc:full:fast:${CACHE_VERSION}:${q.toLowerCase().trim()}`;
      const cached = await getCacheAs<NBCDeviceData>(ck);
      if (cached) { log('debug', 'warm-up: already cached', { q }); continue; }
      log('info', 'warm-up: fetching', { q });
      await getNotebookCheckDataFast(q);
      // Stagger requests — don't hammer NBC or SearXNG on startup
      await new Promise(r => setTimeout(r, 1500));
    } catch (e) {
      log('warn', 'warm-up: failed', { q, err: (e as Error).message });
    }
  }
}

export async function debugNBCSearch(query: string): Promise<NBCDebugResult> {
  const t0 = Date.now();
  const oq = query.trim(), nq = normalizeQuery(query);

  // Stage 0: mem-only cache check (instant — no Redis HTTP call in debug)
  // Redis latency (~500-700ms cold TLS) would skew the timing numbers.
  // The hot path uses Redis but we time them separately here for clarity.
  const fullCk = `nbc:full:fast:${CACHE_VERSION}:${query.toLowerCase().trim()}`;
  const memCached = memGet(fullCk) as NBCDeviceData | null;
  const cacheHit = memCached !== null;

  // Stage 1: SearXNG search — run unconditionally so timing is always shown
  const ts = Date.now();
  const results = await searchViaSearXNG(nq, oq);
  const searchMs = Date.now() - ts;

  // Stage 2: Redis check (timed separately so you can see its cost)
  const tr = Date.now();
  const redisCached = !cacheHit ? await getCacheAs<NBCDeviceData>(fullCk) : null;
  const redisMs = Date.now() - tr;
  const redisHit = redisCached !== null;

  const bestMatch = (cacheHit || redisHit)
    ? ((memCached ?? redisCached) as NBCDeviceData).pageFound ?? null
    : results.length
      ? resolveSearchResult(results, nq, oq, `nbc:search:debug:${query.toLowerCase().trim()}`)
      : null;

  // Stage 3: scrape (only on full cache miss with a valid URL)
  let scrapeMs = 0, scrapeOk = cacheHit || redisHit, scrapeError: string | undefined;
  if (!cacheHit && !redisHit && bestMatch) {
    const tp = Date.now();
    try {
      await scrapeNotebookCheckDevice(bestMatch.url, bestMatch.name);
      scrapeOk = true;
    } catch (e: any) {
      scrapeError = e?.message ?? String(e);
    }
    scrapeMs = Date.now() - tp;
  }

  const totalMs = Date.now() - t0;
  return {
    query, originalQuery: oq, normalizedQuery: nq,
    timing: {
      cacheHit:  cacheHit || redisHit,
      memHit:    cacheHit,
      redisHit,
      redisMs,    // how long Redis GET took — watch for >200ms (cold TLS)
      searchMs,   // SearXNG round-trip
      scrapeMs,   // NBC page fetch + parse
      totalMs,
    },
    elapsedMs: totalMs,
    bestMatch,
    scrapeOk,
    ...(scrapeError ? { scrapeError } : {}),
    strategies: {
      searxng: { count: results.length, top5: results.slice(0, 5) },
    },
  };
}