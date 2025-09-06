// masterScraper.js — batched + no hard limit + 14‑day direct-post window
// - Removes the hard LIMIT 2000 (configurable MAX_TO_SCRAPE; 0 = unlimited)
// - Processes direct-post scraping in chunks (SCRAPE_CHUNK)
// - Enforces a recency window for *direct posts* from DB via DIRECT_POST_WINDOW_DAYS
// - Keeps PROFILE_DISCOVERY_DAYS for TB/IPWT profile discovery (Apify inputs) and adds an extra JS-side filter

import { ApifyClient } from 'apify-client';
import { google } from 'googleapis';
import { Client as PgClient } from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import 'dotenv/config';

/* =========================
   CONFIG
   ========================= */
const APIFY_TOKEN                = process.env.APIFY_TOKEN;
const TIKTOK_ACTOR_ID            = process.env.APIFY_TIKTOK_ACTOR_ID;
const INSTAGRAM_ACTOR_ID         = process.env.APIFY_ACTOR_INSTAGRAM_ID || process.env.APIFY_ACTOR_ID; // support “original”

const DB_HOST                    = process.env.DB_HOST || '127.0.0.1';
const DB_PORT                    = Number(process.env.DB_PORT || 5432);
const DB_NAME                    = process.env.DB_NAME || 'prod';
const DB_USER                    = process.env.DB_USER || 'vm_user';
const DB_PASSWORD                = process.env.DB_PASSWORD || '';
const DB_SSLMODE                 = (process.env.DB_SSLMODE || 'require').toLowerCase();

const SHEET_ID                   = process.env.INFLUENCER_TRACKER_SHEET;
const SHEET_TAB                  = process.env.GOOGLE_SHEETS_RANGE || 'History Matrix';

const TEST5                      = process.argv.includes('--test5') || process.env.SCRAPER_TEST_TIKTOK_5 === '1';

// discovery window (days) for TB/IPWT profile scrape
const PROFILE_DISCOVERY_DAYS     = Number(process.env.PROFILE_DISCOVERY_DAYS || 14);
// recency window (days) for *direct posts* pulled from DB
const DIRECT_POST_WINDOW_DAYS    = Number(process.env.DIRECT_POST_WINDOW_DAYS || 14);

// batching controls
const MAX_TO_SCRAPE              = Number(process.env.MAX_TO_SCRAPE || 0);   // 0 = unlimited
const SCRAPE_CHUNK               = Number(process.env.SCRAPE_CHUNK || 300);   // per-platform batch size
const SLEEP_BETWEEN_BATCHES_MS   = Number(process.env.SLEEP_BETWEEN_BATCHES_MS || 0);

// tag filters (used for profile discovery)
const TAGS_CANON = [
  "@In Print We Trust", "@in print we trust", "@InPrintWeTrust", "@inprintwetrust",
  "@inprintwetrust.co", "@InPrintWeTrust.co", "#InPrintWeTrust", "#inprintwetrust",
  "#IPWT", "#ipwt"
];

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

/* =========================
   UTILS
   ========================= */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function canonLink(url) {
  if (!url) return null;
  let s = String(url).trim();
  s = s.replace(/\?[^#]*$/, '');
  s = s.replace(/#.*/, '');
  s = s.replace(/\/+$, '');
  return s || null;
}
function firstNumber(...xs) {
  for (const x of xs) {
    const n = Number(x);
    if (Number.isFinite(n)) return n;
  }
  return null;
}
function daysAgoIso(days) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - Number(days || 0));
  return d.toISOString().split('T')[0];
}
function currentSlotUTC() {
  const now = new Date();
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth();
  const d = now.getUTCDate();
  const h = now.getUTCHours();
  let slotHour; let y2 = y, m2 = m, d2 = d;
  if (h < 7) { const prev = new Date(Date.UTC(y, m, d)); prev.setUTCDate(prev.getUTCDate() - 1); y2 = prev.getUTCFullYear(); m2 = prev.getUTCMonth(); d2 = prev.getUTCDate(); slotHour = 19;
  } else if (h < 13) slotHour = 7;
    else if (h < 19) slotHour = 13;
    else slotHour = 19;
  const slotStart = new Date(Date.UTC(y2, m2, d2, slotHour, 0, 0));
  const slotLabel = slotStart.toISOString().replace('.000Z', 'Z');
  return { slotStart, slotLabel };
}

// TikTok helpers
function getTiktokPostId(url) {
  if (!url) return '';
  const m = url.match(/\/(video|photo)\/(\d+)/);
  return m ? m[2] : '';
}
function tiktokIsTagged(item) {
  const txt = [
    item.text, item.caption, item.desc, item.description,
    Array.isArray(item.hashtags) ? item.hashtags.join(' ') : '',
  ].filter(Boolean).join(' ').toLowerCase();
  return TAGS_CANON.some(t => txt.includes(t.toLowerCase()));
}

// Instagram helpers (shortCode-first)
function getIGShortCode(url) {
  if (!url) return '';
  const m = url.match(/\/(p|reel|tv)\/([^\/?#]+)(?:[\/?#]|$)/);
  return m ? m[2] : '';
}
function getIGShortCodeFromResult(item) {
  return (item.shortCode && String(item.shortCode)) || getIGShortCode(item.url || item.postUrl || item.inputUrl);
}
function igIsTagged(item) {
  const caption = (item.caption || item.captionText || item.text || item.description || '');
  const tags = Array.isArray(item.hashtags) ? item.hashtags.join(' ') : '';
  const mentions = Array.isArray(item.mentions)
    ? item.mentions.join(' ')
    : Array.isArray(item.userTags)
      ? item.userTags.map(u => (typeof u === 'string' ? u : (u?.username || u?.name || ''))).join(' ')
      : '';
  const haystack = `${caption} ${tags} ${mentions}`.toLowerCase();
  return TAGS_CANON.some(t => haystack.includes(t.toLowerCase()));
}

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function coerceMs(ts) {
  if (ts == null) return NaN;
  if (typeof ts === 'number') return ts < 1e12 ? ts * 1000 : ts; // handle seconds vs ms
  const n = Date.parse(ts);
  return Number.isFinite(n) ? n : NaN;
}

/* =========================
   POSTGRES
   ========================= */
function pgClient() {
  const ssl = DB_SSLMODE === 'disable' ? false : { rejectUnauthorized: false };
  return new PgClient({ host: DB_HOST, port: DB_PORT, database: DB_NAME, user: DB_USER, password: DB_PASSWORD, ssl });
}

async function ensureHistoryTables(pg) {
  await pg.query(`CREATE SCHEMA IF NOT EXISTS analytics;`);
  await pg.query(`
    CREATE TABLE IF NOT EXISTS analytics.history_posts(
      post_link       TEXT PRIMARY KEY,
      created_at_date DATE,
      profile_link    TEXT,
      rev_stream      TEXT
    );
  `);
  await pg.query(`
    CREATE TABLE IF NOT EXISTS analytics.history_post_metrics(
      post_link  TEXT NOT NULL REFERENCES analytics.history_posts(post_link) ON DELETE CASCADE,
      slot_start TIMESTAMPTZ NOT NULL,
      views      BIGINT,
      PRIMARY KEY (post_link, slot_start)
    );
  `);
}

async function cleanupHistory(pg) {
  await pg.query(`DELETE FROM analytics.history_posts
                  WHERE post_link IS NULL OR btrim(post_link) = '' OR profile_link IS NULL OR btrim(profile_link) = '';`);
  await pg.query(`DELETE FROM analytics.history_post_metrics
                  WHERE post_link IS NULL OR btrim(post_link) = '' OR slot_start IS NULL;`);
}

async function persistHistoryMetrics(pg, snapshots, slotStartISO) {
  if (!snapshots || snapshots.length === 0) return { written: 0, skipped: 0 };
  const sql = `
    INSERT INTO analytics.history_post_metrics (post_link, slot_start, views)
    VALUES ($1, $2::timestamptz, $3)
    ON CONFLICT (post_link, slot_start) DO UPDATE
      SET views = EXCLUDED.views;
  `;
  let written = 0, skipped = 0;
  for (const s of snapshots) {
    const post = canonLink(s.post_link);
    const views = s.views == null ? null : Number(s.views);
    if (!post) { skipped++; continue; }
    await pg.query(sql, [post, slotStartISO, Number.isFinite(views) ? views : null]);
    written++;
  }
  return { written, skipped };
}

async function refreshHistoryMVs(pg, nSlots = 90) {
  try {
    await pg.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.history_matrix_long_mv;`);
  } catch {
    await pg.query(`REFRESH MATERIALIZED VIEW analytics.history_matrix_long_mv;`);
  }
  await pg.query(`SELECT analytics.rebuild_history_matrix_wide($1);`, [nSlots]);
}

/** Pick posts to scrape (DB → history posts / or test5). Respects DIRECT_POST_WINDOW_DAYS. */
async function selectPostsToScrape(pg) {
  if (TEST5) {
    const q = `
      SELECT
        regexp_replace(regexp_replace(regexp_replace("Original.Post", '\\?[^#]*$', ''), '#.*$', ''), '/+$','') AS post_link,
        'tiktok' AS platform
      FROM analytics.posts_full
      WHERE ("Original.Post" IS NOT NULL AND "Original.Post" <> '')
        AND (lower("Post.Type") = 'tiktok' OR "Original.Post" ILIKE 'https://www.tiktok.com/%')
      ORDER BY ("Created.At..ISO8601.")::timestamptz DESC
      LIMIT 5;
    `;
    const { rows } = await pg.query(q);
    return rows;
  }

  const limitClause = MAX_TO_SCRAPE > 0 ? `LIMIT ${MAX_TO_SCRAPE}` : '';
  const q = `
    WITH latest_slot AS (
      SELECT COALESCE(max(slot_start), '1970-01-01'::timestamptz) AS s
      FROM analytics.history_matrix_long_mv
    )
    SELECT h.post_link,
           CASE
             WHEN h.profile_link ILIKE 'https://www.tiktok.com/%' THEN 'tiktok'
             WHEN h.profile_link ILIKE 'https://www.instagram.com/%' THEN 'instagram'
             ELSE CASE
                    WHEN h.post_link ILIKE 'https://www.tiktok.com/%' THEN 'tiktok'
                    WHEN h.post_link ILIKE 'https://www.instagram.com/%' THEN 'instagram'
                    ELSE 'unknown'
                  END
           END AS platform
    FROM analytics.history_posts h
    CROSS JOIN latest_slot ls
    LEFT JOIN analytics.history_matrix_long_mv m
      ON m.post_link = h.post_link AND m.slot_start = ls.s
    WHERE (m.post_link IS NULL)
      AND h.post_link IS NOT NULL
      AND h.created_at_date IS NOT NULL
      AND h.created_at_date >= CURRENT_DATE - INTERVAL '${DIRECT_POST_WINDOW_DAYS} days'
    ORDER BY h.created_at_date DESC
    ${limitClause};
  `;
  const { rows } = await pg.query(q);
  return rows;
}

// Pull TB/IPWT profiles from DB within window
async function selectTbIpwtProfiles(pg, days) {
  const since = daysAgoIso(days);
  const { rows } = await pg.query(
    `
    SELECT DISTINCT profile_link
    FROM analytics.history_posts
    WHERE created_at_date >= $1
      AND COALESCE(LOWER(rev_stream), '') IN ('trailblazer','ipwt')
      AND profile_link IS NOT NULL
      AND profile_link <> ''
    `,
    [since]
  );
  const tt = []; const ig = [];
  for (const r of rows) {
    const p = String(r.profile_link || '');
    if (p.includes('tiktok.com/')) tt.push(p);
    else if (p.includes('instagram.com/')) ig.push(p);
  }
  return { ttProfiles: tt, igProfiles: ig };
}

/* =========================
   GOOGLE SHEETS
   ========================= */
function resolveCredentialsPath() {
  const candidates = [
    process.env.GOOGLE_APPLICATION_CREDENTIALS,
    process.env.GOOGLE_CREDENTIALS_JSON_PATH,
    path.resolve(process.cwd(), 'credentials.json'),
    path.resolve(__dirname, 'credentials.json'),
  ].filter(Boolean);
  for (const p of candidates) { try { if (p && fs.existsSync(p)) return p; } catch { /* noop */ } }
  return null;
}
function getGoogleAuth() {
  const keyFile = resolveCredentialsPath();
  if (keyFile) {
    console.log(`Sheets: using credentials file → ${keyFile}`);
    return new google.auth.GoogleAuth({ keyFile, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  }
  if (process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL && process.env.GOOGLE_PRIVATE_KEY) {
    const key = process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n');
    console.log(`Sheets: using inline service account from env.`);
    return new google.auth.JWT({ email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL, key, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  }
  throw new Error('Google auth not configured. Provide credentials.json or service-account env vars.');
}
async function sanityCheckSheets(sheets) {
  if (!SHEET_ID) return;
  await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID, includeGridData: false });
  console.log('Sheets: auth OK & spreadsheet reachable.');
}
async function findSheetIdByTitle(sheets, title) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = meta.data.sheets.find(s => s.properties?.title === title);
  if (!sheet) throw new Error(`Sheet tab "${title}" not found.`);
  return sheet.properties.sheetId;
}
async function ensureSheetSlotColumn(sheets, slotLabel) {
  const sheetId = await findSheetIdByTitle(sheets, SHEET_TAB);
  const headerResp = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${SHEET_TAB}!A1:ZZ1` });
  const header = headerResp.data.values?.[0] || [];
  const existingIndex = header.findIndex(v => String(v).trim() === slotLabel);
  if (existingIndex >= 0) return existingIndex + 1;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests: [ { insertDimension: { range: { sheetId, dimension: 'COLUMNS', startIndex: 4, endIndex: 5 }, inheritFromBefore: false } } ] }
  });
  await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${SHEET_TAB}!E1`, valueInputOption: 'RAW', requestBody: { values: [[slotLabel]] } });
  return 5; // column E
}
async function buildSheetRowMap(sheets) {
  const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${SHEET_TAB}!C2:C` });
  const rows = resp.data.values || [];
  const map = new Map();
  for (let i = 0; i < rows.length; i++) {
    const link = rows[i]?.[0] ? canonLink(rows[i][0]) : null;
    const rowNumber = 2 + i;
    if (link) map.set(link, rowNumber);
  }
  return map;
}
function colIndex1bToA1(idx1b) {
  let n = idx1b, s = '';
  while (n > 0) { const r = (n - 1) % 26; s = String.fromCharCode(65 + r) + s; n = Math.floor((n - 1) / 26); }
  return s;
}
async function writeViewsToSheet(sheets, colIndex1b, snapshots, prebuiltRowMap) {
  if (!SHEET_ID) return;
  const rowMap = prebuiltRowMap || await buildSheetRowMap(sheets);
  const updates = [];
  for (const s of snapshots) {
    const link = canonLink(s.post_link);
    if (!link) continue;
    const row = rowMap.get(link);
    if (!row) continue;
    const colA1 = colIndex1bToA1(colIndex1b);
    updates.push({ range: `${SHEET_TAB}!${colA1}${row}`, values: [[Number.isFinite(Number(s.views)) ? Number(s.views) : '']] });
  }
  if (!updates.length) { console.log('Sheets: no matching rows to update.'); return; }
  await sheets.spreadsheets.values.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { valueInputOption: 'RAW', data: updates } });
  console.log(`Sheets: updated ${updates.length} cells in column ${colIndex1bToA1(colIndex1b)}.`);
}

// Sheet1 helpers (TT B:F, IG M:Q)
async function sheetColumnValues(sheets, a1Range) {
  const { data } = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: a1Range });
  return (data.values || []).map(r => r[0]).filter(Boolean);
}
async function sheet1TiktokIdSet(sheets) {
  const vals = await sheetColumnValues(sheets, `Sheet1!C2:C`);
  return new Set(vals.map(getTiktokPostId).filter(Boolean));
}
async function sheet1IGShortSet(sheets) {
  const vals = await sheetColumnValues(sheets, `Sheet1!N2:N`);
  return new Set(vals.map(getIGShortCode).filter(Boolean));
}
async function sheetAppendRows(sheets, startCol, rows) {
  if (!rows.length) return;
  // find next empty row in target block
  const endCol = String.fromCharCode(startCol.charCodeAt(0)+4);
  const { data } = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `Sheet1!${startCol}:${endCol}` });
  const used = (data.values || []).length;
  const startRow = Math.max(2, used + 1);
  const endRow = startRow + rows.length - 1;
  await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `Sheet1!${startCol}${startRow}:${endCol}${endRow}`, valueInputOption: 'RAW', resource: { values: rows } });
}

/* =========================
   APIFY (original input shapes)
   ========================= */
function apify() { if (!APIFY_TOKEN) throw new Error('APIFY_TOKEN missing.'); return new ApifyClient({ token: APIFY_TOKEN }); }
async function fetchAllDatasetItems(client, datasetId) {
  let items = []; let offset = 0; const limit = 5000;
  while (true) {
    const resp = await client.dataset(datasetId).listItems({ clean: true, offset, limit });
    items = items.concat(resp.items || []);
    if (!resp.items?.length || resp.items.length < limit) break;
    offset += resp.items.length;
  }
  return items;
}

// TikTok: direct posts -> { postURLs: [...] }
async function scrapeTiktokDirectPosts(postUrls) {
  if (!postUrls.length || !TIKTOK_ACTOR_ID) return [];
  const client = apify();
  const input = { postURLs: postUrls };
  console.log(`Apify TT (direct): ${postUrls.length} urls via key postURLs`);
  const run = await client.actor(TIKTOK_ACTOR_ID).call(input);
  console.log(`Apify TT runId=${run.id} datasetId=${run.defaultDatasetId} status=${run.status}`);
  if (!run.defaultDatasetId) return [];
  return fetchAllDatasetItems(client, run.defaultDatasetId);
}
// TikTok: profiles -> { profiles:[usernames], captionKeywords, ... }
async function scrapeTiktokProfiles(profileLinks, captionKeywords = [], windowUnified = '30 days') {
  if (!profileLinks.length || !TIKTOK_ACTOR_ID) return [];
  const cleaned = profileLinks.map(url => { let username = (url.split('/').filter(Boolean).pop() || '').trim(); if (username.startsWith('@')) username = username.slice(1); return username; }).filter(Boolean);
  const client = apify();
  const input = { profiles: cleaned, captionKeywords, profileSorting: 'latest', oldestPostDateUnified: windowUnified };
  console.log(`Apify TT (profiles): ${cleaned.length} profiles via key profiles`);
  const run = await client.actor(TIKTOK_ACTOR_ID).call(input);
  console.log(`Apify TT runId=${run.id} datasetId=${run.defaultDatasetId} status=${run.status}`);
  if (!run.defaultDatasetId) return [];
  return fetchAllDatasetItems(client, run.defaultDatasetId);
}

// Instagram: direct posts -> { directUrls: [...] }
async function scrapeInstagramDirectPosts(postUrls) {
  if (!postUrls.length || !INSTAGRAM_ACTOR_ID) return [];
  const client = apify();
  const input = { directUrls: postUrls };
  console.log(`Apify IG (direct): ${postUrls.length} urls via key directUrls`);
  const run = await client.actor(INSTAGRAM_ACTOR_ID).call(input);
  console.log(`Apify IG runId=${run.id} datasetId=${run.defaultDatasetId} status=${run.status}`);
  if (!run.defaultDatasetId) return [];
  return fetchAllDatasetItems(client, run.defaultDatasetId);
}
// Instagram: profiles -> { directUrls:[profiles], resultsType:'posts', ... }
async function scrapeInstagramProfiles(profileLinks, sinceISO, captionKeywords = [], limit = 40) {
  if (!profileLinks.length || !INSTAGRAM_ACTOR_ID) return [];
  const client = apify();
  const input = { directUrls: profileLinks, resultsType: 'posts', onlyPostsNewerThan: sinceISO, resultsLimit: limit, captionKeywords, addParentData: false };
  console.log(`Apify IG (profiles): ${profileLinks.length} profiles via key directUrls`);
  const run = await client.actor(INSTAGRAM_ACTOR_ID).call(input);
  console.log(`Apify IG runId=${run.id} datasetId=${run.defaultDatasetId} status=${run.status}`);
  if (!run.defaultDatasetId) return [];
  return fetchAllDatasetItems(client, run.defaultDatasetId);
}

// Normalizers for direct-post snapshots (post_link+views)
function normalizeTikTokItem(it) {
  const link = canonLink(it.webVideoUrl || it.url || it.link || it.itemUrl || it.postUrl || it.shareUrl);
  const views = firstNumber(it.playCount, it.viewCount, it.views, it.statistics?.playCount, it.metrics?.plays);
  return link ? { post_link: link, views } : null;
}
function normalizeInstagramItem(it) {
  const link = canonLink(it.url || it.postUrl || it.link || it.canonicalUrl || it.inputUrl);
  const views = firstNumber(it.videoPlayCount, it.videoViewCount, it.views, it.insights?.video_views, it.metrics?.plays);
  return link ? { post_link: link, views } : null;
}

/* =========================
   MAIN
   ========================= */
(async () => {
  if (!SHEET_ID) console.warn('INFLUENCER_TRACKER_SHEET not set — Sheets mirroring will be skipped.');

  // 1) DB connect & table sanity
  const pg = pgClient();
  await pg.connect();
  console.log(`DB: connected → ${DB_HOST}/${DB_NAME} as ${DB_USER}`);
  await ensureHistoryTables(pg);

  // 2) Select candidate posts from DB (now respects DIRECT_POST_WINDOW_DAYS)
  let candidates = await selectPostsToScrape(pg);
  candidates = candidates.filter(c => !!c.post_link);

  // log sample
  console.log('Posts to scrape (from DB):');
  candidates.slice(0, 20).forEach((c, i) => console.log(`${i + 1}. [${c.platform}] ${c.post_link}`));
  console.log(`Total candidates: ${candidates.length}${TEST5 ? '  (TEST5: latest 5 TikToks)' : ''}`);

  // 3) Abort window
  console.log('Pausing 5 seconds… press Ctrl+C to abort.');
  await sleep(5000);

  // 4) Split by platform
  const tiktokLinks = candidates
    .filter(r => r.platform === 'tiktok' || (r.post_link || '').toLowerCase().includes('tiktok.com'))
    .map(r => r.post_link);
  const instagramLinks = candidates
    .filter(r => r.platform === 'instagram' || (r.post_link || '').toLowerCase().includes('instagram.com'))
    .map(r => r.post_link);

  const { slotLabel } = currentSlotUTC();

  // 5) Sheets prep (create new slot col E; build row map once)
  let sheets, colIndex1b, rowMap;
  if (SHEET_ID) {
    const auth = getGoogleAuth();
    sheets = google.sheets({ version: 'v4', auth });
    await sanityCheckSheets(sheets);
    colIndex1b = await ensureSheetSlotColumn(sheets, slotLabel);
    rowMap = await buildSheetRowMap(sheets);
  }

  // 6) Direct-post scraping in batches (Option B)
  async function processDirectChunks(platform, links) {
    if (!links.length) return;
    const chunks = chunk(links, Math.max(1, SCRAPE_CHUNK));
    console.log(`[${platform}] total ${links.length} urls → ${chunks.length} batch(es) of up to ${SCRAPE_CHUNK}.`);

    for (let i = 0; i < chunks.length; i++) {
      const batch = chunks[i];
      const candSetBatch = new Set(batch.map(canonLink));
      console.log(`[${platform}] batch ${i+1}/${chunks.length} (${batch.length} urls)`);

      let items = [];
      if (platform === 'tiktok') items = await scrapeTiktokDirectPosts(batch);
      else if (platform === 'instagram') items = await scrapeInstagramDirectPosts(batch);

      // normalize → snapshots
      const raw = (platform === 'tiktok')
        ? items.map(normalizeTikTokItem).filter(Boolean)
        : items.map(normalizeInstagramItem).filter(Boolean);

      // keep only intended candidates (avoid actor overreach)
      const snapshots = raw.filter(s => s.post_link && candSetBatch.has(canonLink(s.post_link)));

      // write to DB
      const dbRes = await persistHistoryMetrics(pg, snapshots, slotLabel);
      console.log(`DB: upserted ${dbRes.written} (${dbRes.skipped} skipped) for this ${platform} batch.`);

      // mirror to Sheets for this batch
      if (SHEET_ID && colIndex1b) await writeViewsToSheet(sheets, colIndex1b, snapshots, rowMap);

      if (SLEEP_BETWEEN_BATCHES_MS > 0 && i < chunks.length - 1) {
        console.log(`Sleeping ${SLEEP_BETWEEN_BATCHES_MS}ms before next batch…`);
        await sleep(SLEEP_BETWEEN_BATCHES_MS);
      }
    }
  }

  await processDirectChunks('tiktok', tiktokLinks);
  await processDirectChunks('instagram', instagramLinks);

  // 7) Cleanup & MV refresh once
  await cleanupHistory(pg);
  await refreshHistoryMVs(pg, 90);
  console.log('DB: materialized views refreshed; wide matrix rebuilt.');

  // 8) TB/IPWT profile discovery (DB → profiles → tagged posts)
  const { ttProfiles, igProfiles } = await selectTbIpwtProfiles(pg, PROFILE_DISCOVERY_DAYS);

  // TikTok profile run (tag-gated results) + JS-side recency filter
  let ttProfileItems = [];
  if (ttProfiles.length) {
    ttProfileItems = await scrapeTiktokProfiles(ttProfiles, TAGS_CANON, `${PROFILE_DISCOVERY_DAYS} days`);
    const cutoffMs = Date.now() - PROFILE_DISCOVERY_DAYS * 86400_000;
    ttProfileItems = ttProfileItems.filter(it => coerceMs(it.createTime ?? it.createDate ?? it.timestamp ?? it.createTimestamp) >= cutoffMs);
  }

  // Instagram profile run (tag-gated results) + JS-side recency filter
  let igProfileItems = [];
  if (igProfiles.length) {
    const sinceISO = daysAgoIso(PROFILE_DISCOVERY_DAYS);
    igProfileItems = await scrapeInstagramProfiles(igProfiles, sinceISO, TAGS_CANON, 60);
    const cutoffMs = Date.now() - PROFILE_DISCOVERY_DAYS * 86400_000;
    igProfileItems = igProfileItems.filter(it => coerceMs(it.timestamp) >= cutoffMs);
  }

  // Filter ONLY tagged posts
  const ttTagged = ttProfileItems.filter(tiktokIsTagged);
  const igTagged = igProfileItems.filter(igIsTagged);

  // Build Sheet1 dedupe sets
  if (SHEET_ID) {
    const ttSheet1Ids = await sheet1TiktokIdSet(sheets);
    const igSheet1Shorts = await sheet1IGShortSet(sheets);

    const matrixUpdatesFromDiscovery = []; // update col E where matrix contains the post
    const sheet1TTAppends = [];            // B:F rows
    const sheet1IGAppends = [];            // M:Q rows

    // --- TT discovery: update matrix E or append to Sheet1 B:F ---
    for (const post of ttTagged) {
      const url = canonLink(post.webVideoUrl || post.url || post.link || post.postUrl || post.shareUrl || '');
      const postId = getTiktokPostId(url);
      if (!url || !postId) continue;

      const views = firstNumber(post.playCount, post.viewCount, post.views, post.statistics?.playCount, post.metrics?.plays) ?? '';
      const row = rowMap?.get(url);

      if (row) {
        matrixUpdatesFromDiscovery.push({ range: `${SHEET_TAB}!E${row}`, values: [[views]] });
      } else if (!ttSheet1Ids.has(postId)) {
        const username = post.authorUsername || post.username || post.ownerUsername || ((url.match(/tiktok\.com\/@([^\/]+)/) || [,''])[1]);
        const profileLink = username ? `https://www.tiktok.com/@${username}` : '';
        const rawCreated = post.createTime ?? post.createDate ?? post.timestamp ?? post.createTimestamp ?? '';
        const createdMs = coerceMs(rawCreated);
        const pretty = Number.isFinite(createdMs) ? new Date(createdMs).toLocaleDateString('en-US', { year:'numeric', month:'long', day:'numeric' }) : '';
        const createdIso = Number.isFinite(createdMs) ? new Date(createdMs).toISOString() : '';
        sheet1TTAppends.push([profileLink, url, views, pretty, createdIso]); // B:F
      }
    }

    // --- IG discovery: shortCode-first; update matrix E or append to Sheet1 M:Q ---
    for (const post of igTagged) {
      const link = canonLink(post.url || post.postUrl || post.inputUrl || '');
      const sc = getIGShortCodeFromResult(post);
      if (!sc) continue;

      const views = firstNumber(post.videoPlayCount, post.videoViewCount, post.views, post.insights?.video_views, post.metrics?.plays) ?? '';
      const row = link ? rowMap?.get(link) : null;

      if (row) {
        matrixUpdatesFromDiscovery.push({ range: `${SHEET_TAB}!E${row}`, values: [[views]] });
      } else if (!igSheet1Shorts.has(sc)) {
        const ownerUsername = post.ownerUsername || '';
        const profileLink = ownerUsername ? `https://www.instagram.com/${ownerUsername}/reels` : (post.ownerUrl || '');
        const ts = post.timestamp || '';
        const pretty = ts ? new Date(coerceMs(ts)).toLocaleDateString('en-US', { year:'numeric', month:'long', day:'numeric' }) : '';
        const urlFinal = link || (sc ? `https://www.instagram.com/p/${sc}/` : '');
        sheet1IGAppends.push([profileLink, urlFinal, views, pretty, ts]); // M:Q
      }
    }

    // apply matrix updates from discovery (col E)
    if (matrixUpdatesFromDiscovery.length) {
      await sheets.spreadsheets.values.batchUpdate({ spreadsheetId: SHEET_ID, resource: { valueInputOption: 'RAW', data: matrixUpdatesFromDiscovery } });
      console.log(`Sheets (discovery): updated ${matrixUpdatesFromDiscovery.length} matrix cells in column E.`);
    }

    // append Sheet1 rows
    if (sheet1TTAppends.length) { await sheetAppendRows(sheets, 'B', sheet1TTAppends); console.log(`Sheet1: appended ${sheet1TTAppends.length} TikTok rows to B:F.`); }
    if (sheet1IGAppends.length) { await sheetAppendRows(sheets, 'M', sheet1IGAppends); console.log(`Sheet1: appended ${sheet1IGAppends.length} Instagram rows to M:Q.`); }
  }

  await pg.end();
  console.log('✅ done.');
})().catch((err) => {
  console.error('❌ fatal error:', err?.stack || err);
  process.exit(1);
});
