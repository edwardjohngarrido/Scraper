// masterScraper.js
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
const APIFY_TOKEN            = process.env.APIFY_TOKEN;
const TIKTOK_ACTOR_ID        = process.env.APIFY_TIKTOK_ACTOR_ID;
const INSTAGRAM_ACTOR_ID     = process.env.APIFY_ACTOR_INSTAGRAM_ID || process.env.APIFY_ACTOR_ID; // support “original”
const DB_HOST                = process.env.DB_HOST || '127.0.0.1';
const DB_PORT                = Number(process.env.DB_PORT || 5432);
const DB_NAME                = process.env.DB_NAME || 'prod';
const DB_USER                = process.env.DB_USER || 'vm_user';
const DB_PASSWORD            = process.env.DB_PASSWORD || '';
const DB_SSLMODE             = (process.env.DB_SSLMODE || 'require').toLowerCase();
const SHEET_ID               = process.env.INFLUENCER_TRACKER_SHEET;
const SHEET_TAB              = process.env.GOOGLE_SHEETS_RANGE || 'History Matrix';
const TEST5                  = process.argv.includes('--test5') || process.env.SCRAPER_TEST_TIKTOK_5 === '1';

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
  s = s.replace(/#.*$/, '');
  s = s.replace(/\/+$/, '');
  return s || null;
}
function firstNumber(...xs) {
  for (const x of xs) {
    const n = Number(x);
    if (Number.isFinite(n)) return n;
  }
  return null;
}
function currentSlotUTC() {
  const now = new Date();
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth();
  const d = now.getUTCDate();
  const h = now.getUTCHours();
  let slotHour;
  let y2 = y, m2 = m, d2 = d;

  if (h < 7) {
    const prev = new Date(Date.UTC(y, m, d));
    prev.setUTCDate(prev.getUTCDate() - 1);
    y2 = prev.getUTCFullYear(); m2 = prev.getUTCMonth(); d2 = prev.getUTCDate();
    slotHour = 19;
  } else if (h < 13) slotHour = 7;
  else if (h < 19)  slotHour = 13;
  else              slotHour = 19;

  const slotStart = new Date(Date.UTC(y2, m2, d2, slotHour, 0, 0));
  const slotLabel = slotStart.toISOString().replace('.000Z', 'Z');
  return { slotStart, slotLabel };
}

/* =========================
   POSTGRES
   ========================= */
function pgClient() {
  const ssl = DB_SSLMODE === 'disable' ? false : { rejectUnauthorized: false };
  return new PgClient({
    host: DB_HOST, port: DB_PORT, database: DB_NAME,
    user: DB_USER, password: DB_PASSWORD, ssl
  });
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
      post_link  TEXT NOT NULL,
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

/** Pick posts to scrape (DB → history posts / or test5) */
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
    ORDER BY h.created_at_date DESC
    LIMIT 2000;
  `;
  const { rows } = await pg.query(q);
  return rows;
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
  for (const p of candidates) { try { if (p && fs.existsSync(p)) return p; } catch {} }
  return null;
}
function getGoogleAuth() {
  const keyFile = resolveCredentialsPath();
  if (keyFile) {
    console.log(`Sheets: using credentials file → ${keyFile}`);
    return new google.auth.GoogleAuth({
      keyFile,
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
  }
  if (process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL && process.env.GOOGLE_PRIVATE_KEY) {
    const key = process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n');
    console.log(`Sheets: using inline service account from env.`);
    return new google.auth.JWT({
      email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      key,
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
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
  const headerResp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_TAB}!A1:ZZ1`
  });
  const header = headerResp.data.values?.[0] || [];
  const existingIndex = header.findIndex(v => String(v).trim() === slotLabel);
  if (existingIndex >= 0) return existingIndex + 1;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [
        { insertDimension: { range: { sheetId, dimension: 'COLUMNS', startIndex: 4, endIndex: 5 }, inheritFromBefore: false } }
      ]
    }
  });
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_TAB}!E1`,
    valueInputOption: 'RAW',
    requestBody: { values: [[slotLabel]] }
  });
  return 5; // column E
}
async function buildSheetRowMap(sheets) {
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_TAB}!C2:C`
  });
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
async function writeViewsToSheet(sheets, colIndex1b, snapshots) {
  if (!SHEET_ID) return;
  const rowMap = await buildSheetRowMap(sheets);
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
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: 'RAW', data: updates }
  });
  console.log(`Sheets: updated ${updates.length} cells in column ${colIndex1bToA1(colIndex1b)}.`);
}

/* =========================
   APIFY (fixed input shapes)
   ========================= */
function apify() {
  if (!APIFY_TOKEN) throw new Error('APIFY_TOKEN missing.');
  return new ApifyClient({ token: APIFY_TOKEN });
}
async function fetchAllDatasetItems(client, datasetId) {
  let items = [];
  let offset = 0;
  const limit = 5000;
  while (true) {
    const resp = await client.dataset(datasetId).listItems({ clean: true, offset, limit });
    items = items.concat(resp.items || []);
    if (!resp.items?.length || resp.items.length < limit) break;
    offset += resp.items.length;
  }
  return items;
}

// --- TikTok: direct posts (ORIGINAL SHAPE) -> { postURLs: [...] }
async function scrapeTiktokDirectPosts(postUrls) {
  if (!postUrls.length || !TIKTOK_ACTOR_ID) return [];
  const client = apify();
  const input = { postURLs: postUrls };
  console.log(`Apify TT (direct): ${postUrls.length} urls via key postURLs`);
  const run = await client.actor(TIKTOK_ACTOR_ID).call(input);
  console.log(`Apify TT runId=${run.id} datasetId=${run.defaultDatasetId} status=${run.status}`);
  if (!run.defaultDatasetId) return [];
  const items = await fetchAllDatasetItems(client, run.defaultDatasetId);
  return items || [];
}

// --- TikTok: profiles (ORIGINAL SHAPE) -> { profiles: [usernames], ... }
async function scrapeTiktokProfiles(profileLinks, captionKeywords = [], windowUnified = '30 days') {
  if (!profileLinks.length || !TIKTOK_ACTOR_ID) return [];
  // convert "https://www.tiktok.com/@user" -> "user"
  const cleaned = profileLinks.map(url => {
    let username = (url.split('/').filter(Boolean).pop() || '').trim();
    if (username.startsWith('@')) username = username.slice(1);
    return username;
  }).filter(Boolean);
  const client = apify();
  const input = {
    profiles: cleaned,
    captionKeywords,
    profileSorting: 'latest',
    oldestPostDateUnified: windowUnified,
  };
  console.log(`Apify TT (profiles): ${cleaned.length} profiles via key profiles`);
  const run = await client.actor(TIKTOK_ACTOR_ID).call(input);
  console.log(`Apify TT runId=${run.id} datasetId=${run.defaultDatasetId} status=${run.status}`);
  if (!run.defaultDatasetId) return [];
  const items = await fetchAllDatasetItems(client, run.defaultDatasetId);
  return items || [];
}

// --- Instagram: direct posts (ORIGINAL SHAPE) -> { directUrls: [...] }
async function scrapeInstagramDirectPosts(postUrls) {
  if (!postUrls.length || !INSTAGRAM_ACTOR_ID) return [];
  const client = apify();
  const input = { directUrls: postUrls };
  console.log(`Apify IG (direct): ${postUrls.length} urls via key directUrls`);
  const run = await client.actor(INSTAGRAM_ACTOR_ID).call(input);
  console.log(`Apify IG runId=${run.id} datasetId=${run.defaultDatasetId} status=${run.status}`);
  if (!run.defaultDatasetId) return [];
  const items = await fetchAllDatasetItems(client, run.defaultDatasetId);
  return items || [];
}

// --- Instagram: profiles (ORIGINAL SHAPE) -> { directUrls: [profile], resultsType:'posts', ... }
async function scrapeInstagramProfiles(profileLinks, sinceISO, captionKeywords = [], limit = 40) {
  if (!profileLinks.length || !INSTAGRAM_ACTOR_ID) return [];
  const client = apify();
  const input = {
    directUrls: profileLinks,
    resultsType: 'posts',
    onlyPostsNewerThan: sinceISO,
    resultsLimit: limit,
    captionKeywords,
    addParentData: false
  };
  console.log(`Apify IG (profiles): ${profileLinks.length} profiles via key directUrls`);
  const run = await client.actor(INSTAGRAM_ACTOR_ID).call(input);
  console.log(`Apify IG runId=${run.id} datasetId=${run.defaultDatasetId} status=${run.status}`);
  if (!run.defaultDatasetId) return [];
  const items = await fetchAllDatasetItems(client, run.defaultDatasetId);
  return items || [];
}

// Normalizers
function normalizeTikTokItem(it) {
  const link = canonLink(it.webVideoUrl || it.url || it.link || it.itemUrl || it.postUrl || it.shareUrl);
  const views = firstNumber(it.playCount, it.viewCount, it.views, it.statistics?.playCount, it.metrics?.plays);
  return link ? { post_link: link, views } : null;
}
function normalizeInstagramItem(it) {
  const link = canonLink(it.url || it.postUrl || it.link || it.canonicalUrl);
  const views = firstNumber(it.videoPlayCount, it.views, it.viewCount, it.insights?.video_views, it.metrics?.plays);
  return link ? { post_link: link, views } : null;
}

/* =========================
   MAIN
   ========================= */
(async () => {
  if (!SHEET_ID) console.warn('INFLUENCER_TRACKER_SHEET not set — Sheets mirroring will be skipped.');

  // Connect DB
  const pg = pgClient();
  await pg.connect();
  console.log(`DB: connected → ${DB_HOST}/${DB_NAME} as ${DB_USER}`);
  await ensureHistoryTables(pg);

  // Select posts from DB
  let candidates = await selectPostsToScrape(pg);
  candidates = candidates.filter(c => !!c.post_link);

  console.log('Posts to scrape (from DB):');
  candidates.slice(0, 20).forEach((c, i) => console.log(`${i + 1}. [${c.platform}] ${c.post_link}`));
  console.log(`Total candidates: ${candidates.length}${TEST5 ? '  (TEST5: latest 5 TikToks)' : ''}`);

  // Abort window
  console.log('Pausing 5 seconds… press Ctrl+C to abort.');
  await sleep(5000);

  // Split by platform
  const tiktokLinks = candidates
    .filter(r => r.platform === 'tiktok' || (r.post_link || '').toLowerCase().includes('tiktok.com'))
    .map(r => r.post_link);
  const instagramLinks = candidates
    .filter(r => r.platform === 'instagram' || (r.post_link || '').toLowerCase().includes('instagram.com'))
    .map(r => r.post_link);

  // Run Apify with ORIGINAL input shapes
  const ttItems = await scrapeTiktokDirectPosts(tiktokLinks);
  const igItems = await scrapeInstagramDirectPosts(instagramLinks);

  const scraped = [];
  for (const it of ttItems) { const row = normalizeTikTokItem(it); if (row) scraped.push(row); }
  for (const it of igItems) { const row = normalizeInstagramItem(it); if (row) scraped.push(row); }

  console.log(`Apify: scraped ${scraped.length} snapshots.`);

  // Write to DB → then refresh MVs
  const { slotLabel } = currentSlotUTC();
  const dbRes = await persistHistoryMetrics(pg, scraped, slotLabel);
  console.log(`DB: upserted ${dbRes.written} snapshots (${dbRes.skipped} skipped) for slot ${slotLabel}.`);
  await cleanupHistory(pg);
  await refreshHistoryMVs(pg, 90);
  console.log('DB: materialized views refreshed; wide matrix rebuilt.');

  // Mirror to Google Sheets
  if (SHEET_ID) {
    const auth = getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    await sanityCheckSheets(sheets);
    const colIndex1b = await ensureSheetSlotColumn(sheets, slotLabel);
    await writeViewsToSheet(sheets, colIndex1b, scraped);
  }

  await pg.end();
  console.log('✅ done.');
})().catch((err) => {
  console.error('❌ fatal error:', err?.stack || err);
  process.exit(1);
});
