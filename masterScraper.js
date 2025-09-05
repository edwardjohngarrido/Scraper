// masterScraper.js
import { ApifyClient } from 'apify-client';
import { google } from 'googleapis';
import { Client as PgClient } from 'pg';
import 'dotenv/config';

// ---------- config ----------
const APIFY_TOKEN            = process.env.APIFY_TOKEN;
const TIKTOK_ACTOR_ID        = process.env.APIFY_ACTOR_TIKTOK;
const INSTAGRAM_ACTOR_ID     = process.env.APIFY_ACTOR_INSTAGRAM;

const DB_HOST                = process.env.DB_HOST || '127.0.0.1';
const DB_PORT                = Number(process.env.DB_PORT || 5432);
const DB_NAME                = process.env.DB_NAME || 'prod';
const DB_USER                = process.env.DB_USER || 'vm_user';
const DB_PASSWORD            = process.env.DB_PASSWORD || '';
const DB_SSLMODE             = (process.env.DB_SSLMODE || 'require').toLowerCase();

const SHEET_ID               = process.env.GOOGLE_SHEETS_ID;
const SHEET_TAB              = process.env.GOOGLE_SHEETS_RANGE || 'History Matrix';

// test-only latest 5 tiktok posts
const TEST5                  = process.argv.includes('--test5') || process.env.SCRAPER_TEST_TIKTOK_5 === '1';

// ---------- small utils ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function canonLink(url) {
  if (!url) return null;
  let s = String(url).trim();
  s = s.replace(/\?[^#]*$/, '');     // strip query
  s = s.replace(/#.*$/, '');         // strip fragment
  s = s.replace(/\/+$/, '');         // drop trailing slash
  return s || null;
}

// UTC slot chooser (07:00, 13:00, 19:00)
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
  const slotLabel = slotStart.toISOString().replace('.000Z', 'Z'); // "YYYY-MM-DDTHH:MM:SSZ"
  return { slotStart, slotLabel };
}

// ---------- postgres ----------
function pgClient() {
  const ssl =
    DB_SSLMODE === 'disable'
      ? false
      : { rejectUnauthorized: false };
  return new PgClient({
    host: DB_HOST,
    port: DB_PORT,
    database: DB_NAME,
    user: DB_USER,
    password: DB_PASSWORD,
    ssl
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
                  WHERE post_link IS NULL OR btrim(post_link) = '';`);
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

// Pick posts to scrape
async function selectPostsToScrape(pg) {
  if (TEST5) {
    const q = `
      SELECT
        regexp_replace(regexp_replace(regexp_replace("Original.Post", '\\?[^#]*$', ''), '#.*$', ''), '/+$','') AS post_link,
        CASE
          WHEN lower("Post.Type") = 'tiktok' OR "Original.Post" ILIKE 'https://www.tiktok.com/%' THEN 'tiktok'
          WHEN lower("Post.Type") IN ('reel','instagram') OR "Original.Post" ILIKE 'https://www.instagram.com/%' THEN 'instagram'
          ELSE lower("Post.Type")
        END AS platform
      FROM analytics.posts_full
      WHERE ("Original.Post" IS NOT NULL AND "Original.Post" <> '')
        AND (lower("Post.Type") = 'tiktok' OR "Original.Post" ILIKE 'https://www.tiktok.com/%')
      ORDER BY ("Created.At..ISO8601.")::timestamptz DESC
      LIMIT 5;
    `;
    const { rows } = await pg.query(q);
    return rows;
  }

  // normal: get posts missing the current slot
  const q = `
    WITH slot AS (SELECT GREATEST(
                     (date_trunc('hour', now() at time zone 'utc') - interval '1 hour')::timestamp,
                     (SELECT COALESCE(max(slot_start), '1970-01-01'::timestamptz) FROM analytics.history_matrix_long_mv)
                   ) AS s)
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
    CROSS JOIN slot
    LEFT JOIN analytics.history_matrix_long_mv m
      ON m.post_link = h.post_link AND m.slot_start = (SELECT max(slot_start) FROM analytics.history_matrix_long_mv)
    WHERE (m.post_link IS NULL)                -- not yet measured for newest slot
      AND h.post_link IS NOT NULL
    ORDER BY h.created_at_date DESC
    LIMIT 2000;
  `;
  const { rows } = await pg.query(q);
  return rows;
}

// ---------- google sheets ----------
function getGoogleAuth() {
  // 1) service account json path
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    return new google.auth.GoogleAuth({
      keyFile: process.env.GOOGLE_APPLICATION_CREDENTIALS,
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
  }
  // 2) direct key envs
  if (process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL && process.env.GOOGLE_PRIVATE_KEY) {
    return new google.auth.JWT({
      email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
  }
  throw new Error('Google auth env missing. Provide GOOGLE_APPLICATION_CREDENTIALS or service-account envs.');
}

async function ensureSheetSlotColumn(sheets, slotLabel) {
  // read header row
  const headerResp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_TAB}!A1:Z1`
  });
  const header = headerResp.data.values?.[0] || [];

  // if E1 already == slotLabel, nothing to do
  if (header[4] === slotLabel) return 5; // 1-based index

  // insert a new column at E (index 4 zero-based in Sheets API)
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [
        {
          insertDimension: {
            range: { sheetId: undefined, dimension: 'COLUMNS', startIndex: 4, endIndex: 5 },
            inheritFromBefore: false
          }
        }
      ]
    }
  });

  // set header text
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_TAB}!E1`,
    valueInputOption: 'RAW',
    requestBody: { values: [[slotLabel]] }
  });

  return 5; // column E
}

async function buildSheetRowMap(sheets) {
  // read col C (post link) from row 2 downward
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_TAB}!C2:C`
  });
  const rows = resp.data.values || [];
  const map = new Map();
  for (let i = 0; i < rows.length; i++) {
    const link = rows[i]?.[0] ? canonLink(rows[i][0]) : null;
    const rowNumber = 2 + i; // sheet row
    if (link) map.set(link, rowNumber);
  }
  return map;
}

async function writeViewsToSheet(sheets, colIndex1b, snapshots) {
  // build row map once
  const rowMap = await buildSheetRowMap(sheets);
  const updates = [];
  for (const s of snapshots) {
    const link = canonLink(s.post_link);
    if (!link) continue;
    const row = rowMap.get(link);
    if (!row) continue;

    const colA1 = colIndex1bToA1(colIndex1b); // e.g., 5 -> 'E'
    updates.push({
      range: `${SHEET_TAB}!${colA1}${row}`,
      values: [[Number.isFinite(Number(s.views)) ? Number(s.views) : '']]
    });
  }

  if (updates.length === 0) {
    console.log('Sheets: no matching rows to update.');
    return;
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      valueInputOption: 'RAW',
      data: updates
    }
  });
  console.log(`Sheets: updated ${updates.length} cells in column ${colIndex1bToA1(colIndex1b)}.`);
}

function colIndex1bToA1(idx1b) {
  // 1 -> A, 2 -> B, ...
  let n = idx1b, s = '';
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

// ---------- Apify ----------
function apify() {
  if (!APIFY_TOKEN) throw new Error('APIFY_TOKEN missing.');
  return new ApifyClient({ token: APIFY_TOKEN });
}

async function runActorSingle(actorId, input) {
  const client = apify();
  const run = await client.actor(actorId).call(input);
  const { items } = await client.dataset(run.defaultDatasetId).listItems({ clean: true });
  return items || [];
}

function splitByPlatform(rows) {
  const tt = [];
  const ig = [];
  for (const r of rows) {
    if (!r.post_link) continue;
    if (r.platform === 'tiktok') tt.push(r.post_link);
    else if (r.platform === 'instagram') ig.push(r.post_link);
    else {
      // heuristic by URL
      const u = r.post_link.toLowerCase();
      if (u.includes('tiktok.com')) tt.push(r.post_link);
      else if (u.includes('instagram.com')) ig.push(r.post_link);
    }
  }
  return { tiktok: tt, instagram: ig };
}

async function scrapePosts({ tiktokLinks, instagramLinks }) {
  const results = [];

  if (tiktokLinks.length && !TIKTOK_ACTOR_ID) {
    console.warn('TikTok links present but APIFY_ACTOR_TIKTOK not set — skipping TikTok scrape.');
  }
  if (instagramLinks.length && !INSTAGRAM_ACTOR_ID) {
    console.warn('Instagram links present but APIFY_ACTOR_INSTAGRAM not set — skipping Instagram scrape.');
  }

  if (tiktokLinks.length && TIKTOK_ACTOR_ID) {
    console.log(`Apify: TikTok actor → ${tiktokLinks.length} posts`);
    const items = await runActorSingle(TIKTOK_ACTOR_ID, {
      startUrls: tiktokLinks.map(u => ({ url: u })),
      // keep your existing actor’s input keys the same — only logic changed
    });
    // normalize → { post_link, views }
    for (const it of items) {
      const link = canonLink(it.url || it.link || it.shareUrl || it.itemUrl || it.postUrl);
      const views = firstNumber(it.playCount, it.viewCount, it.views);
      if (link) results.push({ post_link: link, views });
    }
  }

  if (instagramLinks.length && INSTAGRAM_ACTOR_ID) {
    console.log(`Apify: Instagram actor → ${instagramLinks.length} posts`);
    const items = await runActorSingle(INSTAGRAM_ACTOR_ID, {
      startUrls: instagramLinks.map(u => ({ url: u })),
    });
    for (const it of items) {
      const link = canonLink(it.url || it.link || it.permalink || it.postUrl);
      // IG actors differ; do your usual mapping
      const views = firstNumber(it.playCount, it.viewCount, it.views, it.video_view_count);
      if (link) results.push({ post_link: link, views });
    }
  }

  return results;
}

function firstNumber(...xs) {
  for (const x of xs) {
    const n = Number(x);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

// ---------- main ----------
(async () => {
  if (!SHEET_ID) console.warn('GOOGLE_SHEETS_ID not set — Sheets mirroring will fail.');

  const pg = pgClient();
  await pg.connect();
  console.log(`DB: connected → ${DB_HOST}/${DB_NAME} as ${DB_USER}`);

  await ensureHistoryTables(pg);

  // choose posts to scrape
  let candidates = await selectPostsToScrape(pg);
  candidates = candidates.filter(c => !!c.post_link);
  // show preview
  console.log('Posts to scrape (from DB):');
  candidates.slice(0, 20).forEach((c, i) => console.log(`${i + 1}. [${c.platform}] ${c.post_link}`));
  console.log(`Total candidates: ${candidates.length}${TEST5 ? '  (TEST5: latest 5 TikToks)' : ''}`);

  // pause 5s to allow manual abort
  console.log('Pausing 5 seconds… press Ctrl+C to abort.');
  await sleep(5000);

  const { tiktok, instagram } = splitByPlatform(candidates);

  // run apify and collect snapshots
  const scraped = await scrapePosts({ tiktokLinks: tiktok, instagramLinks: instagram });
  console.log(`Apify: scraped ${scraped.length} snapshots.`);

  const { slotLabel } = currentSlotUTC();

  // write to DB first
  const dbRes = await persistHistoryMetrics(pg, scraped, slotLabel);
  console.log(`DB: upserted ${dbRes.written} snapshots (${dbRes.skipped} skipped) for slot ${slotLabel}.`);

  await cleanupHistory(pg);
  await refreshHistoryMVs(pg, 90);
  console.log('DB: materialized views refreshed; wide matrix rebuilt.');

  // mirror to Google Sheets (keep your sheet behavior)
  if (SHEET_ID) {
    const auth = getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    const colIndex1b = await ensureSheetSlotColumn(sheets, slotLabel);
    await writeViewsToSheet(sheets, colIndex1b, scraped);
  }

  await pg.end();
  console.log('✅ done.');
})().catch(async (err) => {
  console.error('❌ fatal error:', err?.stack || err);
  process.exit(1);
});
