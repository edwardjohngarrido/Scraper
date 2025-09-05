// masterScraper.js
import { ApifyClient } from 'apify-client';
import { google } from 'googleapis';
import { Client as PgClient } from 'pg';
import 'dotenv/config';

// === CONFIG (unchanged names) ===
const APIFY_TIKTOK_ACTOR_ID = process.env.APIFY_TIKTOK_ACTOR_ID;
const APIFY_IG_ACTOR_ID     = process.env.APIFY_ACTOR_ID; // (unchanged)
const APIFY_TOKEN           = process.env.APIFY_TOKEN;

const SHEET_ID     = process.env.INFLUENCER_TRACKER_SHEET;
const HISTORY_MATRIX = 'History Matrix';
const SHEET1        = 'Sheet1';
const CREDS_PATH    = './credentials.json';

// DB env (new)
const DB_HOST     = process.env.DB_HOST     || '172.23.128.3';
const DB_PORT     = parseInt(process.env.DB_PORT || '5432', 10);
const DB_NAME     = process.env.DB_NAME     || 'prod';
const DB_USER     = process.env.DB_USER     || 'vm_user';
const DB_PASSWORD = process.env.DB_PASSWORD || 'Something115566!!!';
const DB_SSLMODE  = (process.env.DB_SSLMODE || 'require');

// scrape/update windows
const MATRIX_WINDOW_DAYS = 14; // how far back to pull posts for direct updates
const PROFILE_WINDOW_DAYS = 14; // how far back to select TB/IPWT profiles to crawl
const MV_WIDE_COLUMNS = 90; // used if we refresh matrix wide

// === UTILS ===
function daysAgoIso(days) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().split('T')[0];
}
function normalizeUrl(url) {
  if (!url) return '';
  return url.split('?')[0].split('#')[0].replace(/\/$/, '');
}
function getTiktokPostId(url) {
  if (!url) return '';
  const m = url.match(/\/(video|photo)\/(\d+)/);
  return m ? m[2] : '';
}
function getIGShortCode(url) {
  if (!url) return '';
  const m = url.match(/\/(p|reel|tv)\/([^/?#]+)(?:[/?#]|$)/);
  return m ? m[2] : '';
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// === GOOGLE SHEETS HELPERS (unchanged names/behavior) ===
async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: CREDS_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}
// Loads the current, live Col C of the History Matrix sheet (excluding header)
async function loadLiveHistoryMatrixIds(sheets) {
  const histMatrixPostLinks = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${HISTORY_MATRIX}!C2:C`,
  });
  const urls = (histMatrixPostLinks.data.values || []).flat();
  return {
    tiktokIds: urls.map(getTiktokPostId),
    igShortCodes: urls.map(getIGShortCode),
    urls: urls.map(normalizeUrl),
  };
}
// TikTok Sheet1 col C -> IDs
async function loadSheet1TiktokPostIds(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET1}!C2:C`,
  });
  return new Set((data.values || []).flat().map(getTiktokPostId).filter(Boolean));
}
// IG Sheet1 col N -> shortCodes
async function loadSheet1IGShortCodes(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET1}!N2:N`,
  });
  return new Set((data.values || []).flat().map(getIGShortCode).filter(Boolean));
}
async function batchUpdateHistoryMatrix(sheets, updates) {
  if (!updates.length) return;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    resource: { valueInputOption: 'RAW', data: updates },
  });
}

// === DB HELPERS ===
function pgClient() {
  return new PgClient({
    host: DB_HOST,
    port: DB_PORT,
    database: DB_NAME,
    user: DB_USER,
    password: DB_PASSWORD,
    ssl: DB_SSLMODE === 'require' ? { rejectUnauthorized: false } : false,
  });
}

// Pull a matrix-like set of rows from DB (maps to [date, profile, postUrl, revStream, views?])
// Uses analytics.history_posts as the source of truth.
async function loadHistoryMatrixFromDB(client, windowDays) {
  // We’ll mirror columns the old sheet logic expects: date, profile_link, post_link, rev_stream
  const { rows } = await client.query(
    `
    SELECT
      created_at_date::date         AS created_date,
      profile_link,
      post_link,
      COALESCE(rev_stream,'')       AS rev_stream
    FROM analytics.history_posts
    WHERE created_at_date >= CURRENT_DATE - INTERVAL '${windowDays} days'
      AND post_link IS NOT NULL AND post_link <> ''
      AND profile_link IS NOT NULL AND profile_link <> ''
      AND (
        post_link ILIKE 'https://www.tiktok.com/%'
        OR post_link ILIKE 'https://www.instagram.com/%'
      )
    ORDER BY created_at_date DESC, post_link DESC;
    `
  );
  // reshape to the array-of-arrays the rest of your code already uses:
  // [ [date, profile, postLink, revStream], ... ]
  return rows.map(r => [
    r.created_date?.toISOString?.() ? r.created_date.toISOString().split('T')[0] : String(r.created_date),
    r.profile_link,
    r.post_link,
    r.rev_stream,
  ]);
}

// Create a simple daily snapshots table (idempotent). This is where we record scraped views.
// The MVs in your DB can be adjusted (or already are) to read from this.
async function ensureHistorySnapshotsDaily(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS analytics.history_snapshots_daily (
      post_link  TEXT NOT NULL,
      slot_date  DATE NOT NULL,
      views      BIGINT,
      PRIMARY KEY (post_link, slot_date)
    );
  `);
}

// Upsert today’s views snapshot for an array of { url, views }
async function upsertViewsSnapshot(client, items) {
  if (!items.length) return;
  const slotDate = new Date().toISOString().split('T')[0]; // daily slot (UTC date)
  // insert in chunks to keep the param list manageable
  const chunkSize = 500;
  for (let i = 0; i < items.length; i += chunkSize) {
    const chunk = items.slice(i, i + chunkSize);
    const values = [];
    const params = [];
    chunk.forEach(({ url, views }, idx) => {
      params.push(url, slotDate, views ?? null);
      values.push(`($${idx*3+1}, $${idx*3+2}, $${idx*3+3})`);
    });
    await client.query(
      `
      INSERT INTO analytics.history_snapshots_daily (post_link, slot_date, views)
      VALUES ${values.join(', ')}
      ON CONFLICT (post_link, slot_date) DO UPDATE
      SET views = EXCLUDED.views;
      `,
      params
    );
  }
}

// Optional: try to refresh MVs & wide matrix after writing snapshots
async function refreshHistoryMatrix(client) {
  try {
    await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.history_matrix_long_mv;');
  } catch { /* fall back */ 
    try { await client.query('REFRESH MATERIALIZED VIEW analytics.history_matrix_long_mv;'); } catch {}
  }
  try {
    await client.query(`SELECT analytics.rebuild_history_matrix_wide(${MV_WIDE_COLUMNS});`);
  } catch {
    // non-critical
  }
}

// === “Build jobs” logic (reused) ===
function buildHistoryIndex(historyRows) {
  // historyRows: [date, profileLink, postUrl, revStream]
  const tt = new Map(); // postId -> meta
  const ig = new Map(); // shortCode -> meta
  historyRows.forEach((row, i) => {
    const createdDate = row[0] || '';
    const profile = row[1] || '';
    const postUrl = row[2] || '';
    const type     = (row[3] || '').toLowerCase();
    const rowNum = i + 2;
    if (/tiktok\.com\/@[^/]+\/(video|photo)\/\d+/.test(postUrl)) {
      const id = getTiktokPostId(postUrl);
      if (id) tt.set(id, { rowNum, type, profile, createdDate, postUrl });
    }
    if (/instagram\.com\/(p|reel|tv)\/[^/]+/.test(postUrl)) {
      const sc = getIGShortCode(postUrl);
      if (sc) ig.set(sc, { rowNum, type, profile, createdDate, postUrl });
    }
  });
  return { tt, ig };
}
function buildJobs(historyRows, windowDays) {
  const cutoff = new Date(daysAgoIso(windowDays));
  const tiktokOrganicOrGiftedPosts = [];
  const tiktokTbIpwtProfiles = new Set();
  const igOrganicOrGiftedPosts = [];
  const igTbIpwtProfiles = new Set();

  historyRows.forEach(row => {
    const [date, profile, postUrl, type] = row;
    if (!profile || !postUrl || !date) return;
    const createdAt = new Date(date);
    const lowerType = (type || '').toLowerCase();

    if (profile.includes('tiktok.com')) {
      if (lowerType === 'trailblazer' || lowerType === 'ipwt') {
        if (createdAt >= cutoff) tiktokTbIpwtProfiles.add(profile);
      } else if (createdAt >= cutoff) {
        tiktokOrganicOrGiftedPosts.push({ profile, postUrl, row });
      }
    }
    if (profile.includes('instagram.com')) {
      if (lowerType === 'trailblazer' || lowerType === 'ipwt') {
        if (createdAt >= cutoff) igTbIpwtProfiles.add(profile);
      } else if (createdAt >= cutoff) {
        igOrganicOrGiftedPosts.push({ profile, postUrl, row });
      }
    }
  });

  return {
    tiktokOrganicOrGiftedPosts,
    tiktokTbIpwtProfiles: Array.from(tiktokTbIpwtProfiles),
    igOrganicOrGiftedPosts,
    igTbIpwtProfiles: Array.from(igTbIpwtProfiles),
  };
}

// === APIFY SCRAPERS (same actors/tokens) ===
async function scrapeTiktokDirectPosts(postUrls) {
  if (!postUrls.length) return [];
  const client = new ApifyClient({ token: APIFY_TOKEN });
  const run = await client.actor(APIFY_TIKTOK_ACTOR_ID).call({ postURLs: postUrls });
  const { items } = await client.dataset(run.defaultDatasetId).listItems();
  return items || [];
}
async function scrapeTiktokProfiles(profiles, sinceDate, captionKeywords) {
  if (!profiles.length) return [];
  const client = new ApifyClient({ token: APIFY_TOKEN });
  const cleanedProfiles = profiles.map(url => {
    let username = url.split('/').filter(Boolean).pop() || '';
    if (username.startsWith('@')) username = username.slice(1);
    return username;
  });
  const input = {
    profiles: cleanedProfiles,
    captionKeywords,
    profileSorting: "latest",
    oldestPostDateUnified: "30 days",
  };
  const run = await client.actor(APIFY_TIKTOK_ACTOR_ID).call(input);
  const { items } = await client.dataset(run.defaultDatasetId).listItems();
  return items || [];
}
async function scrapeInstagramDirectPosts(postUrls) {
  if (!postUrls.length) return [];
  const client = new ApifyClient({ token: APIFY_TOKEN });
  const run = await client.actor(APIFY_IG_ACTOR_ID).call({ directUrls: postUrls });
  const { items } = await client.dataset(run.defaultDatasetId).listItems();
  return items || [];
}
async function scrapeInstagramProfiles(profiles, sinceDate, captionKeywords) {
  if (!profiles.length) return [];
  const client = new ApifyClient({ token: APIFY_TOKEN });
  const input = {
    directUrls: profiles,
    resultsType: 'posts',
    onlyPostsNewerThan: sinceDate, // yyyy-mm-dd
    resultsLimit: 40,
    captionKeywords,
    addParentData: false,
  };
  const run = await client.actor(APIFY_IG_ACTOR_ID).call(input);
  const { items } = await client.dataset(run.defaultDatasetId).listItems();
  return items || [];
}

// === MAIN ===
(async () => {
  // 0) Make Sheets client (for output) & DB client (new source)
  const sheets = await getSheetsClient();
  const pg = pgClient();
  await pg.connect();

  // 1) Preflight: pull “History Matrix” rows from DB
  const historyRows = await loadHistoryMatrixFromDB(pg, Math.max(MATRIX_WINDOW_DAYS, PROFILE_WINDOW_DAYS));
  const historyIndex = buildHistoryIndex(historyRows);

  // Build planned scrape sets (from DB rows)
  const { igOrganicOrGiftedPosts, igTbIpwtProfiles, tiktokOrganicOrGiftedPosts, tiktokTbIpwtProfiles } =
    buildJobs(historyRows, Math.max(MATRIX_WINDOW_DAYS, PROFILE_WINDOW_DAYS));

  // Collect direct post links we’ll scrape within window
  const sinceDateMatrix = new Date(daysAgoIso(MATRIX_WINDOW_DAYS));
  const matrixTikTokLinks = historyRows
    .filter(row => {
      const url = row[2] || '';
      const dateStr = row[0] || '';
      if (!/tiktok\.com\/@[^/]+\/(video|photo)\/\d+/.test(url)) return false;
      if (!dateStr) return false;
      return new Date(dateStr) >= sinceDateMatrix;
    })
    .map(row => row[2]);

  const matrixIGLinks = historyRows
    .filter(row => {
      const url = row[2] || '';
      const dateStr = row[0] || '';
      if (!/instagram\.com\/(p|reel|tv)\/[^/]+/.test(url)) return false;
      if (!dateStr) return false;
      return new Date(dateStr) >= sinceDateMatrix;
    })
    .map(row => row[2]);

  // Preflight preview (so you can abort)
  console.log('--- DB Preflight OK ---');
  console.log(`[DB] Rows loaded from analytics.history_posts (window ${Math.max(MATRIX_WINDOW_DAYS, PROFILE_WINDOW_DAYS)}d): ${historyRows.length}`);
  console.log(`[TT] Direct posts to scrape: ${matrixTikTokLinks.length}`);
  console.log('     sample:', matrixTikTokLinks.slice(0, 5));
  console.log(`[IG] Direct posts to scrape: ${matrixIGLinks.length}`);
  console.log('     sample:', matrixIGLinks.slice(0, 5));
  console.log(`[TT] TB/IPWT profile count (window ${PROFILE_WINDOW_DAYS}d): ${tiktokTbIpwtProfiles.length}`);
  console.log(`[IG] TB/IPWT profile count (window ${PROFILE_WINDOW_DAYS}d): ${igTbIpwtProfiles.length}`);

  console.log('Pausing 5 seconds before starting scrapers (Ctrl+C to abort) ...');
  await sleep(5000);

  // 2) Ensure snapshots table (DB) — store today’s views after scraping
  await ensureHistorySnapshotsDaily(pg);

  // 3) Helpers used later for sheet updates
  const { tiktokIds: liveTikTokIds, igShortCodes: liveIGShortCodes } = await loadLiveHistoryMatrixIds(sheets);
  const sheet1TiktokPostIds = await loadSheet1TiktokPostIds(sheets);
  const sheet1IGShortCodes  = await loadSheet1IGShortCodes(sheets);
  const isTBorIPWT = (rev) => rev === 'trailblazer' || rev === 'ipwt';

  // Tracking
  const postsToUpdate = new Set();
  const postsUpdated  = new Set();
  const postsAppended = new Set();

  // ===== TikTok: direct post updates =====
  let updateBatch = [];
  const ttDirect = await scrapeTiktokDirectPosts(matrixTikTokLinks);
  // upsert DB snapshots
  await upsertViewsSnapshot(pg, ttDirect.map(p => ({
    url: normalizeUrl(p.webVideoUrl || p.url),
    views: p.playCount ?? p.viewCount ?? p.views ?? null,
  })));
  // update sheet History Matrix:E
  for (const post of ttDirect) {
    const postId = getTiktokPostId(post.webVideoUrl || post.url);
    const views  = post.playCount ?? post.viewCount ?? post.views;
    const rowIdx = liveTikTokIds.findIndex(id => id === postId);
    if (rowIdx !== -1) {
      const rowNum = rowIdx + 2;
      updateBatch.push({ range: `${HISTORY_MATRIX}!E${rowNum}`, values: [[views]] });
      postsUpdated.add(postId);
    } else {
      console.warn('❌ TikTok post not found in sheet matrix:', post.webVideoUrl || post.url);
    }
    postsToUpdate.add(postId);
  }
  await batchUpdateHistoryMatrix(sheets, updateBatch);

  // TikTok → Sheet1 B:F (append if TB/IPWT, otherwise update views only)
  {
    const sheet1TT = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${SHEET1}!C2:C` });
    const ttIdToRow = new Map();
    (sheet1TT.data.values || []).forEach((row, i) => {
      const link = (row[0] || '').trim();
      const id = getTiktokPostId(link);
      if (id) ttIdToRow.set(id, i + 2);
    });

    const ttViewUpdates = [];
    const ttAppendRows  = [];

    for (const post of ttDirect) {
      const url = post.webVideoUrl || post.url || '';
      const postId = getTiktokPostId(url);
      if (!postId) continue;

      const views = post.playCount ?? post.viewCount ?? post.views ?? '';
      const existingRow = ttIdToRow.get(postId);

      if (existingRow) {
        ttViewUpdates.push({ range: `${SHEET1}!D${existingRow}`, values: [[views]] });
      } else {
        const tagType = (historyIndex.tt.get(postId)?.type || '').toLowerCase();
        if (!isTBorIPWT(tagType)) continue;

        const username =
          post.authorUsername || post.username || post.ownerUsername ||
          ((url.match(/tiktok\.com\/@([^/]+)/) || [,''])[1]);
        const profileLink = username ? `https://www.tiktok.com/@${username}` : '';
        const rawCreated  = post.createTime ?? post.createDate ?? post.timestamp ?? post.createTimestamp ?? '';
        const createdMs   = typeof rawCreated === 'number'
          ? (rawCreated < 1e12 ? rawCreated * 1000 : rawCreated)
          : Date.parse(rawCreated);
        const pretty      = isNaN(createdMs) ? '' :
          new Date(createdMs).toLocaleDateString('en-US', { year:'numeric', month:'long', day:'numeric' });
        const createdIso  = isNaN(createdMs) ? '' : new Date(createdMs).toISOString();

        ttAppendRows.push([profileLink, url, views, pretty, createdIso]);
        postsAppended.add(postId);
      }
    }

    if (ttViewUpdates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: { valueInputOption: 'RAW', data: ttViewUpdates },
      });
    }
    if (ttAppendRows.length) {
      const existingTT = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID, range: `${SHEET1}!B:F`,
      });
      const used = (existingTT.data.values || []).length;
      const startRow = Math.max(2, used + 1);
      const endRow   = startRow + ttAppendRows.length - 1;

      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SHEET1}!B${startRow}:F${endRow}`,
        valueInputOption: 'RAW',
        resource: { values: ttAppendRows },
      });
    }
  }

  // ===== TikTok: TB/IPWT discovery (profiles) =====
  const TIKTOK_TAGS = [
    "@In Print We Trust", "@in print we trust", "@InPrintWeTrust", "@inprintwetrust",
    "@inprintwetrust.co", "@InPrintWeTrust.co", "#InPrintWeTrust", "#inprintwetrust",
    "#IPWT", "#ipwt"
  ];
  const tiktokProfileResults = await scrapeTiktokProfiles(tiktokTbIpwtProfiles, daysAgoIso(PROFILE_WINDOW_DAYS), TIKTOK_TAGS);
  // upsert DB snapshots
  await upsertViewsSnapshot(pg, tiktokProfileResults.map(p => ({
    url: normalizeUrl(p.webVideoUrl || p.url),
    views: p.playCount ?? p.viewCount ?? p.views ?? null,
  })));

  {
    const newTTAppends = [];
    for (const post of tiktokProfileResults) {
      const url = post.webVideoUrl || post.url || '';
      const postId = getTiktokPostId(url);
      if (!postId) continue;

      const caption = (post.text || '').toLowerCase();
      const isTagged = TIKTOK_TAGS.some(tag => caption.includes(tag.toLowerCase()));
      if (!isTagged) continue;

      if (historyIndex.tt.has(postId)) continue;
      if (sheet1TiktokPostIds.has(postId)) continue;

      const username =
        post.authorUsername || post.username || post.ownerUsername ||
        ((url.match(/tiktok\.com\/@([^/]+)/) || [,''])[1]);
      const profileLink = username ? `https://www.tiktok.com/@${username}` : '';
      const views = post.playCount ?? post.viewCount ?? post.views ?? '';
      const rawCreated = post.createTime ?? post.createDate ?? post.timestamp ?? post.createTimestamp ?? '';
      const createdMs = typeof rawCreated === 'number'
        ? (rawCreated < 1e12 ? rawCreated * 1000 : rawCreated)
        : Date.parse(rawCreated);
      const pretty = isNaN(createdMs) ? '' :
        new Date(createdMs).toLocaleDateString('en-US', { year:'numeric', month:'long', day:'numeric' });
      const createdIso = isNaN(createdMs) ? '' : new Date(createdMs).toISOString();

      newTTAppends.push([profileLink, url, views, pretty, createdIso]);
      postsAppended.add(postId);
    }

    if (newTTAppends.length) {
      const existingTT = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID, range: `${SHEET1}!B:F`,
      });
      const used = (existingTT.data.values || []).length;
      const startRow = Math.max(2, used + 1);
      const endRow   = startRow + newTTAppends.length - 1;

      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SHEET1}!B${startRow}:F${endRow}`,
        valueInputOption: 'RAW',
        resource: { values: newTTAppends },
      });
    }
  }

  // ===== Instagram: direct post updates =====
  const igDirect = await scrapeInstagramDirectPosts(matrixIGLinks);
  // DB snapshots
  await upsertViewsSnapshot(pg, igDirect.map(p => ({
    url: normalizeUrl(p.url || p.postUrl),
    views: p.videoPlayCount ?? p.views ?? null,
  })));

  updateBatch = [];
  for (const post of igDirect) {
    const shortCode = getIGShortCode(post.url || post.postUrl);
    const views     = post.videoPlayCount ?? post.views;
    const rowIdx    = liveIGShortCodes.findIndex(sc => sc === shortCode);
    if (rowIdx !== -1) {
      const rowNum = rowIdx + 2;
      updateBatch.push({ range: `${HISTORY_MATRIX}!E${rowNum}`, values: [[views]] });
      postsUpdated.add(shortCode);
    }
    postsToUpdate.add(shortCode);
  }
  await batchUpdateHistoryMatrix(sheets, updateBatch);

  // IG Sheet1: if present (N), update O
  {
    const sheet1IG = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID, range: `${SHEET1}!N2:N`,
    });
    const igShortToRow = new Map();
    (sheet1IG.data.values || []).forEach((row, i) => {
      const link = (row[0] || '').trim();
      const sc = getIGShortCode(link);
      if (sc) igShortToRow.set(sc, i + 2);
    });

    const igSheet1Updates = [];
    for (const post of igDirect) {
      const shortCode = getIGShortCode(post.url || post.postUrl);
      if (!shortCode) continue;
      const views = post.videoPlayCount ?? post.views ?? '';
      const row = igShortToRow.get(shortCode);
      if (row) igSheet1Updates.push({ range: `${SHEET1}!O${row}`, values: [[views]] });
    }
    if (igSheet1Updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: { valueInputOption: 'RAW', data: igSheet1Updates },
      });
    }
  }

  // ===== Instagram: matrix -> Sheet1 TB/IPWT sync (last 14 days) =====
  const igMatrixCutoff = new Date(daysAgoIso(PROFILE_WINDOW_DAYS));
  const igByShort = new Map(
    igDirect
      .map(p => [getIGShortCode(p.url || p.postUrl), p])
      .filter(([k]) => !!k)
  );

  const sheet1IGLinks = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID, range: `${SHEET1}!N2:N`,
  });
  const sheet1IGMap = new Map();
  (sheet1IGLinks.data.values || []).forEach((row, i) => {
    const link = (row[0] || '').trim();
    const sc = getIGShortCode(link);
    if (sc) sheet1IGMap.set(sc, i + 2);
  });

  const igMatrixToSheetAppends = [];
  for (const row of historyRows) {
    const createdStr  = row[0] || '';
    const profileLink = row[1] || '';
    const postUrl     = row[2] || '';
    const revStream   = (row[3] || '').toLowerCase();

    if (!/instagram\.com\/(p|reel|tv)\//.test(postUrl)) continue;
    if (!(revStream === 'trailblazer' || revStream === 'ipwt')) continue;
    if (!createdStr) continue;
    if (new Date(createdStr) < igMatrixCutoff) continue;

    const sc = getIGShortCode(postUrl);
    if (!sc) continue;
    if (sheet1IGMap.has(sc)) continue;

    const scrapedPost = igByShort.get(sc);
    const views = scrapedPost ? (scrapedPost.videoPlayCount ?? scrapedPost.views ?? '') : '';

    const ownerUsername = scrapedPost?.ownerUsername || '';
    const finalProfileLink = ownerUsername
      ? `https://www.instagram.com/${ownerUsername}/reels`
      : profileLink;

    const timestamp = scrapedPost?.timestamp || createdStr;
    const prettyDate = timestamp
      ? new Date(timestamp).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
      : '';

    igMatrixToSheetAppends.push([finalProfileLink, postUrl, views, prettyDate, timestamp]);
  }

  if (igMatrixToSheetAppends.length) {
    const existingIG = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID, range: `${SHEET1}!M:Q`,
    });
    const used = (existingIG.data.values || []).length;
    const startRow = Math.max(2, used + 1);
    const endRow   = startRow + igMatrixToSheetAppends.length - 1;

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET1}!M${startRow}:Q${endRow}`,
      valueInputOption: 'RAW',
      resource: { values: igMatrixToSheetAppends },
    });
  }

  // ===== Instagram: TB/IPWT profile discovery =====
  const IG_TAGS = [
    "@In Print We Trust", "@in print we trust", "@InPrintWeTrust", "@inprintwetrust",
    "@inprintwetrust.co", "@InPrintWeTrust.co", "#InPrintWeTrust", "#inprintwetrust",
    "#IPWT", "#ipwt"
  ];
  const igProfileResults = await scrapeInstagramProfiles(igTbIpwtProfiles, daysAgoIso(PROFILE_WINDOW_DAYS), IG_TAGS);
  // DB snapshots
  await upsertViewsSnapshot(pg, igProfileResults.map(p => ({
    url: normalizeUrl(p.url || p.postUrl),
    views: p.videoPlayCount ?? p.views ?? null,
  })));

  {
    const sheet1IG_forProfile = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID, range: `${SHEET1}!N2:N`,
    });
    const igShortToRow_forProfile = new Map();
    (sheet1IG_forProfile.data.values || []).forEach((row, i) => {
      const link = (row[0] || '').trim();
      const sc = getIGShortCode(link);
      if (sc) igShortToRow_forProfile.set(sc, i + 2);
    });

    let igProfileMatrixUpdates = [];
    const igSheet1ProfileUpdates = [];
    const igAppendRows = [];

    for (const post of igProfileResults) {
      const shortCode = getIGShortCode(post.url || post.postUrl);
      const views = post.videoPlayCount ?? '';
      if (!shortCode) continue;

      // Update History Matrix:E if present
      const rowIdx = liveIGShortCodes.findIndex(sc => sc === shortCode);
      if (rowIdx !== -1) {
        const rowNum = rowIdx + 2;
        igProfileMatrixUpdates.push({ range: `${HISTORY_MATRIX}!E${rowNum}`, values: [[views]] });
        postsUpdated.add(shortCode);
      }
      postsToUpdate.add(shortCode);

      // Sheet1: if exists in N, update O
      const rowInSheet1 = igShortToRow_forProfile.get(shortCode);
      if (rowInSheet1) {
        igSheet1ProfileUpdates.push({ range: `${SHEET1}!O${rowInSheet1}`, values: [[views]] });
        continue;
      }

      // Else append to M:Q if tagged and not already present
      if (IG_TAGS.some(tag => (post.caption || '').toLowerCase().includes(tag.toLowerCase()))
          && !sheet1IGShortCodes.has(shortCode)) {
        const ownerUsername = post.ownerUsername || '';
        const profileLink = ownerUsername ? `https://www.instagram.com/${ownerUsername}/reels` : '';
        const prettyDate = post.timestamp
          ? new Date(post.timestamp).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
          : '';
        igAppendRows.push([profileLink, post.url || post.postUrl, views, prettyDate, post.timestamp || '']);
        postsAppended.add(shortCode);
      }
    }

    if (igSheet1ProfileUpdates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: { valueInputOption: 'RAW', data: igSheet1ProfileUpdates },
      });
    }
    if (igProfileMatrixUpdates.length) {
      await batchUpdateHistoryMatrix(sheets, igProfileMatrixUpdates);
    }
    if (igAppendRows.length) {
      const existingIG = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID, range: `${SHEET1}!M:Q`,
      });
      const used = (existingIG.data.values || []).length;
      const startRow = Math.max(2, used + 1);
      const endRow   = startRow + igAppendRows.length - 1;

      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SHEET1}!M${startRow}:Q${endRow}`,
        valueInputOption: 'RAW',
        resource: { values: igAppendRows },
      });
    }
  }

  // 4) Try to refresh MVs/wide in DB, since we added snapshots
  await refreshHistoryMatrix(pg);

  // 5) Report
  const failedPosts = [...postsToUpdate].filter(id => !postsUpdated.has(id) && !postsAppended.has(id));
  if (failedPosts.length) {
    console.log(`❌ Failed to update ${failedPosts.length} posts (not found in matrix & not appended):`);
    failedPosts.forEach(x => console.log('  ' + x));
  } else {
    console.log(`✅ All intended posts updated/appended.`);
  }

  await pg.end();
})().catch(err => {
  console.error('Fatal error in masterScraper:', err);
  process.exit(1);
});
