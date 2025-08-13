import { ApifyClient } from 'apify-client';
import { google } from 'googleapis';
import 'dotenv/config';

// === CONFIG ===
const APIFY_TIKTOK_ACTOR_ID = process.env.APIFY_TIKTOK_ACTOR_ID;
const APIFY_IG_ACTOR_ID = process.env.APIFY_ACTOR_ID;
const APIFY_TOKEN = process.env.APIFY_TOKEN;
const SHEET_ID = process.env.INFLUENCER_TRACKER_SHEET;
const HISTORY_MATRIX = 'History Matrix';
const SHEET1 = 'Sheet1';
const CREDS_PATH = './credentials.json';

// === UTILS ===
function daysAgoIso(days) {
    const date = new Date();
    date.setDate(date.getDate() - days);
    return date.toISOString().split('T')[0];
}

async function getSheetsClient() {
    const auth = new google.auth.GoogleAuth({
        keyFile: CREDS_PATH,
        scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
    return google.sheets({ version: 'v4', auth });
}

// === LOAD DATA FROM SHEETS ===
async function loadHistoryMatrix(sheets) {
    const range = `${HISTORY_MATRIX}!A2:E`; // Adjust columns as needed
    const { data } = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range,
    });
    return data.values || [];
}

// Utility to load TikTok Sheet1 Col C (post links)
async function loadSheet1TiktokPostLinks(sheets) {
    const range = `${SHEET1}!C2:C`;
    const { data } = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range,
    });
    return new Set((data.values || []).flat().filter(Boolean));
}
// Utility to load IG Sheet1 Col N (post links)
async function loadSheet1IGPostLinks(sheets) {
    const range = `${SHEET1}!N2:N`;
    const { data } = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range,
    });
    return new Set((data.values || []).flat().filter(Boolean));
}

// Loads the current, live Col C of the History Matrix sheet (excluding header)
async function loadLiveHistoryMatrixPostLinks(sheets) {
    const histMatrixPostLinks = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: `${HISTORY_MATRIX}!C2:C`,
    });
    return (histMatrixPostLinks.data.values || []).flat();
}

function normalizeUrl(url) {
    if (!url) return '';
    return url.split('?')[0].split('#')[0].replace(/\/$/, '');
}
function getTiktokPostId(url) {
    if (!url) return '';
    const match = url.match(/\/(video|photo)\/(\d+)/);
    return match ? match[2] : '';
}
function getIGShortCode(url) {
  if (!url) return '';
  const m = url.match(/\/(p|reel|tv)\/([^/?#]+)(?:[/?#]|$)/);
  return m ? m[2] : '';
}

async function loadLiveHistoryMatrixIds(sheets) {
    const histMatrixPostLinks = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: `${HISTORY_MATRIX}!C2:C`,
    });
    const urls = (histMatrixPostLinks.data.values || []).flat();
    return {
        tiktokIds: urls.map(getTiktokPostId),
        igShortCodes: urls.map(getIGShortCode),
        urls: urls.map(normalizeUrl), // fallback/legacy if needed
    };
}

// Utility to load TikTok Sheet1 Col C (post links) as IDs
async function loadSheet1TiktokPostIds(sheets) {
    const range = `${SHEET1}!C2:C`;
    const { data } = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range,
    });
    return new Set((data.values || []).flat().map(getTiktokPostId).filter(Boolean));
}

// Utility to load IG Sheet1 Col N (post links) as shortCodes
async function loadSheet1IGShortCodes(sheets) {
    const range = `${SHEET1}!N2:N`;
    const { data } = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range,
    });
    return new Set((data.values || []).flat().map(getIGShortCode).filter(Boolean));
}


// === BUILD JOBS ===
function buildJobs(historyRows, windowDays) {
    const tiktokOrganicOrGiftedPosts = [];
    const tiktokTbIpwtProfiles = new Set();

    const igOrganicOrGiftedPosts = [];
    const igTbIpwtProfiles = new Set();

    historyRows.forEach(row => {
        const [date, profile, postUrl, type, views] = row;
        if (!profile || !postUrl) return;
        const lowerType = type ? type.toLowerCase() : '';
        // TikTok logic
        if (profile.includes('tiktok.com')) {
            if (lowerType === 'trailblazer' || lowerType === 'ipwt') {
                tiktokTbIpwtProfiles.add(profile);
            } else if (new Date(date) > new Date(daysAgoIso(windowDays))) {
                tiktokOrganicOrGiftedPosts.push({ profile, postUrl, row });
            }
        }
        // IG logic
        if (profile.includes('instagram.com')) {
            if (lowerType === 'trailblazer' || lowerType === 'ipwt') {
                igTbIpwtProfiles.add(profile);
            } else if (new Date(date) > new Date(daysAgoIso(windowDays))) {
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

// === SCRAPE TIKTOK/IG VIA APIFY ===
async function scrapeTiktokDirectPosts(postUrls) {
    if (!postUrls.length) return [];
    const client = new ApifyClient({ token: APIFY_TOKEN });
    const input = { postURLs: postUrls };
    const run = await client.actor(APIFY_TIKTOK_ACTOR_ID).call(input);
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
        profiles: cleanedProfiles, // CORRECT FIELD!
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
    const input = { directUrls: postUrls };
    const run = await client.actor(APIFY_IG_ACTOR_ID).call(input);
    const { items } = await client.dataset(run.defaultDatasetId).listItems();
    return items || [];
}
async function scrapeInstagramProfiles(profiles, sinceDate, captionKeywords) {
    if (!profiles.length) return [];
    const client = new ApifyClient({ token: APIFY_TOKEN });
    const input = {
        directUrls: profiles,
        resultsType: 'posts',
        onlyPostsNewerThan: sinceDate,
        resultsLimit: 40, // you can adjust this as needed
        captionKeywords,
        addParentData: false
    };
    const run = await client.actor(APIFY_IG_ACTOR_ID).call(input);
    const { items } = await client.dataset(run.defaultDatasetId).listItems();
    return items || [];
}

// === UPDATE SHEETS ===
async function batchUpdateHistoryMatrix(sheets, updates) {
    if (!updates.length) return;
    await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: { valueInputOption: 'RAW', data: updates },
    });
}

async function appendToSheet1(sheets, rows, colStart) {
    if (!rows.length) return;
    const range = `${SHEET1}!${colStart}`; // e.g., C:G for TikTok, M:R for IG
    await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range,
        valueInputOption: 'RAW',
        resource: { values: rows },
    });
}

// === MAIN ===
(async () => {
    const sheets = await getSheetsClient();
    const historyRows = await loadHistoryMatrix(sheets);
    const windowDays = 30;
    const sinceDate = daysAgoIso(windowDays);

    const { tiktokIds: liveTikTokIds, igShortCodes: liveIGShortCodes } = await loadLiveHistoryMatrixIds(sheets);
    const sheet1TiktokPostIds = await loadSheet1TiktokPostIds(sheets);
    const sheet1IGShortCodes = await loadSheet1IGShortCodes(sheets);

    // Build jobs for all platforms/types
    const {
        tiktokOrganicOrGiftedPosts,
        tiktokTbIpwtProfiles,
        igOrganicOrGiftedPosts,
        igTbIpwtProfiles,
    } = buildJobs(historyRows, windowDays);

    // Track intended/actual updates
    const postsToUpdate = new Set();
    const postsUpdated = new Set();
    const postsAppended = new Set();

    // === TikTok: Update ALL tracked posts in matrix ===

// === TikTok: Update only posts from the last 14 days ===

const twoWeeksAgo = new Date();
twoWeeksAgo.setDate(twoWeeksAgo.getDate() - 14);

const matrixTikTokLinks = (historyRows
    .filter(row => {
        const url = row[2] || '';
        const dateStr = row[0] || '';
        if (!/tiktok\.com\/@[^\/]+\/(video|photo)\/\d+/.test(url)) return false;
        if (!dateStr) return false;
        const createdAt = new Date(dateStr);
        return createdAt >= twoWeeksAgo;
    })
    .map(row => row[2])
);

console.log(`[TT] About to process ${matrixTikTokLinks.length} TikTok post links`);
if (matrixTikTokLinks.length) {
    console.log('[TT] First 5 TikTok links:', matrixTikTokLinks.slice(0, 5));
}

// 2. Scrape views for all these post URLs via Apify
const tiktokDirectResults = await scrapeTiktokDirectPosts(matrixTikTokLinks);

// 3. Update Col E in matrix using post ID match
let updateBatch = [];
for (const post of tiktokDirectResults) {
    const postId = getTiktokPostId(post.webVideoUrl || post.url);
    const views = post.playCount || post.viewCount || post.views;
    const rowIdx = liveTikTokIds.findIndex(id => id === postId);
    if (rowIdx !== -1) {
        const rowNum = rowIdx + 2;
        updateBatch.push({
            range: `${HISTORY_MATRIX}!E${rowNum}`,
            values: [[views]]
        });
        postsUpdated.add(postId);
    } else {
        console.warn('❌ TikTok post not found:', post.webVideoUrl || post.url, 'ID:', postId);
    }
    postsToUpdate.add(postId);
}
await batchUpdateHistoryMatrix(sheets, updateBatch);

    // === TikTok → Sheet1 (B:F): append new OR update only views in D if already present ===
{
  // Build a map: TT postId -> row number in Sheet1 (from column C)
  const sheet1TT = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID, range: `${SHEET1}!C2:C`,
  });
  const ttIdToRow = new Map();
  (sheet1TT.data.values || []).forEach((row, i) => {
    const link = (row[0] || '').trim();
    const id = getTiktokPostId(link);
    if (id) ttIdToRow.set(id, i + 2); // +2 for header offset
  });

  const ttViewUpdates = []; // updates for D{row}
  const ttAppendRows = [];  // rows for B:F append

  for (const post of tiktokDirectResults) {
    const url = post.webVideoUrl || post.url || '';
    const postId = getTiktokPostId(url);
    if (!postId) continue;

    const views = post.playCount ?? post.viewCount ?? post.views ?? '';
    const existingRow = ttIdToRow.get(postId);

    if (existingRow) {
      // Update only views in column D
      ttViewUpdates.push({ range: `${SHEET1}!D${existingRow}`, values: [[views]] });
    } else {
      // Append B:F = [profileLink, postLink, views, prettyDate, createdDatetime]
      const username =
        post.authorUsername || post.username || post.ownerUsername ||
        ((url.match(/tiktok\.com\/@([^\/]+)/) || [,''])[1]);
      const profileLink = username ? `https://www.tiktok.com/@${username}` : '';
      const rawCreated = post.createTime ?? post.createDate ?? post.timestamp ?? post.createTimestamp ?? '';
const createdMs = typeof rawCreated === 'number'
  ? (rawCreated < 1e12 ? rawCreated * 1000 : rawCreated)  // handle seconds vs ms
  : Date.parse(rawCreated);

const pretty = isNaN(createdMs)
  ? ''
  : new Date(createdMs).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });

// Use ISO datetime (UTC) for Sheet1 column F instead of epoch
const createdIso = isNaN(createdMs) ? '' : new Date(createdMs).toISOString();

ttAppendRows.push([profileLink, url, views, pretty, createdIso]);
    }
  }

  if (ttViewUpdates.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      resource: { valueInputOption: 'RAW', data: ttViewUpdates },
    });
  }
if (ttAppendRows.length) {
  // Find next empty row in the B:F region
  const existingTT = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET1}!B:F`,
  });
  const used = (existingTT.data.values || []).length;   // rows in B:F (includes header)
  const startRow = Math.max(2, used + 1);               // never overwrite header row
  const endRow = startRow + ttAppendRows.length - 1;

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET1}!B${startRow}:F${endRow}`,
    valueInputOption: 'RAW',
    resource: { values: ttAppendRows },
  });
}

}

    // === Instagram: Organic/Gifted ===
    // Set your desired window, e.g., 14 for two weeks
const igWindowDays = 14; // or any window you want
const igSince = new Date();
igSince.setDate(igSince.getDate() - igWindowDays);

// Get all IG post links from matrix from last X days
const matrixIGLinks = historyRows
    .filter(row => {
        const url = row[2] || '';
        const dateStr = row[0] || '';
        if (!/instagram\.com\/(p|reel|tv)\/[^/]+/.test(url)) return false;
        if (!dateStr) return false;
        const createdAt = new Date(dateStr);
        return createdAt >= igSince;
    })
    .map(row => row[2]);

console.log(`[IG] About to process ${matrixIGLinks.length} Instagram post links`);
if (matrixIGLinks.length) {
    console.log('[IG] First 5 Instagram links:', matrixIGLinks.slice(0, 5));
}

// Use this for directUrls input to Apify IG actor
const igDirectResults = await scrapeInstagramDirectPosts(matrixIGLinks);

    updateBatch = [];
    for (const post of igDirectResults) {
        const shortCode = getIGShortCode(post.url || post.postUrl);
        const views = post.videoPlayCount || post.views;
        const rowIdx = liveIGShortCodes.findIndex(sc => sc === shortCode);
        if (rowIdx !== -1) {
            const rowNum = rowIdx + 2;
            updateBatch.push({
                range: `${HISTORY_MATRIX}!E${rowNum}`,
                values: [[views]]
            });
            postsUpdated.add(shortCode);
        }
        postsToUpdate.add(shortCode);
    }

    await batchUpdateHistoryMatrix(sheets, updateBatch);

    // Also update Sheet1 column O for IG posts already in N (no append here)
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
  for (const post of igDirectResults) {
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

// === Instagram: TB/IPWT profiles ===
const TAGS = [
  "@In Print We Trust", "@in print we trust", "@InPrintWeTrust", "@inprintwetrust",
  "@inprintwetrust.co", "@InPrintWeTrust.co", "#InPrintWeTrust", "#inprintwetrust",
  "#IPWT", "#ipwt"
];

// Get recent posts from the TB/IPWT profiles
const igProfileResults = await scrapeInstagramProfiles(igTbIpwtProfiles, sinceDate, TAGS);

// Build N -> row map for Sheet1 updates
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

  // Update History Matrix (E) if present
  const rowIdx = liveIGShortCodes.findIndex(sc => sc === shortCode);
  if (rowIdx !== -1) {
    const rowNum = rowIdx + 2;
    igProfileMatrixUpdates.push({ range: `${HISTORY_MATRIX}!E${rowNum}`, values: [[views]] });
    postsUpdated.add(shortCode);
  }
  postsToUpdate.add(shortCode);

  // Sheet1: if exists in N, update O only
  const rowInSheet1 = igShortToRow_forProfile.get(shortCode);
  if (rowInSheet1) {
    igSheet1ProfileUpdates.push({ range: `${SHEET1}!O${rowInSheet1}`, values: [[views]] });
    continue;
  }

  // Else append to M:Q if tagged and not already present
  if (TAGS.some(tag => (post.caption || '').toLowerCase().includes(tag.toLowerCase())) &&
      !sheet1IGShortCodes.has(shortCode)) {
    const ownerUsername = post.ownerUsername || '';
    const profileLink = ownerUsername ? `https://www.instagram.com/${ownerUsername}/reels` : '';
    const prettyDate = post.timestamp
      ? new Date(post.timestamp).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
      : '';
    igAppendRows.push([profileLink, post.url || post.postUrl, views, prettyDate, post.timestamp || '']);
    postsAppended.add(shortCode);
  }
}

// Write Sheet1 view updates (O)
if (igSheet1ProfileUpdates.length) {
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    resource: { valueInputOption: 'RAW', data: igSheet1ProfileUpdates },
  });
}

// Write matrix updates for IG profiles
if (igProfileMatrixUpdates.length) {
  await batchUpdateHistoryMatrix(sheets, igProfileMatrixUpdates);
}

// Append new IG rows to M:Q
if (igAppendRows.length) {
  // Find next empty row in the M:Q region
  const existingIG = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET1}!M:Q`,
  });
  const used = (existingIG.data.values || []).length;   // rows in M:Q (includes header)
  const startRow = Math.max(2, used + 1);
  const endRow = startRow + igAppendRows.length - 1;

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET1}!M${startRow}:Q${endRow}`,
    valueInputOption: 'RAW',
    resource: { values: igAppendRows },
  });
}

    // === Reporting failed posts ===
    const failedPosts = [...postsToUpdate].filter(id => !postsUpdated.has(id) && !postsAppended.has(id));
    if (failedPosts.length) {
        console.log(`❌ Failed to update ${failedPosts.length} posts:`);
        failedPosts.forEach(url => console.log('  ' + url));
    } else {
        console.log(`✅ All intended posts updated/appended.`);
    }
})();