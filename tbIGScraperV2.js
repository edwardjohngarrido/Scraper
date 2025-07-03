import { google } from 'googleapis';
import { ApifyClient } from 'apify-client';
import 'dotenv/config';

// CONFIG — fill these in
const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'Sheet1';
const APIFY_TOKEN = process.env.APIFY_TOKEN;
const APIFY_ACTOR_ID = process.env.APIFY_ACTOR_ID; // apify/instagram-scraper
const CREDENTIALS_PATH = './credentials.json';

const INSTAGRAM_TAGS = [
  '@In Print We Trust', '@in print we trust', '@InPrintWeTrust', '@inprintwetrust',
  '@inprintwetrust.co', '@InPrintWeTrust.co', '#InPrintWeTrust', '#inprintwetrust',
  '#IPWT', '#ipwt'
];
const TAG_REGEX = new RegExp(
  INSTAGRAM_TAGS.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|'),
  'i'
);
const SPECIAL_CASE_URLS = [
  'https://www.instagram.com/ipwtstreetalk/',
  'https://www.instagram.com/inprintwetrust.co/reels'
];

function formatDateNice(isoString) {
  if (!isoString) return '';
  const d = new Date(isoString);
  if (isNaN(d)) return '';
  // e.g., "July 1, 2025"
  return d.toLocaleString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC'
  });
}

// Google Sheets Auth
async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: CREDENTIALS_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  return google.sheets({ version: 'v4', auth });
}

// 1. Gather profile URLs and priority info from Sheet1
async function getProfileUrls(sheets) {
  const range = `${SHEET_NAME}!V2:Y`;
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range
  });
  const rows = data.values || [];

  const highPriority = [];
  const lowPriority = [];
  const allSpecialCaseUrls = new Set();

  rows.forEach(row => {
    const url = (row[0] || '').trim();
    const isActive = (row[1] || '').toUpperCase() === 'TRUE';
    const isFailed = (row[2] || '').toUpperCase() === 'TRUE';
    const isFinished = (row[3] || '').toUpperCase() === 'TRUE';

    if (!/instagram\.com/i.test(url)) return;
    if (isFinished) return;

    // Always include special-case URLs
    if (SPECIAL_CASE_URLS.includes(url)) {
      allSpecialCaseUrls.add(url);
      return;
    }
    if (isActive) {
      highPriority.push(url);
      return;
    }
    if (isFailed && !isActive) {
      lowPriority.push(url);
    }
  });
  // Ensure special-case URLs are always high-priority
  SPECIAL_CASE_URLS.forEach(u => {
    if (allSpecialCaseUrls.has(u) && !highPriority.includes(u)) highPriority.push(u);
  });

  return { highPriority, lowPriority };
}

// 2. Build Apify jobs
function buildApifyJobs({ highPriority, lowPriority }) {
  const jobs = [];
  if (highPriority.length) {
    jobs.push({
      label: 'high-priority',
      input: {
        directUrls: highPriority,
        resultsType: 'posts',
        onlyPostsNewerThan: '2 weeks',
        resultsLimit: 20,
        addParentData: false
      }
    });
  }
  if (lowPriority.length) {
    jobs.push({
      label: 'low-priority',
      input: {
        directUrls: lowPriority,
        resultsType: 'posts',
        onlyPostsNewerThan: '2 weeks',
        resultsLimit: 10,
        addParentData: false
      }
    });
  }
  return jobs;
}

// 3. Launch Apify, get results
async function runApifyAndCollectPosts(jobs) {
  const client = new ApifyClient({ token: APIFY_TOKEN });
  const allPosts = [];
  for (const job of jobs) {
    console.log(`Running Apify IG job: ${job.label} for ${job.input.directUrls.length} URLs...`);
    const run = await client.actor(APIFY_ACTOR_ID).call(job.input);
    const { items } = await client.dataset(run.defaultDatasetId).listItems();
    items.forEach(item => {
      allPosts.push(item);
    });
  }
  return allPosts;
}

// 4. Update Sheet1 (Cols M:N:O:P:Q:R)
async function updateSheetWithPosts(sheets, allPosts) {
  // Fetch all of M:R to find existing posts and empty spots
  const range = `${SHEET_NAME}!M2:R`;
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range
  });
  const rows = data.values || [];
  const urlToRowIdx = new Map();
  rows.forEach((row, idx) => {
    const url = (row[1] || '').trim();
    if (url) urlToRowIdx.set(url, idx);
  });

  // For each post:
  for (const post of allPosts) {
    if (post.type !== "Video") continue;

    let profileUrl = post.inputUrl || '';
    if (!profileUrl && post.ownerUsername) {
      profileUrl = `https://www.instagram.com/${post.ownerUsername}/`;
    }
    if (!profileUrl) continue;

    const isSpecialCase = SPECIAL_CASE_URLS.includes(profileUrl);
    if (!isSpecialCase && !TAG_REGEX.test(post.caption || '')) continue;

    const postUrl = post.url;
    const videoPlayCount = post.videoPlayCount || 0;
    const timestamp = post.timestamp || '';
    const niceDate = formatDateNice(timestamp);
    const type = post.type || '';

    if (!postUrl) continue;

    if (urlToRowIdx.has(postUrl)) {
      // Existing post: update Col O (videoplaycount)
      const rowIdx = urlToRowIdx.get(postUrl);
      const prevViews = (rows[rowIdx][2] !== undefined && rows[rowIdx][2] !== '') ? rows[rowIdx][2] : null;
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!O${rowIdx + 2}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[videoPlayCount]] }
      });
      console.log(
        `Updated view count for ${postUrl} at row ${rowIdx + 2}` +
        (prevViews !== null ? ` (was ${prevViews}, now ${videoPlayCount})` : ` (set to ${videoPlayCount})`)
      );
    } else {
      // Find the first empty row in M
      let emptyIdx = rows.findIndex(row => !row[0] || row[0].trim() === '');
      if (emptyIdx === -1) emptyIdx = rows.length;

      // Prepare values for M:R
      const values = [[
        profileUrl,
        postUrl,
        videoPlayCount,
        niceDate,    // P
        timestamp,   // Q
        type         // R
      ]];
      const cellRange = `${SHEET_NAME}!M${emptyIdx + 2}:R${emptyIdx + 2}`;

      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: cellRange,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values }
      });
      rows[emptyIdx] = values[0];
      console.log(`Added new post at row ${emptyIdx + 2} for profile ${profileUrl}`);
    }
  }
}

(async () => {
  // 0. Google Sheets API setup
  const sheets = await getSheetsClient();

  // 1. Get profile URLs/priorities
  const { highPriority, lowPriority } = await getProfileUrls(sheets);

  if (!highPriority.length && !lowPriority.length) {
    console.log('No profiles to process.');
    return;
  }

  // 2. Prepare Apify jobs
  const jobs = buildApifyJobs({ highPriority, lowPriority });
  if (!jobs.length) {
    console.log('No jobs to run.');
    return;
  }

  // 3. Run Apify and collect all posts
  const allPosts = await runApifyAndCollectPosts(jobs);

  if (!allPosts.length) {
    console.log('No posts found from Apify.');
    return;
  }

  // 4. Update Sheet1 M:N:O:P:Q:R (first empty row for new, update O for existing)
  await updateSheetWithPosts(sheets, allPosts);

  console.log('Done.');
})();
