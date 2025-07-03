import { google } from 'googleapis';
import { ApifyClient } from 'apify-client';
import 'dotenv/config';

// ==== CONFIG =====
const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'General History Matrix';
const APIFY_TOKEN = process.env.APIFY_TOKEN;
const APIFY_ACTOR_ID = process.env.APIFY_ACTOR_ID; // apify/instagram-scraper
const CREDENTIALS_PATH = './credentials.json';

// ==== HELPERS ====
function parseDateMMDDYYYY(s) {
  if (!s) return null;
  const parts = s.split('/');
  if (parts.length !== 3) return null;
  let [m, d, y] = parts.map(Number);
  if (y < 1000) [d, m, y] = parts.map(Number); // fallback for weird data
  return new Date(y, m - 1, d);
}

function daysAgo(date) {
  const now = new Date();
  return (now - date) / 86400000;
}

function extractIGShortcode(url) {
  if (!url) return '';
  const m = url.match(/instagram\.com\/(?:reel|p|tv)\/([a-zA-Z0-9_-]+)/i);
  return m ? m[1] : '';
}

// ==== AUTH ====
async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: CREDENTIALS_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  return google.sheets({ version: 'v4', auth });
}

// ==== MAIN LOGIC ====
(async () => {
  const sheets = await getSheetsClient();

  // 1. Fetch all relevant data
  const range = `${SHEET_NAME}!A2:E`; // A = date, B = profile, C = post, D = type, E = views
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range
  });
  const rows = data.values || [];

  // 2. Filter and map rows by shortcode: 14 days, not Trailblazer, not IPWT, valid IG post link
  const shortcodeToRowIdx = new Map();
  for (let i = 0; i < rows.length; ++i) {
    const [dateStr, profileUrl, postUrl, channelType] = [
      rows[i][0] || '', rows[i][1] || '', rows[i][2] || '', rows[i][3] || ''
    ];
    if (!dateStr || !profileUrl || !postUrl) continue;

    const dateObj = parseDateMMDDYYYY(dateStr);
    if (!dateObj || daysAgo(dateObj) > 14) continue;

    const chType = (channelType || '').trim().toLowerCase();
    if (chType === 'trailblazer' || chType === 'ipwt') continue;

    if (!/instagram\.com\/(p|reel|tv)\//i.test(postUrl)) continue; // Only valid IG post URLs

    const shortcode = extractIGShortcode(postUrl);
    if (!shortcode || shortcodeToRowIdx.has(shortcode)) continue;
    shortcodeToRowIdx.set(shortcode, i);
  }

  const shortcodes = Array.from(shortcodeToRowIdx.keys());
  console.log(`Found ${shortcodes.length} unique IG posts to process (excluding Trailblazer and IPWT).`);

  if (!shortcodes.length) {
    console.log('No qualifying posts found.');
    return;
  }

  // 3. Build post URLs for Apify (reconstruct with /p/ to ensure valid)
  const postUrls = shortcodes.map(sc => `https://www.instagram.com/p/${sc}/`);

  // 4. Apify scrape by post URLs (single batch)
  const apify = new ApifyClient({ token: APIFY_TOKEN });
  let apifyResults = [];
  try {
    console.log(`Scraping ${postUrls.length} posts directly using Apify...`);
    const run = await apify.actor(APIFY_ACTOR_ID).call({
      directUrls: postUrls,
      resultsType: 'posts',
      addParentData: false
    });
    const { items } = await apify.dataset(run.defaultDatasetId).listItems();
    apifyResults = items || [];
    console.log(`Fetched results for ${apifyResults.length} posts from Apify.`);
  } catch (err) {
    console.error(`Error scraping posts:`, err.message);
    return;
  }

  if (!apifyResults.length) {
    console.log('No new views found from Apify.');
    return;
  }

  // 5. Update Col E for matching post shortcodes (log each update)
  let updates = 0;
  for (const post of apifyResults) {
    const shortcode = post.shortCode || extractIGShortcode(post.url);
    if (!shortcode || !post.videoPlayCount) continue;
    const rowIdx = shortcodeToRowIdx.get(shortcode);
    if (rowIdx === undefined) continue;
    const prevViews = (rows[rowIdx][4] !== undefined && rows[rowIdx][4] !== '') ? rows[rowIdx][4] : null;
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!E${rowIdx + 2}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[post.videoPlayCount]] }
    });
    updates++;
    console.log(
      `Updated row ${rowIdx + 2} | Post shortcode: ${shortcode}\n` +
      `    Previous views: ${prevViews}\n` +
      `    New views: ${post.videoPlayCount}\n`
    );
  }
  console.log(`Done. Updated ${updates} post(s) with new views.`);
})();
