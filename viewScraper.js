const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { google } = require('googleapis');
const fs = require('fs');

puppeteer.use(StealthPlugin());

const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'Sheet11';

async function initSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'credentials.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth: await auth.getClient() });
}

function getRandomUserAgent() {
  const agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)... Chrome/120.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)... Chrome/119.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64)... Chrome/118.0.0.0',
  ];
  return agents[Math.floor(Math.random() * agents.length)];
}

async function launchBrowser() {
  const args = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-blink-features=AutomationControlled',
    `--user-agent=${getRandomUserAgent()}`,
    '--window-size=1920,1080',
  ];
  return puppeteer.launch({ headless: true, args });
}

function normalizeViews(viewStr) {
  if (!viewStr) return null;
  viewStr = viewStr.replace(/,/g, '').trim().toUpperCase();
  if (viewStr.endsWith('K')) return Math.round(parseFloat(viewStr) * 1000);
  if (viewStr.endsWith('M')) return Math.round(parseFloat(viewStr) * 1000000);
  if (viewStr.endsWith('B')) return Math.round(parseFloat(viewStr) * 1000000000);
  const num = parseInt(viewStr);
  return isNaN(num) ? null : num;
}

async function scrapeViewsFromProfile(page, profileUrl, postIdToRow, columnLetter) {
  try {
    await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await new Promise(res => setTimeout(res, 3000));

    const seen = new Set();
    const postData = [];
    const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
    let scrolls = 0, tooOldCount = 0, maxScrolls = 10;

    while (scrolls++ < maxScrolls) {
      const newPosts = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('a[href*="/video/"], a[href*="/photo/"]'))
          .map(post => {
            const href = post.href?.split('?')[0];
            const view = post.querySelector('strong[data-e2e="video-views"]')?.innerText || null;
            return { href, view };
          });
      });

      let newDataFound = false;
      for (const { href, view } of newPosts) {
        const postId = href?.match(/\/(video|photo)\/(\d+)/)?.[2];
        if (!postId || seen.has(postId)) continue;
        seen.add(postId);

        const ts = convertPostIdToTimestamp(postId);
        if (!ts) continue;

        if (ts < cutoff) {
          tooOldCount++;
          if (tooOldCount >= 3) break;
        } else {
          tooOldCount = 0;
          newDataFound = true;
          postData.push({ href, view, postId });
        }
      }

      if (!newDataFound) break;
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await new Promise(res => setTimeout(res, 2500));
    }

    const updates = [];
    for (const { href, view, postId } of postData) {
      const row = postIdToRow[postId];
      const normalized = normalizeViews(view);
      if (postId && normalized !== null && row) {
        updates.push({ range: `${SHEET_NAME}!${columnLetter}${row}`, values: [[normalized]] });
        console.log(`✅ ${href} → ${view} → ${normalized}`);
      }
    }
    return updates;
  } catch (err) {
    console.error(`❌ Failed to scrape ${profileUrl}: ${err.message}`);
    return [];
  }
}

function getColumnLetter(index) {
  const A = 'A'.charCodeAt(0);
  let result = '';
  while (index >= 0) {
    result = String.fromCharCode(A + (index % 26)) + result;
    index = Math.floor(index / 26) - 1;
  }
  return result;
}

(async () => {
  const sheets = await initSheets();

  // Step 1: Read existing E1
  const e1Res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!E1`,
  });

  let useExistingColumn = false;
  const now = new Date();
  let currentColumnIndex = 4; // Column E = index 4 (0-based)
  const currentUtcIso = now.toISOString();

  if (e1Res.data.values && e1Res.data.values[0]) {
    const e1 = e1Res.data.values[0][0];
    const match = e1.match(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z/);
    if (match) {
      const parsed = new Date(match[0]);
      const diffMs = Math.abs(now - parsed);
      if (diffMs <= 3 * 60 * 60 * 1000) {
        useExistingColumn = true;
        console.log(`🕒 Reusing existing column E (E1 is within ±3h): ${match[0]}`);
      }
    }
  }

  if (!useExistingColumn) {
    // Step 2: Insert new blank column before E
    // Step 2a: Get sheetId from title
const sheetMeta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
const targetSheet = sheetMeta.data.sheets.find(s => s.properties.title === SHEET_NAME);
if (!targetSheet) throw new Error(`❌ Sheet "${SHEET_NAME}" not found.`);
const sheetId = targetSheet.properties.sheetId;

// Step 2b: Insert new blank column before E
await sheets.spreadsheets.batchUpdate({
  spreadsheetId: SHEET_ID,
  requestBody: {
    requests: [{
      insertDimension: {
        range: {
          sheetId,
          dimension: 'COLUMNS',
          startIndex: currentColumnIndex,
          endIndex: currentColumnIndex + 1,
        },
        inheritFromBefore: true
      }
    }]
  }
});
console.log(`➕ Inserted new column before E (Sheet ID: ${sheetId})`);

  }

  const columnLetter = getColumnLetter(currentColumnIndex); // Always E unless columns were inserted
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!${columnLetter}1`,
    valueInputOption: 'RAW',
    resource: { values: [[`Scraped at UTC: ${currentUtcIso}`]] },
  });

  // Step 3: Pull post list and scrape
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:Z`,
  });

  const rows = res.data.values || [];
  const profileToPostRows = {};

  rows.forEach((row, i) => {
    const profileUrl = row[1];
    const postUrl = row[2];

    if (!profileUrl || !postUrl) return;
    if (profileUrl.includes('instagram.com') || postUrl.includes('instagram.com')) return;

    const postId = postUrl.match(/\/(video|photo)\/(\d+)/)?.[2];
    if (!postId) return;

    if (!profileToPostRows[profileUrl]) profileToPostRows[profileUrl] = {};
    profileToPostRows[profileUrl][postId] = i + 1;
  });

  const profiles = Object.entries(profileToPostRows);
  let batchThreshold = Math.floor(Math.random() * 3) + 4;
  let batchCounter = 0;

  let browser = await launchBrowser();
  let page = await browser.newPage();

  for (const [profileUrl, postIdToRow] of profiles) {
    const updates = await scrapeViewsFromProfile(page, profileUrl, postIdToRow, columnLetter);

    if (updates.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: { valueInputOption: 'RAW', data: updates },
      });
      console.log(`📤 Updated ${updates.length} posts for ${profileUrl}`);
    }

    batchCounter++;
    if (batchCounter >= batchThreshold) {
      console.log('♻️ Restarting browser to refresh session...');
      await page.close();
      await browser.close();

      browser = await launchBrowser();
      page = await browser.newPage();

      batchThreshold = Math.floor(Math.random() * 3) + 4;
      batchCounter = 0;
    }
  }

  await page.close();
  await browser.close();
  console.log('✅ All profiles processed.');
})();
