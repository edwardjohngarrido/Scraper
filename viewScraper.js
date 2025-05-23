// Updated viewScraper.js with improvements from tbScraper.js
// Proxy logic included but commented out for now

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

puppeteer.use(StealthPlugin());

const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'Sheet11';
const CREDENTIALS_PATH = 'credentials.json';

// const SMARTPROXY_AUTH = 'username:password';
// const SMARTPROXY_HOST = 'gate.smartproxy.com';
// const SMARTPROXY_PORT = 7000;

function convertPostIdToTimestamp(postId) {
  try {
    const binaryId = BigInt(postId).toString(2).padStart(64, '0');
    return parseInt(binaryId.substring(0, 32), 2) * 1000;
  } catch {
    return null;
  }
}

async function initSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: CREDENTIALS_PATH,
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

  // Uncomment below to enable Smartproxy
  // args.push(`--proxy-server=http://${SMARTPROXY_HOST}:${SMARTPROXY_PORT}`);

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

function getColumnLetter(index) {
  const A = 'A'.charCodeAt(0);
  let result = '';
  while (index >= 0) {
    result = String.fromCharCode(A + (index % 26)) + result;
    index = Math.floor(index / 26) - 1;
  }
  return result;
}

async function dismissInterestModal(page) {
  try {
    await page.evaluate(() => {
      const modal = document.querySelector('[data-e2e="interest-login-modal"]');
      if (modal) {
        const closeBtn = modal.querySelector('svg');
        if (closeBtn) closeBtn.click();
      }
    });
  } catch (e) {
    console.warn('⚠️ Could not dismiss modal:', e.message);
  }
}

async function scrapeViewsFromProfile(page, profileUrl, postIdToRow, columnLetter) {
  try {
    console.log(`\n🌐 Scraping profile: ${profileUrl}`);
    await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForSelector('a[href*="/video/"], a[href*="/photo/"]', { timeout: 15000 });
    await dismissInterestModal(page);
    await new Promise(res => setTimeout(res, 5000));

    const seen = new Set();
    const collected = [];
    const cutoff = Date.now() - 14 * 24 * 60 * 60 * 1000; // 14-day window
    let scrolls = 0;
    const maxScrolls = 20;
    let tooOldCount = 0;

    while (scrolls++ < maxScrolls) {
      const posts = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('a[href*="/video/"], a[href*="/photo/"]'))
          .map(post => {
            const href = post.getAttribute('href')?.split('?')[0];
            const view = post.querySelector('strong[data-e2e="video-views"]')?.innerText || null;
            return { href, view };
          });
      });

      console.log(`🔍 Scroll ${scrolls}: Found ${posts.length} posts (checking last 14 days)...`);

      let foundNew = false;

      for (const { href, view } of posts) {
        const match = href?.match(/\/(video|photo)\/(\d+)/);
        const postId = match?.[2];
        if (!postId || seen.has(postId)) continue;
        seen.add(postId);

        const ts = convertPostIdToTimestamp(postId);
        if (!ts) continue;

        const postDate = new Date(ts).toISOString();
        console.log(`   ↪ Found post ${href} | Views: ${view || 'N/A'} | Timestamp: ${postDate}`);

        if (ts < cutoff) {
          tooOldCount++;
          if (tooOldCount >= 15) {
            console.log("🛑 Stopping: 15 consecutive posts older than 14 days.");
            scrolls = maxScrolls;
            break;
          }
        } else {
          tooOldCount = 0;
          foundNew = true;
          collected.push({ href, view, postId });
        }
      }

      if (!foundNew) break;
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await new Promise(res => setTimeout(res, 4000));
    }

    const updates = [];
    for (const { href, view, postId } of collected) {
      const row = postIdToRow[postId];
      const normalized = normalizeViews(view);
      if (postId && normalized !== null && row) {
        updates.push({ range: `${SHEET_NAME}!${columnLetter}${row}`, values: [[normalized]] });
        console.log(`✅ Row ${row}: ${href} → ${view} → ${normalized}`);
      }
    }

    return updates;
  } catch (err) {
    console.error(`❌ Failed to scrape ${profileUrl}: ${err.message}`);
    return [];
  }
}


// 🧠 MAIN EXECUTION
(async () => {
  const sheets = await initSheets();
  const e1Res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${SHEET_NAME}!E1` });

  let useExistingColumn = false;
  const now = new Date();
  let currentColumnIndex = 4;
  const currentUtcIso = now.toISOString();

  if (e1Res.data.values && e1Res.data.values[0]) {
    const e1 = e1Res.data.values[0][0];
    const match = e1.match(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/);
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
    const sheetMeta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const targetSheet = sheetMeta.data.sheets.find(s => s.properties.title === SHEET_NAME);
    if (!targetSheet) throw new Error(`❌ Sheet "${SHEET_NAME}" not found.`);
    const sheetId = targetSheet.properties.sheetId;

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

  const columnLetter = getColumnLetter(currentColumnIndex);
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!${columnLetter}1`,
    valueInputOption: 'RAW',
    resource: { values: [[`Scraped at UTC: ${currentUtcIso}`]] },
  });

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
  await page.setViewport({ width: 1200, height: 800 });
  await page.setJavaScriptEnabled(true);

  for (const [profileUrl, postIdToRow] of profiles) {
    try {
      console.log(`🧾 Starting scrape for: ${profileUrl}`);
      const updates = await scrapeViewsFromProfile(page, profileUrl, postIdToRow, columnLetter);

      if (updates.length > 0) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: SHEET_ID,
          resource: { valueInputOption: 'RAW', data: updates },
        });
        console.log(`📤 Updated ${updates.length} posts for ${profileUrl}`);
      } else {
        console.log(`⚠️ No updates for ${profileUrl}`);
      }
    } catch (err) {
      console.error(`💥 Error during profile scrape: ${profileUrl} → ${err.message}`);
    }

    batchCounter++;
    if (batchCounter >= batchThreshold) {
      console.log('♻️ Restarting browser to refresh session...');
      await page.close();
      await browser.close();
      browser = await launchBrowser();
      page = await browser.newPage();
      await page.setViewport({ width: 1200, height: 800 });
      await page.setJavaScriptEnabled(true);
      batchThreshold = Math.floor(Math.random() * 3) + 4;
      batchCounter = 0;
    }
  }

  await page.close();
  await browser.close();
  console.log('✅ All profiles processed.');
})();