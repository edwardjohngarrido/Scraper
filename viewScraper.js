const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const path = require('path');
const { google } = require('googleapis');
const fs = require('fs');

puppeteer.use(StealthPlugin());

const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'General History Matrix';

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
  let extensionPath = "C:\\Users\\edwar\\Downloads\\TikTok-Captcha-Solver-Chrome-Web-Store";
  const secondaryPath = "C:\\Users\\edwardjohngarrido\\Desktop\\Scraper\\TikTok-Captcha-Solver-Chrome-Web-Store";
  
      // Switch to secondary if default path doesn't exist
      if (!fs.existsSync(extensionPath) && fs.existsSync(secondaryPath)) {
          console.warn("⚠️ Default extension path not found. Using secondary extension path.");
          extensionPath = secondaryPath;
      }

  const args = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-blink-features=AutomationControlled',
    `--user-agent=${getRandomUserAgent()}`,
    '--window-size=1920,1080',
    `--disable-extensions-except=${extensionPath}`,
    `--load-extension=${extensionPath}`
  ];

  return puppeteer.launch({
    headless: true,
    args,
    executablePath: puppeteer.executablePath(),
    ignoreDefaultArgs: ["--disable-extensions"],
    defaultViewport: null
  });
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
    let retries = 0;
let found = false;
while (retries < 5 && !found) {
  try {
    await page.waitForSelector('a[href*="/video/"], a[href*="/photo/"]', { timeout: 15000 });
    found = true;
  } catch {
    retries++;
    console.warn(`🔁 Retry ${retries}/5: Selector not found yet.`);
    await new Promise(res => setTimeout(res, 2000));
  }
}
if (!found) throw new Error('Failed to find video/photo posts after 5 retries.');
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

async function runWorker(profileSubset) {
  const sheets = await initSheets();
  const columnLetter = 'E';
  let browser = await launchBrowser();
  let page = await browser.newPage();
  await page.setViewport({ width: 1200, height: 800 });
  await page.setJavaScriptEnabled(true);

  for (const [profileUrl, postIdToRow] of profileSubset) {
    try {
      console.log(`🧾 Starting scrape for: ${profileUrl}`);
      let updates = [];

      try {
        updates = await scrapeViewsFromProfile(page, profileUrl, postIdToRow, columnLetter);
      } catch (err) {
        console.error(`🔥 Page error for ${profileUrl}: ${err.message}`);
        try { await page.close(); } catch {}
        try {
          page = await browser.newPage();
          await page.setViewport({ width: 1200, height: 800 });
          await page.setJavaScriptEnabled(true);
          console.log('🔄 New page created after crash.');
        } catch (recoveryError) {
          console.error('🚫 Failed to recover new page:', recoveryError.message);
        }
        continue;
      }

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
  }

  await page.close();
  await browser.close();
  console.log('✅ Worker finished.');
}


(async () => {
  const sheets = await initSheets();
  const columnLetter = 'E';

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:Z`,
  });

  const rows = res.data.values || [];
  const profileToPostRows = {};
  const cutoff = Date.now() - 14 * 24 * 60 * 60 * 1000;

  rows.forEach((row, i) => {
    const createdAt = row[0];
    const profileUrl = row[1];
    const postUrl = row[2];
    const tag = (row[3] || '').trim().toLowerCase();

    if (!createdAt || !profileUrl || !postUrl) return;
    if (profileUrl.includes('instagram.com') || postUrl.includes('instagram.com')) return;
    if (tag === 'trailblazer' || tag === 'ipwt') return;

    let dateMs = null;
    if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(createdAt)) {
      const [month, day, year] = createdAt.split('/').map(Number);
      dateMs = new Date(year, month - 1, day).getTime();
    } else if (!isNaN(Date.parse(createdAt))) {
      dateMs = new Date(createdAt).getTime();
    }
    if (!dateMs || dateMs < cutoff) return;

    const postId = postUrl.match(/\/(video|photo)\/(\d+)/)?.[2];
    if (!postId) return;
    if (!profileToPostRows[profileUrl]) profileToPostRows[profileUrl] = {};
    profileToPostRows[profileUrl][postId] = i + 1;
  });

  const profiles = Object.entries(profileToPostRows);
  const workerCount = 3;
  const chunkSize = Math.ceil(profiles.length / workerCount);
  const chunks = Array.from({ length: workerCount }, (_, i) =>
    profiles.slice(i * chunkSize, (i + 1) * chunkSize)
  );

  console.log(`🚀 Launching ${workerCount} workers...`);
  await Promise.all(chunks.map(chunk => runWorker(chunk)));
  console.log('🎉 All workers complete!');
})();
