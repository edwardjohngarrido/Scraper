// FULL viewScraper.js with VM fixes: deeper scrolls, added delays, improved rendering reliability

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { google } = require('googleapis');
const fs = require('fs');

puppeteer.use(StealthPlugin());

const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'Sheet11';

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

function getColumnLetter(index) {
  const A = 'A'.charCodeAt(0);
  let result = '';
  while (index >= 0) {
    result = String.fromCharCode(A + (index % 26)) + result;
    index = Math.floor(index / 26) - 1;
  }
  return result;
}

async function scrapeViewsFromProfile(page, profileUrl, postIdToRow, columnLetter) {
  try {
    console.log(`\n🌐 Scraping profile: ${profileUrl}`);
    await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForSelector('a[href*="/video/"], a[href*="/photo/"]', { timeout: 15000 });
    await new Promise(res => setTimeout(res, 5000));

    const seen = new Set();
    const collected = [];
    const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
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

      console.log(`🔍 Scroll ${scrolls}: Found ${posts.length} posts.`);

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
          if (tooOldCount >= 5) {
            console.log("🛑 Stopping: 5 consecutive posts older than 7 days.");
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
