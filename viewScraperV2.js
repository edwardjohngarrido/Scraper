import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { google } from 'googleapis';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

puppeteer.use(StealthPlugin());

// For __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'History Matrix';

const DEFAULT_EXTENSION_PATH = "C:\\Users\\edwar\\Downloads\\TikTok-Captcha-Solver-Chrome-Web-Store";

// Number of parallel browsers (adjust as needed)
const NUM_BOTS = 5;

// Make these folders yourself, or let Puppeteer create them:
const chromeProfiles = Array.from({ length: NUM_BOTS }, (_, i) =>
  `D:/puppeteer_profiles/chrome-profile-bot${i + 1}`
);

// --------------------------
// Utility Functions
// --------------------------

function getRandomUserAgent() {
  return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
}

async function initBrowserWithExtension(userDataDir) {
  let extensionPath = DEFAULT_EXTENSION_PATH;
  const secondaryPath = "C:\\Users\\edwardjohngarrido\\Desktop\\Scraper\\TikTok-Captcha-Solver-Chrome-Web-Store";
  if (!fs.existsSync(extensionPath) && fs.existsSync(secondaryPath)) {
    console.warn("⚠️ Default extension path not found. Using secondary extension path.");
    extensionPath = secondaryPath;
  }
  if (!fs.existsSync(extensionPath)) throw new Error('❌ Extension path not found!');

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-infobars",
      "--disable-background-networking",
      "--disable-gpu",
      "--disable-web-security",
      "--disable-blink-features=AutomationControlled",
      `--user-agent=${getRandomUserAgent()}`,
      `--disable-extensions-except=${extensionPath}`,
      `--load-extension=${extensionPath}`,
    ],
    ignoreDefaultArgs: ["--disable-extensions"],
    executablePath: puppeteer.executablePath(),
    userDataDir, // <- use the argument now
    protocolTimeout: 300000,
  });
  return browser;
}

function randomDelay(min = 3000, max = 8000) {
  return new Promise(r => setTimeout(r, min + Math.random() * (max - min)));
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

function convertPostIdToTimestamp(postId) {
  try {
    const binaryId = BigInt(postId).toString(2).padStart(64, '0');
    return parseInt(binaryId.substring(0, 32), 2) * 1000;
  } catch {
    return null;
  }
}

async function parallelMap(arr, concurrency, fn) {
  const results = [];
  let idx = 0;
  const total = arr.length;
  async function worker(botIdx) {
    while (true) {
      let i;
      i = idx++;
      if (i >= total) break;
      // Progress log
      const left = total - i;
      console.log(`[Bot #${botIdx + 1}] (${i + 1}/${total}) Scraping profile: ${arr[i][0]} (${left} left for this bot)`);
      results[i] = await fn(arr[i], i, botIdx);
    }
  }
  await Promise.all(Array(concurrency).fill().map((_, botIdx) => worker(botIdx)));
  return results;
}

async function dismissLoginModal(page) {
  try {
    await page.evaluate(() => {
      const modal = document.querySelector('[data-e2e="login-modal"],[data-e2e="login-guide"]');
      if (modal) {
        const closeBtn = modal.querySelector('svg');
        if (closeBtn) closeBtn.click();
      }
    });
    await new Promise(r => setTimeout(r, 1200));
  } catch (err) {}
}

async function initSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: path.resolve(__dirname, 'credentials.json'),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth: await auth.getClient() });
}

function extractTikTokProfileFromPost(postLink) {
  // e.g. https://www.tiktok.com/@username/video/1234567890
  const match = postLink.match(/^https:\/\/www\.tiktok\.com\/@([^/]+)/i);
  return match ? `https://www.tiktok.com/@${match[1]}` : null;
}

function isTikTokLink(link) {
  return /^https:\/\/www\.tiktok\.com\//i.test(link);
}

function parseSheetDate(dateStr) {
  // Accepts e.g. "7/22/2025" or "2025-07-22"
  if (!dateStr) return null;
  let d = Date.parse(dateStr);
  if (!isNaN(d)) return d;
  const mdy = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mdy) {
    return new Date(`${mdy[3]}-${mdy[1].padStart(2,'0')}-${mdy[2].padStart(2,'0')}`).getTime();
  }
  return null;
}

// --------------------------
// Main Async Function
// --------------------------
async function scrapeProfilePosts(profileUrl, postIdsToScrape, minTargetDate, maxScrolls = 100, stopOlderThanCount = 5, botIdx = 0) {

  let lastError = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    let browser, page;
    try {
      const userDataDir = chromeProfiles[botIdx % chromeProfiles.length];
      browser = await initBrowserWithExtension(userDataDir);
      page = await browser.newPage();
      await randomDelay(3000, 9000); // Pause before visiting a profile
      await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await new Promise(r => setTimeout(r, 3500));

      // Early detect: Profile does not exist
      const notFound = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('p')).some(
          el => el.textContent && el.textContent.includes("Couldn't find this account")
        );
      });
      if (notFound) {
        console.warn(`❌ Profile not found: ${profileUrl}, skipping.`);
        await browser.close();
        return {};
      }

      await randomDelay(2000, 6000); // Pause as if waiting for human to "look"

      let foundGrid = false;
      try {
        await page.waitForSelector('div[data-e2e="user-post-item"]', { timeout: 15000 });
        foundGrid = true;
      } catch {
        await dismissLoginModal(page);
        try {
          await page.waitForSelector('div[data-e2e="user-post-item"]', { timeout: 12000 });
          foundGrid = true;
        } catch (err) {
          lastError = err;
        }
      }
      if (!foundGrid) throw new Error("No grid found");

      const scrapedViews = {};
      let consecutiveOld = 0;
      let seenPosts = new Set();
      for (let scrollIters = 0; scrollIters < maxScrolls; scrollIters++) {
        // Evaluate all posts in grid so far
        const posts = await page.$$eval(
          'div[data-e2e="user-post-item"] a[href*="/video/"],div[data-e2e="user-post-item"] a[href*="/photo/"]',
          els => els.map(a => {
            const container = a.closest('div[data-e2e="user-post-item"]');
            let views = '';
            if (container) {
              const viewEl = container.querySelector('strong[data-e2e="video-views"],.video-count');
              views = viewEl ? viewEl.innerText : '';
            }
            const m = a.href.match(/\/(video|photo)\/(\d+)/);
            const postId = m ? m[2] : null;
            return { href: a.href.split('?')[0], postId, views };
          })
        );

        let foundAnyNew = false;
        let thisScrollOld = 0;

        for (const { postId, views } of posts) {
          if (!postId || seenPosts.has(postId)) continue;
          seenPosts.add(postId);

          const postDate = convertPostIdToTimestamp(postId);
          if (!postDate) continue;

          // If this is a target post, record its views
          if (postIdsToScrape.has(postId)) {
            if (scrapedViews[postId] === undefined) {
              scrapedViews[postId] = normalizeViews(views);
              foundAnyNew = true;
            }
          }

          // If this post is older than the oldest needed, count for stopping logic
          if (postDate < minTargetDate) {
            thisScrollOld += 1;
          }
        }

        if (!foundAnyNew) consecutiveOld += thisScrollOld;
        else consecutiveOld = 0;

        // If we've seen enough consecutive irrelevant (too-old) posts, stop
        if (consecutiveOld >= stopOlderThanCount) {
          console.log(`  Hit ${stopOlderThanCount} consecutive posts older than pool, breaking scroll.`);
          break;
        }

        await page.evaluate(() => window.scrollBy(0, 1500));
        await randomDelay(1200, 3500); // Pause between scrolls
        await new Promise(r => setTimeout(r, 800));
      }
      await randomDelay(1000, 3000); // Pause before closing, simulating a linger
      await browser.close();
      return scrapedViews;
    } catch (err) {
      if (browser) await browser.close();
      lastError = err;
      console.warn(`⚠️ [Attempt ${attempt}/2] Failed to scrape ${profileUrl}: ${err.message}`);
      if (attempt < 2) await new Promise(r => setTimeout(r, 3000 + Math.random() * 2000));
    }
  }
  console.error(`❌ Failed to scrape profile after 2 attempts: ${profileUrl}`);
  return {};
}

(async () => {
  const sheets = await initSheets();

  // 1. Read data from sheet
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2:D`, // adjust as needed
  });
  const rows = data.values || [];
  const today = Date.now();
  const twoWeeksAgo = today - 14 * 24 * 60 * 60 * 1000;

  // 2. Filter rows to process
  const toProcess = [];
  for (let i = 0; i < rows.length; ++i) {
    const row = rows[i];
    const [createdAt, profileLink, postLink, tag] = row;

    const createdMs = parseSheetDate(createdAt);
    if (!isTikTokLink(profileLink)) continue;
    if (tag && (tag.trim().toLowerCase() === 'trailblazer' || tag.trim().toLowerCase() === 'ipwt')) continue;
    if (!postLink || !isTikTokLink(postLink)) continue;
    if (!createdMs || createdMs < twoWeeksAgo || createdMs > today) continue;
    toProcess.push({ rowIdx: i+2, profileLink, postLink }); // rowIdx is 1-based including header
  }

  // 3. Map: profile -> set of needed post IDs
  const profileMap = {};
  for (const { profileLink, postLink } of toProcess) {
    const profileUrl = extractTikTokProfileFromPost(profileLink);
    if (!profileUrl) continue;
    if (!profileMap[profileUrl]) profileMap[profileUrl] = new Set();
    const m = postLink.match(/\/(video|photo)\/(\d+)/);
    if (!m) continue;
    const postId = m[2];
    profileMap[profileUrl].add(postId);
  }

  // 4. Scrape all relevant posts per profile (parallel)
  const allViews = {}; // postId -> views
  const profileEntries = Object.entries(profileMap);

await parallelMap(profileEntries, 5, async ([profileUrl, postIdsToScrape], i, botIdx) => {
  // Calculate the minimum date among the post IDs for this profile
  let minTargetDate = Infinity;
  for (const postId of postIdsToScrape) {
    const t = convertPostIdToTimestamp(postId);
    if (t && t < minTargetDate) minTargetDate = t;
  }
  if (!isFinite(minTargetDate)) {
    console.warn(`No valid target dates for profile: ${profileUrl}, skipping.`);
    return;
  }
  console.log(`  Scraping profile with ${postIdsToScrape.size} posts to check (oldest: ${new Date(minTargetDate).toISOString()})`);

  const gridViews = await scrapeProfilePosts(profileUrl, postIdsToScrape, minTargetDate, 100, 5, botIdx);

  let success = 0, fail = 0;
  for (const postId of postIdsToScrape) {
    if (gridViews[postId] !== undefined) {
      allViews[postId] = gridViews[postId];
      success++;
    } else {
      fail++;
    }
  }
  console.log(`    Result for profile ${profileUrl}: Scraped ${success}/${postIdsToScrape.size} (${fail} failed)`);
});

  // 5. Prepare batch update for col E
  const updates = [];
  for (const { rowIdx, postLink } of toProcess) {
    const m = postLink.match(/\/(video|photo)\/(\d+)/);
    if (!m) continue;
    const postId = m[2];
    if (allViews[postId] !== undefined) {
      updates.push({
        range: `${SHEET_NAME}!E${rowIdx}`,
        values: [[allViews[postId]]],
      });
      console.log(`Updating row ${rowIdx}: ${postLink} (ID ${postId}) → ${allViews[postId]}`);
    }
  }

  // 6. Write to sheet in batch
  if (updates.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      resource: { valueInputOption: 'RAW', data: updates },
    });
    console.log(`✅ Updated ${updates.length} rows with views.`);
  } else {
    console.log(`No updates found.`);
  }
})();
