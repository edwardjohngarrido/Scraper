// Instagram Reels Scraper - Tier 3 Upgraded with Smart Scroll and Proxy Logic

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const { google } = require('googleapis');
puppeteer.use(StealthPlugin());

const COOKIES_PATH = './cookies.json';
const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'Sheet1';
const SCROLL_WAIT_TIME = 4000;
const MAX_SCROLLS = 15;
const SMARTPROXY_ENABLED = false;

function getRandomUserAgent() {
  const agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)...',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...'
  ];
  return agents[Math.floor(Math.random() * agents.length)];
}

function normalizeViewCount(text) {
  if (!text) return null;
  const num = text.replace(/,/g, '').toUpperCase();
  if (num.endsWith('K')) return Math.round(parseFloat(num) * 1000);
  if (num.endsWith('M')) return Math.round(parseFloat(num) * 1000000);
  if (!isNaN(num)) return parseInt(num, 10);
  return null;
}

async function launchBrowser() {
  const args = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    `--user-agent=${getRandomUserAgent()}`
  ];

  return await puppeteer.launch({
    headless: false,
    args
  });
}

async function loadCookies(page) {
  if (fs.existsSync(COOKIES_PATH)) {
    const cookies = JSON.parse(fs.readFileSync(COOKIES_PATH, 'utf-8'));
    await page.setCookie(...cookies);
    console.log('🍪 Cookies loaded into browser session.');
  } else {
    console.log('⚠️ No cookies found. Please run login flow first.');
  }
}

async function initSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'credentials.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth: await auth.getClient() });
}

async function fetchSheetData() {
  const sheets = await initSheets();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!M2:Q`,
  });

  const rows = res.data.values || [];
  const profileMap = {};

  rows.forEach((row, index) => {
    const profileLink = row[0];  // Column M
    const postLinkRaw = row[1];  // Column N
    if (!profileLink || !postLinkRaw) return;
    if (!profileLink.includes('instagram.com')) {
      console.warn(`⚠️ Skipping invalid profile link at row ${index + 2}`);
      return;
    }
    const postLink = postLinkRaw.split('?')[0].replace(/\/$/, '');
    if (!profileMap[profileLink]) profileMap[profileLink] = [];
    profileMap[profileLink].push({ postLink, rowIndex: index + 2 });
  });

  return profileMap;
}

async function scrollAndScrape(page, profile, posts, updateQueue) {
  const profileUrl = profile.endsWith('/') ? profile : profile + '/';
  console.log(`🔍 Navigating to: ${profileUrl}`);
  await page.goto(profileUrl, { timeout: 60000, waitUntil: 'domcontentloaded' });

  if (page.url().includes('/accounts/login')) {
    console.error('❌ Not logged in. Instagram redirected to login page.');
    return;
  }

  const postIdToRow = {};
  const postIdTargets = new Set();
  const foundIds = new Set();

  posts.forEach(({ postLink, rowIndex }) => {
    const postId = postLink.split('/reel/')[1]?.replaceAll('/', '').split('?')[0];
    if (postId) {
      postIdToRow[postId] = rowIndex;
      postIdTargets.add(postId);
    }
  });

  console.log(`🎯 Targeting ${posts.length} posts: ${[...postIdTargets].join(', ')}`);

  let scrollCount = 0;
  let lastHeight = await page.evaluate(() => document.body.scrollHeight);
  let noNewMatchesStreak = 0;

  while (scrollCount < MAX_SCROLLS && foundIds.size < postIdTargets.size) {
    console.log(`🌀 Scroll attempt ${scrollCount + 1}`);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await new Promise(resolve => setTimeout(resolve, SCROLL_WAIT_TIME));

    const anchors = await page.$$('a[href*="/reel/"]');
    console.log(`📸 Found ${anchors.length} visible reel links on grid.`);

    let matchesThisScroll = 0;

    for (const anchor of anchors) {
      const href = await anchor.evaluate(a => a.href);
      if (!href) continue;

      const match = href.match(/\/reel\/([\w-]+)/);
      const postId = match ? match[1] : null;
      if (!postId || foundIds.has(postId) || !postIdTargets.has(postId)) continue;

      foundIds.add(postId);
      matchesThisScroll++;
      console.log(`✅ Match found for post ID ${postId}: ${href}`);

      try {
        const svg = await anchor.$('svg[aria-label="View Count Icon"]');
        const viewSpan = svg ? await svg.evaluateHandle(el => el.parentElement.nextElementSibling) : null;
        const viewsRaw = viewSpan ? await viewSpan.evaluate(el => el.innerText) : 'N/A';
        const views = normalizeViewCount(viewsRaw);
        if (!viewsRaw) console.warn(`⚠️ No views found for post ${href}`);
        console.log(`👁️ View Count: ${views}`);

        await anchor.click();
        await page.waitForSelector('time[datetime]', { timeout: 10000 });
        const dateTime = await page.$eval('time[datetime]', el => el.getAttribute('datetime'));
        if (!dateTime) console.warn(`⚠️ No datetime found for post ${href}`);
        console.log(`📅 Post Date: ${dateTime}`);

        const row = postIdToRow[postId];
        updateQueue.push({ range: `${SHEET_NAME}!O${row}`, values: [[views]] });
        updateQueue.push({ range: `${SHEET_NAME}!Q${row}`, values: [[dateTime]] });

        await page.keyboard.press('Escape');
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (err) {
        console.warn(`⚠️ Error scraping post ${href}: ${err.message}`);
        continue;
      }
    }

    if (matchesThisScroll === 0) noNewMatchesStreak++;
    else noNewMatchesStreak = 0;

    if (noNewMatchesStreak >= 3) {
      console.log('🛑 No new posts found in 3 scrolls. Exiting early.');
      break;
    }

    const newHeight = await page.evaluate(() => document.body.scrollHeight);
    if (newHeight === lastHeight) {
      console.log('📉 Reached bottom of page.');
      break;
    }

    lastHeight = newHeight;
    scrollCount++;
  }

  const missing = [...postIdTargets].filter(id => !foundIds.has(id));
  if (missing.length) console.warn(`🚫 Posts not found: ${missing.join(', ')}`);
}

async function updateSheetBatch(updateQueue) {
  if (!updateQueue.length) return;
  const sheets = await initSheets();
  try {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      resource: {
        valueInputOption: 'RAW',
        data: updateQueue
      }
    });
    console.log('✅ Sheet updated successfully.');
  } catch (err) {
    console.error('❌ Failed to update sheet:', err.message);
    console.log('🔁 Retrying sheet update in 10 seconds...');
    await new Promise(resolve => setTimeout(resolve, 10000));
    try {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: {
          valueInputOption: 'RAW',
          data: updateQueue
        }
      });
      console.log('✅ Retry successful.');
    } catch (retryErr) {
      console.error('❌ Retry also failed:', retryErr.message);
    }
  }
}

async function runScraper() {
  const browser = await launchBrowser();
  const page = await browser.newPage();
  await loadCookies(page);

  const profileMap = await fetchSheetData();
  const updateQueue = [];

  for (const profile in profileMap) {
    const start = Date.now();
    await scrollAndScrape(page, profile, profileMap[profile], updateQueue);
    const end = Date.now();
    console.log(`⏱️ Finished profile in ${(end - start) / 1000}s`);
    console.log(`⏸ Pausing 5 seconds before next profile...`);
    await new Promise(resolve => setTimeout(resolve, 5000));
  }

  await updateSheetBatch(updateQueue);
  await browser.close();
  console.log('✅ All profiles processed.');
}

runScraper().catch(console.error);
