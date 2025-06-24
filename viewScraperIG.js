// Instagram Reels Scraper - Tier 3 Upgraded with Smart Scroll and Proxy Logic + Humanized Post Clicks

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const { google } = require('googleapis');
const readline = require('readline');
puppeteer.use(StealthPlugin());

const COOKIES_PATH = './cookies.json';
const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'General History Matrix';
const SCROLL_WAIT_TIME = 4000;
const MAX_SCROLLS = 15;
const SMARTPROXY_ENABLED = false;
const RANDOM_CLICK_CHANCE = 0.08; // 8% chance to open even if datetime exists

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
  return await puppeteer.launch({ headless: false, args });
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
  range: `${SHEET_NAME}!A2:E`, // expand if you want more cols
});


const rows = res.data.values || [];
const profileMap = {};
const today = new Date();

rows.forEach((row, index) => {
  const dateStr = row[0];
  const profileLink = row[1];
  const postLinkRaw = row[2];

  // Exclude rows where Col D is "IPWT" or "Trailblazer"
  const revStream = row[3] ? row[3].toLowerCase() : '';
  if (revStream.includes('ipwt') || revStream.includes('trailblazer')) return;


  // Parse date (assuming dd/mm/yyyy)
  let postDate;
  if (dateStr) {
    const [day, month, year] = dateStr.split('/').map(Number);
    postDate = new Date(year, month - 1, day);
  }

  // Only process if date is valid & within 14 days
  if (!postDate || (today - postDate) / (1000 * 60 * 60 * 24) > 14) return;
  if (!profileLink || !postLinkRaw) return;
  if (!profileLink.includes('instagram.com')) return;

  const postLink = postLinkRaw.split('?')[0].replace(/\/$/, '');
  if (!profileMap[profileLink]) profileMap[profileLink] = [];
  profileMap[profileLink].push({
    postLink,
    rowIndex: index + 2 // Sheet row number (offset by header)
  });
});


  return profileMap;
}


async function hoverElement(page, elementHandle) {
  const box = await elementHandle.boundingBox();
  if (box) {
    const x = box.x + box.width / 2 + (Math.random() * 10 - 5);
    const y = box.y + box.height / 2 + (Math.random() * 10 - 5);
    await page.mouse.move(x, y);
    await new Promise(resolve => setTimeout(resolve, 300 + Math.random() * 700));
  }
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
  const postMeta = {};

posts.forEach(({ postLink, rowIndex }) => {
  const postId = postLink.split('/reel/')[1]?.replaceAll('/', '').split('?')[0];
  if (postId) {
    postIdToRow[postId] = rowIndex;
    postIdTargets.add(postId);
  }
});


  const foundIds = new Set();
  let scrollCount = 0;
  let lastHeight = await page.evaluate(() => document.body.scrollHeight);
  let noNewMatchesStreak = 0;

  while (scrollCount < MAX_SCROLLS && foundIds.size < postIdTargets.size) {
    console.log(`🌀 Scroll attempt ${scrollCount + 1}`);

    // Optional scroll jitter
    const jitter = Math.random() < 0.3;
    if (jitter) {
      console.log('↕️ Scroll jitter triggered.');
      await page.evaluate(() => window.scrollBy(0, -200));
      await new Promise(resolve => setTimeout(resolve, 1000 + Math.random() * 1000));
    }

    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await new Promise(resolve => setTimeout(resolve, SCROLL_WAIT_TIME + Math.random() * 2000));

    const anchors = await page.$$('a[href*="/reel/"]');
    console.log(`📸 Found ${anchors.length} visible reel links on grid.`);

    let matchesThisScroll = 0;
    let wrongClickHappened = false;

    for (const anchor of anchors) {
      const href = await anchor.evaluate(a => a.href);
      if (!href) continue;

      const match = href.match(/\/reel\/([\w-]+)/);
      const postId = match ? match[1] : null;
      if (!postId) continue;

      await hoverElement(page, anchor);

      if (!postIdTargets.has(postId) && !wrongClickHappened && Math.random() < 0.1) {
        console.log(`🙃 Randomly opening non-target post: ${href}`);
        await anchor.click();
        await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * 2000));
        await page.keyboard.press('Escape');
        await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * 2000));
        wrongClickHappened = true;
        continue;
      }

      if (foundIds.has(postId) || !postIdTargets.has(postId)) continue;

      foundIds.add(postId);
      matchesThisScroll++;
      console.log(`✅ Match found for post ID ${postId}: ${href}`);

      try {
        const svg = await anchor.$('svg[aria-label="View Count Icon"]');
        const viewSpan = svg ? await svg.evaluateHandle(el => el.parentElement.nextElementSibling) : null;
        const viewsRaw = viewSpan ? await viewSpan.evaluate(el => el.innerText) : 'N/A';
        const views = normalizeViewCount(viewsRaw);
        if (!viewsRaw) {
          console.warn(`⚠️ No views found for post ${href}`);
        }
        console.log(`👁️ View Count: ${views}`);

        const row = postIdToRow[postId];
        if (views !== null && views !== undefined) {
          updateQueue.push({ range: `${SHEET_NAME}!E${row}`, values: [[views]] });
        } else {
          console.warn(`⏩ Skipping update for ${href} (no view count found, keeping old value)`);
        }

        const shouldOpen = postMeta[postId] || Math.random() < RANDOM_CLICK_CHANCE;

        if (shouldOpen) {
          console.log(`🧠 Opening viewer for ${postId} to get datetime.`);
          await anchor.click();
          await page.waitForSelector('time[datetime]', { timeout: 10000 });
          const dateTime = await page.$eval('time[datetime]', el => el.getAttribute('datetime'));
          if (!dateTime) console.warn(`⚠️ No datetime found for post ${href}`);
          console.log(`📅 Post Date: ${dateTime}`);
          //updateQueue.push({ range: `${SHEET_NAME}!Q${row}`, values: [[dateTime]] });
          const delay = Math.floor(Math.random() * 3000) + 2000;
          await new Promise(resolve => setTimeout(resolve, delay));
          await page.keyboard.press('Escape');
          await new Promise(resolve => setTimeout(resolve, 1000 + Math.random() * 1000));
        } else {
          console.log(`🙈 Skipping viewer for ${postId} (datetime already present)`);
        }
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

  const profileKeys = Object.keys(profileMap);
  const totalProfiles = profileKeys.length;
  let processedProfiles = 0;

  console.log(`🔢 Total profiles to process: ${totalProfiles}`);

  let profilesSinceUpdate = 0;
  let batchSize = Math.floor(Math.random() * 11) + 10; // 10-20

  for (const profile of profileKeys) {
    processedProfiles++;
    console.log(`➡️ Processing profile ${processedProfiles}/${totalProfiles}: ${profile}`);

    const start = Date.now();
    await scrollAndScrape(page, profile, profileMap[profile], updateQueue);
    const end = Date.now();
    const pause = 3000 + Math.random() * 5000;
    console.log(`⏱️ Finished profile in ${(end - start) / 1000}s`);
    console.log(`⏸ Pausing ${(pause / 1000).toFixed(1)}s before next profile...`);
    await new Promise(resolve => setTimeout(resolve, pause));

    profilesSinceUpdate++;
    if (profilesSinceUpdate >= batchSize) {
      console.log(`🚀 Updating sheet after ${profilesSinceUpdate} profiles...`);
      await updateSheetBatch(updateQueue);
      updateQueue.length = 0; // Clear the queue
      profilesSinceUpdate = 0;
      batchSize = Math.floor(Math.random() * 11) + 10; // New random batch size
    }
  }

  // 🌐 Wander to explore page between profiles
  if (Math.random() < 0.7) {
    console.log('🧭 Wandering through explore page...');
    await page.goto('https://www.instagram.com/explore/', { waitUntil: 'domcontentloaded' });
    await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * 2000));
  }

  // Final batch update for any remaining updates
  if (updateQueue.length > 0) {
    console.log(`🚀 Final sheet update for remaining profiles...`);
    await updateSheetBatch(updateQueue);
  }

  await browser.close();
  console.log('✅ All profiles processed.');
}

runScraper().catch(console.error);

async function loginAndSaveCookies() {
  const browser = await launchBrowser();
  const page = await browser.newPage();
  await page.goto('https://www.instagram.com/accounts/login/', {
    waitUntil: 'networkidle2',
    timeout: 60000,
  });
  console.log('🔐 Please log in manually in the browser.');
  console.log('✅ Once you are fully logged in and see your feed, press Enter in the terminal.');
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.question('', async () => {
    const cookies = await page.cookies();
    fs.writeFileSync(COOKIES_PATH, JSON.stringify(cookies, null, 2));
    console.log('✅ Cookies saved to cookies.json');
    await browser.close();
    rl.close();
    process.exit();
  });
}

//loginAndSaveCookies();
