// Instagram Reels Scraper - Merged: Add New + Update ALL Views for All Posts
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const { google } = require('googleapis');
const readline = require('readline');
puppeteer.use(StealthPlugin());

const COOKIES_PATH = './cookies.json';
const SHEET_ID = '19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4';
const SHEET_NAME = 'Sheet1';
const SCROLL_WAIT_TIME = 4000;
const MAX_SCROLLS = 15;
const RANDOM_CLICK_CHANCE = 0.08;

const BRAND_TAGS = [
  '@In Print We Trust', '@in print we trust', '@InPrintWeTrust', '@inprintwetrust',
  '@inprintwetrust.co', '@InPrintWeTrust.co', '#InPrintWeTrust', '#inprintwetrust',
  '#IPWT', '#ipwt'
];

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

async function fetchIGReelsProfilesFromU() {
  const sheets = await initSheets();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `Sheet1!U2:U`,
  });
  return (res.data.values || [])
    .map(r => r[0])
    .filter(link =>
      typeof link === 'string' &&
      link.includes('instagram.com') &&
      link.includes('/reels')
    )
    .map(link => link.trim().replace(/\/$/, ''));
}

async function getExistingPostLinks() {
  const sheets = await initSheets();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'Sheet1!N2:N',
  });
  const links = (res.data.values || []).map(r => r[0]?.split('?')[0]?.replace(/\/$/, ''));
  return new Set(links.filter(Boolean));
}

async function findNextEmptyRowM() {
  const sheets = await initSheets();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'Sheet1!M2:M',
  });
  return (res.data.values ? res.data.values.length : 0) + 2;
}

// Helper to append a new row with datetime and return row index
async function appendRowAndReturnIndex(profileUrl, postUrl, dateTime) {
  const sheets = await initSheets();
  const nextRow = await findNextEmptyRowM();
  // Columns: M (profile), N (post), Q (datetime)
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `Sheet1!M${nextRow}:Q${nextRow}`,
    valueInputOption: "RAW",
    resource: { values: [[profileUrl, postUrl, '', '', dateTime]] }
  });
  return nextRow;
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
    const profileLink = row[0];
    const postLinkRaw = row[1];
    const hasDatetime = !!row[4]; // Column Q (index 4)
    if (!profileLink || !postLinkRaw) return;
    if (!profileLink.includes('instagram.com')) {
      // skip invalid profile
      return;
    }
    const postLink = postLinkRaw.split('?')[0].replace(/\/$/, '');
    if (!profileMap[profileLink]) profileMap[profileLink] = [];
    profileMap[profileLink].push({ postLink, rowIndex: index + 2, needsDate: !hasDatetime });
  });

  return profileMap;
}

async function hoverElement(page, elementHandle) {
  try {
    if (!elementHandle) return;
    const box = await elementHandle.boundingBox();
    if (box) {
      const x = box.x + box.width / 2 + (Math.random() * 10 - 5);
      const y = box.y + box.height / 2 + (Math.random() * 10 - 5);
      await page.mouse.move(x, y);
      await new Promise(resolve => setTimeout(resolve, 300 + Math.random() * 700));
    }
  } catch (err) {
    console.warn('⚠️ Failed to move mouse:', err.message);
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

  posts.forEach(({ postLink, rowIndex, needsDate }) => {
    const postId = postLink.split('/reel/')[1]?.replaceAll('/', '').split('?')[0];
    if (postId) {
      postIdToRow[postId] = rowIndex;
      postIdTargets.add(postId);
      postMeta[postId] = { needsDate };
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
        if (!viewsRaw) console.warn(`⚠️ No views found for post ${href}`);
        console.log(`👁️ View Count: ${views}`);

        const row = postIdToRow[postId];
        updateQueue.push({ range: `${SHEET_NAME}!O${row}`, values: [[views]] });

        const shouldOpen = postMeta[postId].needsDate || Math.random() < RANDOM_CLICK_CHANCE;

        if (shouldOpen) {
          console.log(`🧠 Opening viewer for ${postId} to get datetime.`);
          await anchor.click();
          await page.waitForSelector('time[datetime]', { timeout: 10000 });
          const dateTime = await page.$eval('time[datetime]', el => el.getAttribute('datetime'));
          if (!dateTime) console.warn(`⚠️ No datetime found for post ${href}`);
          console.log(`📅 Post Date: ${dateTime}`);
          updateQueue.push({ range: `${SHEET_NAME}!Q${row}`, values: [[dateTime]] });
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

// =========================
// Main Merged Scraper Flow
// =========================
async function runIGBrandTagScraper() {
  const browser = await launchBrowser();
  const page = await browser.newPage();
  await loadCookies(page);

  // 1. ADD NEW POSTS from profile links in U
  const igReelsProfiles = await fetchIGReelsProfilesFromU();
  const existingPostLinks = await getExistingPostLinks();

  let profileIndex = 1;
  for (const profileUrl of igReelsProfiles) {
    console.log(`\n=========== [${profileIndex}/${igReelsProfiles.length}] Processing IG Profile: ${profileUrl} ===========`);
    profileIndex++;

    const isInPrintWeTrust = /instagram\.com\/inprintwetrust.co\/reels/i.test(profileUrl);

    await page.goto(profileUrl, { timeout: 60000, waitUntil: 'domcontentloaded' });
    await new Promise(resolve => setTimeout(resolve, 3000 + Math.random() * 2000));
    for (let i = 0; i < 2 + Math.floor(Math.random()*3); i++) {
      try {
  await page.mouse.move(100+Math.random()*500, 200+Math.random()*300, { steps: 3 + Math.floor(Math.random()*5) });
} catch (err) {
  console.warn("⚠️ Simulated mouse move failed:", err.message);
}

      await page.mouse.wheel({ deltaY: 500 + Math.random()*300 });
      await new Promise(resolve => setTimeout(resolve, 1200 + Math.random()*1100));
      console.log(`   ↕️ Simulated grid scroll (${i + 1})`);
    }

    const anchors = await page.$$('a[href*="/reel/"]');
    if (!anchors.length) continue;
    await hoverElement(page, anchors[0]);
    try {
  await anchors[0].click();
} catch (err) {
  console.warn("⚠️ Failed to click anchor:", err.message);
}

    await new Promise(resolve => setTimeout(resolve, 3500 + Math.random()*1500));
    console.log(`🎬 Opened first reel to enter viewer.`);

    let taggedPosts = [];
    let consecutiveInvalid = 0;
    let seenLinks = new Set();

    // Main viewer navigation loop
    while (consecutiveInvalid < 8) {
      let curUrl = page.url().split('?')[0].replace(/\/$/, '');
      console.log(`      → Viewing reel: ${curUrl}`);
      if (seenLinks.has(curUrl)) {
        consecutiveInvalid++;
        console.log(`        ⛔️ No match (${consecutiveInvalid} consecutive).`);
      } else {
        seenLinks.add(curUrl);
        let desc = '';
try {
  await page.waitForSelector('h1._ap3a', { timeout: 10000 }); // waits up to 10 seconds
  desc = await page.$eval('h1._ap3a', el => el.innerText);
} catch {
  try {
    await page.waitForSelector('div.C4VMK > span', { timeout: 5000 });
    desc = await page.$eval('div.C4VMK > span', el => el.innerText);
  } catch {
    try {
      await page.waitForSelector('span[role="link"]', { timeout: 3000 });
      desc = await page.$eval('span[role="link"]', el => el.innerText);
    } catch {}
  }
}
        let shouldCollect = false;
        if (isInPrintWeTrust) {
          shouldCollect = !existingPostLinks.has(curUrl) && !taggedPosts.some(p => p.post === curUrl);
        } else {
          const isTagged = BRAND_TAGS.some(tag => desc.includes(tag));
          shouldCollect = isTagged && !existingPostLinks.has(curUrl) && !taggedPosts.some(p => p.post === curUrl);
        }
        if (shouldCollect) {
          let dateTime = '';
          try {
            dateTime = await page.$eval('time[datetime]', el => el.getAttribute('datetime'));
            console.log(`📅 Post Date: ${dateTime}`);
          } catch {
            console.warn(`⚠️ No datetime found for ${curUrl}`);
          }
          const newRowIdx = await appendRowAndReturnIndex(profileUrl, curUrl, dateTime);
          taggedPosts.push({ profile: profileUrl, post: curUrl, row: newRowIdx });
          consecutiveInvalid = 0;
          console.log(`[COLLECTED] ${curUrl} - "${desc.slice(0,60)}..."`);
          if (isInPrintWeTrust) {
            console.log(`        ⭐ All posts collected for @inprintwetrust.`);
          } else {
            console.log(`        ✅ Tag found. Resetting consecutiveInvalid.`);
          }
        } else {
          consecutiveInvalid++;
        }
      }
      // Human random delays/actions
      if (Math.random() < 0.2) {
  try {
    await page.mouse.move(200+Math.random()*400, 200+Math.random()*300, { steps: 7 + Math.floor(Math.random()*7) });
  } catch (err) {
    console.warn("⚠️ Random mouse move in viewer failed:", err.message);
  }
  await new Promise(resolve => setTimeout(resolve, 800 + Math.random()*800));
}

      if (Math.random() < 0.25) {
        await page.keyboard.press('Space');
        await new Promise(resolve => setTimeout(resolve, 300 + Math.random()*800));
        await page.keyboard.press('Space');
      }
      const delayBeforeNext = 700 + Math.random() * 2000;
      console.log(`        ⏳ Waiting ${Math.round(delayBeforeNext)}ms before moving to next reel...`);
      await new Promise(resolve => setTimeout(resolve, delayBeforeNext));
      const nextButtons = await page.$$('button._abl-');
      let nextButton = null;
      for (const btn of nextButtons) {
        const svg = await btn.$('svg[aria-label="Next"]');
        if (svg) { nextButton = btn; break; }
        const title = await btn.$eval('svg > title', t => t.textContent).catch(() => null);
        if (title && title.trim() === 'Next') { nextButton = btn; break; }
      }
      if (nextButton) {
        await hoverElement(page, nextButton);
        try {
  await nextButton.click();
  console.log("        🖱️ Clicked the Next button.");
  await new Promise(resolve => setTimeout(resolve, 2200 + Math.random()*1600));
} catch (err) {
  console.warn("⚠️ Failed to click Next button:", err.message);
  break;
}

      } else {
        console.log("        ⚠️ Next button not found; exiting viewer loop.");
        break;
      }
    }
    await page.keyboard.press('Escape');
    await new Promise(resolve => setTimeout(resolve, 2000 + Math.random()*1700));
    console.log(`   🚪 Exited reel viewer. Found ${taggedPosts.length} new tagged post(s).`);
    if (taggedPosts.length === 0) console.log(`   🟡 No new tagged posts to add.`);
    // Random wander is OPTIONAL; you may enable it if needed for human-like behavior.
    // await new Promise(resolve => setTimeout(resolve, 3000 + Math.random()*3000));
  }

  // 2. UPDATE ALL VIEWS AND DATETIMES FOR ALL POSTS IN SHEET
  const profileMap = await fetchSheetData();
  const updateQueue = [];

  for (const profile in profileMap) {
    const start = Date.now();
    await scrollAndScrape(page, profile, profileMap[profile], updateQueue);
    const end = Date.now();
    const pause = 3000 + Math.random() * 5000;
    console.log(`⏱️ Finished profile in ${(end - start) / 1000}s`);
    console.log(`⏸ Pausing ${(pause / 1000).toFixed(1)}s before next profile...`);
    await new Promise(resolve => setTimeout(resolve, pause));
  }
  await updateSheetBatch(updateQueue);
  await browser.close();
  console.log('✅ All IG Reels profiles processed.');
}

runIGBrandTagScraper().catch(console.error);

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
