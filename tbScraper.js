const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { google } = require('googleapis');
const fs = require('fs');
const axios = require('axios');
const fetch = require('node-fetch');

puppeteer.use(StealthPlugin());

const SHEET_ID = "19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4";

let updateQueue = []; // Global queue for batch updates

/** Convert TikTok post ID to Unix timestamp */
function convertPostIdToUnix(postId) {
    if (!postId) return null;
    const binaryId = BigInt(postId).toString(2).padStart(64, '0');
    const epoch = parseInt(binaryId.substring(0, 32), 2);
    postDate = new Date(epoch*1000);
    return postDate.toISOString();
}

const proxyChain = require('proxy-chain');

const proxyList = [
    'http://spynfny9yy:4Ceet67~xzzDbH1spC@gb.decodo.com:30000'
];

function getPreviousRunUsage() {
    try {
        const log = JSON.parse(fs.readFileSync('run_log.json', 'utf-8'));
        return log.latestUsage || 0.81;
    } catch {
        return 0.81; // fallback default
    }
}

function getDynamicProxyChance() {
    const base = Math.random();
    const timeFactor = new Date().getMinutes() / 59;
    const chaos = Math.sin(Date.now() % 3600);
    let chance = (base * 0.4 + timeFactor * 0.3 + Math.abs(chaos) * 0.3);
    return Math.min(chance, 0.5);
}

// function shouldUseProxyForProfile(profileName, prioritizedProfiles) {
//     if (!prioritizedProfiles.has(profileName)) return false;
//     const probability = getDynamicProxyChance();
//     const roll = Math.random();
//     return roll < probability;
// }

async function initBrowser(useProxy = false) {
    let extensionPath = "C:\\Users\\edwar\\Downloads\\TikTok-Captcha-Solver-Chrome-Web-Store";
    const secondaryPath = "C:\\Users\\edwardjohngarrido\\Desktop\\Scraper\\TikTok-Captcha-Solver-Chrome-Web-Store";
    if (!fs.existsSync(extensionPath) && fs.existsSync(secondaryPath)) {
        console.warn("⚠️ Default extension path not found. Using secondary extension path.");
        extensionPath = secondaryPath;
    }

    const traffic = await fetchSmartproxyTraffic();
    const remaining = traffic.limit - traffic.used;
    const estimatedRunUsage = getPreviousRunUsage();
    const randomProxy = proxyList[Math.floor(Math.random() * proxyList.length)];

    // USE PROXY if flag is true, AND you have quota remaining
    if (useProxy && remaining > estimatedRunUsage) {
        console.log(`✅ Proxy allowed for this run`);
    } else if (useProxy && remaining <= estimatedRunUsage) {
        console.log(`💸 Would've used proxy but you're outta budget. Using static IP instead.`);
        useProxy = false;
    }

    let args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-infobars",
        "--disable-background-networking",
        "--disable-gpu",
        '--mute-audio',
        "--window-size=1200,800",
        "--disable-web-security",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        `--user-agent=${getRandomUserAgent()}`,
        `--disable-extensions-except=${extensionPath}`,
        `--load-extension=${extensionPath}`,
    ];

    if (useProxy) {
        const newProxyUrl = await proxyChain.anonymizeProxy(randomProxy);
        args.push(`--proxy-server=${newProxyUrl}`);
        console.log(`🔀 Selected Proxy: ${randomProxy}`);
    }

    return await puppeteer.launch({
        headless: true,
        args,
        ignoreDefaultArgs: ["--disable-extensions"],
        executablePath: puppeteer.executablePath(),
        protocolTimeout: 300000,
        userDataDir: `D:/puppeteer_profiles/bulk_run_${Date.now()}_${Math.floor(Math.random()*100000)}`
    });
}

// Function to generate random user agents
function getRandomUserAgent() {
    const userAgents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    ];
    return userAgents[Math.floor(Math.random() * userAgents.length)];
}

function splitArrayIntoChunks(array, numChunks) {
    const result = [];
    const chunkSize = Math.ceil(array.length / numChunks);
    for (let i = 0; i < numChunks; i++) {
        result.push(array.slice(i * chunkSize, (i + 1) * chunkSize));
    }
    return result;
}


/** Initialize Google Sheets API */
async function initSheets() {
    const auth = new google.auth.GoogleAuth({
        keyFile: 'credentials.json',
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    return google.sheets({ version: 'v4', auth: await auth.getClient() });
}

function cleanUpTempProfile(browser) {
    if (browser && browser.process && browser._userDataDir) {
        const fs = require('fs');
        try {
            fs.rmSync(browser._userDataDir, { recursive: true, force: true });
            console.log(`🧹 Deleted temp profile: ${browser._userDataDir}`);
        } catch (err) {
            console.warn(`⚠️ Failed to delete temp profile: ${err.message}`);
        }
    }
}

async function isVerificationModalPresent(page) {
    // CSS-based detection for "Verifying..." modal or spinner (case-insensitive)
    const text = await page.evaluate(() => document.body.innerText);
    return text.toLowerCase().includes("verifying");
}
async function isUnableToVerify(page) {
    // CSS-based detection for "Unable to verify. Please try again." (case-insensitive)
    const text = await page.evaluate(() => document.body.innerText);
    return text.toLowerCase().includes("unable to verify");
}


function convertPostIdToDate(postId) {
    if (!postId) {
        console.log("⚠️ No post ID provided to convertPostIdToDate");
        return null;
    }

    try {
        const binaryId = BigInt(postId).toString(2).padStart(64, '0');
        const epoch = parseInt(binaryId.substring(0, 32), 2) * 1000; // Convert to milliseconds

        const postDate = new Date(epoch);
        console.log(`📅 Converted Post ID ${postId} to Date: ${postDate.toUTCString()}`);
        return postDate; // Return Date object
    } catch (error) {
        console.log(`❌ Error converting post ID: ${postId}`, error);
        return null;
    }
}

function normalizeViews(viewStr) {
    if (!viewStr) return 0;
    viewStr = ("" + viewStr).trim().toUpperCase();
    if (viewStr.endsWith('K')) return Math.round(parseFloat(viewStr) * 1000);
    if (viewStr.endsWith('M')) return Math.round(parseFloat(viewStr) * 1000000);
    if (viewStr.endsWith('B')) return Math.round(parseFloat(viewStr) * 1000000000);
    return parseInt(viewStr.replace(/,/g, '')) || 0;
}
function prettyDate(isoDateStr) {
  if (!isoDateStr) return '';
  const d = new Date(isoDateStr);
  if (isNaN(d)) return '';
  // e.g., "July 1, 2025"
  return d.toLocaleString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC'
  });
}

async function randomDelay(min, max) {
    const delay = Math.floor(Math.random() * (max - min + 1)) + min;
    await new Promise(resolve => setTimeout(resolve, delay));
}

async function scrollPage(page, profileDateRange, existingPosts) {
    let lastHeight = await page.evaluate(() => document.body.scrollHeight);
    let stopScrolling = false;
    let allLoadedPosts = new Set();
    let allLoadedPostTimestamps = new Set();
    let emptyScrollCount = 0;
    let scrollCount = 0;
    const maxScrolls = 10;

    console.log(`🕒 Profile Date Range: Min=${profileDateRange.minDate.toUTCString()}, Max=${profileDateRange.maxDate.toUTCString()}`);

    function extractPostId(url) {
        const match = url.match(/\/(video|photo)\/(\d+)/);
        return match ? match[2] : null;
    }

    let tooOldCount = 0;
const cutoffDate = new Date();
cutoffDate.setMonth(cutoffDate.getMonth() - 2);
console.log(`🧹 Skipping posts older than: ${cutoffDate.toUTCString()}`);

while (!stopScrolling || (allLoadedPosts.size === 0 && scrollCount < maxScrolls)) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await randomDelay(3000, 12000);

    const postLinks = await page.$$eval('a[href*="/video/"], a[href*="/photo/"]', posts => posts.map(post => post.href));
    console.log("🧩 Visible post links on profile grid:");
postLinks.forEach(link => {
  const cleaned = link.split('?')[0];
  const postId = cleaned.match(/\/(video|photo)\/(\d+)/)?.[2];
  const postDate = convertPostIdToDate(postId);
  console.log(`🔗 ${cleaned} | ID: ${postId} | Date: ${postDate?.toISOString?.() ?? 'invalid'}`);
});

    let newPostsFound = false;
    let allOutOfRange = true;

    let tooOldCount = 0;
    const cutoffDate = new Date();
    cutoffDate.setMonth(cutoffDate.getMonth() - 2);

for (const link of postLinks) {
    const cleanedLink = link.split('?')[0];
    const postId = extractPostId(cleanedLink);
    
    if (!postId) continue;

    const postDate = convertPostIdToDate(postId);
    if (!postDate || isNaN(postDate.getTime())) continue;

    if (postDate < cutoffDate) {
        tooOldCount++;
        console.log(`⏳ Too old: ${cleanedLink} (${postDate.toISOString()})`);
        if (tooOldCount >= 3) {
            console.log("🛑 Stopping scroll — 3 consecutive posts were too old.");
            stopScrolling = true;
            break;
        }
    } else {
        tooOldCount = 0;
        allLoadedPosts.add(cleanedLink);
        allLoadedPostTimestamps.add(postDate);
    }
}


    if (stopScrolling) break;

    const newHeight = await page.evaluate(() => document.body.scrollHeight);

    if (allOutOfRange && allLoadedPosts.size > 0) {
        console.log("❌ All posts are out of date range. Stopping scroll.");
        stopScrolling = true;
        break;
    }

    if (allLoadedPostTimestamps.size > 0 && (!newPostsFound || newHeight === lastHeight)) {
        emptyScrollCount++;

        const minTimestamp = new Date(Math.min(...Array.from(allLoadedPostTimestamps).map(d => d.getTime())));
        if (minTimestamp <= profileDateRange.minDate) {
            console.log('✅ All dataset posts within date range found. Stopping scroll.');
            stopScrolling = true;
            break;
        } else if (emptyScrollCount >= 5) {
            console.log("🔄 Detected scrolling stuck. Resetting scroll position...");
            await page.evaluate(() => window.scrollTo(0, 0));
            await randomDelay(2000, 5000);
            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
            await randomDelay(2000, 5000);
            emptyScrollCount = 0;
        } else {
            console.log('🔄 More posts needed within date range. Continuing scroll...');
        }
    } else {
        emptyScrollCount = 0;
    }

    lastHeight = newHeight;
    scrollCount++;
}


    console.log('⌛ Final wait for posts to stabilize...');
    await randomDelay(5000, 10000);

    return allLoadedPosts;
}

async function scrapeProfile(page, profileUrl, profileDateRange, existingPosts, lastKnownLink, isInprint, sheets, isHighPriority, isLowPriority) {
  const BRAND_TAGS = [
    '@In Print We Trust', '@in print we trust', '@InPrintWeTrust', '@inprintwetrust',
    '@inprintwetrust.co', '@InPrintWeTrust.co', '#InPrintWeTrust', '#inprintwetrust',
    '#IPWT', '#ipwt'
  ];

let profileLoaded = false;
    let profileRetries = 0;
    const maxProfileRetries = 3;

    while (!profileLoaded && profileRetries < maxProfileRetries) {
        try {
            if (page.isClosed()) throw new Error("Page already closed");
            await page.goto(profileUrl, { waitUntil: 'domcontentloaded'});
            await new Promise(res => setTimeout(res, 10000));
        await dismissInterestModal(page);
        await randomDelay(3000, 6000);
        const isDeletedProfile = await page.$('p.css-1y4x9xk-PTitle');
        const fallbackText = await page.$eval('body', el => el.innerText).catch(() => '');
        if (isDeletedProfile || fallbackText.includes("Couldn't find this account")) {
            console.log("❌ Detected deleted account. Skipping...");
            return;
        }
        profileLoaded = true; // success!
    } catch (err) {
            profileRetries++;
            if (err.message && err.message.includes('closed')) {
                console.log(`⚠️ Page/browser closed unexpectedly. Breaking retry loop for: ${profileUrl}`);
                break;
            }
            console.log(`❌ Failed to load profile ${profileUrl} (attempt ${profileRetries}): ${err.message}`);
            if (profileRetries < maxProfileRetries) {
                await page.reload({ waitUntil: 'domcontentloaded' }).catch(() => {});
                await new Promise(res => setTimeout(res, 5000));
            }
        }
    }
    if (!profileLoaded) {
        console.log(`💀 Giving up on profile ${profileUrl} after ${maxProfileRetries} tries.`);
        return;
    }

    // PATCH: captcha check right here!
if (await isVerificationModalPresent(page) || await isUnableToVerify(page)) {
    console.log("🛑 Verification/captcha detected during grid scraping (post profile load). Closing browser/session and retrying later.");
    try { await page.close(); } catch(e) {}
    if (page.browser()) {
        try { await page.browser().close(); } catch(e) {}
    }
    if (typeof cleanUpTempProfile === "function") cleanUpTempProfile(page.browser());
    return;
}

// Wait for the profile grid container to appear before trying to get thumbnails
const gridSelector = 'main [data-e2e="user-post-list"]';
let gridLoaded = false;
let hadInitialGrid = false;  // <--- NEW FLAG

try {
    await page.waitForSelector(gridSelector, {timeout: 15000});
    gridLoaded = true;
} catch (e) {
    console.warn("⚠️ Profile grid did not load (selector timeout). Checking for captcha or slow load...");
    if (await isVerificationModalPresent(page) || await isUnableToVerify(page)) {
        console.log("🛑 Verification/captcha detected after grid selector timeout.");
        try { await page.close(); } catch(e) {}
        if (page.browser()) { try { await page.browser().close(); } catch(e) {} }
        if (typeof cleanUpTempProfile === "function") cleanUpTempProfile(page.browser());
        return;
    }
    await randomDelay(8000, 12000);
}

// Now, try for thumbnails
let thumbnails = await page.$$('a[href*="/video/"], a[href*="/photo/"]');
let retries = 0;
if (thumbnails.length > 0) {
    hadInitialGrid = true;
}
while (thumbnails.length === 0 && retries < 5) {
    retries++;
    if (await isVerificationModalPresent(page) || await isUnableToVerify(page)) {
        console.log("🛑 Verification or captcha error detected. Halting further retries and waiting...");
        await randomDelay(30000, 120000);
        return;
    }
    console.warn(`🔁 Retry ${retries}/5 — No thumbnails found on ${profileUrl}`);
    await page.reload({ waitUntil: 'domcontentloaded' });
    await randomDelay(5000, 10000);
    thumbnails = await page.$$('a[href*="/video/"], a[href*="/photo/"]');
    if (thumbnails.length > 0) {
        hadInitialGrid = true;
    }
}

// FINAL check after all retries
if (thumbnails.length === 0) {
    if (await isVerificationModalPresent(page) || await isUnableToVerify(page)) {
        console.log("🛑 Verification/captcha detected after 0 thumbnails.");
        try { await page.close(); } catch(e) {}
        if (page.browser()) { try { await page.browser().close(); } catch(e) {} }
        if (typeof cleanUpTempProfile === "function") cleanUpTempProfile(page.browser());
        return;
    }
    // Only treat as empty if you NEVER had any thumbnails
    if (!hadInitialGrid) {
        const pageText = await page.evaluate(() => document.body.innerText);
        if (pageText.toLowerCase().includes("no content") || pageText.toLowerCase().includes("no posts yet")) {
            console.log("🛑 No posts or content on this profile. Skipping.");
            return;
        }
        console.warn("⚠️ Loaded 0 thumbnails after scrolling, but no captcha detected. Skipping.");
        return { links: [], fromCache: false, skipLinkScrape: true };
    } else {
        // If you had posts before, be extremely conservative: retry up to 12 times (and NEVER skip unless 12x fails)
        let retryAttempts = 0;
        let postGridThumbs = [];
        const maxSafeRetries = 12;
        while (retryAttempts < maxSafeRetries && postGridThumbs.length === 0) {
            retryAttempts++;
            console.warn(`⚠️ Had grid before; now no thumbs. Reloading grid for views scrape... [try ${retryAttempts}/${maxSafeRetries}]`);
            await page.reload({ waitUntil: 'domcontentloaded' });
            await randomDelay(4000, 8000);
            postGridThumbs = await page.$$('a[href*="/video/"], a[href*="/photo/"]');
            if (postGridThumbs.length > 0) break;
        }
        if (postGridThumbs.length === 0) {
            console.warn("❗ FINAL WARNING: Had grid/thumbnails earlier, but TikTok hid them after 12 reloads. Skipping update for this profile. If you see this often, TikTok is rate-limiting or blocking grid access!");
            return;
        }
        thumbnails = postGridThumbs;
    }
}

// === PATCH: extract all post data from the grid ===
const gridPosts = await page.$$eval('a[href*="/video/"], a[href*="/photo/"]', links =>
  links.map(a => {
    const href = a.href.split('?')[0];
    const viewEl = a.querySelector('strong[data-e2e="video-views"]');
    const views = viewEl?.innerText || null;
    const match = href.match(/\/(video|photo)\/(\d+)/);
    const postId = match ? match[2] : null;
    return { href, views, postId };
  })
);
// Add post date for each
gridPosts.forEach(post => {
  post.date = post.postId ? convertPostIdToDate(post.postId) : null;
});

// --- Set viewer scraping depth based on profile priority ---
let maxViewerDepth = gridPosts.length; // default: all
if (isHighPriority) {
  maxViewerDepth = gridPosts.length <= 10 ? gridPosts.length : Math.ceil(gridPosts.length / 2);
} else if (isLowPriority) {
  maxViewerDepth = gridPosts.length <= 10 ? gridPosts.length : Math.ceil(gridPosts.length / 4);
}
console.log(`🔎 Will scrape up to ${maxViewerDepth} posts in viewer for this profile.`);


if (isInprint) {
  // For InPrintWeTrust and IPWTStreetalk: Just use grid data, skip viewer logic.
  // (Optionally filter by date range here if needed)
  for (const post of gridPosts) {
    if (!post.postId || !post.views) continue;
    // Example: Only include posts from last 2 months
    const cutoff = new Date(); cutoff.setMonth(cutoff.getMonth() - 2);
    if (!post.date || post.date < cutoff) continue;
    // Write to Google Sheets: adjust this for your schema
    // If you want to batch update, prepare `updateQueue` here
    // E.g., find row number, then push to updateQueue
    // See your existing "Scrape views from grid" code for updateQueue usage

    // This is just for demonstration:
    const rowNumber = existingPosts[post.postId];
    if (rowNumber) {
      const postISO = convertPostIdToUnix(post.postId);
      const pretty = prettyDate(postISO);
      const normalizedViews = normalizeViews(post.views);
      updateQueue.push({
        range: `Sheet1!D${rowNumber}:F${rowNumber}`,
        values: [[
          normalizedViews, // D (views)
          pretty,          // E
          postISO          // F
        ]]
      });
      console.log(`✅ [INPRINT] Updating: views=${normalizedViews}, G=${pretty}, H=${postISO} for ${post.href}`);
    }
  }
  await updateGoogleSheets();
  // End this profile's processing immediately
  return;
}

// Build a list of post links to ensure we always get the current grid elements
const postLinks = await page.$$eval('a[href*="/video/"], a[href*="/photo/"]', links =>
  links.map(a => a.href.split('?')[0])
);

// Try each post link individually
let viewerOpened = false;
for (const link of postLinks) {
  const thumb = await page.$(`a[href*="${link.split('/').pop()}"]`);
  if (!thumb) {
    console.log(`⚠️ Could not find thumbnail for ${link}. Skipping.`);
    continue;
  }
  await thumb.evaluate(node => node.scrollIntoView({behavior: "auto", block: "center"}));
  await randomDelay(500, 1200);

  let clickTries = 0;
  while (!viewerOpened && clickTries < 5) {
    await thumb.click();
    await randomDelay(2000, 6000);
    viewerOpened = await page.evaluate(() =>
      !!document.querySelector('[data-e2e="browse-video-feed"]') ||
      window.location.pathname.includes('/video/') ||
      window.location.pathname.includes('/photo/')
    );
    if (!viewerOpened) {
      console.log('Viewer not open yet. Retrying post click...');
      await randomDelay(1000, 2000);
    }
    clickTries++;
  }
  if (viewerOpened) {
    console.log(`✅ Viewer opened for post: ${link}`);
    break; // Stop after successfully opening the first modal!
  }
}
if (!viewerOpened) {
  console.warn(`⚠️ Could not open viewer for any post on profile. Skipping scraping.`);
  return;
}

// Now begin your scraping loop (while true, ArrowDown, etc)

if (!page.url().includes('/video/') && !page.url().includes('/photo/')) {
  console.warn("⚠️ Viewer still not opened after retry. Will continue but viewer may be stuck.");
}


  const cutoffDate = new Date();
  cutoffDate.setMonth(cutoffDate.getMonth() - 2);

  const seenLinks = new Set();
  let collectedLinks = [];
  let consecutiveExisting = 0;
  let viewerDepth = 0;

while (true) {
  await randomDelay(3000, 12000);
  await dismissInterestModal(page);

  const currentUrl = page.url().split('?')[0];
  const postIdMatch = currentUrl.match(/\/(video|photo)\/(\d+)/);
  const postId = postIdMatch ? postIdMatch[2] : null;

  if (!postId || seenLinks.has(postId)) {
  // Check if Down button exists (visible)
  const downBtnExists = await page.$('button[aria-label="Scroll down"]');
  if (!downBtnExists) {
    console.log("⬇️ Down arrow not present, breaking out of scrape loop.");
    break;
  }
  try { await page.keyboard.press('ArrowDown'); } catch {}
  continue;
}

  seenLinks.add(postId);

  // ✅ Instead of breaking immediately on lastKnownLink, count 5 consecutive matches
  if (lastKnownLink && lastKnownLink.includes(currentUrl)) {
    console.log(`⚠️ Post is already in last known links: ${currentUrl}`);
    consecutiveExisting++;
  } else {
    const postDate = convertPostIdToDate(postId);
    if (!postDate || isNaN(postDate.getTime())) continue;

    let isCollected = false;
    if (postId in existingPosts) {
      console.log(`⚠️ Post already logged: ${postId}`);
      consecutiveExisting++;
    } else if (postDate >= cutoffDate) {
      const desc = await page.$eval('div[data-e2e="browse-video-desc"]', el => el.innerText).catch(() => '');
      const isTagged = isInprint || BRAND_TAGS.some(tag => desc.includes(tag));
      if (isTagged) {
        console.log(`📥 Collected valid post: ${currentUrl}`);
        collectedLinks.push(currentUrl);
        isCollected = true;
        consecutiveExisting = 0;
      } else {
        console.log(`⏭️ Skipped (no tag match): ${currentUrl}`);
        consecutiveExisting++;
      }
    } else {
      console.log(`⏳ Post ${postId} is older than cutoff (${postDate.toISOString()}). Skipping.`);
      consecutiveExisting++;
    }
  }

  if (consecutiveExisting >= 8) {
    console.log("🛑 Stopping — 8 consecutive uncollectable or known posts.");
    break;
  }

  try { await page.keyboard.press('ArrowDown'); } catch { break; }
  viewerDepth++;
if (viewerDepth >= maxViewerDepth) {
  console.log(`🛑 Reached viewer depth limit (${maxViewerDepth}) for this profile.`);
  break;
}
}


  if (collectedLinks.length > 0) {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!C:C'
    });
    const existingValues = existing.data.values || [];
    const nextRow = existingValues.length + 1;
    const values = collectedLinks.map(link => [link]);

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `Sheet1!C${nextRow}`,
      valueInputOption: 'RAW',
      resource: { values }
    });

    console.log(`✅ Appended ${collectedLinks.length} links to Sheet1 starting at row ${nextRow}`);
  } else {
    console.log("ℹ️ No new links collected.");
  }

  existingPosts = await refreshExistingPosts();
  console.log(`🔁 Refreshed existingPosts (${Object.keys(existingPosts).length} total).`);

// 1. Gather brand-tagged new posts (collectedLinks already has the links)
let taggedNewPosts = [];
for (const link of collectedLinks) {
  const gridData = gridPosts.find(p => p.href === link);
  if (gridData && gridData.postId) {
    taggedNewPosts.push(gridData);
  }
}

// 2. Gather all posts already in the sheet (by postId), from gridPosts
let existingSheetPosts = [];
for (const post of gridPosts) {
  if (post.postId && existingPosts[post.postId]) {
    existingSheetPosts.push(post);
  }
}

// 3. De-duplicate in case new posts are already in the sheet
let allToUpdate = {};
[...taggedNewPosts, ...existingSheetPosts].forEach(post => {
  if (post && post.postId) allToUpdate[post.postId] = post;
});

// 4. Push updates for each post
for (const postId in allToUpdate) {
  const post = allToUpdate[postId];
  const rowNumber = existingPosts[postId];
  if (!rowNumber) continue;
  const postISO = convertPostIdToUnix(postId);
  const pretty = prettyDate(postISO);
  const normalizedViews = normalizeViews(post.views);

  updateQueue.push({
    range: `Sheet1!D${rowNumber}:F${rowNumber}`,
    values: [[
      normalizedViews, // D (views, integer)
      pretty,          // E
      postISO          // F
    ]]
  });
  console.log(`✅ Updating (no grid revisit): views=${normalizedViews}, G=${pretty}, H=${postISO} for ${post.href}`);
}

await updateGoogleSheets();
return; // Done with this profile

// // 1. Go back to the grid view
// await page.goto(profileUrl, { waitUntil: 'domcontentloaded' });
// await dismissInterestModal(page);
// await randomDelay(3000, 6000);

// // PATCH: captcha check right here!
// if (await isVerificationModalPresent(page) || await isUnableToVerify(page)) {
//     console.log("🛑 Verification/captcha detected after redirect to profile grid. Closing browser/session and retrying later.");
//     try { await page.close(); } catch(e) {}
//     if (page.browser()) {
//         try { await page.browser().close(); } catch(e) {}
//     }
//     if (typeof cleanUpTempProfile === "function") cleanUpTempProfile(page.browser());
//     return;
// }

// // 2. Scroll grid until all visible posts are older than 2 months
// const gridCutoffDate = new Date();
// gridCutoffDate.setMonth(gridCutoffDate.getMonth() - 2);

// let prevCount = 0;
// let currCount = 0;
// let scrollTries = 0;
// let keepScrolling = true;

// while (keepScrolling && scrollTries < 20) {
//   prevCount = currCount;
//   await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
//   await randomDelay(3000, 6000);

//   // Get all post links and their post IDs
//   const postInfo = await page.$$eval('a[href*="/video/"], a[href*="/photo/"]', els =>
//     els.map(a => {
//       const href = a.getAttribute('href');
//       const match = href ? href.match(/\/(video|photo)\/(\d+)/) : null;
//       return { href: href?.split('?')[0], postId: match?.[2] };
//     })
//   );
//   currCount = postInfo.length;

//   // Convert postId to date and check if any are within cutoff
//   let foundRecent = false;
//   for (const { postId } of postInfo) {
//     if (!postId) continue;
//     const postDate = convertPostIdToDate(postId); // ← your helper
//     if (postDate && postDate >= gridCutoffDate) {
//       foundRecent = true;
//       break;
//     }
//   }

//   // Stop scrolling if no recent posts left, or no new posts loaded
//   if (!foundRecent || currCount <= prevCount) keepScrolling = false;
//   scrollTries++;
// }

// await randomDelay(1200, 1800);

// console.log(`🧩 Finished grid scroll. Loaded ${currCount} thumbnails.`);

// // 3. Scrape views from grid
// const viewsData = await page.evaluate(() => {
//   const posts = Array.from(document.querySelectorAll('a[href*="/video/"], a[href*="/photo/"]'));
//   return posts.map(post => {
//     const href = post.getAttribute('href')?.split('?')[0];
//     const viewEl = post.querySelector('strong[data-e2e="video-views"]');
//     const views = viewEl?.innerText || null;
//     return { href, views };
//   });
// });

// const filteredViewsData = viewsData.filter(d => d.href && (d.href.includes('/video/') || d.href.includes('/photo/')));
// for (let { href, views } of filteredViewsData) {
//   const match = href.match(/\/(video|photo)\/(\d+)/);
//   const postId = match?.[2];
//   if (!postId || !views) continue;

//   const rowNumber = existingPosts[postId];
//   if (!rowNumber) continue;

//   // --- Get/derive dates ---
//   const postISO = convertPostIdToUnix(postId); // returns ISO string
//   const pretty = prettyDate(postISO);
//   const normalizedViews = normalizeViews(views);

//   // Write to: Col D = normalized views, Col G = pretty date, Col H = ISO
//   // (Col D = 4th col, G = 7th, H = 8th)
//   updateQueue.push({ range: `Sheet1!D${rowNumber}:H${rowNumber}`, values: [[
//     normalizedViews, // D (views, integer)
//     '', '',          // E, F (keep as-is)
//     pretty,          // G
//     postISO          // H
//   ]]});
//   console.log(`✅ Updating: views=${normalizedViews}, G=${pretty}, H=${postISO} for ${href}`);
// }

// await updateGoogleSheets();

}

async function refreshExistingPosts() {
    const sheets = await initSheets();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!A:D',
    });
  
    let result = {};
    response.data.values.forEach((row, index) => {
      const link = row[2];
      if (link) {
        const match = link.match(/\/(video|photo)\/(\d+)/);
        if (match) result[match[2]] = index + 1;
      }
    });
  
    return result;
  }
  

async function updateGoogleSheets() {
    if (updateQueue.length === 0) {
        console.log("⚠️ No updates to push to Google Sheets.");
        return;
    }

    console.log(`📌 Updating Google Sheets with ${updateQueue.length} entries...`);
    const sheets = await initSheets();

    try {
        const batchUpdateRequest = {
            spreadsheetId: SHEET_ID,
            valueInputOption: "RAW",
            data: updateQueue
        };
        
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SHEET_ID,
            resource: batchUpdateRequest,
        });

        console.log("✅ Google Sheets successfully updated.");
        updateQueue = []; // Clear queue after successful update
    } catch (error) {
        console.error("❌ Error updating Google Sheets:", error);
        
        // Retry failed updates
        console.log("🔄 Retrying failed updates...");
        await randomDelay(15000, 30000);

        try {
            await sheets.spreadsheets.values.batchUpdate({
                spreadsheetId: SHEET_ID,
                resource: batchUpdateRequest,
            });
            console.log("✅ Retry successful.");
            updateQueue = [];
        } catch (retryError) {
            console.error("❌ Retry failed. Some updates were not applied.", retryError);
        }
    }
}

async function fetchSmartproxyTraffic() {
    const credentials = JSON.parse(fs.readFileSync('credentials.json', 'utf-8'));
    const apiKey = credentials.smartproxy_api_key;

    const now = new Date();
    const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    const startDateStr = startOfMonth.toISOString().slice(0, 19).replace('T', ' ');
    const endDateStr = now.toISOString().slice(0, 19).replace('T', ' ');

    let usedBytes = 0;
    let trafficLimit = 8;

    try {
        // 1️⃣ Get usage from traffic endpoint
        const trafficUrl = `https://dashboard.smartproxy.com/subscription-api/v1/api/public/statistics/traffic?api_key=${apiKey}`;
        const trafficRes = await fetch(trafficUrl, {
            method: 'POST',
            headers: {
                accept: 'application/json',
                'content-type': 'application/json'
            },
            body: JSON.stringify({
                proxyType: 'residential_proxies',
                startDate: startDateStr,
                endDate: endDateStr,
                groupBy: 'target',
                limit: 500,
                page: 1,
                sortBy: 'grouping_key',
                sortOrder: 'asc'
            })
        });

        const trafficData = await trafficRes.json();
        if (Array.isArray(trafficData.data)) {
            for (const entry of trafficData.data) {
                usedBytes += entry.rx_tx_bytes || 0;
            }
        }

        // 2️⃣ Get limit and (string) usage from subscription endpoint
        const subRes = await fetch(`https://api.smartproxy.com/v2/subscriptions?api-key=${apiKey}`, {
            method: 'GET',
            headers: {
              accept: 'application/json'
            }
        });                  
        const subs = await subRes.json();
        //JSON Response from proxy api
        //console.log("📦 Subscription response:", JSON.stringify(subs, null, 2));

        let usedGB_bytes = +(usedBytes / (1024 ** 3)).toFixed(2); // from bytes endpoint
        let usedGB_sub = null;

        if (Array.isArray(subs) && subs.length > 0) {
            trafficLimit = parseFloat(subs[0].traffic_limit) || 8;
            usedGB_sub = parseFloat(subs[0].traffic); // subscription API gives string like "8.01"
        } else {
            console.warn("⚠️ Failed to retrieve traffic limit from subscription. Falling back to 8 GB.");
        }

        let usedGB = usedGB_bytes;
        if (!isNaN(usedGB_sub)) {
            usedGB = Math.max(usedGB_bytes, usedGB_sub);
        }

        console.log(`📊 Traffic used this month: ${usedGB} GB / ${trafficLimit} GB`);
        return {
            used: usedGB,
            limit: trafficLimit
        };
    } catch (err) {
        console.error("❌ Failed to fetch traffic data:", err.message);
        return { used: Infinity, limit: 0 }; // fallback
    }
}

async function getLastKnownLinks() {
    const sheets = await initSheets();
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!A:C',
    });

    const rows = response.data.values || [];
    const lastKnownLinks = {};

    for (let i = rows.length - 1; i >= 0; i--) {
        const row = rows[i];
        const profile = row[1];
        const postUrl = row[2];

        if (!profile || !postUrl) continue;

        const cleanUrl = postUrl.split('?')[0];
        const match = cleanUrl.match(/\/(video|photo)\/(\d+)/);
        if (!match) continue;

        if (!lastKnownLinks[profile]) {
            lastKnownLinks[profile] = [];
        }

        // Push only if not already included
        if (!lastKnownLinks[profile].includes(cleanUrl)) {
            lastKnownLinks[profile].push(cleanUrl);
        }

        // Stop once we have 5 recent links
        if (lastKnownLinks[profile].length >= 5) continue;
    }

    // Normalize profile keys
    const normalized = {};
    for (const [profile, links] of Object.entries(lastKnownLinks)) {
        normalized[profile.replace(/\/$/, '')] = links;
    }

    //console.log("🧾 Last known links loaded (top 5):", normalized);
    return normalized;
}

async function processProfilesChunk(
    profiles,
    sheets,
    prioritizedProfileLinks,
    lastKnownMap,
    botNumber,
    totalHigh,
    totalLow
) {
    let scrapedCount = 0;
    let batchCounter = 0;
    let batchThreshold = Math.floor(Math.random() * 3) + 4;
    let highProcessed = 0;
    let lowProcessed = 0;

    let curBrowser = null;
    let curPage = null;
    let useProxy = false;
    let traffic = null;
    let isHighPriorityBatch = false;

    for (let i = 0; i < profiles.length; i++) {
        const profileObj = profiles[i];
        const profileUrl = profileObj.link;
        const cleanProfile = profileUrl.trim().replace(/\/$/, '');
        const isHighPriority = profileObj.isHighPriority;
        const isLowPriority = profileObj.isLowPriority;
        const isInprint =
            cleanProfile.includes('@inprintwetrust') ||
            cleanProfile.includes('@ipwtstreetalk');
        const recentLinks = lastKnownMap[cleanProfile] || null;
        const existingPosts = await refreshExistingPosts();

        // Only check traffic and init browser at the start of a batch, or if no browser yet
        if (!curBrowser || batchCounter === 0) {
            if (curBrowser) {
                try { await curBrowser.close(); } catch (e) { console.warn('Failed to close browser:', e); }
                if (typeof cleanUpTempProfile === "function") cleanUpTempProfile(curBrowser);
            }
            // Figure out proxy for the batch: use high priority of first profile in batch
            isHighPriorityBatch = isHighPriority;
            useProxy = false;
            if (isHighPriorityBatch) {
                traffic = await fetchSmartproxyTraffic();
                const probability = getDynamicProxyChance();
                useProxy = Math.random() < probability;
                if (traffic && traffic.used >= 7) {
                    useProxy = false;
                    console.log("💸 Smartproxy usage is >= 7GB, skipping proxy use for this batch.");
                }
            }
            curBrowser = await initBrowser(useProxy);
            batchThreshold = Math.floor(Math.random() * 3) + 4;
            batchCounter = 0;
        }

        let scrapeAttempts = 0;
        let scrapeSuccess = false;

        while (!scrapeSuccess && scrapeAttempts < 3) {
            try {
                curPage = await curBrowser.newPage();
                await curPage.setViewport({ width: 1200, height: 800 });
                await curPage.setJavaScriptEnabled(true);

                await scrapeProfile(
                    curPage,
                    cleanProfile,
                    {},
                    existingPosts,
                    recentLinks,
                    isInprint,
                    sheets,
                    isHighPriority,
                    isLowPriority
                );

                scrapeSuccess = true;
            } catch (err) {
                scrapeAttempts++;
                console.error(`❌ Error scraping profile: ${cleanProfile} (attempt ${scrapeAttempts}): ${err && err.message ? err.message : err}`);
                // If we hit an error, close browser & force batch restart on this profile next loop
                try { if (curPage) await curPage.close(); } catch (e) {}
                try { if (curBrowser) await curBrowser.close(); } catch (e) {}
                if (curBrowser && typeof cleanUpTempProfile === "function") cleanUpTempProfile(curBrowser);
                curBrowser = null;
                batchCounter = 0;
                // retry with new browser
            } finally {
                try { if (curPage) await curPage.close(); } catch (e) {}
            }
        }

        if (!scrapeSuccess) {
            console.error(`💀 Giving up on profile ${cleanProfile} after 3 tries.`);
        }

        scrapedCount++;
        batchCounter++;

        // Progress counters
        if (isHighPriority) {
            highProcessed++;
            console.log(`[BOT${botNumber}] 🔵 (${highProcessed}/${profiles.length}) High-priority: ${cleanProfile}`);
        } else if (isLowPriority) {
            lowProcessed++;
            console.log(`[BOT${botNumber}] 🟠 (${lowProcessed}/${profiles.length}) Low-priority: ${cleanProfile}`);
        }

        // Batch-based browser refresh after X profiles, unless next is a retry from error
        if (batchCounter >= batchThreshold) {
            console.log('♻️ Restarting browser to refresh session...');
            try { if (curBrowser) await curBrowser.close(); } catch (e) {}
            if (curBrowser && typeof cleanUpTempProfile === "function") cleanUpTempProfile(curBrowser);
            curBrowser = null;
            batchCounter = 0;
        }
    }

    // Final cleanup
    try { if (curBrowser) await curBrowser.close(); } catch (e) {}
    if (curBrowser && typeof cleanUpTempProfile === "function") cleanUpTempProfile(curBrowser);
}

// async function processProfilesChunk(profiles, sheets, prioritizedProfiles, workerId, totalHigh, totalLow) {
//     const lastKnownMap = await getLastKnownLinks();

//     let scrapedCount = 0;
//     let batchThreshold = Math.floor(Math.random() * 3) + 4; // Random batch size 4–6
//     let batchCounter = 0;

//     let highProcessed = 0;
//     let lowProcessed = 0;

// let curBrowser = null;
// let curPage = null;

// let traffic = await fetchSmartproxyTraffic();

// for (const profileObj of profiles) {
//     const profileUrl = profileObj.link;
//     const cleanProfile = profileUrl.trim().replace(/\/$/, '');
//     const isHighPriority = profileObj.isHighPriority;
//     const isLowPriority = profileObj.isLowPriority;
//     const isInprint =
//         cleanProfile.includes('@inprintwetrust') ||
//         cleanProfile.includes('@ipwtstreetalk');
//     const recentLinks = lastKnownMap[cleanProfile] || null;
//     const existingPosts = await refreshExistingPosts();

//     let scrapeAttempts = 0;
//     let scrapeSuccess = false;

//     // 🛑 Always close the old browser before starting a new one!
//     if (curBrowser) {
//         try { await curBrowser.close(); } catch (e) { console.warn('Failed to close browser:', e); }
//         if (typeof cleanUpTempProfile === "function") cleanUpTempProfile(curBrowser);
//     }
// let useProxy = false;
// if (isHighPriority) {
//     // Always check Smartproxy usage before deciding
//     traffic = await fetchSmartproxyTraffic();
//     const probability = getDynamicProxyChance();
//     useProxy = Math.random() < probability;
//     if (traffic && traffic.used >= 7) {
//         useProxy = false;
//         console.log("💸 Smartproxy usage is >= 7GB, skipping proxy use for this profile.");
//     }
// }
// curBrowser = await initBrowser(useProxy);

//     curPage = await curBrowser.newPage();
//     await curPage.setViewport({ width: 1200, height: 800 });
//     await curPage.setJavaScriptEnabled(true);

//     try {
//         while (!scrapeSuccess && scrapeAttempts < 3) {
//             if (curPage && typeof curPage.close === "function") {
//                 await new Promise(res => setTimeout(res, 10000)); // 10 seconds
//                 try { await curPage.close(); } catch (e) {}
//             }
//             curPage = await curBrowser.newPage();
//             await curPage.setViewport({ width: 1200, height: 800 });
//             await curPage.setJavaScriptEnabled(true);

//             try {
//                 await scrapeProfile(curPage, cleanProfile, {}, existingPosts, recentLinks, isInprint, sheets, isHighPriority, isLowPriority);
//                 scrapeSuccess = true;
//             } catch (err) {
//                 scrapeAttempts++;
//                 console.error(`❌ Error scraping profile: ${cleanProfile} (attempt ${scrapeAttempts}): ${err && err.message ? err.message : err}`);
//                 await new Promise(res => setTimeout(res, Math.floor(Math.random() * 5000) + 7000)); // Wait 7-12s before retry
//             }
//         }
//         if (!scrapeSuccess) {
//             console.error(`💀 Giving up on profile ${cleanProfile} after 3 tries.`);
//         }
//     } finally {
//         try { if (curPage) await curPage.close(); } catch (e) {}
//         // Don't close curBrowser here—let the top of the next loop handle it!
//         // Only do final cleanup after the loop or on SIGINT/exit.
//     }

//     scrapedCount++;
//     batchCounter++;

//     // Progress counters
//     if (isHighPriority) {
//         highProcessed++;
//         console.log(`🔵 [${highProcessed}/${totalHigh}] Finished high-priority profile: ${cleanProfile}`);
//     } else if (isLowPriority) {
//         lowProcessed++;
//         console.log(`🟠 [${lowProcessed}/${totalLow}] Finished low-priority profile: ${cleanProfile}`);
//     }

//     // Batch-based browser refresh...
//     if (batchCounter >= batchThreshold) {
//         console.log('♻️ Restarting browser to refresh session...');
//         try { if (curPage) await curPage.close(); } catch (e) {}
//         try { if (curBrowser) await curBrowser.close(); } catch (e) {}
//         if (curBrowser && typeof cleanUpTempProfile === "function") cleanUpTempProfile(curBrowser);

//         curBrowser = await initBrowser(isHighPriority);
//         curPage = await curBrowser.newPage();
//         await curPage.setViewport({ width: 1200, height: 800 });
//         await curPage.setJavaScriptEnabled(true);
//         batchThreshold = Math.floor(Math.random() * 3) + 4;
//         batchCounter = 0;
//     }
// }

// // After all profiles, final cleanup:
// try { if (curPage) await curPage.close(); } catch (e) {}
// try { if (curBrowser) await curBrowser.close(); } catch (e) {}
// if (curBrowser && typeof cleanUpTempProfile === "function") cleanUpTempProfile(curBrowser);

//     console.log(`[BOT${workerId}] ✅ Finished processing all profiles.`);
// }

async function dismissInterestModal(page) {
  try {
    const modalVisible = await page.$('[data-e2e="login-modal"]');
    if (!modalVisible) return;

    const skipped = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const skipBtn = buttons.find(btn =>
        btn.textContent?.trim().toLowerCase() === 'skip' &&
        !btn.disabled
      );
      if (skipBtn) {
        skipBtn.click();
        return true;
      }
      return false;
    });

    if (skipped) {
      console.log("⚠️ 'Choose your interests' modal dismissed.");
    } else {
      console.log("❌ 'Skip' button not found in modal.");
    }
  } catch (err) {
    console.log(`❌ Error dismissing modal: ${err.message}`);
  }
}

(async () => {
    const sheets = await initSheets();
    const prioritizedProfiles = new Set();

    // 1. Fetch all profiles as before
    const rangeResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!V2:Y'
    });

    if (!rangeResponse.data || !rangeResponse.data.values) {
        console.error("❌ No values returned from Sheets.");
        process.exit(1);
    }

    const profileRows = (rangeResponse.data.values || [])
    .map(row => ({
        link: typeof row[0] === 'string' ? row[0].trim().replace(/\/$/, '') : '',
        isHighPriority: ((row[1] || '').toString().toUpperCase() === 'TRUE'),
        isLowPriority:  ((row[2] || '').toString().toUpperCase() === 'TRUE'),
        isFinished:     ((row[3] || '').toString().toUpperCase() === 'TRUE')
    }))
    .filter(profile =>
        profile.link &&
        profile.link.includes('tiktok.com') &&
        profile.link.includes('/@') &&
        !profile.isFinished &&
        (profile.isHighPriority || profile.isLowPriority)
    );

const highPriority = profileRows.filter(p => p.isHighPriority);
const lowPriority  = profileRows.filter(p => p.isLowPriority);
const sortedProfiles = highPriority.concat(lowPriority);

const prioritizedProfileLinks = new Set(highPriority.map(p => p.link));

console.log(`🔵 High-priority profiles to scrape: ${highPriority.length}`);
console.log(`🟠 Low-priority profiles to scrape: ${lowPriority.length}`);

    if (profileRows.length === 0) {
        console.warn("⚠️ No TikTok profile URLs found. Exiting.");
        return;
    }

    const lastKnownMap = await getLastKnownLinks();

    // 2. Split into 3 chunks
    const numBots = 5;
    const chunks = splitArrayIntoChunks(sortedProfiles, numBots);

    // 3. Run all bots in parallel
// await Promise.all(
//   chunks.map((chunk, idx) => processProfilesChunk(chunk, sheets, prioritizedProfileLinks, idx + 1, highPriority.length, lowPriority.length))
// );
await Promise.all(
  chunks.map((chunk, idx) =>
    processProfilesChunk(
      chunk,
      sheets,
      prioritizedProfileLinks,
      lastKnownMap,
      idx + 1,
      highPriority.length,
      lowPriority.length
    )
  )
);
    console.log("✅ All bots finished scraping!");
})();