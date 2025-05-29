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
    'http://spynfny9yy:4Ceet67~xzzDbH1spC@gb.smartproxy.com:30000'
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

function shouldUseProxyForProfile(profileName, prioritizedProfiles) {
    if (!prioritizedProfiles.has(profileName)) return false;
    const probability = getDynamicProxyChance();
    const roll = Math.random();
    return roll < probability;
}

async function initBrowser(profileName, prioritizedProfiles) {
    let extensionPath = "C:\\Users\\edwar\\Downloads\\TikTok-Captcha-Solver-Chrome-Web-Store";
    const secondaryPath = "C:\\Users\\edwardjohngarrido\\Desktop\\Scraper\\TikTok-Captcha-Solver-Chrome-Web-Store";

    // Switch to secondary if default path doesn't exist
    if (!fs.existsSync(extensionPath) && fs.existsSync(secondaryPath)) {
        console.warn("⚠️ Default extension path not found. Using secondary extension path.");
        extensionPath = secondaryPath;
    }

    const traffic = await fetchSmartproxyTraffic();
    const remaining = traffic.limit - traffic.used;
    const estimatedRunUsage = getPreviousRunUsage();

    const wouldUseProxy = shouldUseProxyForProfile(profileName, prioritizedProfiles);
    const shouldUseProxy = remaining > estimatedRunUsage && wouldUseProxy;
    const randomProxy = proxyList[Math.floor(Math.random() * proxyList.length)];

    if (shouldUseProxy) {
        console.log(`✅ Proxy allowed for ${profileName}`);
    } else if (wouldUseProxy && remaining <= estimatedRunUsage) {
        console.log(`💸 Would've used proxy for ${profileName} but you're outta budget. Using static IP instead. 🧍`);
        console.log(`🤫 If we had enough traffic left, this proxy would've been used: ${randomProxy}`);
        console.log(`💀 Broke Mode Activated. Using static IP like it’s 1999.`);
    } else {
        console.log(`🌐 Proxy skipped for ${profileName} (not in randomized group this run).`);
    }

    let args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-infobars",
        "--disable-background-networking",
        "--disable-gpu",
        '--mute-audio',
        "--window-size=1920,1080",
        "--disable-web-security",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        `--user-agent=${getRandomUserAgent()}`,
        `--disable-extensions-except=${extensionPath}`,
        `--load-extension=${extensionPath}`,
    ];

    if (shouldUseProxy) {
        const newProxyUrl = await proxyChain.anonymizeProxy(randomProxy);
        args.push(`--proxy-server=${newProxyUrl}`);
        console.log(`🔀 Selected Proxy: ${randomProxy}`);
    }

    return await puppeteer.launch({
        headless: false,
        args,
        protocolTimeout: 120000 // ⬅️ increase timeout to 2 minutes
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

/** Initialize Google Sheets API */
async function initSheets() {
    const auth = new google.auth.GoogleAuth({
        keyFile: 'credentials.json',
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    return google.sheets({ version: 'v4', auth: await auth.getClient() });
}

async function getProfileDateRanges() {
    const sheets = await initSheets();
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!A:D',
    });

    let profileDateRanges = {};
    let profilePostCounts = {}; // 👈 counts per profile

    response.data.values.slice(1).forEach(row => {
        const profile = row[1] ? row[1].trim() : null;
        const postUrl = row[2] ? row[2].trim() : "";

        console.log(`🔗 Checking URL: ${postUrl}`);

        // Extract post ID from video/photo URLs
        const postIdMatch = postUrl.match(/\/(video|photo)\/(\d+)/);
        const postId = postIdMatch ? postIdMatch[2] : null;

        if (!postId) {
            console.log(`⚠️ No valid post ID found in: ${postUrl}`);
            return;
        }

        const postDate = convertPostIdToDate(postId);
        console.log(`📅 Extracted Post Date: ${postDate.toUTCString()} (from Post ID: ${postId})`);

        if (!postDate || isNaN(postDate.getTime())) {
            console.log(`❌ Invalid date detected for post ID: ${postId}, URL: ${postUrl}`);
            return;
        }

        // ✅ Count posts per profile
        if (profile) {
            profilePostCounts[profile] = (profilePostCounts[profile] || 0) + 1;
        }

        if (!profileDateRanges[profile]) {
            profileDateRanges[profile] = {
                minDate: postDate,
                maxDate: postDate
            };
        } else {
            profileDateRanges[profile].minDate = postDate < profileDateRanges[profile].minDate ? postDate : profileDateRanges[profile].minDate;
            profileDateRanges[profile].maxDate = postDate > profileDateRanges[profile].maxDate ? postDate : profileDateRanges[profile].maxDate;
        }

        console.log(`✅ Profile: ${profile} | minDate: ${profileDateRanges[profile].minDate.toUTCString()} | maxDate: ${profileDateRanges[profile].maxDate.toUTCString()}`);
    });

    const sortedProfiles = Object.keys(profileDateRanges).sort((a, b) => {
        const countA = profilePostCounts[a] || 0;
        const countB = profilePostCounts[b] || 0;
        return countB - countA;
    });

    return { profileDateRanges, sortedProfiles };
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
    await new Promise(resolve => setTimeout(resolve, 3000));

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
            await new Promise(resolve => setTimeout(resolve, 2000));
            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
            await new Promise(resolve => setTimeout(resolve, 3000));
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
    await new Promise(resolve => setTimeout(resolve, 5000));

    return allLoadedPosts;
}

async function scrapeProfile(page, profileUrl, profileDateRange, existingPosts, lastKnownLink, isInprint, sheets) {
  const BRAND_TAGS = [
    '@In Print We Trust', '@in print we trust', '@InPrintWeTrust', '@inprintwetrust',
    '@inprintwetrust.co', '@InPrintWeTrust.co', '#InPrintWeTrust', '#inprintwetrust',
    '#IPWT', '#ipwt'
  ];

  console.log(`📍 Starting scrape for profile: ${profileUrl}`);
  console.log(`🧠 lastKnownLink passed: ${lastKnownLink ? lastKnownLink.join(', ') : 'null'}`);

let profileLoaded = false;
let profileRetries = 0;
const maxProfileRetries = 3;

while (!profileLoaded && profileRetries < maxProfileRetries) {
    try {
        await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await dismissInterestModal(page);
        await new Promise(resolve => setTimeout(resolve, 3000));
        const isDeletedProfile = await page.$('p.css-1y4x9xk-PTitle');
        const fallbackText = await page.$eval('body', el => el.innerText).catch(() => '');
        if (isDeletedProfile || fallbackText.includes("Couldn't find this account")) {
            console.log("❌ Detected deleted account. Skipping...");
            return;
        }
        profileLoaded = true; // success!
    } catch (err) {
        profileRetries++;
        console.log(`❌ Failed to load profile ${profileUrl} (attempt ${profileRetries}): ${err.message}`);
        // Optional: hard browser refresh after first retry
        if (profileRetries < maxProfileRetries) {
            console.log("🔁 Refreshing browser & retrying...");
            // Refresh Puppeteer browser instance (optional but recommended)
            await page.reload({ waitUntil: 'domcontentloaded' }).catch(() => {});
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}
if (!profileLoaded) {
    console.log(`💀 Giving up on profile ${profileUrl} after ${maxProfileRetries} tries.`);
    return;
}

let thumbnails = await page.$$('a[href*="/video/"], a[href*="/photo/"]');
let retries = 0;

while (thumbnails.length === 0 && retries < 5) {
  retries++;
  console.warn(`🔁 Retry ${retries}/5 — No thumbnails found on ${profileUrl}`);
  await page.reload({ waitUntil: 'domcontentloaded' });
  await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for page content to load
  thumbnails = await page.$$('a[href*="/video/"], a[href*="/photo/"]');
}

if (thumbnails.length === 0) {
  console.log(`⚠️ Still no thumbnails after 5 retries on ${profileUrl}. Proceeding to views scraping.`);
  return { links: [], fromCache: false, skipLinkScrape: true }; // Do NOT skip — allow views scrape
}



  // After clicking the first thumbnail and dismissing modal
await thumbnails[0].click();
await new Promise(resolve => setTimeout(resolve, 3000));

// 🧠 Ensure viewer mode is actually opened (check for /video/ or /photo/ in URL)
let retryCount = 0;
while (!page.url().includes('/video/') && !page.url().includes('/photo/') && retryCount < 5) {
  console.log("⏳ Viewer not open yet. Retrying post click...");
  await dismissInterestModal(page);
  let retryCount = 0;
let viewerOpened = false;
while (!viewerOpened && retryCount < 10) {
  console.log("Viewer not open yet. Retrying post click...");
  await thumbnails[0].click();
  await new Promise(resolve => setTimeout(resolve, 3000));
  viewerOpened = await page.evaluate(() => {
    return !!document.querySelector('[data-e2e="browse-video-feed"]');
  });
  retryCount++;
}
if (!viewerOpened) {
  console.warn("⚠️ Viewer failed to open after 10 tries. Skipping post.");
  continue;
}

  retryCount++;
}

if (!page.url().includes('/video/') && !page.url().includes('/photo/')) {
  console.warn("⚠️ Viewer still not opened after retry. Will continue but viewer may be stuck.");
}


  const cutoffDate = new Date();
  cutoffDate.setMonth(cutoffDate.getMonth() - 2);

  const seenLinks = new Set();
  let collectedLinks = [];
  let consecutiveExisting = 0;

while (true) {
  await new Promise(resolve => setTimeout(resolve, 1500));
  await dismissInterestModal(page);

  const currentUrl = page.url().split('?')[0];
  const postIdMatch = currentUrl.match(/\/(video|photo)\/(\d+)/);
  const postId = postIdMatch ? postIdMatch[2] : null;

  if (!postId || seenLinks.has(postId)) {
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

  if (consecutiveExisting >= 10) {
    console.log("🛑 Stopping — 10 consecutive uncollectable or known posts.");
    break;
  }

  try { await page.keyboard.press('ArrowDown'); } catch { break; }
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

  console.log("⏳ Waiting for grid posts to load...");
  await new Promise(resolve => setTimeout(resolve, 5000));

  const viewsData = await page.evaluate(() => {
    const posts = Array.from(document.querySelectorAll('a[href*="/video/"], a[href*="/photo/"]'));
    return posts.map(post => {
      const href = post.getAttribute('href')?.split('?')[0];
      const viewEl = post.querySelector('strong[data-e2e="video-views"]');
      const views = viewEl?.innerText || null;
      return { href, views };
    });
  });

  const filteredViewsData = viewsData.filter(d => d.href && (d.href.includes('/video/') || d.href.includes('/photo/')));
  for (let { href, views } of filteredViewsData) {
    const match = href.match(/\/(video|photo)\/(\d+)/);
    const postId = match?.[2];
    if (!postId || !views) continue;

    const rowNumber = existingPosts[postId];
    if (!rowNumber) continue;

    console.log(`✅ Updating view count: ${views} for ${href}`);
    updateQueue.push({ range: `Sheet1!D${rowNumber}`, values: [[views]] });
  }

  await updateGoogleSheets();
}


async function processProfiles(page, sheets) {
  try {
    console.log("📥 Fetching TikTok profiles from Column U...");
    const profileRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!U2:U'
    });

    const allProfiles = (profileRes.data.values || [])
      .flat()
      .filter(link =>
        typeof link === 'string' &&
        link.includes('tiktok.com') &&
        link.includes('/@')
      )
      .map(link => link.trim().replace(/\/$/, ''));

    if (allProfiles.length === 0) {
      console.warn("⚠️ No valid TikTok profiles found in Column U. Exiting.");
      return;
    }

    console.log(`🔍 Found ${allProfiles.length} TikTok profiles to scrape.`);

    // Load reference data from columns A–D
    const rawSheetData = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!A:D'
    });

    const rows = rawSheetData.data.values || [];

    // Build map of last-known post links by normalized TikTok profile URL
    const lastKnownMap = {};
    for (const row of rows) {
      const rawProfile = (row[1] || '').trim().replace(/\/$/, ''); // Column B
      const postLink = row[2]?.trim();                             // Column C
      if (!rawProfile || !postLink || !rawProfile.includes('/@')) continue;

      if (!lastKnownMap[rawProfile]) lastKnownMap[rawProfile] = [];
      lastKnownMap[rawProfile].push(postLink);
    }

    // Trim each to last 5 post links max
    for (const key in lastKnownMap) {
      lastKnownMap[key] = lastKnownMap[key].slice(-5);
    }

    for (const profileUrl of allProfiles) {
      const cleanProfile = profileUrl.trim().replace(/\/$/, '');
      const isInprint = cleanProfile.includes('@inprintwetrust');
      const recentLinks = lastKnownMap[cleanProfile] || null;
      const existingPosts = await refreshExistingPosts();

      console.log(`\n📍 Starting scrape for profile: ${cleanProfile}`);
      console.log(`🧠 lastKnownLink passed: ${recentLinks ? recentLinks.join(', ') : 'null'}`);
      await scrapeProfile(page, cleanProfile, {}, existingPosts, recentLinks, isInprint, sheets);
    }

    console.log("✅ Finished processing all TikTok profiles.");
    process.exit(0);
  } catch (err) {
    console.error("❌ Error in processProfiles:", err.message);
    process.exit(1);
  }
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
        await new Promise(resolve => setTimeout(resolve, 15000)); // Wait before retrying

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

        // 2️⃣ Get limit from subscription endpoint
        const subRes = await fetch(`https://api.smartproxy.com/v2/subscriptions?api-key=${apiKey}`, {
            method: 'GET',
            headers: {
              accept: 'application/json'
            }
          });                  
        const subs = await subRes.json();
        console.log("📦 Subscription response:", JSON.stringify(subs, null, 2));
        if (Array.isArray(subs) && subs.length > 0) {
            trafficLimit = parseFloat(subs[0].traffic_limit) || 8;
        } else {
            console.warn("⚠️ Failed to retrieve traffic limit from subscription. Falling back to 8 GB.");
        }

        const usedGB = +(usedBytes / (1024 ** 3)).toFixed(2);

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

    console.log("🧾 Last known links loaded (top 5):", normalized);
    return normalized;
}

async function processProfiles(page, sheets) {
  try {
    console.log("📥 Fetching profiles from Column U...");
    const rangeResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!U2:U'
    });

    const profiles = (rangeResponse.data.values || []).flat().filter(Boolean);
    if (profiles.length === 0) {
      console.warn("⚠️ No TikTok profile URLs found in Column U. Exiting.");
      return;
    }

    console.log(`🔍 Found ${profiles.length} profiles to scrape.`);

    const lastKnownMap = await getLastKnownLinks();

for (const profileUrl of profiles) {
  const cleanProfile = profileUrl.trim().replace(/\/$/, '');
  const isInprint = cleanProfile.includes('@inprintwetrust');
  const recentLinks = lastKnownMap[cleanProfile] || null;
  const existingPosts = await refreshExistingPosts();

  console.log(`\n📍 Starting scrape for profile: ${cleanProfile}`);
  console.log(`🧠 lastKnownLink passed: ${recentLinks ? recentLinks.join(', ') : 'null'}`);
  await scrapeProfile(page, cleanProfile, {}, existingPosts, recentLinks, isInprint, sheets);
}


    console.log("✅ Finished processing all profiles.");
    process.exit(0);
  } catch (err) {
    console.error("❌ Error in processProfiles:", err.message);
    process.exit(1);
  }
}

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
  const browser = await initBrowser("bulk_run", prioritizedProfiles); // ✅ fixed
  const page = await browser.newPage();
  await processProfiles(page, sheets);
  await browser.close();
})();

