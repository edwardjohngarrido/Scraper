// tbScraperV2.cleaned.js

import { ApifyClient } from 'apify-client';
import 'dotenv/config';

import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { google } from 'googleapis';
import fs from 'fs';
import fetch from 'node-fetch';
import proxyChain from 'proxy-chain';

// =====================[ CONFIG ]=====================
const APIFY_TOKEN = process.env.APIFY_TOKEN;
const TIKTOK_ACTOR_ID = process.env.TIKTOK_ACTOR_ID;
const SHEET_ID = "19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4";
const client = new ApifyClient({ token: APIFY_TOKEN });

const proxyList = [
    'http://spynfny9yy:4Ceet67~xzzDbH1spC@gb.decodo.com:30000'
];

// =====================[ UTILS ]======================

// Convert TikTok post ID to Date object
function convertPostIdToDate(postId) {
    if (!postId) return null;
    try {
        const binaryId = BigInt(postId).toString(2).padStart(64, '0');
        const epoch = parseInt(binaryId.substring(0, 32), 2) * 1000;
        return new Date(epoch);
    } catch {
        return null;
    }
}

// Convert TikTok post ID to ISO string
function convertPostIdToIso(postId) {
    const date = convertPostIdToDate(postId);
    return date ? date.toISOString() : '';
}

// Normalize TikTok view counts (e.g., "2.3K" -> 2300)
function normalizeViews(viewStr) {
    if (!viewStr) return 0;
    viewStr = ("" + viewStr).trim().toUpperCase();
    if (viewStr.endsWith('K')) return Math.round(parseFloat(viewStr) * 1000);
    if (viewStr.endsWith('M')) return Math.round(parseFloat(viewStr) * 1_000_000);
    if (viewStr.endsWith('B')) return Math.round(parseFloat(viewStr) * 1_000_000_000);
    return parseInt(viewStr.replace(/,/g, '')) || 0;
}

// Pretty date for Google Sheets display
function prettyDate(isoDateStr) {
    if (!isoDateStr) return '';
    const d = new Date(isoDateStr);
    if (isNaN(d)) return '';
    return d.toLocaleString('en-US', {
        month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC'
    });
}

// Random delay
async function randomDelay(min, max) {
    const delay = Math.floor(Math.random() * (max - min + 1)) + min;
    await new Promise(resolve => setTimeout(resolve, delay));
}

// Split array into N chunks (for parallel runs)
function splitArrayIntoChunks(array, numChunks) {
    const result = [];
    const chunkSize = Math.ceil(array.length / numChunks);
    for (let i = 0; i < numChunks; i++) {
        result.push(array.slice(i * chunkSize, (i + 1) * chunkSize));
    }
    return result;
}

// Generate random user agent
function getRandomUserAgent() {
    const userAgents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    ];
    return userAgents[Math.floor(Math.random() * userAgents.length)];
}

// Google Sheets API setup
async function initSheets() {
    const auth = new google.auth.GoogleAuth({
        keyFile: 'credentials.json',
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    return google.sheets({ version: 'v4', auth: await auth.getClient() });
}

// ==================[ PROXY & BROWSER ]===================

// Get proxy chance based on time/random factors
function getDynamicProxyChance() {
    const base = Math.random();
    const timeFactor = new Date().getMinutes() / 59;
    const chaos = Math.sin(Date.now() % 3600);
    let chance = (base * 0.4 + timeFactor * 0.3 + Math.abs(chaos) * 0.3);
    return Math.min(chance, 0.5);
}

// Start Puppeteer browser (optionally with proxy)
async function initBrowser(useProxy = false) {
    let extensionPath = "C:\\Users\\edwar\\Downloads\\TikTok-Captcha-Solver-Chrome-Web-Store";
    const secondaryPath = "C:\\Users\\edwardjohngarrido\\Desktop\\Scraper\\TikTok-Captcha-Solver-Chrome-Web-Store";
    if (!fs.existsSync(extensionPath) && fs.existsSync(secondaryPath)) {
        extensionPath = secondaryPath;
    }

    const randomProxy = proxyList[Math.floor(Math.random() * proxyList.length)];
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
        console.log(`🔀 Using Proxy: ${randomProxy}`);
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

function cleanUpTempProfile(browser) {
    if (browser && browser.process && browser._userDataDir) {
        try {
            fs.rmSync(browser._userDataDir, { recursive: true, force: true });
            console.log(`🧹 Deleted temp profile: ${browser._userDataDir}`);
        } catch (err) {
            console.warn(`⚠️ Failed to delete temp profile: ${err.message}`);
        }
    }
}

// =====================[ GOOGLE SHEETS HELPERS ]======================

async function refreshExistingPosts() {
    const sheets = await initSheets();
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!A:D',
    });
    let result = {};
    (response.data.values || []).forEach((row, index) => {
        const link = row[2];
        if (link) {
            const match = link.match(/\/(video|photo)\/(\d+)/);
            if (match) result[match[2]] = index + 1;
        }
    });
    return result;
}

async function updateGoogleSheets(updateQueue, logContext = "") {
    if (!updateQueue.length) {
        console.log(`⚠️ [${logContext}] No updates to push to Google Sheets.`);
        return;
    }
    console.log(`📌 [${logContext}] Updating Google Sheets with ${updateQueue.length} entries...`);
    const sheets = await initSheets();
    try {
        for (const update of updateQueue) {
            const { range, values } = update;
            console.log(`   ↳ Updating range ${range} with values: ${JSON.stringify(values)} (${logContext})`);
        }
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SHEET_ID,
            resource: { valueInputOption: "RAW", data: updateQueue },
        });
        await randomDelay(1500, 3200); // simulate "typing"/writing to Sheets
        console.log(`✅ [${logContext}] Google Sheets updated with batch of ${updateQueue.length}.`);
    } catch (error) {
        console.error(`❌ [${logContext}] Error updating Google Sheets:`, error);
    }
}

// =====================[ SMARTPROXY TRAFFIC ]====================

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

        const subRes = await fetch(`https://api.smartproxy.com/v2/subscriptions?api-key=${apiKey}`);
        const subs = await subRes.json();
        let usedGB_bytes = +(usedBytes / (1024 ** 3)).toFixed(2);
        let usedGB_sub = null;
        if (Array.isArray(subs) && subs.length > 0) {
            trafficLimit = parseFloat(subs[0].traffic_limit) || 8;
            usedGB_sub = parseFloat(subs[0].traffic);
        }
        let usedGB = usedGB_bytes;
        if (!isNaN(usedGB_sub)) usedGB = Math.max(usedGB_bytes, usedGB_sub);
        console.log(`📊 Traffic used this month: ${usedGB} GB / ${trafficLimit} GB`);
        return { used: usedGB, limit: trafficLimit };
    } catch (err) {
        console.error("❌ Failed to fetch traffic data:", err.message);
        return { used: Infinity, limit: 0 };
    }
}

// =====================[ PROFILE SCRAPING LOGIC ]======================

// Scraping: give logs on what it's doing, and simulate human speed
async function scrapeProfile(page, profileUrl, lastKnownLink, isInprint, sheets, isHighPriority, isLowPriority) {
    const BRAND_TAGS = [
        "@In Print We Trust", "@in print we trust", "@InPrintWeTrust", "@inprintwetrust",
        "@inprintwetrust.co", "@InPrintWeTrust.co", "#InPrintWeTrust", "#inprintwetrust",
        "#IPWT", "#ipwt"
    ];
    let updateQueue = [];
    let newRows = [];

    // Load TikTok profile grid
    console.log(`🟣 Scraping profile: ${profileUrl} [${isHighPriority ? "High Priority" : isLowPriority ? "Low Priority" : "Normal"}]`);

    let foundGrid = false;
    for (let attempt = 1; attempt <= 3; attempt++) {
        await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await randomDelay(2800, 4500);
        try {
           await page.waitForSelector('div[data-e2e="user-post-item"]', { timeout: 20000 });
            foundGrid = true;
            break; // success!
        } catch {
            console.error(`❌ No grid found for ${profileUrl} (attempt ${attempt}/3)`);
            if (attempt < 3) {
                await randomDelay(2500, 4500);
                // Optionally: page.reload() here, but re-goto is fine
            }
        }
    }
    if (!foundGrid) {
        console.error(`💀 Giving up on grid for ${profileUrl} after 3 attempts.`);
        await randomDelay(1400, 1800);
        return;
    }

    // Scroll with human-like pauses
    let scrollIters = 0, prevCount = 0;
    for (; scrollIters < 60; scrollIters++) {
        await page.evaluate(() => window.scrollBy(0, 1500));
        await randomDelay(400 + Math.random()*1200, 900 + Math.random()*1700);
        const currCount = await page.$$eval('div[data-e2e="user-post-item"]', els => els.length);
        if (currCount === prevCount) break;
        prevCount = currCount;
    }
    await randomDelay(1800, 3500);

    // Extract all posts in the grid
    const posts = await page.$$eval(
        'div[data-e2e="user-post-item"] a[href*="/video/"],div[data-e2e="user-post-item"] a[href*="/photo/"]',
        els => els.map(a => {
            const container = a.closest('div[data-e2e="user-post-item"]');
            let views = '', caption = '';
            if (container) {
                const img = container.querySelector('img[alt]');
                caption = img?.getAttribute('alt') || '';
                const viewEl = container.querySelector('strong[data-e2e="video-views"],.video-count');
                views = viewEl ? viewEl.innerText : '';
            }
            const m = a.href.match(/\/(video|photo)\/(\d+)/);
            const postId = m ? m[2] : null;
            return { href: a.href.split('?')[0], postId, caption, views };
        })
    );

    if (!posts.length) {
        console.log(`⚠️ No posts found for ${profileUrl}`);
        await randomDelay(1400, 1900);
        return;
    }

    // Build postUrl->row lookup for batch updates
    const existing = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!C:F',
    });
    const sheetRows = existing.data.values || [];
    const postUrlToRow = {};
    sheetRows.forEach((row, idx) => {
        const postUrl = row[0];
        if (postUrl) postUrlToRow[postUrl] = idx + 1;
    });

    // Only keep posts within 14 days and that match your tag (unless isInprint)
    const now = Date.now();
    const msCutoff = now - 14 * 24 * 60 * 60 * 1000;

    let updateCount = 0, appendCount = 0;
    for (const post of posts) {
        const { href, postId, caption, views } = post;
        if (!href || !postId) continue;
        const postDate = convertPostIdToDate(postId);
        if (!postDate) continue;

        // Always skip posts older than 14 days
        if (postDate.getTime() < msCutoff) continue;

        // Tag-matching only for non-exceptions
        if (!isInprint) {
            let tagMatched = BRAND_TAGS.some(tag => (caption || '').toLowerCase().includes(tag.toLowerCase()));
            if (!tagMatched) continue;
        }

        const pretty = prettyDate(postDate.toISOString());
        const normalizedViews = normalizeViews(views);

        if (postUrlToRow[href]) {
            const rowNum = postUrlToRow[href];
            updateQueue.push({
                range: `Sheet1!D${rowNum}:F${rowNum}`,
                values: [[normalizedViews, pretty, postDate.toISOString()]]
            });
            updateCount++;
            console.log(`   [${profileUrl}] 🔄 Updating post (existing): ${href} — views: ${normalizedViews}, date: ${pretty}`);
        } else {
            newRows.push([href, normalizedViews, pretty, postDate.toISOString()]);
            appendCount++;
            console.log(`   [${profileUrl}] ➕ Appending new post: ${href} — views: ${normalizedViews}, date: ${pretty}`);
        }
    }

    if (updateCount === 0 && appendCount === 0) {
        console.log(`⚪ [${profileUrl}] No new posts or updates for this profile.`);
    }

    // Update and append to Sheets with context
    await updateGoogleSheets(updateQueue, `Local - ${profileUrl}`);

    if (newRows.length) {
        const nextRow = sheetRows.length + 1;
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `Sheet1!C${nextRow}`,
            valueInputOption: 'RAW',
            resource: { values: newRows }
        });
        await randomDelay(1400, 2700);
        console.log(`✅ [${profileUrl}] Appended ${newRows.length} new post(s) to Sheet1`);
    }
    await randomDelay(900, 1800); // simulate short pause after profile
}

// ==================[ APIFY (CLOUD BOT) LOGIC ]===================

// APIFY CHUNK HANDLER with good logs
async function runApifyActorWithChunk(chunk, chunkIdx) {
    const sheets = await initSheets();
    const input = {
        profiles: chunk.map(p => p.link),
        brandTags: [
            "@In Print We Trust", "@in print we trust", "@InPrintWeTrust", "@inprintwetrust",
            "@inprintwetrust.co", "@InPrintWeTrust.co", "#InPrintWeTrust", "#inprintwetrust",
            "#IPWT", "#ipwt"
        ],
        profileExceptions: ["@inprintwetrust", "@ipwtstreetalk"],
        maxDaysOld: 14,
        concurrency: 1
    };
    console.log(`🌥️ [APIFY #${chunkIdx}] Running Apify Actor for ${chunk.length} profiles...`);
    const run = await client.actor(TIKTOK_ACTOR_ID).call(input);
    if (run.status !== 'SUCCEEDED') {
        await client.run(run.id).waitForFinish();
    }
    const datasetId = run.defaultDatasetId || (run.output && run.output.datasetId);
    if (!datasetId) throw new Error('No datasetId from Apify actor run!');

    const results = [];
    let offset = 0;
    const limit = 1000;
    while (true) {
        const url = `https://api.apify.com/v2/datasets/${datasetId}/items?format=json&clean=true&offset=${offset}&limit=${limit}&token=${APIFY_TOKEN}`;
        const res = await fetch(url);
        const chunkData = await res.json();
        if (!chunkData.length) break;
        results.push(...chunkData);
        offset += chunkData.length;
        if (chunkData.length < limit) break;
    }

    // Existing post map from Google Sheets
    const existing = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!C:F',
    });
    const existingRows = existing.data.values || [];
    const postUrlToRow = {};
    existingRows.forEach((row, idx) => {
        const postUrl = row[0];
        if (postUrl) postUrlToRow[postUrl] = idx + 1;
    });

    let updateQueue = [];
    let newRows = [];
    let updateCount = 0, appendCount = 0;
    for (const row of results) {
        const postUrl = row.postUrl;
        const views = row.views || 0;
        const postDate = row.postDate || '';
        const pretty = prettyDate(postDate);

        if (postUrlToRow[postUrl]) {
            const rowNum = postUrlToRow[postUrl];
            updateQueue.push({
                range: `Sheet1!D${rowNum}:F${rowNum}`,
                values: [[views, pretty, postDate]]
            });
            updateCount++;
            console.log(`   [APIFY #${chunkIdx}] 🔄 Updating existing post: ${postUrl} — views: ${views}, date: ${pretty}`);
        } else {
            newRows.push([postUrl, views, pretty, postDate]);
            appendCount++;
            console.log(`   [APIFY #${chunkIdx}] ➕ Appending new post: ${postUrl} — views: ${views}, date: ${pretty}`);
        }
    }

    if (updateCount === 0 && appendCount === 0) {
        console.log(`⚪ [APIFY #${chunkIdx}] No new posts or updates for this chunk.`);
    }

    await updateGoogleSheets(updateQueue, `APIFY #${chunkIdx}`);

    if (newRows.length) {
        const nextRow = existingRows.length + 1;
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `Sheet1!C${nextRow}`,
            valueInputOption: 'RAW',
            resource: { values: newRows }
        });
        await randomDelay(1800, 3500);
        console.log(`✅ [APIFY #${chunkIdx}] Appended ${newRows.length} new post(s) to Sheet1`);
    }
    await randomDelay(1500, 2500);
}

// =====================[ MAIN CONTROLLER ]======================

(async () => {
    puppeteer.use(StealthPlugin());
    const sheets = await initSheets();

    // Fetch TikTok profiles and priorities from Google Sheets
    const rangeResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!V2:Y'
    });
    const profileRows = (rangeResponse.data.values || [])
        .map(row => ({
            link: typeof row[0] === 'string' ? row[0].trim().replace(/\/$/, '') : '',
            isHighPriority: ((row[1] || '').toString().toUpperCase() === 'TRUE'),
            isLowPriority: ((row[2] || '').toString().toUpperCase() === 'TRUE'),
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

    if (!sortedProfiles.length) {
        console.warn("⚠️ No TikTok profiles to scrape. Exiting.");
        return;
    }

    // Split into 8 chunks (5 local, 3 Apify)
    const numChunks = 5;
    const chunks = splitArrayIntoChunks(sortedProfiles, numChunks);

    // 3 Apify, 5 local bots in parallel
    await Promise.all([
        //...chunks.slice(0, 3).map((chunk, idx) => runApifyActorWithChunk(chunk, idx + 1)),
        ...chunks.slice(0).map(async (chunk, idx) => {
            let browser = null;
            let batchCounter = 0;
            let batchThreshold = Math.floor(Math.random() * 3) + 4;
            for (let i = 0; i < chunk.length; i++) {
                const profileObj = chunk[i];
                const profileUrl = profileObj.link;
                const cleanProfile = profileUrl.trim().replace(/\/$/, '');
                const isHighPriority = profileObj.isHighPriority;
                const isLowPriority = profileObj.isLowPriority;
                const isInprint =
                    cleanProfile.includes('@inprintwetrust') ||
                    cleanProfile.includes('@ipwtstreetalk');

                console.log(`[${i + 1}/${chunk.length}] Scraping profile: ${profileUrl}`);

                if (!browser || batchCounter === 0) {
                    if (browser) {
                        try { await browser.close(); } catch (e) { }
                        cleanUpTempProfile(browser);
                    }
                    let useProxy = false;
                    if (isHighPriority) {
                        const traffic = await fetchSmartproxyTraffic();
                        const probability = getDynamicProxyChance();
                        useProxy = Math.random() < probability && (traffic && traffic.used < 7);
                        if (traffic && traffic.used >= 7) useProxy = false;
                    }
                    browser = await initBrowser(useProxy);
                    batchThreshold = Math.floor(Math.random() * 3) + 4;
                    batchCounter = 0;
                }

                let scrapeAttempts = 0, scrapeSuccess = false;
                while (!scrapeSuccess && scrapeAttempts < 3) {
                    let page = null;
                    try {
                        page = await browser.newPage();
                        await page.setViewport({ width: 1200, height: 800 });
                        await page.setJavaScriptEnabled(true);
                        await scrapeProfile(
                            page,
                            cleanProfile,
                            null, // lastKnownLink
                            isInprint,
                            sheets,
                            isHighPriority,
                            isLowPriority
                        );
                        scrapeSuccess = true;
                    } catch (err) {
                        scrapeAttempts++;
                        console.error(`❌ Error scraping profile: ${cleanProfile} (attempt ${scrapeAttempts}): ${err && err.message ? err.message : err}`);
                        try { if (page) await page.close(); } catch (e) { }
                        try { if (browser) await browser.close(); } catch (e) { }
                        cleanUpTempProfile(browser);
                        browser = null;
                        batchCounter = 0;
                    } finally {
                        try { if (page) await page.close(); } catch (e) { }
                    }
                }
                if (!scrapeSuccess) {
                    console.error(`💀 Giving up on profile ${cleanProfile} after 3 tries.`);
                }
                batchCounter++;
                if (batchCounter >= batchThreshold) {
                    try { if (browser) await browser.close(); } catch (e) { }
                    cleanUpTempProfile(browser);
                    browser = null;
                    batchCounter = 0;
                }
            }
            if (browser) {
                try { await browser.close(); } catch (e) { }
                cleanUpTempProfile(browser);
            }
        })
    ]);
    console.log("✅ All bots finished scraping!");
})();
