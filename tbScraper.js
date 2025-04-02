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
    const extensionPath = "C:\\Users\\edwar\\Downloads\\TikTok-Captcha-Solver-Chrome-Web-Store";

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
        "--window-size=1920,1080",
        "--disable-web-security",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        `--user-agent=${getRandomUserAgent()}`,
        `--disable-extensions-except=${extensionPath}`,
        `--load-extension=${extensionPath}`
    ];

    if (shouldUseProxy) {
        const newProxyUrl = await proxyChain.anonymizeProxy(randomProxy);
        args.push(`--proxy-server=${newProxyUrl}`);
        console.log(`🔀 Selected Proxy: ${randomProxy}`);
    }

    return await puppeteer.launch({
        headless: true,
        args
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

async function scrollPage(page, profileDateRange) {
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

    while (!stopScrolling || allLoadedPosts.size === 0 && scrollCount < maxScrolls) {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await new Promise(resolve => setTimeout(resolve, 3000));

        const postLinks = await page.$$eval('a[href*="/video/"], a[href*="/photo/"]', posts => posts.map(post => post.href));
        let newPostsFound = false;
        let allOutOfRange = true; // Flag to track if all posts are out of range

        for (const link of postLinks) {
            const postId = extractPostId(link);
            if (postId && !allLoadedPosts.has(link)) {
                const postDate = convertPostIdToDate(postId);
                
                console.log(`🔍 Post found: ${link} | Date: ${postDate.toUTCString()}`);

                if (!isNaN(postDate.getTime())) {
                    allLoadedPosts.add(link);
                    allLoadedPostTimestamps.add(postDate);

                    // ✅ Check if post is within date range
                    if (postDate >= profileDateRange.minDate && postDate <= profileDateRange.maxDate) {
                        newPostsFound = true;
                        allOutOfRange = false;
                    }
                }
            }
        }

        const newHeight = await page.evaluate(() => document.body.scrollHeight);

        // ✅ Stop scrolling if all posts are outside of date range
        if (allOutOfRange && allLoadedPosts.size > 0) {
            console.log("❌ All posts are out of date range. Stopping scroll.");
            stopScrolling = true;
            break;
        }

        if (allLoadedPostTimestamps.size > 0 && (!newPostsFound || newHeight === lastHeight)) {
            emptyScrollCount++;

            const minTimestamp = new Date(Math.min(...Array.from(allLoadedPostTimestamps).map(d => d.getTime())));
            const maxTimestamp = new Date(Math.max(...Array.from(allLoadedPostTimestamps).map(d => d.getTime())));

            console.log(`🔍 Loaded Posts Range: Min=${minTimestamp.toUTCString()}, Max=${maxTimestamp.toUTCString()}`);

            if (minTimestamp <= profileDateRange.minDate) {
                console.log('✅ All dataset posts within date range found. Stopping scroll.');
                stopScrolling = true;
                break;
            } else if (emptyScrollCount >= 5 && !stopScrolling) {
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

async function scrapeProfile(page, profileUrl, profileDateRange, existingPosts) {
    console.log(`Navigating to ${profileUrl}`);

    let retryAttempts = 5; // Maximum refresh attempts
    let captchaWaitTime = 120000; // Max wait time for CAPTCHA solver

    for (let attempt = 1; attempt <= retryAttempts; attempt++) {
        if (attempt > 1) {
            console.log(`🔄 Refreshing Profile... Attempt ${attempt}/${retryAttempts}`);
            try {
                await page.reload({ waitUntil: 'domcontentloaded', timeout: 60000 });
            } catch (err) {
                console.log(`❌ Reload failed on attempt ${attempt}:`, err.message);
                continue; // continue to next attempt
            }
            await new Promise(resolve => setTimeout(resolve, 10000));
        } else {
            //await page.goto(profileUrl, { waitUntil: 'networkidle2', timeout: 120000 });
            try {
                await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

                // 👇 Check if profile doesn't exist
                const notFound = await page.$eval('body', el => el.innerText).catch(() => '');
                if (notFound.includes("Couldn't find this account")) {
                    console.log("❌ Profile not found (deleted or never existed). Skipping...");
                    return;
                }

            } catch (err) {
                console.error(`❌ Navigation failed for ${profileUrl}:`, err.message);
                return; // Skip this profile and continue
            }       

            
        }

        // ⏳ Short delay before checking (prevents false negatives)
        await new Promise(resolve => setTimeout(resolve, 10000));

        // 🚨 Step 1: Detect CAPTCHA
        const captchaSelector = 'div[data-e2e="captcha"], iframe[src*="tiktok.com/captcha"]';
        const isCaptchaPresent = await page.$(captchaSelector);

        if (isCaptchaPresent) {
            console.log("⚠️ CAPTCHA detected! Waiting for solver...");
            let captchaSolved = false;
            let startTime = Date.now();

            while (Date.now() - startTime < captchaWaitTime) {
                await new Promise(resolve => setTimeout(resolve, 12000));
                const stillCaptcha = await page.$(captchaSelector);
                if (!stillCaptcha) {
                    captchaSolved = true;
                    break;
                }
            }

            if (captchaSolved) {
                console.log("✅ CAPTCHA solved! Proceeding...");
                break;
            } else {
                console.log("❌ CAPTCHA not solved in time. Skipping profile.");
                return;
            }
        }

        // 🚨 Step 2: Check if Posts Have Loaded
        //const videosSectionExists = await page.$('div[data-e2e="user-post-item"]');
        const videosSectionExists = await page.$('a[href*="/video/"]');


        if (videosSectionExists) {
            console.log("✅ Profile loaded successfully! Posts detected.");
            break;
        }

        console.log(`⚠️ No CAPTCHA found, but posts didn't load. Attempt ${attempt}/${retryAttempts}`);

        if (attempt === retryAttempts) {
            console.log("❌ Profile did not load after multiple attempts. Skipping...");
            return;
        }
    }

    console.log("⏳ Waiting 5s for posts to fully load...");
    await new Promise(resolve => setTimeout(resolve, 5000));

    console.log("📜 Starting scrolling for posts...");
    const scrapedPosts = await scrollPage(page, profileDateRange);

    for (let post of scrapedPosts) {
        // ✅ **Only process posts that are in Google Sheets**
        // if (!(post in existingPosts)) {
        //     console.log(`⚠️ Skipping post not found in Google Sheets: ${post}`);
        //     continue;
        // }

        const scrapedPostIdMatch = post.match(/\/(video|photo)\/(\d+)/);
        if (!scrapedPostIdMatch) continue;
        const scrapedPostId = scrapedPostIdMatch[2];

        if (!(scrapedPostId in existingPosts)) {
            console.log(`⚠️ Skipping post not found in Google Sheets: ${post}`);
            continue;
        }


        try {
            //const viewsSelector = `a[href='${post}'] strong[data-e2e="video-views"]`;
            const viewsSelector = `a[href*='${scrapedPostId}'] strong[data-e2e="video-views"]`;
            const views = await page.$eval(viewsSelector, el => el.innerText);
            console.log(`🔍 Scraped post: ${post} | Views: ${views}`);

            //const rowNumber = existingPosts[post];
            const rowNumber = existingPosts[scrapedPostId];

            // ✅ Ensure the row number is valid before updating
            if (!rowNumber || isNaN(rowNumber)) {
                console.log(`⚠️ Skipping update for ${post}, invalid row number: ${rowNumber}`);
                continue;
            }

            updateQueue.push({ range: `Sheet1!D${rowNumber}`, values: [[views]] });
            console.log(`✅ Added to update queue: ${post} -> Row ${rowNumber} | Views: ${views}`);
        } catch (error) {
            console.log(`⚠️ Failed to extract views for post: ${post}`);
        }
    }

    // ✅ Call update function after processing each profile
    await updateGoogleSheets();
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
        await new Promise(resolve => setTimeout(resolve, 5000)); // Wait before retrying

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

/** Process TikTok Profiles */
async function processProfiles() {
    const { profileDateRanges, sortedProfiles } = await getProfileDateRanges();
    const profiles = [...sortedProfiles]; // prioritized order
    console.log(`🚀 Processing ${profiles.length} profiles...`);

    const prioritizedProfiles = new Set(sortedProfiles.slice(0, 10));

    // Load existing post links to match them to correct rows
    const sheets = await initSheets();
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!A:D',
    });
    
    let existingPosts = {};
    response.data.values.forEach((row, index) => {
        if (row[2]) {
            const match = row[2].match(/\/(video|photo)\/(\d+)/);
            if (match) existingPosts[match[2]] = index + 1; 
        }
    });
    
    while (profiles.length > 0) {
        const profile = profiles.shift();
        console.log(`📌 Processing profile: ${profile}`);
    
        const browser = await initBrowser(profile, prioritizedProfiles);
        const page = await browser.newPage();
    
        await page.setRequestInterception(true);
        page.on('request', (req) => {
            const url = req.url();
            if (
                url.includes('analytics') ||
                url.includes('doubleclick') ||
                url.includes('facebook.com/tr') ||
                url.includes('pixel') ||
                url.includes('ads.tiktok.com')
            ) {
                console.log(`🛑 Blocking tracking: ${url}`);
                req.abort();
            } else {
                req.continue();
            }
        });
    
        await scrapeProfile(page, profile, profileDateRanges[profile], existingPosts);
        await page.close();
        await browser.close(); // ✅ properly close each browser here
    }

    console.log(`✅ Finished processing all profiles.`);

    const traffic = await fetchSmartproxyTraffic();
    const estimatedThisRun = 0.81;

    // Save run log
    fs.writeFileSync('run_log.json', JSON.stringify({
      latestUsage: estimatedThisRun,
      usedGB: traffic.used,
      trafficLimit: traffic.limit,
      timestamp: new Date().toISOString()
    }, null, 2));

    // Append to CSV
    const csvHeader = 'timestamp,usedGB,trafficLimit,estimatedRunGB\n';
    const csvLine = `${new Date().toISOString()},${traffic.used},${traffic.limit},${estimatedThisRun}\n`;

    if (!fs.existsSync('traffic_log.csv')) {
      fs.writeFileSync('traffic_log.csv', csvHeader);
    }
    fs.appendFileSync('traffic_log.csv', csvLine);

}

processProfiles().catch(console.error);
