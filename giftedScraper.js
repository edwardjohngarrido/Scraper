const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { google } = require('googleapis');
const fs = require('fs');

puppeteer.use(StealthPlugin());

const SHEET_ID = "19DsWqJW09VxMfNojPH9mnGJ4MCQl7m3Ud3LNLkn-Ag4";
const REPROCESS_INTERVAL = 3 * 60 * 60 * 1000; // 3 hours

let updateQueue = []; // Global queue for batch updates

/** Convert TikTok post ID to Unix timestamp */
function convertPostIdToUnix(postId) {
    if (!postId) return null;
    const binaryId = BigInt(postId).toString(2).padStart(64, '0');
    return parseInt(binaryId.substring(0, 32), 2);
}

/** Initialize Puppeteer Browser */
async function initBrowser() {
    return await puppeteer.launch({
        headless: true, // Set to false for debugging, change later if needed
        args: [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-infobars",
            "--disable-background-networking",
            "--disable-extensions",
            "--disable-gpu",
            "--window-size=1920,1080",
        ],
    });
}

/** Initialize Google Sheets API */
async function initSheets() {
    const auth = new google.auth.GoogleAuth({
        keyFile: 'credentials.json',
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    return google.sheets({ version: 'v4', auth: await auth.getClient() });
}

/** Extract profile date ranges from Google Sheets using TikTok Post IDs */
async function getProfileDateRanges() {
    const sheets = await initSheets();
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet3!A:D',
    });

    let profileDateRanges = {};
    response.data.values.slice(1).forEach(row => {
        const profile = row[1] ? row[1].trim() : null;
        const postIdMatch = row[2] ? row[2].match(/\/video\/(\d+)/) : null;
        const postTimestamp = postIdMatch ? convertPostIdToUnix(postIdMatch[1]) : null;

        if (profile && postTimestamp) {
            if (!profileDateRanges[profile]) {
                profileDateRanges[profile] = { minDate: postTimestamp, maxDate: postTimestamp };
            } else {
                profileDateRanges[profile].minDate = Math.min(profileDateRanges[profile].minDate, postTimestamp);
                profileDateRanges[profile].maxDate = Math.max(profileDateRanges[profile].maxDate, postTimestamp);
            }
        }
    });

    console.log("📊 Extracted Profile Date Ranges:", profileDateRanges);
    return profileDateRanges;
}

/** Scroll page dynamically */
async function scrollPage(page, profileDateRange) {
    let lastHeight = await page.evaluate(() => document.body.scrollHeight);
    let stopScrolling = false;
    let allLoadedPosts = new Set(); // Track all posts loaded
    let allLoadedPostTimestamps = new Set(); // Track timestamps
    let emptyScrollCount = 0; // Count consecutive scrolls without new posts

    console.log(`🕒 Profile Date Range: Min=${profileDateRange.minDate}, Max=${profileDateRange.maxDate}`);

    function extractPostId(url) {
        const match = url.match(/\/(video|photo)\/(\d+)/);
        return match ? match[2] : null;
    }

    while (!stopScrolling || allLoadedPosts.size === 0) {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await new Promise(resolve => setTimeout(resolve, 3000));

        const postLinks = await page.$$eval('a[href*="/video/"], a[href*="/photo/"]', posts => posts.map(post => post.href));
        let newPostsFound = false;

        for (const link of postLinks) {
            const postId = extractPostId(link);
            if (postId && !allLoadedPosts.has(link)) {
                const postTimestamp = convertPostIdToUnix(postId);
                if (postTimestamp >= profileDateRange.minDate && postTimestamp <= profileDateRange.maxDate) {
                    allLoadedPosts.add(link);
                    allLoadedPostTimestamps.add(postTimestamp);
                    newPostsFound = true;
                }
            }
        }

        const newHeight = await page.evaluate(() => document.body.scrollHeight);
        if (allLoadedPostTimestamps.size > 0 && (!newPostsFound || newHeight === lastHeight)) {
            emptyScrollCount++;

            const minTimestamp = Math.min(...allLoadedPostTimestamps);
            const maxTimestamp = Math.max(...allLoadedPostTimestamps);

            console.log(`🔍 Loaded Posts Range: Min=${minTimestamp}, Max=${maxTimestamp}`);

            if (minTimestamp <= profileDateRange.minDate) {
                console.log('✅ All dataset posts within date range found. Stopping scroll.');
                stopScrolling = true;
            } else if (emptyScrollCount >= 5 && !stopScrolling) {
                // If stuck for 5 scrolls, scroll up then scroll down again
                console.log("🔄 Detected scrolling stuck. Performing up-down reset...");
                await page.evaluate(() => window.scrollTo(0, 0)); // Scroll UP
                await new Promise(resolve => setTimeout(resolve, 2000)); // Wait
                await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)); // Scroll DOWN
                await new Promise(resolve => setTimeout(resolve, 3000));
                emptyScrollCount = 0; // Reset counter
            } else {
                console.log('🔄 More posts needed within date range. Continuing scroll...');
            }
        } else {
            emptyScrollCount = 0; // Reset counter if new posts were found
        }

        lastHeight = newHeight;
    }

    console.log('⌛ Final wait for posts to stabilize...');
    await new Promise(resolve => setTimeout(resolve, 5000));

    return allLoadedPosts;
}

/** Scrape TikTok Profile */
async function scrapeProfile(page, profileUrl, profileDateRange, existingPosts) {
    console.log(`Navigating to ${profileUrl}`);
    await page.goto(profileUrl, { waitUntil: 'networkidle2', timeout: 120000 });
    console.log("⏳ Waiting 5s for posts to fully load...");
    await new Promise(resolve => setTimeout(resolve, 5000));
    console.log("📜 Starting scrolling for posts...");
    const scrapedPosts = await scrollPage(page, profileDateRange);

    for (let post of scrapedPosts) {
        try {
            // Extract views directly from profile page
            const viewsSelector = `a[href='${post}'] strong[data-e2e="video-views"]`;
            const views = await page.$eval(viewsSelector, el => el.innerText);
            console.log(`🔍 Scraped post: ${post} | Views: ${views}`);
            
            // Find correct row number for the post link
            if (existingPosts[post]) {
                const rowNumber = existingPosts[post];
                updateQueue.push({ range: `Sheet3!D${rowNumber}`, values: [[views]] });
            }
        } catch (error) {
            console.log(`⚠️ Failed to extract views for post: ${post}`);
        }
    }
}

/** Update Google Sheets after processing each batch */
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
    } catch (error) {
        console.error("❌ Error updating Google Sheets:", error);
    }
    
    updateQueue = [];
}

/** Process TikTok Profiles */
async function processProfiles() {
    const profileDateRanges = await getProfileDateRanges();
    const profiles = Object.keys(profileDateRanges);
    console.log(`🚀 Processing ${profiles.length} profiles...`);

    // Load existing post links to match them to correct rows
    const sheets = await initSheets();
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet3!A:D',
    });
    
    let existingPosts = {};
    response.data.values.forEach((row, index) => {
        if (row[2]) {
            existingPosts[row[2]] = index + 1; // Store post link and its row number
        }
    });
    
    const browser = await initBrowser();
    while (profiles.length > 0) {
        const chunk = profiles.splice(0, 5);
        console.log(`Processing batch of ${chunk.length} profiles in parallel...`);

        await Promise.all(chunk.map(async (profile) => {
            const page = await browser.newPage();
            await scrapeProfile(page, profile, profileDateRanges[profile], existingPosts);
            await page.close();
        }));

        await updateGoogleSheets();
    }

    await browser.close();
    console.log(`✅ Finished processing all profiles.`);
}

processProfiles().catch(console.error);
