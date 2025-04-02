const puppeteer = require('puppeteer');
const proxyChain = require('proxy-chain');

(async () => {
    const proxyUrl = 'http://spynfny9yy:4Ceet67~xzzDbH1spC@gate.smartproxy.com:10001';  // Your Smartproxy URL
    const newProxyUrl = await proxyChain.anonymizeProxy(proxyUrl);

    const browser = await puppeteer.launch({
        headless: false, // open browser to visually confirm
        args: [`--proxy-server=${newProxyUrl}`],
    });

    const page = await browser.newPage();

    // Visit Smartproxy's IP-check page (shows current proxy IP)
    await page.goto('https://ip.smartproxy.com/json', { waitUntil: 'networkidle2' });

    // Screenshot to verify proxy success visually
    await page.screenshot({ path: 'proxy-test-result.png' });

    await browser.close();
    await proxyChain.closeAnonymizedProxy(newProxyUrl, true);

    console.log('Proxy test complete. Check "proxy-test-result.png".');
})();
