import { Actor, log } from 'apify';
import { PuppeteerCrawler, sleep } from 'crawlee';

const PAGE_SIZE           = 25;
const RATE_LIMIT_DELAY_MS = 1500;

function parseApolloUrl(rawUrl) {
  const hashPart = rawUrl.split('#')[1] || '';
  const qIndex   = hashPart.indexOf('?');
  const queryStr = qIndex >= 0 ? hashPart.slice(qIndex + 1) : '';
  return Object.fromEntries(new URLSearchParams(queryStr).entries());
}

await Actor.init();

const input = await Actor.getInput();

if (!input?.apolloUrl)       throw new Error('Missing input: apolloUrl');
if (!input?.resultsFileName) throw new Error('Missing input: resultsFileName');
if (!input?.leadsCount)      throw new Error('Missing input: leadsCount');

const { apolloUrl, resultsFileName, leadsCount } = input;

log.info(`Starting — Target: ${leadsCount} leads | File: ${resultsFileName}`);

const urlParams  = parseApolloUrl(apolloUrl);
const totalPages = Math.ceil(leadsCount / PAGE_SIZE);
const dataset    = await Actor.openDataset(resultsFileName);

let collected    = 0;
let startPage    = parseInt(urlParams.page || '1', 10);

const crawler = new PuppeteerCrawler({
  launchContext: {
    launchOptions: {
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    },
  },
  requestHandlerTimeoutSecs: 120,

  async requestHandler({ page, request }) {
    const { pageNum } = request.userData;

    log.info(`[${collected}/${leadsCount}] Scraping page ${pageNum}...`);

    await page.waitForSelector('[class*="zp_cS3Ap"], [data-cy="person-row"]', {
      timeout: 30_000,
    }).catch(() => log.warning(`Page ${pageNum}: table not found`));

    await sleep(1000);

    const leads = await page.evaluate(() => {
      const rows = [];
      document.querySelectorAll('[class*="zp_cS3Ap"], [data-cy="person-row"]').forEach(row => {
        const getText = sel => row.querySelector(sel)?.textContent?.trim() || '';
        const getHref = sel => row.querySelector(sel)?.href || '';
        rows.push({
          name:        getText('[class*="zp_xYOBY"] a, [data-cy="person-name"] a'),
          title:       getText('[class*="zp_Y3Imd"], [data-cy="person-title"]'),
          company:     getText('[class*="zp_b2uu1"], [data-cy="company-name"]'),
          email:       getText('[data-cy="email"]'),
          phone:       getText('[data-cy="phone"]'),
          linkedinUrl: getHref('a[href*="linkedin.com"]'),
          city:        getText('[data-cy="city"]'),
          country:     getText('[data-cy="country"]'),
          scrapedAt:   new Date().toISOString(),
        });
      });
      return rows;
    });

    if (!leads.length) {
      log.warning(`Page ${pageNum}: 0 leads — check Apollo login/cookies`);
      return;
    }

    const toSave = leads.slice(0, leadsCount - collected);
    await dataset.pushData(toSave);
    collected += toSave.length;
    log.info(`[${collected}/${leadsCount}] Saved ${toSave.length} leads`);
  },

  failedRequestHandler({ request, error }) {
    log.error(`Page ${request.userData.pageNum} failed: ${error.message}`);
  },
});

const requests = [];
for (let p = startPage; p < startPage + totalPages && collected < leadsCount; p++) {
  const url = `https://app.apollo.io/#/people?${new URLSearchParams({ ...urlParams, page: String(p) })}`;
  requests.push({ url, userData: { pageNum: p } });
  if (p > startPage) await sleep(RATE_LIMIT_DELAY_MS);
}

await crawler.addRequests(requests);
await crawler.run();

const info = await dataset.getInfo();
log.info(`Done! Collected: ${collected} | Dataset: ${resultsFileName}`);

await Actor.setValue('RUN_SUMMARY', {
  resultsFileName,
  leadsRequested: leadsCount,
  leadsCollected: collected,
  datasetId:      info?.id,
  completedAt:    new Date().toISOString(),
});

await Actor.exit();
