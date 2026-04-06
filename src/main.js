import { Actor, log } from 'apify';
import { PuppeteerCrawler, sleep } from 'crawlee';

// ─── Constants ────────────────────────────────────────────────────────────────
const PAGE_SIZE           = 25;     // Apollo shows 25 leads per page
const RATE_LIMIT_DELAY_MS = 1500;   // ms between page requests

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Parse Apollo search URL and extract query params.
 * Apollo encodes filters in the URL hash fragment after #.
 */
function parseApolloUrl(rawUrl) {
  try {
    const hashPart = rawUrl.split('#')[1] || '';
    const qIndex   = hashPart.indexOf('?');
    const queryStr = qIndex >= 0 ? hashPart.slice(qIndex + 1) : '';
    return Object.fromEntries(new URLSearchParams(queryStr).entries());
  } catch (err) {
    throw new Error(`Could not parse Apollo URL: ${err.message}`);
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
await Actor.init();

const input = await Actor.getInput();

// Validate required inputs
if (!input?.apolloUrl)       throw new Error('Missing required input: apolloUrl');
if (!input?.resultsFileName) throw new Error('Missing required input: resultsFileName');
if (!input?.leadsCount)      throw new Error('Missing required input: leadsCount');

const { apolloUrl, resultsFileName } = input;

// leadsCount comes in as string from select editor — parse to integer
const leadsCount = parseInt(input.leadsCount, 10);

if (isNaN(leadsCount) || leadsCount <= 0) {
  throw new Error(`Invalid leadsCount value: ${input.leadsCount}`);
}

log.info('═══════════════════════════════════════════════');
log.info('  Apollo Lead Scraper — Boomerang');
log.info('═══════════════════════════════════════════════');
log.info(`  Target leads  : ${leadsCount.toLocaleString()}`);
log.info(`  Output file   : ${resultsFileName}`);
log.info(`  Apollo URL    : ${apolloUrl.slice(0, 80)}...`);
log.info('═══════════════════════════════════════════════');

const urlParams  = parseApolloUrl(apolloUrl);
const totalPages = Math.ceil(leadsCount / PAGE_SIZE);
const dataset    = await Actor.openDataset(resultsFileName);

let collected  = 0;
let startPage  = parseInt(urlParams.page || '1', 10);

// ─── Browser crawler ──────────────────────────────────────────────────────────
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

    // Wait for Apollo's people table
    await page.waitForSelector('[class*="zp_cS3Ap"], [data-cy="person-row"]', {
      timeout: 30_000,
    }).catch(() => log.warning(`Page ${pageNum}: table selector not found`));

    await sleep(1000);

    // Extract leads from DOM
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
          state:       getText('[data-cy="state"]'),
          country:     getText('[data-cy="country"]'),
          employees:   getText('[data-cy="num-employees"]'),
          industry:    getText('[data-cy="industry"]'),
          scrapedAt:   new Date().toISOString(),
        });
      });
      return rows;
    });

    if (!leads.length) {
      log.warning(`Page ${pageNum}: 0 leads found — check Apollo login/cookies`);
      return;
    }

    // Trim to not exceed requested count
    const toSave = leads.slice(0, leadsCount - collected);
    await dataset.pushData(toSave);
    collected += toSave.length;

    log.info(`[${collected}/${leadsCount}] Saved ${toSave.length} leads from page ${pageNum}`);
  },

  failedRequestHandler({ request, error }) {
    log.error(`Page ${request.userData.pageNum} failed: ${error.message}`);
  },
});

// ─── Build page requests ──────────────────────────────────────────────────────
const requests = [];
for (let p = startPage; p < startPage + totalPages && collected < leadsCount; p++) {
  const url = `https://app.apollo.io/#/people?${new URLSearchParams({ ...urlParams, page: String(p) })}`;
  requests.push({ url, userData: { pageNum: p } });
  if (p > startPage) await sleep(RATE_LIMIT_DELAY_MS);
}

await crawler.addRequests(requests);
await crawler.run();

// ─── Final summary ────────────────────────────────────────────────────────────
const info = await dataset.getInfo();

log.info('═══════════════════════════════════════════════');
log.info(`  ✅ Done! Collected : ${collected.toLocaleString()} leads`);
log.info(`  📄 Dataset name   : ${resultsFileName}`);
log.info(`  📦 Total items    : ${info?.itemCount ?? collected}`);
log.info('═══════════════════════════════════════════════');

// Store summary for n8n / webhook pickup
await Actor.setValue('RUN_SUMMARY', {
  resultsFileName,
  leadsRequested: leadsCount,
  leadsCollected: collected,
  datasetId:      info?.id,
  completedAt:    new Date().toISOString(),
});

await Actor.exit();
