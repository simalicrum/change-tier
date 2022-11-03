import { listBlobsFlatByUrl, setBlobsAccessTier } from '@simalicrum/azure-helpers';
import { Command } from 'commander';
import logUpdate from 'log-update';

const program = new Command();

program
  .name('change-tier')
  .description('Recursively changes Azure Storage Blob access tier')
  .version('1.0.0');

program
  .option('-u, -url <url>', 'Base url');

program
  .option('-t, --tier <tier>', 'Azure Storage Access tier');

program
  .option('-k, --account-key <key>', 'Azure Storage Account key');

program.parse();

const options = program.opts();

const url = new URL(options.Url);

const urlPath = url.pathname.split('/');

const [container, prefix] = [urlPath[1], urlPath.slice(2).join('/')]

let blobs = [];
let start = Date.now();
let totalFails = 0;
let totalSuccesses = 0;
const batchSize = 256;

console.log(`Changing everything in ${options.Url} to ${options.tier}`)
console.log("Starting...");

for await (const blob of listBlobsFlatByUrl(options.Url, options.accountKey, { prefix: prefix })) {
  try {
    blobs.push(`${url.origin}/${container}/${blob.name}`);
    if (blobs.length === batchSize) {
      const batchResp = await setBlobsAccessTier(blobs, options.accountKey, options.tier);
      totalFails += batchResp.subResponsesFailedCount;
      totalSuccesses += batchResp.subResponsesSucceededCount;
      const now = Date.now();
      const interval = now - start;
      start = now;
      logUpdate(`
${batchResp.subResponsesSucceededCount} of ${batchSize} tier changed succeeded in current batch, ${Math.round((batchSize / interval) * 60000)} blobs/min.
${totalSuccesses} blobs changed
${totalFails} blobs failed
      `);
      blobs = [];
    }
  } catch (err) { console.log(err) }
}

console.log("Done!");