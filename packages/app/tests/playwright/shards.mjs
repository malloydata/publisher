// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The Playwright suite's CI shards: which spec files each of the three jobs in
// .github/workflows/app-playwright.yml runs. The groups live in shards.json,
// balanced by hand from measured per-file durations (the suite runs on one
// worker, so a shard's time is the sum of its files). Shard 2 also runs the
// model viewer's second pass, so it carries the least of the main suite.
//
//   node tests/playwright/shards.mjs check     # every spec in exactly one group
//   node tests/playwright/shards.mjs files 2   # check, then print shard 2's specs
//
// The check fails when a spec file on disk is in no group, is in more than one,
// or a group names a file that does not exist, so a new spec cannot silently
// drop out of CI. Paths are printed relative to packages/app, which is where
// the workflow runs `playwright test`.

import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const groups = JSON.parse(readFileSync(join(here, "shards.json"), "utf8"));
const onDisk = readdirSync(here)
   .filter((f) => f.endsWith(".spec.ts"))
   .sort();

const problems = [];
const seen = new Map();
for (const [shard, files] of Object.entries(groups)) {
   for (const file of files) {
      if (seen.has(file)) {
         problems.push(
            `${file} is in shard ${seen.get(file)} and shard ${shard}`,
         );
      } else {
         seen.set(file, shard);
      }
      if (!onDisk.includes(file)) {
         problems.push(`shard ${shard} names ${file}, which does not exist`);
      }
   }
}
for (const file of onDisk) {
   if (!seen.has(file)) {
      problems.push(`${file} is in no shard: add it to shards.json`);
   }
}

if (problems.length > 0) {
   for (const p of problems) console.error(`shards.json: ${p}`);
   process.exit(1);
}

const [command, shard] = process.argv.slice(2);
if (command === "check") {
   console.log(
      `shards.json: ${onDisk.length} spec files, each in exactly one of ${Object.keys(groups).length} shards`,
   );
} else if (command === "files" && groups[shard]) {
   console.log(groups[shard].map((f) => `tests/playwright/${f}`).join("\n"));
} else {
   console.error("usage: shards.mjs check | shards.mjs files <shard>");
   process.exit(2);
}
