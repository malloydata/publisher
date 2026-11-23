#!/usr/bin/env bun
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Render the upgrade test's report (upgrade-report.json, which run.ts writes
// into HAMMER_LOG_DIR) as Markdown on stdout, for a CI job summary. The
// rendering is hammer/lib/upgrade_report.ts, where it is unit-tested.
//
//   bun hammer/upgrade/summary.ts <report.json>
//
// A missing report still prints a summary saying the run ended before writing
// one, and exits 0: the job's own result already says it failed.

import { existsSync, readFileSync } from "fs";
import {
   mdText,
   renderUpgradeMarkdown,
   type UpgradeReport,
} from "../lib/upgrade_report";

const [file] = process.argv.slice(2);
if (!file) {
   console.error("Usage: bun hammer/upgrade/summary.ts <report.json>");
   process.exit(2);
}
let report: UpgradeReport | undefined;
try {
   if (existsSync(file))
      report = JSON.parse(readFileSync(file, "utf8")) as UpgradeReport;
} catch {
   report = undefined;
}
process.stdout.write(
   report
      ? renderUpgradeMarkdown(report)
      : `### Upgrade: no report\n\nThe run ended before it wrote ${mdText(file)} (a crash, a kill or the job timeout). See the log artifact.\n`,
);
