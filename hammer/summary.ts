#!/usr/bin/env bun
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Render a hammer run's report (hammer-report.json, which run.ts writes into
// HAMMER_LOG_DIR) as Markdown on stdout, for a CI job summary.
//
//   bun hammer/summary.ts <report.json> [heading]
//
// A missing or unreadable report still prints a summary, saying the run ended
// before it wrote one (a crash, a kill, a job timeout), and exits 0: the job's
// own result already says it failed, and this step should not hide that.

import { existsSync, readFileSync } from "fs";
import { renderMarkdown, type HammerReport } from "./lib/report";

const [file, heading = "Hammer"] = process.argv.slice(2);
if (!file) {
   console.error("Usage: bun hammer/summary.ts <report.json> [heading]");
   process.exit(2);
}
let report: HammerReport | undefined;
try {
   if (existsSync(file))
      report = JSON.parse(readFileSync(file, "utf8")) as HammerReport;
} catch {
   report = undefined;
}
process.stdout.write(
   report
      ? renderMarkdown(report, heading)
      : `### ${heading}: no report\n\nThe run ended before it wrote ${file} (a crash, a kill or the job timeout). See the log artifact.\n`,
);
