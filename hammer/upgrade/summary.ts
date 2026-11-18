#!/usr/bin/env bun
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Render the upgrade test's report (upgrade-report.json, which run.ts writes
// into HAMMER_LOG_DIR) as Markdown on stdout, for a CI job summary.
//
//   bun hammer/upgrade/summary.ts <report.json>
//
// A missing report still prints a summary saying the run ended before writing
// one, and exits 0: the job's own result already says it failed.

import { existsSync, readFileSync } from "fs";
import type { UpgradeReport } from "./run";

const cell = (text: string, max = 300): string => {
   const flat = text.replace(/\s+/g, " ").replace(/\|/g, "\\|").trim();
   return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
};

export function renderUpgradeMarkdown(r: UpgradeReport): string {
   const failed = r.checks.filter((c) => !c.ok);
   const lines = [
      `### Upgrade ${r.fromVersion} → main (${r.toVersion}): ${r.ok ? "passed" : "FAILED"}`,
      "",
      `${r.checks.length - failed.length}/${r.checks.length} checks passed; ` +
         `${(r.durationMs / 60_000).toFixed(1)} min. Released server: ` +
         `\`@malloy-publisher/server@${r.fromVersion}\`.`,
   ];
   if (r.harnessError)
      lines.push("", `**Harness error:** ${cell(r.harnessError, 1000)}`);
   if (failed.length) {
      lines.push("", "| Phase | Check | Detail |", "| --- | --- | --- |");
      for (const c of failed)
         lines.push(
            `| ${cell(c.phase, 80)} | ${cell(c.name, 120)} | ${cell(c.detail ?? "")} |`,
         );
   }
   lines.push(
      "",
      "| Probe | Route | Before | After | After rebuild |",
      "| --- | --- | --- | --- | --- |",
   );
   for (const [id, p] of Object.entries(r.probes)) {
      const tier = (x?: { servedFrom: string | null; error?: string }) =>
         !x ? "–" : x.error ? "error" : (x.servedFrom ?? "not reported");
      lines.push(
         `| \`${id}\` | ${cell(p.route, 80)} | ${tier(p.before)} | ${tier(p.after)} | ${tier(p.afterRebuild)} |`,
      );
   }
   if (r.notes.length) {
      lines.push("", "<details><summary>Notes</summary>", "");
      for (const n of r.notes) lines.push(`- ${n}`);
      lines.push("", "</details>");
   }
   return `${lines.join("\n")}\n`;
}

if (import.meta.main) {
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
         : `### Upgrade: no report\n\nThe run ended before it wrote ${file} (a crash, a kill or the job timeout). See the log artifact.\n`,
   );
}
