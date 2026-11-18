// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The upgrade test's report (hammer/upgrade/run.ts writes it as
// upgrade-report.json) and its Markdown rendering for a CI job summary
// (hammer/upgrade/summary.ts). Read by tooling that posts a nightly result, so
// the shape is versioned and only grows.

export interface UpgradeCheck {
   phase: string;
   name: string;
   ok: boolean;
   detail?: string;
}

export interface ProbeRecord {
   rows: Record<string, unknown>[];
   servedFrom: string | null;
   pathTier?: string;
   /** Set when the query failed. */
   error?: string;
}

export interface UpgradeReport {
   version: 1;
   ok: boolean;
   fromVersion: string;
   toVersion: string;
   startedAt: string;
   durationMs: number;
   harnessError?: string;
   checks: UpgradeCheck[];
   probes: Record<
      string,
      {
         route: string;
         before?: ProbeRecord;
         after?: ProbeRecord;
         afterRebuild?: ProbeRecord;
      }
   >;
   /** What the test saw but does not assert (see run.ts's header). */
   notes: string[];
}

/**
 * A release version as npm publishes Publisher's: `x.y.z`, with an optional
 * prerelease. Anything else is refused before it reaches `npm install` or a
 * job summary.
 */
export const RELEASE_VERSION = /^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/;

export function isReleaseVersion(v: string): boolean {
   return RELEASE_VERSION.test(v);
}

/** Text safe inside one Markdown table cell or line: no markup, one line, bounded. */
export function mdText(text: string, max = 300): string {
   const flat = text
      .replace(/\s+/g, " ")
      .replace(/[\\`*_[\]<>|#!]/g, (c) => `\\${c}`)
      .trim();
   return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
}

export function renderUpgradeMarkdown(r: UpgradeReport): string {
   const failed = r.checks.filter((c) => !c.ok);
   const from = mdText(r.fromVersion || "(unresolved)", 60);
   const lines = [
      `### Upgrade ${from} → main (${mdText(r.toVersion, 60)}): ${r.ok ? "passed" : "FAILED"}`,
      "",
      `${r.checks.length - failed.length}/${r.checks.length} checks passed; ` +
         `${(r.durationMs / 60_000).toFixed(1)} min. Released server: ` +
         `@malloy-publisher/server@${from}.`,
   ];
   if (r.harnessError)
      lines.push("", `**Harness error:** ${mdText(r.harnessError, 1000)}`);
   if (failed.length) {
      lines.push("", "| Phase | Check | Detail |", "| --- | --- | --- |");
      for (const c of failed)
         lines.push(
            `| ${mdText(c.phase, 80)} | ${mdText(c.name, 120)} | ${mdText(c.detail ?? "")} |`,
         );
   }
   lines.push(
      "",
      "| Probe | Route | Before | After | After rebuild |",
      "| --- | --- | --- | --- | --- |",
   );
   const tier = (x?: ProbeRecord): string =>
      !x ? "–" : x.error ? "error" : mdText(x.servedFrom ?? "not reported", 40);
   for (const [id, p] of Object.entries(r.probes)) {
      lines.push(
         `| ${mdText(id, 60)} | ${mdText(p.route, 80)} | ${tier(p.before)} | ${tier(p.after)} | ${tier(p.afterRebuild)} |`,
      );
   }
   if (r.notes.length) {
      lines.push("", "<details><summary>Notes</summary>", "");
      for (const n of r.notes) lines.push(`- ${mdText(n)}`);
      lines.push("", "</details>");
   }
   return `${lines.join("\n")}\n`;
}
