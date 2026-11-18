// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The machine-readable record of a hammer run, and its Markdown rendering.
//
// run.ts writes `hammer-report.json` into HAMMER_LOG_DIR when that is set (CI
// sets it), and `bun hammer/summary.ts <report.json>` renders it as Markdown
// for a job summary. Both are read by tooling that posts a nightly run's
// result somewhere else, so the JSON shape is versioned and only grows.

export type ScenarioStatus = "pass" | "fail" | "known-red" | "fixed" | "skip";

export interface ReportScenario {
   id: string;
   title: string;
   tags: string[];
   status: ScenarioStatus;
   passedChecks: number;
   totalChecks: number;
   /** Only the checks that failed, each with its detail when it has one. */
   failedChecks: { name: string; detail?: string }[];
   /** Why it was skipped (`skip`), or the first line of what it threw. */
   reason?: string;
   ms?: number;
}

export interface HammerReport {
   version: 1;
   /** True when the run's exit code is 0. */
   ok: boolean;
   /** `PUBLISHER_MATERIALIZE` as the run saw it; absent when unset. */
   materialize?: string;
   startedAt: string;
   finishedAt: string;
   durationMs: number;
   /** A failure of the harness itself (Docker, the build, a config), not a scenario. */
   harnessError?: string;
   counts: Record<ScenarioStatus, number>;
   scenarios: ReportScenario[];
}

export interface ResultLike {
   scenario: { id: string; title: string; tags: string[] };
   assert: { checks: { name: string; ok: boolean; detail?: string }[] };
   error?: string;
   skipped?: string;
   ms?: number;
}

/** The same verdict `summarize` prints: a known-red passing is a failure. */
export function statusOf(r: ResultLike, knownRedTag: string): ScenarioStatus {
   if (r.skipped) return "skip";
   const bad = r.assert.checks.some((c) => !c.ok);
   if (r.scenario.tags.includes(knownRedTag))
      return bad ? "known-red" : "fixed";
   return bad ? "fail" : "pass";
}

export function buildReport(opts: {
   results: ResultLike[];
   knownRedTag: string;
   startedAt: Date;
   finishedAt: Date;
   harnessError?: string;
   materialize?: string;
}): HammerReport {
   const counts: Record<ScenarioStatus, number> = {
      pass: 0,
      fail: 0,
      "known-red": 0,
      fixed: 0,
      skip: 0,
   };
   const scenarios = opts.results.map((r): ReportScenario => {
      const status = statusOf(r, opts.knownRedTag);
      counts[status]++;
      const failed = r.assert.checks.filter((c) => !c.ok);
      return {
         id: r.scenario.id,
         title: r.scenario.title,
         tags: r.scenario.tags,
         status,
         passedChecks: r.assert.checks.length - failed.length,
         totalChecks: r.assert.checks.length,
         failedChecks: failed.map((c) =>
            c.detail ? { name: c.name, detail: c.detail } : { name: c.name },
         ),
         ...(r.skipped
            ? { reason: r.skipped }
            : r.error
              ? { reason: r.error.split("\n")[0] }
              : {}),
         ...(r.ms !== undefined ? { ms: Math.round(r.ms) } : {}),
      };
   });
   return {
      version: 1,
      ok: !opts.harnessError && counts.fail === 0 && counts.fixed === 0,
      ...(opts.materialize !== undefined
         ? { materialize: opts.materialize }
         : {}),
      startedAt: opts.startedAt.toISOString(),
      finishedAt: opts.finishedAt.toISOString(),
      durationMs: opts.finishedAt.getTime() - opts.startedAt.getTime(),
      ...(opts.harnessError ? { harnessError: opts.harnessError } : {}),
      counts,
      scenarios,
   };
}

/** One line of a table cell: no pipes or newlines, and bounded. */
function cell(text: string, max = 300): string {
   const flat = text.replace(/\s+/g, " ").replace(/\|/g, "\\|").trim();
   return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
}

/**
 * The job summary: a one-line verdict, the counts, then every scenario that
 * failed (or passes under `known-red`) with each failed check. Passing and
 * known-red scenarios are counted, not listed, so a green run stays short.
 */
export function renderMarkdown(report: HammerReport, heading: string): string {
   const c = report.counts;
   const minutes = (report.durationMs / 60_000).toFixed(1);
   const verdict = report.ok ? "passed" : "FAILED";
   const lines = [
      `### ${heading}: ${verdict}`,
      "",
      `${c.pass} passed, ${c.fail} failed, ${c.fixed} known-red now passing, ` +
         `${c["known-red"]} known-red, ${c.skip} skipped; ${minutes} min.`,
   ];
   if (report.harnessError) {
      lines.push("", `**Harness error:** ${cell(report.harnessError, 1000)}`);
   }
   const bad = report.scenarios.filter(
      (s) => s.status === "fail" || s.status === "fixed",
   );
   if (bad.length) {
      lines.push(
         "",
         "| Scenario | Checks | Failed check | Detail |",
         "| --- | --- | --- | --- |",
      );
      for (const s of bad) {
         const checks = `${s.passedChecks}/${s.totalChecks}`;
         if (s.status === "fixed") {
            lines.push(
               `| \`${s.id}\` | ${checks} | (passes; retire its \`known-red\` tag) | |`,
            );
            continue;
         }
         const failed = s.failedChecks.length
            ? s.failedChecks
            : [{ name: "(no failed check recorded)", detail: s.reason }];
         failed.forEach((f, i) => {
            lines.push(
               `| ${i === 0 ? `\`${s.id}\`` : ""} | ${i === 0 ? checks : ""} | ${cell(f.name, 120)} | ${cell(f.detail ?? "")} |`,
            );
         });
      }
   }
   return `${lines.join("\n")}\n`;
}
