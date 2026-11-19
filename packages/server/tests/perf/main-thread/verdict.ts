// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The verdict on a baseline/candidate A/B of storm runs, and its summary.
 * Pure: `run.ts` feeds it the per-round results. README.md says why each
 * limit is where it is; the calibration runs behind them are in the PR that
 * added this harness.
 */

import type { StormResult } from "./storm";

export interface Limits {
   /** Candidate main-thread CPU per op over the baseline's, at most. */
   cpuPerOpRatio: number;
   /** Candidate `/status` p90 over the baseline's, at most... */
   statusP90Ratio: number;
   /** ...once the candidate's p90 is above this (a 30 -> 70 ms move is noise). */
   statusP90RatioFloorMs: number;
   /** Candidate `/status` p90 under the storm, at most (ms). */
   statusP90BudgetMs: number;
   /** Candidate's longest single event-loop block, at most (ms). */
   maxBlockBudgetMs: number;
   /** Main-thread Malloy compiles on identical re-binds, at most. */
   rebindCompiles: number;
   /** Load-pool dispatches from `/status` polls, at most. */
   statusLoads: number;
   /**
    * Main-thread QueryModel builds in one load of the indexed fixture (120
    * `#(index)` dimensions), at most: 0.12.1-0.12.5 built one per dimension
    * (ms2data/service#7641).
    */
   indexedLoadQueryModelBuilds: number;
}

/** What `run.ts` knows about the machine, for the runner check. */
export interface RunEnv {
   cpus?: number;
   /** When set (CI), a different CPU count makes the run invalid. */
   expectCpus?: number;
   /**
    * When the baseline is the anchor release: the ranges it measured on the
    * calibrated runner (anchor.json). A baseline median beyond twice their
    * top means the runner, not the candidate, changed.
    */
   baselineRange?: {
      statusP90Ms: [number, number];
      maxBlockMs: [number, number];
   };
}

// Calibrated on GitHub-hosted ubuntu-latest (4 vCPU), 3 A/B jobs each of
// 0.12.6 vs 0.13.1, 0.12.6 vs 0.12.6 and 0.12.6 vs the three
// ms2data/service#7795 fixes merged, two rounds a side (README.md):
//  - /status p90, with the indexed package placed (18 A/Bs on main after
//    #1779): 0.12.6 689-1213 ms, main 149-332, 0.13.1 1345-2415. The budget
//    is 1000 ms: main has 3x headroom, 0.13.1 is over it in every run, and
//    0.12.6 is often over it too, which is why a null A/B of 0.12.6 runs
//    with `--limits '{"statusP90BudgetMs":1600}'`. Before the indexed
//    package: 0.12.6 373-728, 0.13.1 1516-2392.
//  - ratios over 45 A/Bs: null and fixed pairs at most 1.13x CPU and 1.40x
//    p90; 0.13.1 at least 1.33x CPU and 1.80x p90, so 1.3x and 1.6x. If PR
//    runs show noise, the p90 ratio moves to 1.75x before the CPU one moves.
//  - longest block: 0.12.6 724-2211 ms, fixed 854-1778, 0.13.1 963-2211. It
//    does not separate them (the common cost is hydrating the large generated
//    package), so it only guards against a gross stall, like 0.12.5's 47 s.
//  - indexed load: 0.12.0, 0.12.6, 0.13.1 and main build 4 QueryModels on
//    the main thread; 0.12.1 and 0.12.5 build 256 (252 fingerprints).
export const DEFAULT_LIMITS: Limits = {
   cpuPerOpRatio: 1.3,
   statusP90Ratio: 1.6,
   statusP90RatioFloorMs: 150,
   statusP90BudgetMs: 1000,
   maxBlockBudgetMs: 3000,
   rebindCompiles: 0,
   statusLoads: 0,
   indexedLoadQueryModelBuilds: 20,
};

export type Outcome = "pass" | "fail" | "invalid";

export interface Check {
   name: string;
   kind: "relative" | "absolute" | "validity";
   outcome: Outcome;
   /** The value judged, as shown. */
   value: string;
   limit: string;
   detail?: string;
}

export interface Verdict {
   outcome: Outcome;
   checks: Check[];
}

export function median(xs: number[]): number {
   if (xs.length === 0) return NaN;
   const s = [...xs].sort((a, b) => a - b);
   const m = Math.floor(s.length / 2);
   return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

const ms = (n: number) => `${Math.round(n)} ms`;
const x = (n: number) => `${n.toFixed(2)}x`;

/**
 * Judge the candidate against the baseline. Every per-side figure is the
 * median over its rounds, except the two counts, where any round over the
 * limit fails: a compile or a load that should never happen is not noise.
 */
export function judge(
   baseline: StormResult[],
   candidate: StormResult[],
   limits: Limits = DEFAULT_LIMITS,
   env: RunEnv = {},
): Verdict {
   const checks: Check[] = [];
   if (baseline.length === 0 || candidate.length === 0) {
      return {
         outcome: "invalid",
         checks: [
            {
               name: "both sides ran",
               kind: "validity",
               outcome: "invalid",
               value: `baseline ${baseline.length} round(s), candidate ${candidate.length}`,
               limit: "at least one round each",
            },
         ],
      };
   }
   if (env.expectCpus !== undefined && env.cpus !== env.expectCpus) {
      checks.push({
         name: "the runner is the calibrated kind",
         kind: "validity",
         outcome: "invalid",
         value: `${env.cpus} CPUs`,
         limit: `${env.expectCpus} CPUs (the absolute budgets were calibrated there)`,
      });
   }
   const med = (rs: StormResult[], f: (r: StormResult) => number) =>
      median(rs.map(f));

   // Validity first: a run that did not do the work proves nothing.
   const validity = (side: string, rs: StormResult[]) => {
      for (const r of rs) {
         const problems: string[] = [];
         if (r.completedOps !== r.options.ops) {
            problems.push(`${r.completedOps} of ${r.options.ops} ops answered`);
         }
         if (r.loadDispatches === 0) {
            problems.push("the probe saw no load dispatch (hook broken?)");
         }
         if (r.status.n < 10)
            problems.push(`only ${r.status.n} /status samples`);
         if (!r.rebind.ok)
            problems.push("the re-bind check's binds did not succeed");
         // Positive controls: each counter must be able to count.
         const broken = Object.entries(r.hooks ?? {})
            .filter(([, ok]) => !ok)
            .map(([h]) => h);
         if (!r.hooks || broken.length) {
            problems.push(
               `probe hooks that did not take: ${broken.join(", ") || "all"}`,
            );
         }
         if ((r.control?.queryCompiles ?? 0) < 1) {
            problems.push(
               "an ad-hoc query counted no compile, so a zero re-bind count proves nothing",
            );
         }
         if ((r.queryModel?.builds ?? 0) < 1) {
            problems.push(
               "the storm counted no QueryModel build, so a zero indexed-load count proves nothing",
            );
         }
         if ((r.quietMs ?? []).some((q) => q < 0)) {
            problems.push("the server never went quiet before a quiet check");
         }
         if (r.indexedLoad?.ok && (r.indexedLoad.builds ?? 0) < 1) {
            problems.push(
               "the indexed load's window counted no QueryModel build, so its zero proves nothing",
            );
         }
         if (!r.indexedLoad?.ok) {
            problems.push(
               "the quiet load of the indexed package did not succeed",
            );
         }
         if (problems.length) {
            checks.push({
               name: `${side} run ${r.label} did the work`,
               kind: "validity",
               outcome: "invalid",
               value: problems.join("; "),
               limit: "all ops answered, loads seen, re-binds bound, counters proven",
            });
         }
      }
   };
   validity("baseline", baseline);
   validity("candidate", candidate);
   // The same workload on both sides: every op type answered OK about as
   // often. An op the baseline never answers OK (a route it predates, like
   // /entities before 0.12.1) is noted, not compared; one the baseline serves
   // and the candidate never does is a different workload.
   const ops = new Set(
      [...baseline, ...candidate].flatMap((r) => Object.keys(r.ops)),
   );
   const mismatched: string[] = [];
   const absent: string[] = [];
   for (const op of [...ops].sort()) {
      const b = med(baseline, (r) => r.ops[op]?.ok ?? 0);
      const c = med(candidate, (r) => r.ops[op]?.ok ?? 0);
      if (b === 0) {
         if (c !== 0) absent.push(`${op} (baseline 0 ok, candidate ${c} ok)`);
         continue;
      }
      if (c === 0) {
         mismatched.push(`${op}: baseline ${b} ok, candidate none`);
         continue;
      }
      if (Math.abs(c - b) > Math.max(3, 0.1 * b)) {
         mismatched.push(`${op}: baseline ${b} ok, candidate ${c} ok`);
      }
   }
   if (mismatched.length) {
      checks.push({
         name: "both sides ran the same workload",
         kind: "validity",
         outcome: "invalid",
         value: mismatched.join("; "),
         limit: "each op type's successes within 10% (or 3)",
         detail:
            "A side refusing work the other accepts changes what CPU per op means; the failure mix is in the table below and the server logs are in the artifact.",
      });
   }
   if (absent.length) {
      checks.push({
         name: "ops the baseline does not serve",
         kind: "validity",
         outcome: "pass",
         value: absent.join("; "),
         limit: "noted, not compared",
      });
   }

   // An anchor baseline far outside what it measured on the calibrated
   // runner means the runner, not the candidate, changed.
   if (env.baselineRange) {
      const bBlock = med(baseline, (r) => r.maxBlockMs);
      const bP90s = med(baseline, (r) => r.status.p90);
      const p90Top = 2 * env.baselineRange.statusP90Ms[1];
      const blockTop = 2 * env.baselineRange.maxBlockMs[1];
      if (bP90s > p90Top || bBlock > blockTop) {
         checks.push({
            name: "the anchor baseline ran as calibrated",
            kind: "validity",
            outcome: "invalid",
            value: `baseline /status p90 ${ms(bP90s)}, longest block ${ms(bBlock)}`,
            limit: `p90 <= ${ms(p90Top)} and block <= ${ms(blockTop)} (2x its calibrated top, anchor.json)`,
            detail:
               "The anchor ran far slower than it did when calibrated: an overloaded or different runner. Re-run; if it repeats, recalibrate.",
         });
      }
   }

   const bCpu = med(baseline, (r) => r.cpuPerOpMs);
   const cCpu = med(candidate, (r) => r.cpuPerOpMs);
   const cpuRatio = cCpu / bCpu;
   checks.push({
      name: "main-thread CPU per op vs baseline",
      kind: "relative",
      outcome: cpuRatio > limits.cpuPerOpRatio ? "fail" : "pass",
      value: `${x(cpuRatio)} (${cCpu.toFixed(1)} vs ${bCpu.toFixed(1)} ms/op)`,
      limit: `<= ${x(limits.cpuPerOpRatio)}`,
   });

   const bP90 = med(baseline, (r) => r.status.p90);
   const cP90 = med(candidate, (r) => r.status.p90);
   const p90Ratio = cP90 / Math.max(1, bP90);
   checks.push({
      name: "/status p90 vs baseline",
      kind: "relative",
      outcome:
         p90Ratio > limits.statusP90Ratio && cP90 > limits.statusP90RatioFloorMs
            ? "fail"
            : "pass",
      value: `${x(p90Ratio)} (${ms(cP90)} vs ${ms(bP90)})`,
      limit: `<= ${x(limits.statusP90Ratio)} (above ${ms(limits.statusP90RatioFloorMs)})`,
   });

   checks.push({
      name: "/status p90 under the storm",
      kind: "absolute",
      outcome: cP90 > limits.statusP90BudgetMs ? "fail" : "pass",
      value: ms(cP90),
      limit: `<= ${ms(limits.statusP90BudgetMs)}`,
   });

   const cBlock = med(candidate, (r) => r.maxBlockMs);
   checks.push({
      name: "longest event-loop block",
      kind: "absolute",
      outcome: cBlock > limits.maxBlockBudgetMs ? "fail" : "pass",
      value: ms(cBlock),
      limit: `<= ${ms(limits.maxBlockBudgetMs)}`,
   });

   const compiles = Math.max(...candidate.map((r) => r.rebind.compiles));
   const callers = mergeCounts(candidate.map((r) => r.rebind.compilesByCaller));
   checks.push({
      name: "main-thread Malloy compiles on an identical re-bind",
      kind: "absolute",
      outcome: compiles > limits.rebindCompiles ? "fail" : "pass",
      value: `${compiles} per ${candidate[0]?.rebind.rebinds ?? 0} re-binds`,
      limit: `<= ${limits.rebindCompiles}`,
      detail: Object.keys(callers).length
         ? `main-thread Malloy compiles by caller: ${formatCounts(callers)}`
         : undefined,
   });

   const loads = Math.max(
      ...candidate.map((r) => r.statusLoads.loadDispatches),
   );
   checks.push({
      name: "loads started by /status",
      kind: "absolute",
      outcome: loads > limits.statusLoads ? "fail" : "pass",
      value: `${loads} in ${candidate[0]?.statusLoads.polls ?? 0} polls`,
      limit: `<= ${limits.statusLoads}`,
   });

   const qm = Math.max(...candidate.map((r) => r.indexedLoad?.builds ?? 0));
   const dims = candidate[0]?.indexedLoad?.indexedDimensions ?? 0;
   const qmCallers = mergeCounts(
      candidate.map((r) => r.indexedLoad?.byCaller ?? {}),
   );
   checks.push({
      name: "main-thread QueryModel builds on an indexed load",
      kind: "absolute",
      outcome: qm > limits.indexedLoadQueryModelBuilds ? "fail" : "pass",
      value: `${qm} for ${dims} #(index) dimensions`,
      limit: `<= ${limits.indexedLoadQueryModelBuilds}`,
      detail: Object.keys(qmCallers).length
         ? `QueryModel builds by caller: ${formatCounts(qmCallers)}`
         : undefined,
   });

   const outcome: Outcome = checks.some((c) => c.outcome === "invalid")
      ? "invalid"
      : checks.some((c) => c.outcome === "fail")
        ? "fail"
        : "pass";
   return { outcome, checks };
}

function mergeCounts(maps: Record<string, number>[]): Record<string, number> {
   const out: Record<string, number> = {};
   for (const m of maps) {
      for (const [k, v] of Object.entries(m)) out[k] = (out[k] ?? 0) + v;
   }
   return out;
}

function failureMix(r: StormResult): string {
   return Object.entries(r.ops)
      .flatMap(([op, o]) =>
         Object.entries(o.failed).map(([code, n]) => `${op} ${code}x${n}`),
      )
      .join(", ");
}

function formatCounts(m: Record<string, number>): string {
   return Object.entries(m)
      .sort((a, b) => b[1] - a[1])
      .map(([k, v]) => `${k} ${v}`)
      .join(", ");
}

const ICON: Record<Outcome, string> = {
   pass: "PASS",
   fail: "**FAIL**",
   invalid: "**INVALID**",
};

/** The markdown summary: verdict, checks, then each round side by side. */
export function summarize(
   v: Verdict,
   baseline: StormResult[],
   candidate: StormResult[],
   names: { baseline: string; candidate: string },
): string {
   const out: string[] = [];
   out.push(
      `## Main-thread regression: ${ICON[v.outcome]}`,
      "",
      `Candidate \`${names.candidate}\` against baseline \`${names.baseline}\`, ` +
         `${candidate.length} round(s) each, alternated. ` +
         `Each figure is the median over rounds; see packages/server/tests/perf/main-thread/README.md.`,
      "",
      "| check | kind | value | limit | result |",
      "|---|---|---|---|---|",
   );
   for (const c of v.checks) {
      out.push(
         `| ${c.name} | ${c.kind} | ${c.value} | ${c.limit} | ${ICON[c.outcome]} |`,
      );
   }
   const details = v.checks.filter((c) => c.detail);
   if (details.length) {
      out.push("");
      for (const c of details) out.push(`- ${c.name}: ${c.detail}`);
   }
   out.push("", "| metric | baseline | candidate |", "|---|---|---|");
   const row = (label: string, f: (r: StormResult) => string) =>
      out.push(
         `| ${label} | ${baseline.map(f).join(" / ")} | ${candidate.map(f).join(" / ")} |`,
      );
   row("server version", (r) => r.version ?? "?");
   row("main-thread CPU per op", (r) => `${r.cpuPerOpMs} ms`);
   row(
      "main-thread CPU total",
      (r) => `${(r.mainThreadCpuMs / 1000).toFixed(1)} s`,
   );
   row("process CPU total", (r) => `${(r.processCpuMs / 1000).toFixed(1)} s`);
   row("/status p50", (r) => `${r.status.p50} ms`);
   row("/status p90", (r) => `${r.status.p90} ms`);
   row("/status max", (r) => `${r.status.max} ms`);
   row("/status samples", (r) => String(r.status.n));
   row("longest event-loop block", (r) => `${r.maxBlockMs} ms`);
   row("blocks >= 250 ms", (r) => String(r.blocksOver["250"] ?? 0));
   row("re-bind compiles", (r) => String(r.rebind.compiles));
   row("loads started by /status", (r) => String(r.statusLoads.loadDispatches));
   row(
      "indexed-load QueryModel builds",
      (r) => `${r.indexedLoad.builds} (${r.indexedLoad.ms} ms)`,
   );
   row(
      "storm QueryModel builds",
      (r) => `${r.queryModel.builds} (${r.queryModel.ms} ms)`,
   );
   row("ad-hoc query compiles (control)", (r) =>
      String(r.control.queryCompiles),
   );
   row("failed ops", (r) => failureMix(r) || "none");
   row("storm wall time", (r) => `${(r.wallMs / 1000).toFixed(0)} s`);
   row("ops answered", (r) => String(r.completedOps));
   row("peak RSS", (r) => `${(r.maxRssBytes / 1024 ** 3).toFixed(2)} GiB`);
   out.push(
      "",
      "Rounds are listed in run order within each side. To see what the main thread did, " +
         "rerun locally with `--cpu-prof` and compare the two profiles (README.md, *Reading a failure*).",
   );
   return out.join("\n") + "\n";
}
