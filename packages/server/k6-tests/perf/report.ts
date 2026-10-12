// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Turns a perf results directory (written by perf/run.ts) into report.md:
// one table per scenario of requests, throughput, avg/p50/p95/p99/max and
// error rate per target × concurrency × op, the mixed-scenario interference
// comparison, the server-side histograms scraped from /metrics around each
// run, and the run context (version, SHA, machine, runtimes, package sizes).
//
//   bun perf/report.ts <results-dir>

import { readFileSync, writeFileSync } from "fs";
import path from "path";

export interface RunEntry {
   scenario: string;
   target: string;
   label: string;
   concurrency: number;
   loadOp?: string;
   durationS: number;
   wallMs: number;
   k6ExitCode: number;
   summary: string;
   k6Log: string;
   metricsBefore: string;
   metricsAfter: string;
   loadAvg1m: number[];
   loadErrorsAfter: number;
}

export interface RunContext {
   startedAt: string;
   finishedAt?: string;
   command: string;
   publisherVersion: string;
   gitSha: string;
   gitBranch: string;
   gitDirty: boolean;
   machine: {
      cpu: string;
      cores: number;
      memGiB: number;
      os: string;
      loadAvgStart: number[];
      loadAvgEnd?: number[];
   };
   runtimes: Record<string, string>;
   serverEnv: Record<string, string>;
   bootMs: number;
   options: Record<string, unknown>;
   packages: Record<string, unknown>;
}

// ── k6 summary ────────────────────────────────────────────────────────────

type SummaryMetric = Record<string, number | Record<string, boolean>>;
interface Summary {
   metrics: Record<string, SummaryMetric>;
}

interface OpRow {
   tags: Record<string, string>;
   count: number;
   failRate: number;
   avg: number;
   med: number;
   p95: number;
   p99: number;
   max: number;
}

function parseKey(key: string): {
   metric: string;
   tags: Record<string, string>;
} {
   const brace = key.indexOf("{");
   if (brace < 0) return { metric: key, tags: {} };
   const tags: Record<string, string> = {};
   for (const pair of key.slice(brace + 1, -1).split(",")) {
      const colon = pair.indexOf(":");
      tags[pair.slice(0, colon)] = pair.slice(colon + 1);
   }
   return { metric: key.slice(0, brace), tags };
}

const tagId = (tags: Record<string, string>): string =>
   Object.keys(tags)
      .sort()
      .map((k) => `${k}=${tags[k]}`)
      .join(",");

function opRows(summary: Summary): OpRow[] {
   const rows = new Map<string, OpRow>();
   const row = (tags: Record<string, string>): OpRow => {
      const id = tagId(tags);
      let r = rows.get(id);
      if (!r) {
         r = {
            tags,
            count: 0,
            failRate: 0,
            avg: 0,
            med: 0,
            p95: 0,
            p99: 0,
            max: 0,
         };
         rows.set(id, r);
      }
      return r;
   };
   for (const [key, m] of Object.entries(summary.metrics)) {
      const { metric, tags } = parseKey(key);
      if (!tags.op || tags.phase !== "measure") continue;
      const num = (k: string): number =>
         typeof m[k] === "number" ? (m[k] as number) : 0;
      if (metric === "http_reqs") row(tags).count = num("count");
      else if (metric === "http_req_failed") row(tags).failRate = num("value");
      else if (metric === "http_req_duration") {
         const r = row(tags);
         r.avg = num("avg");
         r.med = num("med");
         r.p95 = num("p(95)");
         r.p99 = num("p(99)");
         r.max = num("max");
      }
   }
   return [...rows.values()];
}

const OP_ORDER = [
   "register",
   "delete",
   "reload",
   "query_cheap",
   "query_medium",
   "query_heavy",
   "report_list",
   "report_get",
];
const opRank = (op: string | undefined): number => {
   const i = OP_ORDER.indexOf(op ?? "");
   return i < 0 ? OP_ORDER.length : i;
};

const ms = (n: number): string =>
   n >= 10_000
      ? `${(n / 1000).toFixed(1)}s`
      : n >= 100
        ? n.toFixed(0)
        : n.toFixed(1);
const pct = (n: number): string => (n === 0 ? "0" : `${(n * 100).toFixed(1)}`);

function table(headers: string[], rows: string[][]): string {
   const line = (cells: string[]) => `| ${cells.join(" | ")} |`;
   return [
      line(headers),
      line(
         headers.map((h) =>
            /^(target|op|window|loader|item|value|metric|labels)$/.test(h)
               ? "---"
               : "---:",
         ),
      ),
      ...rows.map(line),
   ].join("\n");
}

// ── Prometheus ────────────────────────────────────────────────────────────

/** Histogram families worth cross-checking client timings against. */
const SERVER_HISTOGRAMS = [
   /^malloy_package_load/,
   /^malloy_model_query_duration/,
   /^publisher_report_(discovery|read)_duration/,
   /^http_server_request_duration_ms/,
];

interface Series {
   family: string;
   labels: string;
   buckets: Map<number, number>;
   sum: number;
   count: number;
}

function parseProm(text: string): Map<string, Series> {
   const out = new Map<string, Series>();
   for (const line of text.split("\n")) {
      if (!line || line.startsWith("#")) continue;
      const m = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)/.exec(line);
      if (!m) continue;
      const [, name, rawLabels = "", rawValue] = m;
      const suffix = /_(bucket|sum|count)$/.exec(name!)?.[1];
      if (!suffix) continue;
      const family = name!.slice(0, -suffix.length - 1);
      if (!SERVER_HISTOGRAMS.some((re) => re.test(family))) continue;
      let le: number | undefined;
      // Tidy now, so series that differ only in a dropped label (the
      // per-package name on load durations) sum into one row.
      const labels = tidyLabels(rawLabels);
      if (suffix === "bucket") {
         const leRaw = /le="([^"]+)"/.exec(rawLabels)?.[1];
         le = leRaw === "+Inf" ? Infinity : Number(leRaw);
      }
      const key = `${family}{${labels}}`;
      let s = out.get(key);
      if (!s) {
         s = { family, labels, buckets: new Map(), sum: 0, count: 0 };
         out.set(key, s);
      }
      const value = Number(rawValue);
      if (suffix === "bucket") {
         s.buckets.set(le!, (s.buckets.get(le!) ?? 0) + value);
      } else if (suffix === "sum") s.sum += value;
      else s.count += value;
   }
   return out;
}

/**
 * The bucket a quantile falls in, from cumulative bucket deltas, as its upper
 * bound ("≤ 50"). Interpolating inside a bucket would invent precision the
 * histogram does not have: the buckets are coarse (5, 10, 25, 50 … ms).
 */
function bucketQuantile(
   buckets: [number, number][],
   total: number,
   q: number,
): string {
   const rank = q * total;
   let prevLe = 0;
   for (const [le, cum] of buckets) {
      if (cum >= rank)
         return le === Infinity ? `> ${ms(prevLe)}` : `≤ ${ms(le)}`;
      prevLe = le;
   }
   return `> ${ms(prevLe)}`;
}

/** Labels that are noise in a one-server, one-environment run. */
const DROP_LABELS = new Set([
   "le",
   "otel_scope_name",
   // One series per package name; the harness registers hundreds.
   "malloy_package_name",
   "malloy_environment",
   "malloy_model_query_connection",
]);

function tidyLabels(labels: string): string {
   return labels
      .split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/)
      .map((l) => {
         const eq = l.indexOf("=");
         return [
            l.slice(0, eq),
            l.slice(eq + 1).replace(/^"|"$/g, ""),
         ] as const;
      })
      .filter(([k, v]) => k && v !== "" && !DROP_LABELS.has(k))
      .map(
         ([k, v]) =>
            `${k.replace(/^(http_|malloy_model_query_|malloy_)/, "")}=${v
               .replace(
                  "/api/v0/environments/:environmentName/packages/:packageName",
                  "…/{pkg}",
               )
               .replace("/api/v0/environments/:environmentName", "…/{env}")}`,
      )
      .join(" ");
}

interface ServerRow {
   family: string;
   labels: string;
   count: number;
   avg: number;
   p50: string;
   p95: string;
}

function serverDeltas(beforeText: string, afterText: string): ServerRow[] {
   const before = parseProm(beforeText);
   const after = parseProm(afterText);
   const rows: ServerRow[] = [];
   for (const [key, a] of after) {
      const b = before.get(key);
      const count = a.count - (b?.count ?? 0);
      if (count <= 0) continue;
      const sum = a.sum - (b?.sum ?? 0);
      const buckets = [...a.buckets.entries()]
         .map(
            ([le, v]) =>
               [le, v - (b?.buckets.get(le) ?? 0)] as [number, number],
         )
         .sort((x, y) => x[0] - y[0]);
      rows.push({
         family: a.family,
         labels: a.labels,
         count,
         avg: sum / count,
         p50: bucketQuantile(buckets, count, 0.5),
         p95: bucketQuantile(buckets, count, 0.95),
      });
   }
   // http_server_request_duration_ms is per route; keep it to the routes the
   // harness exercises so the table stays readable.
   return rows
      .filter(
         (r) =>
            r.family !== "http_server_request_duration_ms" ||
            /route=…\//.test(r.labels),
      )
      .sort((x, y) => x.family.localeCompare(y.family) || y.count - x.count);
}

// ── Report ────────────────────────────────────────────────────────────────

const readJson = <T>(p: string): T => JSON.parse(readFileSync(p, "utf8")) as T;

export function buildReport(dir: string): string {
   const context = readJson<RunContext>(path.join(dir, "context.json"));
   const runs = readJson<RunEntry[]>(path.join(dir, "runs.json"));
   const out: string[] = [];

   out.push(`# Publisher perf run — ${context.startedAt}`, "");
   out.push(
      `Publisher **${context.publisherVersion}** at \`${context.gitSha.slice(0, 12)}\` (${context.gitBranch}${context.gitDirty ? ", dirty" : ""}).`,
      "",
   );
   out.push("## Context", "");
   const m = context.machine;
   out.push(
      table(
         ["item", "value"],
         [
            ["command", `\`${context.command}\``],
            ["machine", `${m.cpu}, ${m.cores} cores, ${m.memGiB} GiB, ${m.os}`],
            [
               "load avg (1/5/15m) start → end",
               `${m.loadAvgStart.join(" / ")} → ${(m.loadAvgEnd ?? []).join(" / ")}`,
            ],
            [
               "runtimes",
               Object.entries(context.runtimes)
                  .map(([k, v]) => `${k} ${v}`)
                  .join("; "),
            ],
            [
               "server env",
               Object.entries(context.serverEnv)
                  .map(([k, v]) => `${k}=${v}`)
                  .join("; "),
            ],
            ["server boot to READY", `${context.bootMs} ms`],
            ["options", `\`${JSON.stringify(context.options)}\``],
            ...Object.entries(context.packages).map(([k, v]) => [
               `package ${k}`,
               `\`${JSON.stringify(v)}\``,
            ]),
         ],
      ),
      "",
   );

   const byScenario = new Map<string, RunEntry[]>();
   for (const r of runs) {
      byScenario.set(r.scenario, [...(byScenario.get(r.scenario) ?? []), r]);
   }

   for (const [scenario, entries] of byScenario) {
      out.push(`## Scenario: ${scenario}`, "");
      const isMixed = scenario === "mixed";
      const headers = isMixed
         ? [
              "target",
              "rate/s",
              "window",
              "loader",
              "op",
              "reqs",
              "req/s",
              "avg",
              "p50",
              "p95",
              "p99",
              "max",
              "err %",
           ]
         : [
              "target",
              "conc",
              "op",
              "reqs",
              "req/s",
              "avg",
              "p50",
              "p95",
              "p99",
              "max",
              "err %",
           ];
      const rows: string[][] = [];
      const notes: string[] = [];
      const compare: string[][] = [];
      for (const e of entries) {
         const summary = readJson<Summary>(path.join(dir, e.summary));
         const windowMs =
            (summary.metrics["perf_measure_window_ms"]?.["value"] as
               | number
               | undefined) ?? e.durationS * 1000;
         const dropped = summary.metrics["dropped_iterations"]?.["count"];
         if (typeof dropped === "number" && dropped > 0) {
            notes.push(
               `- ${e.target} ${e.label}: k6 dropped ${dropped} iterations (arrival rate not sustained).`,
            );
         }
         if (e.k6ExitCode !== 0) {
            notes.push(
               `- ${e.target} ${e.label}: k6 exited ${e.k6ExitCode}${e.k6ExitCode === 99 ? " (error-rate threshold crossed)" : ""}; see ${e.k6Log}.`,
            );
         }
         const ops = opRows(summary).sort(
            (a, b) =>
               (a.tags.window ?? "").localeCompare(b.tags.window ?? "") ||
               (a.tags.loader ?? "").localeCompare(b.tags.loader ?? "") ||
               opRank(a.tags.op) - opRank(b.tags.op),
         );
         for (const r of ops) {
            if (r.count === 0) continue;
            const seconds = isMixed ? e.durationS : windowMs / 1000;
            const stats = [
               String(r.count),
               (r.count / seconds).toFixed(2),
               ms(r.avg),
               ms(r.med),
               ms(r.p95),
               ms(r.p99),
               ms(r.max),
               pct(r.failRate),
            ];
            rows.push(
               isMixed
                  ? [
                       e.target,
                       String(e.concurrency),
                       r.tags.window ?? "",
                       r.tags.loader ?? "",
                       r.tags.op!,
                       ...stats,
                    ]
                  : [e.target, String(e.concurrency), r.tags.op!, ...stats],
            );
         }
         if (isMixed) {
            for (const op of OP_ORDER) {
               const base = ops.find(
                  (r) =>
                     r.tags.op === op &&
                     r.tags.window === "baseline" &&
                     !r.tags.loader,
               );
               const load = ops.find(
                  (r) =>
                     r.tags.op === op &&
                     r.tags.window === "under_load" &&
                     !r.tags.loader,
               );
               if (!base || !load || base.count === 0 || load.count === 0)
                  continue;
               const ratio = (a: number, b: number) =>
                  b > 0 ? `${(a / b).toFixed(2)}×` : "–";
               compare.push([
                  e.target,
                  op,
                  ms(base.avg),
                  ms(load.avg),
                  ratio(load.avg, base.avg),
                  ms(base.p95),
                  ms(load.p95),
                  ratio(load.p95, base.p95),
               ]);
            }
         }
      }
      out.push(table(headers, rows), "");
      if (isMixed && compare.length > 0) {
         out.push(
            "Query latency while packages load, against the baseline window of the same run:",
            "",
            table(
               [
                  "target",
                  "op",
                  "avg base",
                  "avg load",
                  "avg ×",
                  "p95 base",
                  "p95 load",
                  "p95 ×",
               ],
               compare,
            ),
            "",
         );
      }
      if (notes.length > 0) out.push(...notes, "");

      out.push(
         `<details><summary>Server-side histograms (/metrics deltas) — ${scenario}</summary>`,
         "",
      );
      for (const e of entries) {
         const srows = serverDeltas(
            readFileSync(path.join(dir, e.metricsBefore), "utf8"),
            readFileSync(path.join(dir, e.metricsAfter), "utf8"),
         );
         out.push(
            `**${e.target} ${e.label}** (load avg 1m ${e.loadAvg1m.join(" → ")}, wall ${(e.wallMs / 1000).toFixed(1)}s incl. warm-up)`,
            "",
         );
         if (srows.length === 0) {
            out.push("_no matching histogram samples_", "");
            continue;
         }
         out.push(
            table(
               [
                  "metric",
                  "labels",
                  "count",
                  "avg ms",
                  "p50 bucket",
                  "p95 bucket",
               ],
               srows.map((s) => [
                  s.family,
                  s.labels.replace(/\|/g, "\\|") || "–",
                  String(s.count),
                  ms(s.avg),
                  s.p50,
                  s.p95,
               ]),
            ),
            "",
         );
      }
      out.push("</details>", "");
   }

   out.push(
      "Latencies are client-side k6 `http_req_duration` in ms (s where noted), measured phase only; warm-up excluded. " +
         "req/s is completed requests over the measured window. Server-side counts include warm-up, so they exceed the client counts; " +
         "server quantiles name the histogram bucket they fall in.",
      "",
   );
   return out.join("\n");
}

export function writeReport(dir: string): string {
   const report = buildReport(dir);
   writeFileSync(path.join(dir, "report.md"), report);
   return report;
}

if (import.meta.main) {
   const dir = process.argv[2];
   if (!dir) {
      console.error("usage: bun perf/report.ts <results-dir>");
      process.exit(2);
   }
   console.log(writeReport(path.resolve(dir)));
}
