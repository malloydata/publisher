// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The conformance fixture (ms2data/service#7098): synthetic packages shaped
 * like real ones, each served three ways (v1 `#@ persist` with `storage=`,
 * v1 colocated, and `#(materialize)`), with the materialize switch off
 * (today's serve shape) and on (the served copy), through the real package
 * path (conformance/package_harness.ts). Every query's served answer must
 * equal live, rows and result metadata (field types with their timeframes,
 * and annotations), and a stored table must have answered it.
 *
 * It is the gate the v1 cutover (materialization v2 phase 3, PR 6) runs on
 * every pull request (plan, question 15). A case main does not serve yet is
 * a declared gap in conformance/shapes.ts, skipped with its reason, so the
 * gap list is readable from the test names. Two metadata differences common
 * to every stored read are tolerated per case and gated once each, below
 * (TOLERANCES). Environment switches:
 *
 *  - CONFORMANCE_GAPS=run asserts the gaps and tolerances too, to see which
 *    have closed;
 *  - CONFORMANCE_REPORT=1 prints every case's outcome as a table.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   type Answer,
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   type Switch,
   withSwitch,
   type WireField,
} from "./conformance/package_harness";
import {
   type ConformanceQuery,
   type Expect,
   type Gap,
   type GapMap,
   renderFiles,
   type Shape,
   SHAPES,
   type Variant,
   VARIANTS,
} from "./conformance/shapes";
import { rowDifference } from "./harness";

const RUN_GAPS = process.env.CONFORMANCE_GAPS === "run";
const REPORT = process.env.CONFORMANCE_REPORT === "1";
const SWITCHES: Switch[] = ["off", "on"];

/**
 * Metadata differences every stored read has on main, tolerated per case so
 * each case still says whether its rows, tier and remaining metadata match,
 * and gated once each at the end of the file.
 */
const TOLERANCES = {
   "number-subtype": {
      reason:
         "a number read from a storage= table loses its integer/decimal subtype (the serve shape and the served copy alike)",
   },
   "drill-metadata": {
      reason:
         "the served copy's #(malloy) drill_expression differs from live's for a dimension it stores (its code is the stored column, or the definition, not live's)",
   },
} as const;
type Tolerance = keyof typeof TOLERANCES;

/** The cases that needed each tolerance, filled as the cases run. */
const tolerated: Record<Tolerance, string[]> = {
   "number-subtype": [],
   "drill-metadata": [],
};

function withoutSubtype(type: unknown): unknown {
   if (type && typeof type === "object" && "subtype" in type) {
      const { subtype: _subtype, ...rest } = type as Record<string, unknown>;
      return rest;
   }
   return type;
}

const isDrill = (a: string) => a.startsWith("#(malloy)");

/**
 * The first difference in result metadata, or undefined. With tolerances,
 * a number's subtype and the `#(malloy)` drill annotations are not compared.
 */
function fieldDifference(
   live: Answer,
   served: Answer,
   tolerate: readonly Tolerance[] = [],
): string | undefined {
   const a = live.fields;
   const b = served.fields;
   if (a.length !== b.length) {
      return `fields ${a.map((f) => f.name).join(",")} against ${b.map((f) => f.name).join(",")}`;
   }
   const type = (f: WireField) =>
      tolerate.includes("number-subtype") ? withoutSubtype(f.type) : f.type;
   const notes = (f: WireField) =>
      tolerate.includes("drill-metadata")
         ? f.annotations.filter((n) => !isDrill(n))
         : f.annotations;
   for (let i = 0; i < a.length; i++) {
      const pairs: [string, unknown, unknown][] = [
         ["name", a[i].name, b[i].name],
         ["type", type(a[i]), type(b[i])],
         ["annotations", notes(a[i]), notes(b[i])],
      ];
      for (const [key, x, y] of pairs) {
         if (JSON.stringify(x) !== JSON.stringify(y)) {
            return `${a[i].name}.${key}: ${JSON.stringify(x)} against ${JSON.stringify(y)}`;
         }
      }
   }
   return undefined;
}

/** The tolerances a served answer needs to match live's metadata. */
function tolerancesNeeded(live: Answer, served: Answer): Tolerance[] {
   const subsets: Tolerance[][] = [
      [],
      ["number-subtype"],
      ["drill-metadata"],
      ["number-subtype", "drill-metadata"],
   ];
   // The smallest that matches; all of them when none does, so a failure
   // names the difference no tolerance covers.
   return (
      subsets.find((s) => fieldDifference(live, served, s) === undefined) ??
      subsets[subsets.length - 1]
   );
}

function tierOf(answer: Answer): string {
   if (answer.servedBy !== "materialized") return answer.servedBy;
   if (answer.via) return `served:${answer.via}`;
   return answer.servedFrom === "storage"
      ? "served:serve_shape"
      : "served:colocated";
}

/** What a case's served run did, for the report. */
function describeOutcome(live: Outcome, served: Outcome): string {
   if ("error" in served) return `error: ${served.error.split("\n")[0]}`;
   if ("error" in live) return `live error: ${live.error.split("\n")[0]}`;
   const tier = tierOf(served.answer);
   const rows = rowDifference(live.answer.rows, served.answer.rows);
   if (rows) return `wrong rows (${tier}): ${rows}`;
   const all = Object.keys(TOLERANCES) as Tolerance[];
   const fields = fieldDifference(live.answer, served.answer, all);
   if (fields) return `wrong metadata (${tier}): ${fields}`;
   const needed = tolerancesNeeded(live.answer, served.answer);
   return needed.length ? `${tier} [${needed.join(", ")}]` : tier;
}

function expectedTier(
   query: ConformanceQuery,
   variant: Variant,
   on: Switch,
): Expect {
   // `#(materialize)` is served only by the served copy, which the switch turns on.
   if (variant === "materialize" && on === "off") return "live";
   return typeof query.expect === "string"
      ? query.expect
      : (query.expect?.[variant] ?? "served");
}

function gapFor(
   shape: Shape,
   query: { gaps?: GapMap },
   variant: Variant,
   on: Switch,
): Gap | undefined {
   for (const map of [query.gaps, shape.gaps]) {
      const gap = map?.[`${variant}/${on}`] ?? map?.[variant] ?? map?.["*"];
      if (gap) return gap;
   }
   return undefined;
}

function entryOf(shape: Shape): string {
   return Object.keys(shape.files)[0];
}

const reportRows: string[] = [];

// Every fixture loads with the switch on; its workers must plan with it on.
beforeAll(freshLoadPool);

afterAll(async () => {
   await freshLoadPool();
   if (!REPORT) return;
   console.log(
      [
         "| shape | variant | switch | query | outcome |",
         "| --- | --- | --- | --- | --- |",
         ...reportRows,
      ].join("\n"),
   );
});

for (const shape of SHAPES) {
   for (const variant of shape.variants ?? VARIANTS) {
      describe(`conformance: ${shape.name} [${variant}]`, () => {
         let restore: () => void = () => {};
         let fixture: LoadedFixture | undefined;
         let loadError: string | undefined;
         const live = new Map<string, Outcome>();
         const served = new Map<string, Outcome>();
         const key = (q: ConformanceQuery, on?: Switch) =>
            on ? `${q.name}/${on}` : q.name;
         const runIn = (
            target: { run: LoadedFixture["run"] },
            q: ConformanceQuery,
         ) =>
            target.run(q.model ?? entryOf(shape), q.malloy, q.givens as never);

         beforeAll(async () => {
            // Loaded with the switch on, so `#(materialize)` is planned; the
            // router reads the switch per query, so both are queried here.
            restore = withSwitch("on");
            try {
               const loader = await loadFixture({
                  ...DATA_FILES,
                  ...renderFiles(shape, variant),
               });
               for (const q of shape.queries) {
                  live.set(key(q), await runIn(loader, q));
               }
               fixture = await loader.bind();
               for (const on of SWITCHES) {
                  restore();
                  restore = withSwitch(on);
                  for (const q of shape.queries) {
                     served.set(key(q, on), await runIn(fixture, q));
                  }
               }
            } catch (error) {
               loadError =
                  error instanceof Error ? error.message : String(error);
            }
            if (REPORT) report();
         });

         function report() {
            const row = (on: string, query: string, outcome: string) =>
               reportRows.push(
                  `| ${shape.name} | ${variant} | ${on} | ${query} | ${outcome
                     .replace(/\\/g, "\\\\")
                     .replace(/\|/g, "\\|")
                     .replace(/\n/g, " ")} |`,
               );
            if (loadError) {
               row("-", "(load)", `load error: ${loadError}`);
               return;
            }
            for (const on of SWITCHES) {
               for (const q of shape.queries) {
                  row(
                     on,
                     q.name,
                     describeOutcome(
                        live.get(key(q))!,
                        served.get(key(q, on))!,
                     ),
                  );
               }
            }
            if (fixture) {
               const errors = Object.entries(fixture.buildErrors);
               row(
                  "-",
                  "(package)",
                  [
                     `verdicts ${fixture.verdicts.join(", ")}`,
                     `built ${fixture.built.map((b) => `${b.name}/${b.tier}`).join(", ")}`,
                     ...(errors.length
                        ? [`build errors ${JSON.stringify(errors)}`]
                        : []),
                     `warnings ${fixture.warnings.map((w) => `${w.code}:${w.subject}: ${(w.message ?? "").slice(0, 200)}`).join(" ;; ")}`,
                  ].join("; "),
               );
            }
         }

         afterAll(async () => {
            restore();
            await fixture?.dispose();
         });

         for (const on of SWITCHES) {
            for (const q of shape.queries) {
               const want = expectedTier(q, variant, on);
               const gap = gapFor(shape, q, variant, on);
               const title = `${q.name}, switch ${on}: ${want === "colocated" ? "served" : want}, equal to live`;
               const test = () => {
                  expect(loadError).toBeUndefined();
                  const l = live.get(key(q))!;
                  const s = served.get(key(q, on))!;
                  if ("error" in l) throw new Error(`live failed: ${l.error}`);
                  if ("error" in s) {
                     throw new Error(`served failed: ${s.error}`);
                  }
                  expect(
                     rowDifference(l.answer.rows, s.answer.rows),
                  ).toBeUndefined();
                  const needed = tolerancesNeeded(l.answer, s.answer);
                  expect(
                     fieldDifference(l.answer, s.answer, needed),
                  ).toBeUndefined();
                  for (const t of needed) {
                     tolerated[t].push(
                        `${shape.name} [${variant}] ${q.name}, switch ${on}`,
                     );
                  }
                  if (want === "live") {
                     expect(s.answer.servedBy).not.toBe("materialized");
                     return;
                  }
                  expect(s.answer.servedBy).toBe("materialized");
                  if (variant === "colocated" || want === "colocated") {
                     expect(s.answer.servedFrom).toBeNull();
                     return;
                  }
                  expect(s.answer.servedFrom).toBe("storage");
                  // The switch decides which storage path answers.
                  expect(s.answer.via).toBe(
                     on === "on" ? "served_copy" : undefined,
                  );
               };
               if (gap && !RUN_GAPS) {
                  it.skip(
                     `${title} [gap ${gap.kind}${gap.issue ? ` ${gap.issue}` : ""}: ${gap.reason}]`,
                     test,
                  );
               } else {
                  it(title, test);
               }
            }
         }
      });
   }
}

describe("conformance: metadata tolerated on every stored read", () => {
   for (const [t, { reason }] of Object.entries(TOLERANCES) as [
      Tolerance,
      { reason: string },
   ][]) {
      const test = () => {
         expect(tolerated[t]).toEqual([]);
      };
      if (RUN_GAPS) it(`no case needs ${t}`, test);
      else it.skip(`no case needs ${t} [gap tolerated: ${reason}]`, test);
   }
});
