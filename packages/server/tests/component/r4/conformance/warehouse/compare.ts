// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One warehouse case's verdict: its served answer against live, by the rules
 * conformance.component.spec.ts asserts under the shipped switch (rows equal,
 * strictly too; result metadata equal up to the two tolerances every stored
 * read has; and the tier the shape expects answered). Kept as a pure
 * function so the report and the assertion read one verdict.
 */

import {
   rowDifference,
   strictRowDifference,
} from "../../../../../src/test_helpers/conformance/answer_difference";
import type {
   Answer,
   Outcome,
   WireField,
} from "../../../../../src/test_helpers/conformance/package_runner";
import { withoutDrillExpression } from "../drill_expression";
import { type Expect, isDatabase, type Variant } from "../shapes";

export const TOLERANCES = ["number-subtype", "drill-metadata"] as const;
export type Tolerance = (typeof TOLERANCES)[number];

function tolerantType(
   type: unknown,
   tolerate: readonly Tolerance[],
   depth = 0,
): unknown {
   if (depth > 32 || type === null || typeof type !== "object") return type;
   if (Array.isArray(type)) {
      return type.map((a) =>
         tolerate.includes("drill-metadata") &&
         typeof (a as { value?: unknown })?.value === "string"
            ? {
                 ...(a as object),
                 value: withoutDrillExpression((a as { value: string }).value),
              }
            : tolerantType(a, tolerate, depth + 1),
      );
   }
   return Object.fromEntries(
      Object.entries(type as Record<string, unknown>)
         .filter(
            ([k]) => !(k === "subtype" && tolerate.includes("number-subtype")),
         )
         .map(([k, v]) => [k, tolerantType(v, tolerate, depth + 1)]),
   );
}

/** The first difference in result metadata, or undefined. */
export function fieldDifference(
   live: Answer,
   served: Answer,
   tolerate: readonly Tolerance[] = [],
): string | undefined {
   const a = live.fields;
   const b = served.fields;
   if (a.length !== b.length) {
      return `fields ${a.map((f) => f.name).join(",")} against ${b.map((f) => f.name).join(",")}`;
   }
   const type = (f: WireField) => tolerantType(f.type, tolerate);
   const notes = (f: WireField) =>
      tolerate.includes("drill-metadata")
         ? f.annotations.map(withoutDrillExpression)
         : f.annotations;
   for (let i = 0; i < a.length; i++) {
      const pairs: [string, unknown, unknown][] = [
         ["name", a[i].name, b[i].name],
         ["kind", a[i].kind, b[i].kind],
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

/** The smallest set of tolerances under which the metadata matches (all of them when none does). */
function tolerancesNeeded(live: Answer, served: Answer): Tolerance[] {
   const subsets: Tolerance[][] = [
      [],
      ["number-subtype"],
      ["drill-metadata"],
      ["number-subtype", "drill-metadata"],
   ];
   return (
      subsets.find((s) => fieldDifference(live, served, s) === undefined) ??
      subsets[subsets.length - 1]
   );
}

/** Which leg answered, as the report names it. */
export function tierOfAnswer(answer: Answer): string {
   if (answer.servedBy !== "materialized") {
      return answer.servedBy === "live" ? "live" : `served:${answer.servedBy}`;
   }
   if (answer.via) {
      return `served:${answer.via}:${answer.servedFrom ?? "?"}`;
   }
   return `served:${answer.servedFrom ?? "?"}`;
}

export interface Verdict {
   /** Undefined when the case passes; else the first thing wrong. */
   failure?: string;
   /** What answered, for the report. */
   answeredBy: string;
   /** The tolerances the metadata needed. */
   tolerated: Tolerance[];
}

/** The served answer against live, at the tier `want` names. */
export function judge(
   live: Outcome | undefined,
   served: Outcome | undefined,
   want: Expect,
   variant: Variant,
): Verdict {
   if (!live || !served) {
      return { failure: "not run", answeredBy: "-", tolerated: [] };
   }
   if ("error" in live) {
      return {
         failure: `live failed: ${live.error}`,
         answeredBy: "-",
         tolerated: [],
      };
   }
   if ("error" in served) {
      return {
         failure: `served failed: ${served.error}`,
         answeredBy: "error",
         tolerated: [],
      };
   }
   const l = live.answer;
   const s = served.answer;
   const answeredBy = tierOfAnswer(s);
   const fail = (failure: string, tolerated: Tolerance[] = []) => ({
      failure,
      answeredBy,
      tolerated,
   });
   const rows =
      rowDifference(l.rows, s.rows) ?? strictRowDifference(l.rows, s.rows);
   if (rows) return fail(`wrong rows: ${rows}`);
   const tolerated = tolerancesNeeded(l, s);
   const fields = fieldDifference(l, s, tolerated);
   if (fields) return fail(`wrong metadata: ${fields}`, tolerated);
   const skip = s.servedSkip ?? s.viewSkip;
   const skipped = skip
      ? ` (skipped: ${skip.reason ?? ""}${skip.detail ? ` ${skip.detail}` : ""})`
      : "";
   if (want === "live") {
      return s.servedBy === "live"
         ? { answeredBy, tolerated }
         : fail(`expected live, answered by ${answeredBy}`, tolerated);
   }
   if (want === "rollup" || want === "view") {
      return s.servedBy === want
         ? { answeredBy, tolerated }
         : fail(
              `expected ${want}, answered by ${answeredBy}${skipped}`,
              tolerated,
           );
   }
   if (s.servedBy !== "materialized") {
      return fail(
         `expected a stored table, answered by ${answeredBy}${skipped}`,
         tolerated,
      );
   }
   const from =
      isDatabase(variant) || want === "database" ? "database" : "storage";
   if (s.servedFrom !== from || s.via !== "served_copy") {
      return fail(
         `expected the served copy from ${from}, answered by ${answeredBy}`,
         tolerated,
      );
   }
   return { answeredBy, tolerated };
}

/**
 * For a case a warehouse expects live by design: why it does not hold, or
 * undefined when it does. The build must have succeeded (a broken `storage=`
 * build also answers live, and must not pass as the decline), and the served
 * answer must carry the decline named: its reason, and its detail when one
 * is named.
 */
export function expectedSkipFailure(
   served: Outcome | undefined,
   skip: { reason: string; detail?: string },
   buildStatus: string | undefined,
): string | undefined {
   if (buildStatus !== "MANIFEST_FILE_READY") {
      return `expected a built stored copy that declines, but the build is ${buildStatus ?? "absent"}`;
   }
   if (!served || "error" in served) return "not run";
   const got = served.answer.servedSkip ?? served.answer.viewSkip;
   if (
      got?.reason !== skip.reason ||
      (skip.detail !== undefined && got?.detail !== skip.detail)
   ) {
      return `expected the stored copy declined with ${skip.reason}${
         skip.detail ? ` (${skip.detail})` : ""
      }, got ${got ? `${got.reason ?? "-"}${got.detail ? ` (${got.detail})` : ""}` : "no skip"}`;
   }
   return undefined;
}
