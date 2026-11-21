// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Strict-origin mode: an opt-in switch for the materialization serving
 * harnesses (the R4 model harness, tests/component/r4/harness.ts, and the
 * package-case runner, package_runner.ts) that makes the permissive dialect
 * the hard case. Off unless `PUBLISHER_TEST_STRICT_ORIGIN=1`; a default run is
 * unchanged.
 *
 * DuckDB, where most of these specs run, compares strings byte-exactly and
 * captures a built table's schema without trouble, so a serving path that
 * fails closed elsewhere (Postgres' `en_US` collation, its schema capture
 * failing on an unqualified name) is never taken. Under the switch the origin:
 *
 * - compares strings non-binary: its DuckDB instance runs
 *   `SET default_collation='nocase'` ({@link STRICT_ORIGIN_SETUP_SQL}), the
 *   origin only, never the storage destination, which stays byte-exact as a
 *   DuckLake destination is;
 * - captures no stored schema: a database table's build-time column capture
 *   (`capturedDatabaseSchema`) is read as failed, so binds take their
 *   fallback paths;
 * - captures no native schema (`capturedNativeSchema`).
 *
 * A strict origin legitimately turns many served answers live, so the specs'
 * own served/live expectations are not what a strict run checks. Every query
 * either harness runs live and served is recorded ({@link recordStrictCase})
 * and the one invariant is asserted per spec file, in an `afterAll`: every
 * served answer equals live, compared as strings. A spec's own expectations
 * may fail under the switch; read the report, not the pass count.
 *
 * The report: one JSON line per case, under `PUBLISHER_TEST_STRICT_REPORT`
 * (default `<tmpdir>/publisher-strict-origin`), one file per spec file,
 * tagged with `PUBLISHER_TEST_STRICT_LABEL` (default `duckdb`) so a Postgres
 * run (`PUBLISHER_TEST_POSTGRES_URL`) can be told apart. Setting
 * `PUBLISHER_TEST_STRICT_REPORT` without the switch records a default run the
 * same way (asserting nothing), for comparing which cases strict mode turns
 * live.
 */

import { afterAll, expect } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { rowDifference, strictRowDifference } from "./answer_difference";

/** Whether strict-origin mode is on for this run. */
export const STRICT_ORIGIN = process.env.PUBLISHER_TEST_STRICT_ORIGIN === "1";

/** What a strict origin's DuckDB instance runs at setup. */
export const STRICT_ORIGIN_SETUP_SQL = "SET default_collation='nocase'";

/** Whether cases are recorded: strict mode, or a default run asked to. */
export const STRICT_RECORD =
   STRICT_ORIGIN || process.env.PUBLISHER_TEST_STRICT_REPORT !== undefined;
const REPORT_DIR =
   process.env.PUBLISHER_TEST_STRICT_REPORT ??
   path.join(os.tmpdir(), "publisher-strict-origin");
const LABEL =
   process.env.PUBLISHER_TEST_STRICT_LABEL ??
   (STRICT_ORIGIN ? "duckdb" : "default");

type Row = Record<string, unknown>;

/** One query a harness ran live and served under strict mode. */
export interface StrictCase {
   harness: "r4" | "package";
   malloy: string;
   model?: string;
   givens?: unknown;
   live?: Row[];
   served?: Row[];
   liveError?: string;
   servedError?: string;
   /** `storage`, `database`… when a stored table answered; null when live. */
   servedFrom?: string | null;
   servedBy?: unknown;
   /** The router's skipped legs, with their reasons. */
   declines?: unknown[];
}

const violations: string[] = [];
let registered = false;

/** The spec file calling into the harness, read off the stack. */
function specFile(): string {
   const stack = new Error().stack ?? "";
   const match = stack.match(/([\w.-]+\.spec\.ts)/);
   return match?.[1] ?? "unknown.spec.ts";
}

/**
 * A value as a string, order-free: every list (the rows, each nest) as the
 * sorted strings of its elements, an object's keys sorted, a bigint as its
 * digits, and a non-integer number to 12 significant digits (the float noise
 * two plans' sums differ by, R4's tolerance).
 */
function canonical(v: unknown): string {
   if (v === null || v === undefined) return "null";
   if (typeof v === "bigint") return v.toString();
   if (typeof v === "number") {
      return Number.isInteger(v)
         ? String(v)
         : String(Number(v.toPrecision(12)));
   }
   if (v instanceof Date) return v.toISOString();
   if (Array.isArray(v)) return `[${v.map(canonical).sort().join(",")}]`;
   if (typeof v === "object") {
      const o = v as Record<string, unknown>;
      return `{${Object.keys(o)
         .sort()
         .map((k) => `${JSON.stringify(k)}:${canonical(o[k])}`)
         .join(",")}}`;
   }
   return JSON.stringify(String(v));
}

/**
 * Record a case, and hold it to the invariant: where both sides answered,
 * the served rows equal live as strings ({@link canonical}).
 */
export function recordStrictCase(c: StrictCase): void {
   if (!STRICT_RECORD) return;
   const file = specFile();
   if (STRICT_ORIGIN && !registered) {
      registered = true;
      afterAll(() => {
         expect(violations).toEqual([]);
      });
   }
   let verdict: string;
   let difference: string | undefined;
   if (c.liveError !== undefined || c.servedError !== undefined) {
      verdict =
         c.liveError !== undefined && c.servedError !== undefined
            ? "both_error"
            : c.servedError !== undefined
              ? "served_error"
              : "live_error";
   } else if (c.live && c.served) {
      // The rows as strings, as a multiset (and each nest's): a difference
      // there is a wrong answer. Rows that agree but come back in another
      // order than the query's (beyond ties) are recorded apart, for review.
      if (canonical(c.live) !== canonical(c.served)) {
         verdict = "WRONG";
         difference =
            strictRowDifference(c.live, c.served) ?? "a nest's rows differ";
      } else {
         difference = rowDifference(c.live, c.served);
         verdict = difference === undefined ? "equal" : "order_differs";
      }
   } else {
      verdict = "unpaired";
   }
   const by =
      typeof c.servedBy === "string"
         ? c.servedBy
         : (c.servedBy as { kind?: unknown } | undefined)?.kind;
   const stored =
      (c.servedFrom !== undefined && c.servedFrom !== null) ||
      (typeof by === "string" && by !== "live")
         ? "served"
         : "live";
   if (verdict === "WRONG" && stored === "served") {
      violations.push(`${file}: ${c.malloy} (${difference})`);
   }
   if (verdict === "served_error") {
      violations.push(`${file}: ${c.malloy} served error: ${c.servedError}`);
   }
   fs.mkdirSync(REPORT_DIR, { recursive: true });
   fs.appendFileSync(
      path.join(REPORT_DIR, `${LABEL}__${file}.jsonl`),
      `${JSON.stringify(
         {
            label: LABEL,
            file,
            verdict,
            stored,
            ...(difference ? { difference } : {}),
            ...c,
         },
         (_k, v) => (typeof v === "bigint" ? `${v}n` : v),
      )}\n`,
   );
}

/**
 * Record a package bind (its serve verdicts, build errors and warning codes),
 * which explains a case answered live with no leg considered: a source the
 * bind left unbound.
 */
export function recordStrictBind(bind: {
   verdicts: string[];
   buildErrors: Record<string, string>;
   warnings: { code?: string; subject?: string; message?: string }[];
}): void {
   if (!STRICT_RECORD) return;
   const file = specFile();
   fs.mkdirSync(REPORT_DIR, { recursive: true });
   fs.appendFileSync(
      path.join(REPORT_DIR, `${LABEL}__${file}.binds.jsonl`),
      `${JSON.stringify({ label: LABEL, file, ...bind })}\n`,
   );
}

/** The skipped legs of a path report's `considered`, with their reasons. */
export function declinesOf(considered: readonly unknown[] | undefined) {
   return (considered ?? []).filter(
      (c) => (c as { outcome?: unknown }).outcome === "skipped",
   );
}
