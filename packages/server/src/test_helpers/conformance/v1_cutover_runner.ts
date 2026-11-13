// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's conformance runner (materialization v2, phase 3 pull
 * request 6e; plan question 15): the measurement behind the cutover design,
 * made a supported library entry point, so Credible's private CI can run it
 * over real package snapshots as a configuration job.
 *
 * In: a package (a directory holding a package whose connections are DuckDB,
 * the data snapshot, or the fixture's files) and a list of queries with their
 * givens. Out: one row per query, answered three ways and classified:
 *
 *  - `live`: before any table is bound;
 *  - `today`: with `PUBLISHER_V1_SERVED_COPY=off` (the storage serve shape,
 *    the `#@ preaggregate` composite, the colocated build manifest);
 *  - `cutover`: a package loaded, built and bound under
 *    `PUBLISHER_V1_SERVED_COPY=on` (the served copy, the rollup and view legs,
 *    or live), compared with that package's own live answer.
 *
 * Each stored answer is compared with live in rows (exactly: strings as
 * strings, integers and bigints exact, floats within a relative tolerance)
 * and result metadata (field types with their timeframes, and annotations),
 * a number's integer or decimal subtype and the `#(malloy)` drill metadata
 * set aside (known limitation 12, accepted for the cutover).
 *
 * Every table is built for real (colocated on the source's own connection,
 * `storage=` into a DuckDB destination `lake`), so the snapshot must be a
 * DuckDB copy of the data. `PERSIST_STORAGE_MODE` is `on` throughout.
 */

import type { GivenValue } from "@malloydata/malloy";
import {
   shadowFieldDifference,
   shadowRowDifference,
} from "../../service/materialization/serve/v1_shadow";
import { installSettings } from "../settings_env";
import {
   type Answer,
   freshLoadPool,
   loadFixture,
   type Outcome,
} from "./package_runner";

/** One query to answer. */
export interface CutoverQuery {
   /** A label for the row. */
   name: string;
   /** The model file it runs in, relative to the package. */
   model: string;
   malloy: string;
   givens?: Record<string, GivenValue>;
}

/** How one way of answering came out, against live. */
export type Classification =
   /** A stored table answered, equal to live. */
   | "served"
   /** Live answered (no stored table did). */
   | "live"
   | "wrong_rows"
   | "wrong_metadata"
   | "error";

/** One query's row. */
export interface CutoverRow {
   query: string;
   today: Classification;
   cutover: Classification;
   /**
    * `served_equal`, `newly_served`, `live_both`, `regression` (served today,
    * live after), or `bug` (the cutover's answer differs from live, or
    * failed where live did not).
    */
   verdict:
      | "served_equal"
      | "newly_served"
      | "live_both"
      | "regression"
      | "bug";
   /** What answered after the cutover (`served_copy`, `rollup`, `view`, `live`). */
   cutoverPath: string;
   /** For a difference or an error: where (a row and column, a field), never a value. */
   detail?: string;
}

const MODES = {
   today: {
      PERSIST_STORAGE_MODE: "on",
      PUBLISHER_MATERIALIZE: "false",
      PUBLISHER_V1_SERVED_COPY: "off",
   },
   cutover: {
      PERSIST_STORAGE_MODE: "on",
      PUBLISHER_MATERIALIZE: "false",
      PUBLISHER_V1_SERVED_COPY: "on",
   },
} as const;

/** Classify a stored answer against live. */
export function classify(
   live: Outcome,
   answer: Outcome,
): { c: Classification; path: string; detail?: string } {
   // Refused live and refused the same way after it (a denied caller, a
   // query the model rejects): the answer is live's, not a difference.
   if ("error" in live && "error" in answer) {
      return live.error === answer.error
         ? { c: "live", path: "live", detail: "refused both ways" }
         : {
              c: "error",
              path: "error",
              detail: "the query failed differently",
           };
   }
   if ("error" in answer) {
      return { c: "error", path: "error", detail: "the query failed" };
   }
   if ("error" in live) {
      // Answered where live refused: possibly rows a caller may not see.
      return {
         c: "error",
         path: "error",
         detail: "live refused the query, and the stored answer did not",
      };
   }
   const path = pathOf(answer.answer);
   const rows = shadowRowDifference(
      live.answer.rows as Record<string, unknown>[],
      answer.answer.rows as Record<string, unknown>[],
   );
   if (rows) return { c: "wrong_rows", path, detail: rows };
   const fields = shadowFieldDifference(
      live.answer.fields.map(asResultField),
      answer.answer.fields.map(asResultField),
   );
   if (fields) return { c: "wrong_metadata", path, detail: fields };
   return { c: path === "live" ? "live" : "served", path };
}

function asResultField(f: Answer["fields"][number]): unknown {
   return {
      name: f.name,
      type: f.type,
      annotations: f.annotations.map((value) => ({ value })),
   };
}

function pathOf(a: Answer): string {
   if (a.servedBy !== "materialized") return a.servedBy;
   if (a.via) return a.via;
   return a.servedFrom === "storage" ? "serve_shape" : "colocated_manifest";
}

function verdictOf(
   today: Classification,
   cutover: Classification,
): CutoverRow["verdict"] {
   if (cutover !== "served" && cutover !== "live") return "bug";
   if (cutover === "served") {
      return today === "served" ? "served_equal" : "newly_served";
   }
   return today === "served" ? "regression" : "live_both";
}

/**
 * Answer every query live, today and after the cutover, and classify each.
 * Loads the package twice (once per switch), each time with a load pool
 * spawned under that switch, and disposes of both.
 */
export async function runV1CutoverConformance(args: {
   /** A package directory (a DuckDB snapshot), or the package's files. */
   package: { packageDir: string } | Record<string, string>;
   queries: readonly CutoverQuery[];
}): Promise<CutoverRow[]> {
   const run = async (mode: keyof typeof MODES) => {
      const { restore } = installSettings({ env: { ...MODES[mode] } });
      try {
         await freshLoadPool();
         const loader = await loadFixture(args.package);
         try {
            // By position: two queries may share a name.
            const live: Outcome[] = [];
            for (const q of args.queries) {
               live.push(await loader.run(q.model, q.malloy, q.givens));
            }
            const fixture = await loader.bind({ includeRollups: true });
            const served: Outcome[] = [];
            for (const q of args.queries) {
               served.push(await fixture.run(q.model, q.malloy, q.givens));
            }
            return { live, served };
         } finally {
            await loader.dispose();
         }
      } finally {
         restore();
         await freshLoadPool();
      }
   };
   const today = await run("today");
   const cutover = await run("cutover");
   return args.queries.map((q, i) => {
      const t = classify(today.live[i], today.served[i]);
      const c = classify(cutover.live[i], cutover.served[i]);
      return {
         query: q.name,
         today: t.c,
         cutover: c.c,
         verdict: verdictOf(t.c, c.c),
         cutoverPath: c.path,
         ...(c.detail ? { detail: c.detail } : {}),
      };
   });
}
