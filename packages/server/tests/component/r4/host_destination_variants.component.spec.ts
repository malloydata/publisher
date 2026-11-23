// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Campaign F-14 / P4-03, P4-04, P4-05 (coverage gap G4): a `#(rollup)`, a
 * persisted view and a persisted named query with no `storage=`, over a
 * source on a database origin (a DuckLake catalog the environment attaches),
 * whose tables the host instructed into a storage destination. The control
 * plane gives a source on a database connection that database as its
 * destination, so the package is bound with `storage` entries none of its
 * annotations name. Each must be answered from its own table, equal to live.
 *
 * The package goes through the real load (Environment, Package, its build
 * plan); each planned table is built from the plan's own SQL on the origin and
 * copied into the destination through Parquet, as a storage build copies it,
 * then bound as the host binds an instructed run's manifest. The unit tests
 * (serve/rollup_variants_serve.spec.ts, serve/view_variants_serve.spec.ts)
 * check only which table is chosen; this compares rows, each value as a
 * string, and tampers each table with its own suffix so an answer read from
 * another table, or from live, is told apart.
 */

import { contractFor } from "../../../src/test_helpers/contract_fixture";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import type { Row } from "../../../src/test_helpers/conformance/answer_difference";
import {
   freshLoadPool,
   LAKE,
   loadFixture,
   type Outcome,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const MODEL = `##! experimental { persistence }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 'open', 10.5), (2, 'shipped', 20.25), (3, 'open', 30.0),
    (4, 'returned', 7.75), (5, 'shipped', 1.5)
  ) AS t(id, status, amount)
""") extend {
  primary_key: id
  #(rollup) grain=["status"]
  measure: revenue is amount.sum()

  #@ persist
  view: by_status is {
    group_by: status
    aggregate: revenue, n is count()
    order_by: status
  }
}

#@ persist
query: top_status is orders -> {
  group_by: status
  aggregate: revenue
  order_by: revenue desc
  limit: 2
}
`;

interface Case {
   label: string;
   /** The plan entry's origin: the table this answer must be read from. */
   origin: "rollup" | "view" | "query";
   malloy: string;
   /** The path report's servedBy kind. */
   servedBy: string;
   /** The suffix its tampered table carries. */
   tag: string;
}

const CASES: Case[] = [
   {
      label: "P4-03: a #(rollup) with no storage=",
      origin: "rollup",
      malloy:
         "run: orders -> { group_by: status; aggregate: revenue; order_by: status }",
      servedBy: "rollup",
      tag: "_r",
   },
   {
      label: "P4-04: a persisted view with no storage=",
      origin: "view",
      malloy: "run: orders -> by_status",
      servedBy: "view",
      tag: "_v",
   },
   {
      label: "P4-05: a persisted named query with no storage=",
      origin: "query",
      malloy: "run: top_status",
      servedBy: "query",
      tag: "_q",
   },
];

/** A value as an exact string: never read back through a double. */
function exact(value: unknown): string {
   if (value instanceof Date) return value.toISOString();
   return typeof value === "object" && value !== null
      ? JSON.stringify(value)
      : String(value);
}

/** Rows with every value an exact string, in the order they came. */
function asStrings(rows: Row[]): Record<string, string>[] {
   return rows.map((r) =>
      Object.fromEntries(Object.entries(r).map(([k, v]) => [k, exact(v)])),
   );
}

function answered(
   o: Outcome | undefined,
): NonNullable<Extract<Outcome, { answer: unknown }>["answer"]> {
   if (!o) throw new Error("no outcome");
   if ("error" in o) throw new Error(o.error);
   return o.answer;
}

describe("F-14: a rollup, view and query over a database origin, in a host-chosen destination", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   const live = new Map<Case, Outcome>();
   /** The destination table each origin was built into. */
   const tableOf: Record<string, string> = {};

   beforeAll(async () => {
      // The origin connection attaches its DuckLake in setupSQL.
      restore = withSwitch("on", { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" });
      await freshLoadPool();
      // `parquet`: the models read a DuckLake catalog the environment's
      // connection attaches (the database origin), every row in Parquet.
      loader = await loadFixture({ "m.malloy": MODEL }, { lake: "parquet" });
      for (const c of CASES)
         live.set(c, await loader.run("m.malloy", c.malloy));

      const pkg = loader.pkg;
      const sources = Object.values(pkg.getBuildPlan()?.sources ?? {});
      const scratch = fs.mkdtempSync(
         path.join(pkg.getPackagePath(), ".host-build-"),
      );
      const stored: Record<string, Record<string, unknown>> = {};
      try {
         for (const c of CASES) {
            const entry = sources.find((s) => s.origin === c.origin);
            if (!entry) throw new Error(`no planned ${c.origin}`);
            expect(entry.annotationFields?.storage).toBeUndefined();
            const table = `host_${c.origin}`;
            const origin = await pkg.getMalloyConnection(
               entry.connectionName ?? "duckdb",
            );
            const file = path
               .join(scratch, `${c.origin}.parquet`)
               .replace(/'/g, "''");
            await origin.runSQL(
               `COPY (${entry.sql}) TO '${file}' (FORMAT PARQUET)`,
            );
            await loader.lake.runSQL(
               `CREATE OR REPLACE TABLE "${table}" AS SELECT * FROM read_parquet('${file}')`,
            );
            const described = await loader.lake.runSQL(`DESCRIBE "${table}"`, {
               rowLimit: 1_000,
            });
            stored[entry.sourceEntityId] = {
               sourceEntityId: entry.sourceEntityId,
               sourceName: entry.name,
               physicalTableName: table,
               contract: contractFor(),
               connectionName: entry.connectionName,
               // The host's choice; no annotation names it.
               storageDestinationName: LAKE,
               realization: "COPY",
               rowCount: null,
               origin: c.origin,
               schema: (
                  described.rows as {
                     column_name: string;
                     column_type: string;
                  }[]
               ).map((r) => ({ name: r.column_name, type: r.column_type })),
            };
            tableOf[c.origin] = table;
         }
      } finally {
         fs.rmSync(scratch, { recursive: true, force: true });
      }
      pkg.materialization().bind({ storage: stored as never });
      for (const modelPath of pkg.getModelPaths()) {
         await pkg.getModel(modelPath)?.prepareServedCopy();
      }
   }, 300_000);

   afterAll(async () => {
      restore?.();
      await loader?.dispose();
      await freshLoadPool();
   });

   for (const c of CASES) {
      it(`${c.label}: answered from its table in the destination, equal to live`, async () => {
         const l = answered(live.get(c));
         expect(l.servedBy).toBe("live");
         expect(l.rows.length).toBeGreaterThan(0);
         const s = answered(await loader.run("m.malloy", c.malloy));
         expect([s.servedBy, s.servedFrom]).toEqual([c.servedBy, "storage"]);
         expect(asStrings(s.rows)).toEqual(asStrings(l.rows));
      });
   }

   it("reads each from its own table: tampered apart, each answer carries its own suffix", async () => {
      for (const c of CASES) {
         await loader.lake.runSQL(
            `UPDATE "${tableOf[c.origin]}" SET status = status || '${c.tag}'`,
         );
      }
      for (const c of CASES) {
         const l = answered(live.get(c));
         const s = answered(await loader.run("m.malloy", c.malloy));
         expect(s.servedBy).toBe(c.servedBy);
         expect(asStrings(s.rows)).toEqual(
            asStrings(
               l.rows.map((r) => ({ ...r, status: `${r.status}${c.tag}` })),
            ),
         );
      }
   });
});
