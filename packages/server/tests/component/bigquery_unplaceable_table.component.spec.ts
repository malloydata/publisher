// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database table on BigQuery needs a dataset, which only a dotted `name=`
 * gives it: a rollup's own or its base's container, or a source's
 * dataset-qualified name. A valid declaration with none is refused at plan,
 * alone, on every mechanism (`#@ persist`, a persisted view, `#(materialize)`,
 * `#(rollup)` and `#@ preaggregate`), with the one refusal record:
 * its mechanism's `*-not-built` warning and a `refusedSources` entry with the
 * reason `needs_qualified_name`. The package loads, the rest is planned, and
 * nothing reaches the warehouse: the check is the plan's.
 *
 * BigQuery is a stand-in: its schema reads come from a fixture, and any SQL
 * it is asked to run is recorded, so the spec proves none was.
 */

import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { BigQueryConnection } from "@malloydata/db-bigquery";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { Package } from "../../src/service/package";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

/** BigQuery with fixture schemas (`k` STRING, `n` INT64), recording any SQL run. */
function standInBigQuery(name: string, ran: string[]): Connection {
   const connection = new BigQueryConnection({
      name,
      projectId: "stand-in",
   } as never);
   Object.assign(connection, {
      getSQLBlockSchema: async () => ({
         fields: [
            { name: "k", type: "STRING", mode: "NULLABLE" },
            { name: "n", type: "INT64", mode: "NULLABLE" },
         ],
      }),
      runSQL: async (sql: string) => {
         ran.push(sql);
         return { rows: [], totalRows: 0 };
      },
   });
   // The plan's container check reads the dataset's metadata through the
   // client; answer it here (the dataset exists) rather than over the
   // network, where a 404 for the stand-in project refuses every table.
   Object.assign((connection as unknown as { bigQuery: object }).bigQuery, {
      dataset: () => ({ getMetadata: async () => [{}] }),
   });
   return connection;
}

/** The rows each source reads: a fixture on BigQuery, literals on DuckDB. */
const rows = (conn: string, extra = "") =>
   `${conn}.sql("SELECT ${conn === "bq" ? "k, n" : "'a' AS k, 1 AS n"}${extra}${conn === "bq" ? " FROM t" : ""}")`;

const MODEL = (conn: string) => `##! experimental { persistence }

// Database #@ persist: a bare name places nothing, a dotted one a dataset.
#@ persist name="bare_t"
source: bare is ${rows(conn)} -> { group_by: k; aggregate: total is n.sum() }
#@ persist name="scratch.placed_t"
source: placed is ${rows(conn)} -> { group_by: k; aggregate: placed_total is n.sum() }
#@ persist
source: unnamed is ${rows(conn)} -> { group_by: k; aggregate: unnamed_total is n.sum() }

// #(materialize): refused by its own planner, for the same reason.
#(materialize)
source: mz_bare is ${rows(conn)}
#(materialize) name="scratch.mz_t"
source: mz_placed is ${rows(conn, ", 1 AS one")}

// Rollups: their own name= container, their base's, or none.
source: facts is ${rows(conn, ", 2 AS two")} extend {
  #(rollup) grain=["k"]
  measure: v2_none is n.sum()
  #(rollup) grain=["k"] name="scratch.facts_rollup"
  measure: v2_named is n.max()
  #@ preaggregate grain="k"
  measure: v1_none is n.min()
}
#@ persist name="scratch.based_t"
source: based is ${rows(conn, ", 3 AS three")} -> { select: * } extend {
  #(rollup) grain=["k"]
  measure: inherits is n.sum()
}

// A persisted view, database, with no name.
source: viewed is ${rows(conn, ", 4 AS four")} extend {
  #@ persist
  view: by_k is { group_by: k; aggregate: vt is n.sum() }
}
`;

let work: string | undefined;
let restore: (() => void) | undefined;
afterEach(() => {
   restore?.();
   if (work) fs.rmSync(work, { recursive: true, force: true });
   work = undefined;
});

async function load(conn: string, connections: Map<string, Connection>) {
   restore = installSettings({
      env: {
         PUBLISHER_MATERIALIZE: "true",
         PUBLISHER_PACKAGE_VERSIONING: "off",
      },
   }).restore;
   work = fs.mkdtempSync(path.join(os.tmpdir(), "bq-unplaceable-"));
   const dir = path.join(work, "pkg");
   writePackageFiles(
      dir,
      { "m.malloy": MODEL(conn) },
      { manifest: { name: "pkg", description: "fixture" } },
   );
   connections.set("duckdb", new DuckDBConnection("duckdb", ":memory:"));
   const pkg = await Package.create("env", "pkg", dir, connections);
   const plan = pkg.getBuildPlan();
   return {
      planned: Object.values(plan?.sources ?? {})
         .map((s) => s.name)
         .sort(),
      refused: Object.values(plan?.refusedSources ?? {})
         .map((r) => [r.name, r.origin, r.reason])
         .sort(),
      warnings: (pkg.getPackageMetadata().warnings ?? [])
         .filter((w) => w.code?.endsWith("-not-built"))
         .map((w) => [w.code, w.subject, w.severity])
         .sort(),
      messages: (pkg.getPackageMetadata().warnings ?? []).map((w) => w.message),
   };
}

describe("a database BigQuery table with no dataset is refused at plan", () => {
   it("refuses each mechanism alone, with one record and reason, and plans the rest", async () => {
      const ran: string[] = [];
      const { planned, refused, warnings, messages } = await load(
         "bq",
         new Map<string, Connection>([["bq", standInBigQuery("bq", ran)]]),
      );
      const reasons = (r: unknown[]) => r[2];
      // Every refusal here is this one.
      expect([...new Set(refused.map(reasons))]).toEqual([
         "needs_qualified_name",
      ]);
      const refusedNames = refused.map((r) => String(r[0]));
      for (const name of ["bare", "unnamed", "mz_bare"]) {
         expect(refusedNames).toContain(name);
      }
      // The view's build source, and `facts`'s one unplaced rollup (its
      // `#(rollup)` and `#@ preaggregate` measures share a grain, so
      // one table), which stays listed beside its refusal, as a refused
      // rollup does.
      expect(refusedNames.filter((n) => n.startsWith("viewed"))).toHaveLength(
         1,
      );
      const refusedRollups = refused
         .filter((r) => r[1] === "rollup")
         .map((r) => String(r[0]));
      expect(refusedRollups).toEqual([
         expect.stringMatching(/^facts__rollup__k__/),
      ]);
      // Placed by a dataset: planned and not refused.
      expect(planned).toContain("placed");
      expect(planned).toContain("mz_placed");
      expect(planned).toContain("based");
      const served = planned.filter(
         (n) => n.includes("__rollup__") && !refusedRollups.includes(n),
      );
      expect(served).toEqual([
         expect.stringMatching(/^based__rollup__k__/),
         expect.stringMatching(/^facts__rollup__k__/),
      ]);
      // Its mechanism's not-built warning, never an error.
      expect(new Set(warnings.map((w) => w[0]))).toEqual(
         new Set([
            "persist-not-built",
            "materialize-not-built",
            "persist-view-not-built",
            "rollup-not-built",
         ]),
      );
      expect(warnings.every((w) => w[2] === "warn")).toBe(true);
      expect(
         messages.some((m) => m?.includes("where a table needs a dataset")),
      ).toBe(true);
      // Plan time only: nothing was run on the warehouse.
      expect(ran).toEqual([]);
   });

   it("refuses nothing for the same model on a warehouse that needs no dataset", async () => {
      const { refused } = await load("duckdb", new Map());
      expect(refused.filter((r) => r[2] === "needs_qualified_name")).toEqual(
         [],
      );
   });
});
