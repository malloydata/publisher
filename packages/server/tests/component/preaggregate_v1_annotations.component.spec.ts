// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The deprecated v1 `#@ preaggregate` rollup answers with the annotations a
// plain compile of the same source returns: its merged measures carry the
// author's measure's notes (`#(classification)`, `#(doc)`, `# currency`), and
// its grain columns (a computed dimension, a timeframe) theirs, built or not,
// in a nest too. The truth is a sibling file declaring the same source without
// the `#@ preaggregate` line, so nothing routes it.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

let rootDir: string;
let restore: () => void;
beforeEach(async () => {
   restore = installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
      },
   }).restore;
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-v1-notes-"));
});
afterEach(async () => {
   restore();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

const model = (
   preaggregate: string,
) => `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (10, 'A', TIMESTAMP '2026-01-01'),
    (20, 'A', TIMESTAMP '2026-02-01'),
    (35, 'B', TIMESTAMP '2026-02-03')
  ) AS t(amount, category, ts)
""") extend {
  #(classification) "none"
  # label="Category tag"
  dimension: cat_tag is concat(category, '!')
  # label="Month"
  dimension: m is ts.month
  #(classification) "none"
  #(doc) "Order revenue, \\"as booked\\" |# not a closer"
  # currency
  #|
    # label="Total"
  |#
  ${preaggregate}
  measure: total is amount.sum()
}
`;

type Field = {
   name: string;
   annotations?: { value: string }[];
   type?: { element_type?: { fields?: Field[] } };
};

/** Each column's author notes, nested columns as `nest.column`; Malloy's own left out, so a leaked `#@` line fails. */
function notesOf(
   fields: readonly Field[],
   prefix = "",
): Record<string, string[]> {
   const out: Record<string, string[]> = {};
   for (const f of fields) {
      out[`${prefix}${f.name}`] = (f.annotations ?? [])
         .map((a) => a.value.trim())
         .filter((v) => !v.startsWith("#(malloy)"));
      Object.assign(
         out,
         notesOf(f.type?.element_type?.fields ?? [], `${f.name}.`),
      );
   }
   return out;
}

async function run(pkg: Package, modelPath: string, query: string) {
   const audit = unrecordedAudit();
   const r = await pkg
      .getModel(modelPath)!
      .runQuery(audit, { query } as never, QueryPrivileges.of({}));
   return {
      rows: r.compactResult,
      notes: notesOf((r.result?.schema?.fields ?? []) as Field[]),
      servedBy: r.path?.servedBy.kind,
      fieldsRead: audit.summary?.fieldsRead,
   };
}

describe("a v1 #@ preaggregate rollup's annotations", () => {
   const queries = [
      "run: orders -> { group_by: cat_tag; aggregate: total }",
      "run: orders -> { group_by: m; aggregate: total }",
      "run: orders -> { group_by: cat_tag; nest: n is { group_by: m; aggregate: total } }",
   ];
   it("are the plain compile's, built and not built", async () => {
      const envPath = path.join(rootDir, "env");
      writePackageFiles(
         path.join(envPath, "pkg"),
         {
            "model.malloy": model('#@ preaggregate grain="cat_tag,m"'),
            "truth.malloy": model(""),
         },
         { manifest: { name: "pkg", description: "fixture" } },
      );
      const env = await Environment.create("testEnv", envPath, [] as never);
      await env.addPackage("pkg");
      const pkg = await env.getPackage(parsePackageName("pkg"), false);

      const truth = [];
      for (const q of queries) truth.push(await run(pkg, "truth.malloy", q));
      expect(truth[0].notes.total).toEqual([
         '#(classification) "none"',
         '#(doc) "Order revenue, \\"as booked\\" |# not a closer"',
         "# currency",
         // A multi-line note, held without its closer: written back as held,
         // it would leave the synthesized rollup's annotation open, and the
         // rollup would not compile.
         '#|\n# label="Total"',
      ]);
      expect(truth[2].notes["n.total"]).toContain("# currency");

      // Not built: the rollup leg has no table, so the author's own source
      // answers live, with its own notes, the `#@ preaggregate` line it
      // declares among them (the truth file declares none).
      const withoutOwnLine = (notes: Record<string, string[]>) =>
         Object.fromEntries(
            Object.entries(notes).map(([k, v]) => [
               k,
               v.filter((n) => !n.startsWith("#@ preaggregate")),
            ]),
         );
      const live = [];
      for (let i = 0; i < queries.length; i++) {
         const unbuilt = await run(pkg, "model.malloy", queries[i]);
         live.push(unbuilt);
         expect(unbuilt.servedBy ?? "live").toBe("live");
         expect(unbuilt.rows).toEqual(truth[i].rows);
         expect(withoutOwnLine(unbuilt.notes)).toEqual(truth[i].notes);
      }

      const ids = [...pkg.getPreaggregateEntityIds()];
      expect(ids).toHaveLength(1);
      const rollup = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
         (s) => s.sourceEntityId === ids[0],
      );
      if (!rollup) throw new Error("no rollup in the plan");
      const connection = (await pkg.getMalloyConnection(
         "duckdb",
      )) as DuckDBConnection;
      await connection.runSQL(
         `CREATE OR REPLACE TABLE built_rollup AS ${rollup.sql}`,
      );
      pkg.bindColocatedServeManifest({
         [ids[0]]: { tableName: "built_rollup", connectionName: "duckdb" },
      } as never);

      for (let i = 0; i < queries.length; i++) {
         const served = await run(pkg, "model.malloy", queries[i]);
         expect(served.servedBy).toBe("rollup");
         expect(served.rows).toEqual(truth[i].rows);
         // Exactly the live answer's notes on the same source.
         expect(served.notes).toEqual(live[i].notes);
         expect(served.fieldsRead).toEqual(truth[i].fieldsRead);
      }
   });
});
