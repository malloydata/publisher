// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A BigQuery table needs a dataset, which only a dotted `name=` gives a table
// built in the source's own warehouse. `#@ persist` (on a source, a view or a
// named query) is refused at plan for one without it, under the same reason
// `#(materialize)` is (`needs_qualified_name`), and warns `persist-not-built`
// (`persist-view-not-built`, `persist-query-not-built`) at load rather than
// publishing a source no build can write. BigQuery is the one dialect whose
// capabilities say so (`needsQualifiedTable`); any other places an unqualified
// table in its connection's default schema, so DuckDB here is the control.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { dialectsWith } from "../domain/dialect";
import { persistRefusalWarnings } from "../package_warnings";
import { derivePackagePlan } from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";

/** A DuckDB connection compiled as BigQuery's dialect: the plan only reads its dialect. */
class BigQueryDialectConnection extends DuckDBConnection {
   get dialectName(): string {
      return "standardsql";
   }
}

const MODEL = (connection: string) => `##! experimental.persistence
source: base is ${connection}.sql("SELECT 1 AS id, 'a' AS cat")
#@ persist
source: no_name is base -> { group_by: cat }
#@ persist name="undotted"
source: undotted is base -> { group_by: id }
#@ persist name="analytics.dotted"
source: dotted is base -> { group_by: cat; aggregate: n is count() }
source: viewed is base extend {
  #@ persist
  view: by_cat is { group_by: cat; aggregate: c is count() }
}
#@ persist
query: totals is base -> { aggregate: total is count() }
`;

const packages = tempBuildPlanPackages();
let restore: (() => void) | undefined;
beforeEach(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});
afterEach(async () => {
   restore?.();
   await packages.cleanup();
});

async function planOver(connection: DuckDBConnection) {
   const connections = new FixedConnectionMap(
      new Map([[connection.name, connection]]),
      connection.name,
   );
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => connections);
   const pkg = await packages.create(
      { "m.malloy": MODEL(connection.name) },
      {
         getMalloyConfig: () => malloyConfig,
         getMalloyConnection: async () => connection,
      },
   );
   const outcome = derivePackagePlan(await compilePackageBuildPlan(pkg), null);
   const warnings = persistRefusalWarnings(
      outcome.plan,
      outcome.refusedRollups,
      outcome.materializePlans,
      outcome.refusalNotices,
   );
   return { outcome, warnings };
}

describe("#@ persist with no dataset-qualified name= on BigQuery", () => {
   it("is BigQuery alone that needs one", () => {
      expect([...dialectsWith("needsQualifiedTable")]).toEqual(["standardsql"]);
   });

   it(
      "is refused at plan and warned at load, as #(materialize) is, on a source, a view and a query",
      async () => {
         const { outcome, warnings } = await planOver(
            new BigQueryDialectConnection("bq", ":memory:"),
         );
         const refused = Object.values(outcome.plan?.refusedSources ?? {});
         expect(
            refused
               .filter((r) => r.reason === "needs_qualified_name")
               .map((r) => r.name)
               .sort(),
         ).toEqual([
            "no_name",
            "totals__query",
            "undotted",
            "viewed__by_cat__view",
         ]);
         // A dotted name places the table: it is planned.
         expect(
            Object.values(outcome.plan?.sources ?? {}).map((s) => s.name),
         ).toEqual(["dotted"]);
         const coded = (code: string) =>
            warnings
               .filter((w) => w.code === code)
               .map((w) => w.subject)
               .sort();
         expect(coded("persist-not-built")).toEqual(["no_name", "undotted"]);
         expect(coded("persist-view-not-built")).toEqual([
            "viewed__by_cat__view",
         ]);
         expect(coded("persist-query-not-built")).toEqual(["totals__query"]);
         for (const w of warnings) {
            expect(w.message).toContain("needs a dataset");
            expect(w.message).toContain("dataset-qualified name=");
         }
      },
      { timeout: 60000 },
   );

   it(
      "is not refused on a dialect whose tables need no container",
      async () => {
         const { outcome } = await planOver(
            new DuckDBConnection("duckdb", ":memory:"),
         );
         expect(
            Object.values(outcome.plan?.refusedSources ?? {}).filter(
               (r) => r.reason === "needs_qualified_name",
            ),
         ).toEqual([]);
      },
      { timeout: 60000 },
   );
});
