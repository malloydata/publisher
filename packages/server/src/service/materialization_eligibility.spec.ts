// Real-compiler contract for the materialization-eligibility gate. The gate
// reads specific compiled-IR shapes (a Parameter's `value: null` for unbound
// params; `refSummary.givenUsage` / `given` IR nodes for given references), so
// it is pinned against the real compiler rather than hand-built stubs — a
// Malloy change to either shape must fail here, not leak an ineligible source
// (a frozen tenant-filtered table) into the tier.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { PersistSource } from "@malloydata/malloy";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { MaterializationEligibilityError } from "../errors";
import { assertMaterializationEligible } from "./materialization_eligibility";

const ROOT = "file:///elig/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

/** Compile a single-file model and return its persist sources by name. */
async function persistSources(
   model: string,
): Promise<Record<string, PersistSource>> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const compiled = await runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
   const byName: Record<string, PersistSource> = {};
   for (const source of Object.values(compiled.getBuildPlan().sources)) {
      byName[source.name] = source;
   }
   return byName;
}

describe("assertMaterializationEligible", () => {
   it("accepts a plain persist source (no params, no givens)", async () => {
      const sources = await persistSources(`##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_plain"
source: mz_plain is base -> { aggregate: c is count() }`);
      expect(sources.mz_plain).toBeDefined();
      expect(() =>
         assertMaterializationEligible(sources.mz_plain),
      ).not.toThrow();
   });

   it("accepts a parameter bound to a constant", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.parameters
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_bound"
source: mz_bound(threshold::number is 5) is base -> { aggregate: c is count() }`);
      expect(sources.mz_bound).toBeDefined();
      expect(() =>
         assertMaterializationEligible(sources.mz_bound),
      ).not.toThrow();
   });

   it("refuses a source with an unbound (free) parameter", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.parameters
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_free"
source: mz_free(threshold::number) is base -> { aggregate: c is count() }`);
      expect(sources.mz_free).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_free)).toThrow(
         MaterializationEligibilityError,
      );
      expect(() => assertMaterializationEligible(sources.mz_free)).toThrow(
         /unbound parameter/i,
      );
   });

   it("refuses a source that references a given (RLAC security refusal)", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
#@ persist name="mz_given"
source: mz_given is base -> { where: tenant = $tenant; aggregate: c is count() }`);
      expect(sources.mz_given).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_given)).toThrow(
         MaterializationEligibilityError,
      );
      expect(() => assertMaterializationEligible(sources.mz_given)).toThrow(
         /given/i,
      );
   });

   it("refuses a source protected by its own #(authorize) gate", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) "$role = 'analyst'"
#@ persist name="mz_authz"
source: mz_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_authz).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_authz)).toThrow(
         MaterializationEligibilityError,
      );
      expect(() => assertMaterializationEligible(sources.mz_authz)).toThrow(
         /authorize/i,
      );
   });

   it("refuses a source that reaches an #(authorize) gate through a JOIN", async () => {
      // The gate is on the joined source, not on mz_authz_joined itself — a join
      // must not launder an authorize-gated source into a frozen table.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string is 'analyst'
#(authorize) "$role = 'analyst'"
source: gated is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
source: joiner is duckdb.sql("SELECT 2 AS n, 'acme' AS tenant")
#@ persist name="mz_authz_joined"
source: mz_authz_joined is joiner extend {
  join_one: g is gated on tenant = g.tenant
} -> { aggregate: c is count() }`);
      expect(sources.mz_authz_joined).toBeDefined();
      expect(() =>
         assertMaterializationEligible(sources.mz_authz_joined),
      ).toThrow(MaterializationEligibilityError);
   });

   it("refuses a source that reaches a given through a JOIN (not just its own pipeline)", async () => {
      // The given lives on a joined source, not on mz_joined's own where/fields.
      // The compiled struct embeds the joined SourceDef, so the fail-closed walk
      // must still reach it — a join must not launder a given-filtered source.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: gated is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant") extend {
  where: tenant = $tenant
}
source: joiner is duckdb.sql("SELECT 2 AS n, 'acme' AS tenant")
#@ persist name="mz_joined"
source: mz_joined is joiner extend {
  join_one: g is gated on tenant = g.tenant
} -> { aggregate: c is count() }`);
      expect(sources.mz_joined).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_joined)).toThrow(
         MaterializationEligibilityError,
      );
   });
});
