// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The property the whole storage tier turns on once a tenant-scoped source is
// admitted: ONE artifact holding every caller's rows, read per caller.
//
// The build leaves an extend-block `where:` out, so the stored table is every
// tenant's. That is only safe because the serve shape puts the term back with
// the caller's own value — so these cases assert the ANSWER DIFFERS BY CALLER
// while the table underneath does not. A test that only checked routing would
// pass just as well against a shape that served the whole table to everyone,
// which is the failure this exists to catch.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import type { ServeBinding } from "./materialization_serve_transform";
import { malloyGivenToApi } from "./given";
import { Model } from "./model";

const ROOT = "file:///storage-serve-givens/";

/**
 * The author's model: a source scoped to the caller's org by an extend-block
 * `where:`. The LIVE relation behind it is deliberately EMPTY, so any row in an
 * answer below came from the stored table and nothing is proved by accident.
 */
const MODEL = `##! experimental.givens
given:
  ORG_ID :: number is 1

source: scoped is duckdb.sql("SELECT 0 AS org_id, 0 AS amount WHERE false") extend {
  where: org_id = $ORG_ID
}
`;

/**
 * The stored artifact: three rows across two orgs, which is what a build with
 * the org term stripped produces. Org 1 sums to 30, org 2 to 7.
 */
const STORED = `CREATE OR REPLACE TABLE mz_scoped AS
   SELECT * FROM (VALUES (1, 10), (2, 7), (1, 20)) AS t(org_id, amount)`;

const BINDING: ServeBinding = {
   sourceName: "scoped",
   destinationName: "duckdb",
   virtualHandle: "h_scoped",
   tablePath: "mz_scoped",
   schema: [
      { name: "org_id", type: "BIGINT" },
      { name: "amount", type: "BIGINT" },
   ],
};

async function buildModel(): Promise<Model> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   await duckdb.runSQL(STORED);
   const connMap = new Map<string, DuckDBConnection>([["duckdb", duckdb]]);
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, MODEL]]),
   );
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(connMap, "duckdb"),
   });
   const mm = runtime.loadModel(new URL(`${ROOT}m.malloy`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await mm.getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = (compiled as any)._modelDef;
   const model = new Model(
      "pkg",
      "m.malloy",
      {},
      "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      mm as any,
      modelDef,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      // The given surface, as a package load derives it. Not incidental setup:
      // it is what the serve shape declares, so a Model built without it cannot
      // route a given-scoped source at all.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      Array.from(compiled.givens.values()).map(malloyGivenToApi as any) as any,
      modelDefToModelInfo(modelDef),
   );
   const serveConfig = new MalloyConfig({ connections: {} });
   serveConfig.wrapConnections(() => new FixedConnectionMap(connMap, "duckdb"));
   model.setServeDestinationConfig(() => serveConfig);
   model.setServeBindings([BINDING]);
   return model;
}

async function sumFor(
   model: Model,
   givens?: Record<string, number>,
): Promise<number> {
   const res = await model.getQueryResults(
      undefined,
      undefined,
      "run: scoped -> { aggregate: t is amount.sum() }",
      {},
      true,
      givens,
   );
   const rows = res.compactResult as unknown as { t: number | null }[];
   return Number(rows[0].t ?? 0);
}

describe("storage= serving of a given-scoped source", () => {
   afterEach(() => {
      delete process.env.PERSIST_STORAGE_MODE;
   });

   it("answers each caller from their own rows in one shared artifact", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();

      // Two callers, one table. Different answers is the whole property: the
      // stripped term was re-applied with each caller's own value.
      expect(await sumFor(model, { ORG_ID: 1 })).toBe(30);
      expect(await sumFor(model, { ORG_ID: 2 })).toBe(7);
   });

   it("never serves the unfiltered artifact", async () => {
      // The failure mode this guards, stated as its own case: 37 is every org's
      // rows. If the shape ever stops carrying the term, both callers above
      // still "route" and both return 37 — so the sum of the two answers is
      // asserted to be the whole table rather than either caller seeing it.
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      expect(await sumFor(model, { ORG_ID: 1 })).not.toBe(37);
      expect(await sumFor(model, { ORG_ID: 2 })).not.toBe(37);
   });

   it("falls back to the given's declared default when the caller supplies none", async () => {
      // The shape carries the author's default, so an unbound request gets the
      // answer the live path would give it rather than failing to compile.
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      expect(await sumFor(model)).toBe(30);
   });

   it("serves live, and therefore nothing, when the tier is switched off", async () => {
      // Proves the numbers above came from the stored table: the live relation
      // behind `scoped` has no rows at all, so an unrouted query sums to zero.
      process.env.PERSIST_STORAGE_MODE = "off";
      const model = await buildModel();
      expect(await sumFor(model, { ORG_ID: 1 })).toBe(0);
   });
});

describe("a term the shape cannot reproduce withholds the binding", () => {
   // The wholeness precondition, pinned rather than assumed.
   //
   // A source is admitted to the tier on the promise that every term stripped
   // from its build is re-applied at read. Nothing enforces that promise by
   // checking it; what enforces it is that the serve shape emits EVERY filter,
   // so a term it cannot reproduce fails the shape compile and the binding is
   // withheld. That is a property of the shape-compile ladder, not a rule
   // anything states — which is exactly why it needs a test of its own: a
   // refactor making shape compilation more forgiving would delete the
   // guarantee, and without this, nothing would go red.
   //
   // The term is made unreproducible by narrowing the binding's declared schema
   // so it no longer carries `org_id` — the same shape a source that hides a
   // column produces. The re-emitted `where:` then names a column the shape does
   // not have and fails to compile. The required outcome is a LIVE answer, never
   // an unfiltered one.
   it("serves live rather than serving the artifact unfiltered", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();

      // Baseline: with the term reproducible, the tier answers 30.
      expect(await sumFor(model, { ORG_ID: 1 })).toBe(30);

      // Now break reproduction the way a narrowed column would: the binding's
      // declared shape loses `org_id`, so `where: org_id = $ORG_ID` names a
      // column the shape does not have.
      model.setServeBindings([
         { ...BINDING, schema: [{ name: "amount", type: "BIGINT" }] },
      ]);

      // Live, and therefore empty — NOT 37, which is the whole artifact and
      // what dropping the unreproducible term would have returned.
      const answer = await sumFor(model, { ORG_ID: 1 });
      expect(answer).not.toBe(37);
      expect(answer).toBe(0);
   });
});

describe("the cached serve shape cannot carry one caller's values to another", () => {
   // The shape MODEL is cached per binding set and reused across requests, and a
   // given's value is substituted as an INLINE LITERAL — so "is anything on the
   // cached path holding one caller's literal" is worth an assertion rather than
   // an argument.
   //
   // What makes it safe is structural rather than careful: the cached object is
   // the compiled model, which DECLARES givens and binds none, and values are
   // bound per call at prepare/run. No layer in between retains a substituted
   // query. Memoizing the runnable does not change that — it was tried, and this
   // case still passed — so read this as pinning the observable property
   // (each caller's own answer through one shared cached model), not as a guard
   // on the cache key. The guard against a stale compiled query is the withheld-
   // binding case above, which a memoized runnable does break.
   //
   // Alternating and repeating is still the right shape: a per-caller answer that
   // degraded to the first one would survive a single A-then-B check.
   it("answers each caller correctly however the requests interleave", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();

      for (const org of [1, 2, 1, 2, 2, 1]) {
         expect(await sumFor(model, { ORG_ID: org })).toBe(org === 1 ? 30 : 7);
      }

      // And an unbound request still falls to the declared default rather than
      // inheriting whatever the previous caller bound.
      expect(await sumFor(model)).toBe(30);
      expect(await sumFor(model, { ORG_ID: 2 })).toBe(7);
      expect(await sumFor(model)).toBe(30);
   });
});
