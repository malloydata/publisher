// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A served copy compiles once per thing it is compiled from, not once per
 * bind (ms2data/service#7795: every manifest bind recompiled every model on
 * the main thread). Driven through the real `Model` over a real in-memory
 * DuckDB, counting the copy's Malloy compiles at `GeneratedFiles.loadModel`,
 * the one seam every inline pass compiles through, and at the load worker
 * pool for a pass a worker takes.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import { logger } from "../logger";
import { installSettings } from "../test_helpers/settings_env";
import { materializationFixture } from "../test_helpers/materialization_fixture";
import type { Binding } from "./materialization/domain/binding";
import { GeneratedFiles } from "./materialization/text/generated_files";
import { PackageLoadPool } from "../package_load/package_load_pool";
import { SourceMap } from "./materialization/text/source_map";
import {
   compileInputOf,
   type ServedCopyServe,
} from "./materialization/serve/served_copy_serve";
import { unrecordedAudit } from "./access_record";
import { Model } from "./model";
import { QueryPrivileges } from "./model_query_request";

const mz = materializationFixture();
afterAll(() => mz.close());

const BINDING: Binding = {
   kind: "persist",
   tier: "storage",
   sourceName: "X",
   destinationName: "duckdb",
   virtualHandle: "h",
   tablePath: "mz_rebind",
   schema: [{ name: "total", type: "BIGINT" }],
};

/** A model over `originalText`, with the context and destinations its package would set. */
async function buildModel(originalText: string, storageSQL: string) {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   await duckdb.runSQL(storageSQL);
   const connMap = new Map<string, DuckDBConnection>([["duckdb", duckdb]]);
   const files = mz.servedCopy({ "m.malloy": originalText });
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(files.byUrl),
      connections: new FixedConnectionMap(connMap, "duckdb"),
   });
   const mm = runtime.loadModel(new URL(files.urlOf("m.malloy")), {
      importBaseURL: new URL(files.root),
   });
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = ((await mm.getModel()) as any)._modelDef;
   const model = new Model({
      packageName: "pkg",
      modelPath: "m.malloy",
      dataStyles: {},
      modelType: "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      modelMaterializer: mm as any,
      modelDef,
      modelInfo: modelDefToModelInfo(modelDef),
   });
   const serveConfig = new MalloyConfig({ connections: {} });
   serveConfig.wrapConnections(() => new FixedConnectionMap(connMap, "duckdb"));
   const destinations = () => serveConfig;
   const plans = {};
   model.setServeDestinationConfig(destinations);
   model.setServedCopyContext(files.dir, connMap, plans);
   return { model, dir: files.dir, connMap, destinations, plans };
}

/** `X`'s total: 0 live, 60 from the stored table. */
async function total(model: Model): Promise<number> {
   const res = await model.runQuery(
      unrecordedAudit(),
      { query: "run: X -> { aggregate: t is total.sum() }", filterParams: {} },
      QueryPrivileges.of({ bypassFilters: true }),
   );
   return Number((res.compactResult as unknown as { t: number }[])[0].t);
}

const PLAIN = `source: X is duckdb.sql("SELECT 0 AS total")`;
const STORED = "CREATE OR REPLACE TABLE mz_rebind AS SELECT 60 AS total";

describe("a served copy compiles once per thing it is compiled from", () => {
   let restore: (() => void) | undefined;
   let compiles: ReturnType<typeof spyOn>;
   let offThread: ReturnType<typeof spyOn>;
   /** The copy's compiles: inline, and those a load worker took. */
   const compileCount = () =>
      compiles.mock.calls.length +
      offThread.mock.results.filter(
         (r: { value: unknown }) => r.value !== undefined,
      ).length;
   beforeEach(() => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      compiles = spyOn(GeneratedFiles.prototype, "loadModel");
      offThread = spyOn(PackageLoadPool.prototype, "compileServedCopy");
   });
   afterEach(() => {
      compiles.mockRestore();
      offThread.mockRestore();
      restore?.();
      restore = undefined;
   });

   it("keeps the compiled copy when its context and destinations are set again unchanged", async () => {
      const { model, dir, connMap, destinations, plans } = await buildModel(
         PLAIN,
         STORED,
      );
      model.setServeBindings([BINDING]);
      await model.prepareServedCopy();
      const first = compileCount();
      expect(first).toBeGreaterThan(0);
      expect(model.servedCopyProblems()).toEqual([]);
      expect(await total(model)).toBe(60);
      const afterQuery = compileCount();

      // What every bind of the same manifest does to each model.
      for (let i = 0; i < 5; i++) {
         model.setServedCopyContext(dir, connMap, plans);
         model.setServeDestinationConfig(destinations);
         await model.prepareServedCopy();
      }
      expect(compileCount()).toBe(afterQuery);
      // And it still answers from the table.
      expect(await total(model)).toBe(60);
   });

   it("recompiles when the context, the destinations or the bindings change", async () => {
      const { model, dir, connMap, plans } = await buildModel(PLAIN, STORED);
      model.setServeBindings([BINDING]);
      await model.prepareServedCopy();
      let seen = compileCount();
      const recompiled = async () => {
         await model.prepareServedCopy();
         const more = compileCount() > seen;
         seen = compileCount();
         return more;
      };

      // Other connections: a reload's new config.
      model.setServedCopyContext(dir, new Map(connMap), plans);
      expect(await recompiled()).toBe(true);
      // Other plans: a re-derived build plan.
      model.setServedCopyContext(dir, connMap, {});
      expect(await recompiled()).toBe(true);
      // Another destinations provider.
      const other = new MalloyConfig({ connections: {} });
      other.wrapConnections(
         () => new FixedConnectionMap(connMap as never, "duckdb"),
      );
      model.setServeDestinationConfig(() => other);
      expect(await recompiled()).toBe(true);
      // Another table for the same source.
      await connMap
         .get("duckdb")!
         .runSQL("CREATE OR REPLACE TABLE mz_rebind_2 AS SELECT 61 AS total");
      model.setServeBindings([{ ...BINDING, tablePath: "mz_rebind_2" }]);
      expect(await recompiled()).toBe(true);
      expect(await recompiled()).toBe(false);
   });

   it("keeps the compiled copy on a rebind that changes only how fresh a table is", async () => {
      const { model } = await buildModel(PLAIN, STORED);
      const asOf = (iso: string): Binding => ({
         ...BINDING,
         freshAsOf: iso,
         freshnessWindowSeconds: 10 * 365 * 86400,
         freshnessFallback: "live",
      });
      model.setServeBindings([asOf("2026-01-01T00:00:00.000Z")]);
      await model.prepareServedCopy();
      expect(await total(model)).toBe(60);
      const seen = compileCount();
      expect(seen).toBeGreaterThan(0);

      // A new data-as-of instant, a new window or fallback, the router's own
      // gate mark: none of it is read by the compile.
      const later = asOf("2026-02-01T00:00:00.000Z");
      model.setServeBindings([later]);
      await model.prepareServedCopy();
      model.setServeBindings([
         { ...later, freshnessWindowSeconds: 20 * 365 * 86400 },
      ]);
      await model.prepareServedCopy();
      const last: Binding = {
         ...later,
         freshnessFallback: "stale_ok",
         ownGateKept: true,
      };
      model.setServeBindings([last]);
      await model.prepareServedCopy();
      expect(compileCount()).toBe(seen);
      expect(await total(model)).toBe(60);
      expect(compileCount()).toBe(seen);

      // An answer reports the bindings pinned now, not those it compiled over.
      const copy = (model as unknown as { servedCopy: ServedCopyServe })
         .servedCopy;
      const answer = await copy.loadQuery(
         "run: X -> { aggregate: t is total.sum() }",
      );
      expect(answer.bindings).toEqual([last]);
      expect(answer.bindings[0]).toBe(last);
      expect(answer.tablesRead[0]).toBe(last);
      expect(compileCount()).toBe(seen);
   });

   it("keeps the latest bind's copy when an older bind's compile finishes after it", async () => {
      const { model } = await buildModel(PLAIN, STORED);
      const asOf = (iso: string): Binding => ({ ...BINDING, freshAsOf: iso });
      model.setServeBindings([asOf("2026-01-01T00:00:00.000Z")]);
      await model.prepareServedCopy();
      const seen = compileCount();
      // What a package's bind does: empty, then the tables again, each
      // compiling at bind without waiting for the one before.
      model.setServeBindings([]);
      const emptied = model.prepareServedCopy();
      model.setServeBindings([asOf("2026-02-01T00:00:00.000Z")]);
      const rebound = model.prepareServedCopy();
      await Promise.all([emptied, rebound]);
      // The empty copy, compiled for the older bind, did not replace the
      // reused one: a freshness-only rebind after it compiles nothing.
      model.setServeBindings([asOf("2026-03-01T00:00:00.000Z")]);
      await model.prepareServedCopy();
      expect(compileCount()).toBe(seen);
      expect(await total(model)).toBe(60);
   });

   it("recompiles for every binding field the compile reads", async () => {
      const { model, connMap } = await buildModel(PLAIN, STORED);
      await connMap
         .get("duckdb")!
         .runSQL("CREATE OR REPLACE TABLE mz_rebind_b AS SELECT 60 AS total");
      model.setServeBindings([BINDING]);
      await model.prepareServedCopy();
      let seen = compileCount();
      const recompiledFor = async (b: Binding) => {
         model.setServeBindings([b]);
         await model.prepareServedCopy();
         const more = compileCount() > seen;
         seen = compileCount();
         return more;
      };
      const changes: Partial<Binding>[] = [
         { tablePath: "mz_rebind_b" },
         { virtualHandle: "h2" },
         { schema: [{ name: "total", type: "INTEGER" }] },
         { nativeSchema: [{ name: "total", type: "HUGEINT" }] },
         { primaryKeys: { x: true } },
         {
            sourceIDs: [
               SourceMap.of(
                  (model as unknown as { modelDef: unknown }).modelDef,
               ).sourceIDOf("X")!,
            ],
         },
         { tier: "colocated" },
         { kind: "materialize" },
      ];
      for (const change of changes) {
         expect([
            Object.keys(change)[0],
            await recompiledFor({ ...BINDING, ...change }),
         ]).toEqual([Object.keys(change)[0], true]);
         // Back to the first binding: a change again, so compiled again.
         expect(await recompiledFor(BINDING)).toBe(true);
      }
      // The compile's inputs are the field list less freshness and the
      // router's marks: a field `Binding` adds is read until it is listed.
      expect(
         compileInputOf({
            ...BINDING,
            freshAsOf: "a",
            freshnessWindowSeconds: 1,
         }),
      ).toBe(compileInputOf(BINDING));
      expect(
         compileInputOf({ ...BINDING, unlisted: 1 } as unknown as Binding),
      ).not.toBe(compileInputOf(BINDING));
   });

   it("compiles nothing for a model with no bindings", async () => {
      const { model } = await buildModel(PLAIN, STORED);
      model.setServeBindings([]);
      await model.prepareServedCopy();
      // A storage binding the tier cannot serve (storage mode off) is none.
      restore?.();
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "off" },
      }).restore;
      model.setServeBindings([BINDING]);
      await model.prepareServedCopy();
      expect(compileCount()).toBe(0);
      expect(model.servedCopySourceIDs()).toEqual(new Set());
      expect(model.servedCopyProblems()).toEqual([]);
   });

   /**
    * The vector model_storage_serve.spec.ts pins: the stored column's
    * captured spelling makes the author's `except:` fail in the copy.
    */
   const FAILING: Binding = {
      ...BINDING,
      schema: [
         { name: "TOTAL", type: "BIGINT" },
         { name: "SECRET", type: "VARCHAR" },
      ],
   };
   const failingModel = () =>
      buildModel(
         `source: X is duckdb.sql("SELECT 0 AS total, 'live' AS secret") ` +
            `extend { except: secret }`,
         "CREATE OR REPLACE TABLE mz_rebind AS " +
            `SELECT 60 AS "TOTAL", 'from_storage' AS "SECRET"`,
      );
   let warn: ReturnType<typeof spyOn<typeof logger, "warn">>;
   beforeEach(() => {
      warn = spyOn(logger, "warn");
   });
   afterEach(() => warn.mockRestore());
   const steppedBack = () =>
      warn.mock.calls.filter((c) =>
         String(c[0]).startsWith("A re-root does not compile"),
      ).length;

   it("compiles a re-root that fails once per model text and binding, and warns once", async () => {
      const { model, connMap, dir } = await failingModel();
      const failing = FAILING;
      {
         model.setServeBindings([failing]);
         await model.prepareServedCopy();
         const first = compileCount();
         expect(first).toBeGreaterThan(0);
         expect(steppedBack()).toBe(1);
         const problems = model.servedCopyProblems();
         expect(problems).toEqual([
            expect.objectContaining({
               sourceName: "X",
               reason: "reroot_not_compiled",
            }),
         ]);

         // The same text and table again (a recompile with a new build
         // plan): the same step back, with no compile and no second warning.
         model.setServedCopyContext(dir, connMap, {});
         await model.prepareServedCopy();
         expect(compileCount()).toBe(first);
         expect(steppedBack()).toBe(1);
         expect(model.servedCopyProblems()).toEqual(problems);
         expect(model.servedCopySourceIDs()).toEqual(new Set());

         // Another table is another copy: compiled (and failing) afresh.
         await connMap
            .get("duckdb")!
            .runSQL(
               "CREATE OR REPLACE TABLE mz_rebind_3 AS " +
                  `SELECT 60 AS "TOTAL", 'from_storage' AS "SECRET"`,
            );
         model.setServeBindings([{ ...failing, tablePath: "mz_rebind_3" }]);
         await model.prepareServedCopy();
         expect(compileCount()).toBeGreaterThan(first);
         expect(steppedBack()).toBe(2);
      }
   });

   it("forgets a failed re-root when the destinations or the warehouse change", async () => {
      const { model, connMap } = await failingModel();
      model.setServeBindings([FAILING]);
      await model.prepareServedCopy();
      let seen = compileCount();
      const recompiled = async () => {
         await model.prepareServedCopy();
         const more = compileCount() > seen;
         seen = compileCount();
         return more;
      };
      // A destination list swap (Environment.invalidateCompiledLegs).
      model.invalidateCompiledLegs();
      expect(await recompiled()).toBe(true);
      expect(steppedBack()).toBe(2);
      // Another destinations provider.
      const other = new MalloyConfig({ connections: {} });
      other.wrapConnections(
         () => new FixedConnectionMap(connMap as never, "duckdb"),
      );
      model.setServeDestinationConfig(() => other);
      expect(await recompiled()).toBe(true);
      expect(steppedBack()).toBe(3);
      expect(model.servedCopyProblems()).toEqual([
         expect.objectContaining({ reason: "reroot_not_compiled" }),
      ]);
   });

   it("neither reads nor seeds the memo from a preview, which pins no time", async () => {
      const { model, connMap, dir } = await failingModel();
      // The live copy fails and remembers it.
      model.setServeBindings([FAILING]);
      await model.prepareServedCopy();
      let seen = compileCount();
      const compiled = () => {
         const more = compileCount() > seen;
         seen = compileCount();
         return more;
      };
      // Each preview compiles for itself, though the live copy's failure is
      // held for the same text and tables.
      await model.previewServedCopy([FAILING]);
      expect(compiled()).toBe(true);
      await model.previewServedCopy([FAILING]);
      expect(compiled()).toBe(true);
      // And the live copy still holds its own: a recompile replays it.
      model.setServedCopyContext(dir, connMap, {});
      await model.prepareServedCopy();
      expect(compiled()).toBe(false);
      expect(model.servedCopyProblems()).toEqual([
         expect.objectContaining({ reason: "reroot_not_compiled" }),
      ]);
   });
});
