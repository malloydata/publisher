// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// End-to-end routing contract for the `storage=` virtual-source serve path,
// exercised through the real Model.runQuery against a real in-memory
// DuckDB. The ORIGINAL source returns 0; the serve binding points at a table
// returning 60 — so the value observed proves whether the query was routed
// through the served copy (60) or served live from the original model (0). This pins the mode gate, the binding gate, and the safe fallback.
import { unrecordedAudit } from "./access_record";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import { Model } from "./model";
import type { ServeBinding } from "./materialization/serve/materialization_serve_transform";
import { installSettings } from "../test_helpers/settings_env";
import { QueryPrivileges } from "./model_query_request";
import {
   type ServedCopyFiles,
   writeServedCopyFiles,
} from "../test_helpers/served_copy_files";

/** Puts back the settings the current test installed. */
let restoreSettings: (() => void) | undefined;

/**
 * Install PERSIST_STORAGE_MODE=`mode` for this test, before its model is built;
 * every other setting is at its default.
 */
function setStorageMode(mode: string): void {
   restoreSettings?.();
   restoreSettings = installSettings({
      env: { PERSIST_STORAGE_MODE: mode },
   }).restore;
}

afterEach(() => {
   restoreSettings?.();
   restoreSettings = undefined;
});

/** The files the last {@link buildModel} wrote, for a sourceID by URL. */
let lastFiles: ServedCopyFiles | undefined;
/** `name`'s sourceID in `file` of the last model built. */
const sourceIDIn = (name: string, file: string) =>
   `${name}@${lastFiles!.urlOf(file)}`;
const QUERY = "run: X -> { aggregate: t is total.sum() }";
const BINDING: ServeBinding = {
   sourceName: "X",
   destinationName: "duckdb",
   virtualHandle: "h",
   tablePath: "mz_real",
   schema: [{ name: "total", type: "BIGINT" }],
};

/**
 * A Model whose original source `X` yields total=0, with a real DuckDB
 * connection carrying `mz_real` (total=60) that the serve binding rebinds to.
 */
async function buildModel(opts?: {
   originalText?: string;
   storageSQL?: string;
   /** Further files beside `m.malloy`, by name. */
   files?: Record<string, string>;
   /** The model to serve; `m.malloy` by default. */
   entry?: string;
}): Promise<Model> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   // DuckDB shares a process-global :memory: db by connection name, so a prior
   // test's table can linger — replace it.
   await duckdb.runSQL(
      opts?.storageSQL ??
         "CREATE OR REPLACE TABLE mz_real AS SELECT 60 AS total",
   );
   const connMap = new Map<string, DuckDBConnection>([["duckdb", duckdb]]);
   const originalText =
      opts?.originalText ?? `source: X is duckdb.sql("SELECT 0 AS total")`;
   // On disk, as a package's files are: the served copy re-roots the
   // author's own text.
   const files = (lastFiles = writeServedCopyFiles({
      "m.malloy": originalText,
      ...(opts?.files ?? {}),
   }));
   const urlReader = new InMemoryURLReader(files.byUrl);
   const entry = opts?.entry ?? "m.malloy";
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(connMap, "duckdb"),
   });
   const mm = runtime.loadModel(new URL(files.urlOf(entry)), {
      importBaseURL: new URL(files.root),
   });
   const compiled = await mm.getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = (compiled as any)._modelDef;
   const modelInfo = modelDefToModelInfo(modelDef);
   const model = new Model({
      packageName: "pkg",
      modelPath: entry,
      dataStyles: {},
      modelType: "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      modelMaterializer: mm as any,
      modelDef,
      modelInfo,
   });
   // The serve shape compiles against the destination connections only, so hand
   // it a config carrying just this one — the same disjointness the Environment
   // enforces in production, expressed for a single-connection test.
   const serveConfig = new MalloyConfig({ connections: {} });
   serveConfig.wrapConnections(() => new FixedConnectionMap(connMap, "duckdb"));
   model.setServeDestinationConfig(() => serveConfig);
   model.setServedCopyContext(files.dir, connMap);
   return model;
}

async function runTotal(model: Model): Promise<number> {
   const res = await model.runQuery(
      unrecordedAudit(),
      { query: QUERY, filterParams: {} },
      QueryPrivileges.of({ bypassFilters: true }),
   );
   // compactResult is the row value array: [{ t: <sum> }].
   const rows = res.compactResult as unknown as { t: number }[];
   return Number(rows[0].t);
}

describe("storage= serve routing (end-to-end)", () => {
   it("mode=on + a binding routes the query to the materialized table", async () => {
      setStorageMode("on");
      const model = await buildModel();
      model.setServeBindings([BINDING]);
      expect(await runTotal(model)).toBe(60);
   });

   it("mode=off serves live even with a binding present (kill switch)", async () => {
      setStorageMode("off");
      const model = await buildModel();
      model.setServeBindings([BINDING]);
      expect(await runTotal(model)).toBe(0);
   });

   it("mode=write-only serves live (build-only rung)", async () => {
      setStorageMode("write-only");
      const model = await buildModel();
      model.setServeBindings([BINDING]);
      expect(await runTotal(model)).toBe(0);
   });

   it("mode=on with no bindings serves live", async () => {
      setStorageMode("on");
      const model = await buildModel();
      expect(await runTotal(model)).toBe(0);
   });

   it("falls back to live when the serve shape cannot compile (bad connection)", async () => {
      setStorageMode("on");
      const model = await buildModel();
      // A binding on a destination the config doesn't have — the serve-shape
      // compile throws, and the query must still succeed, served live.
      model.setServeBindings([{ ...BINDING, destinationName: "missing_dest" }]);
      expect(await runTotal(model)).toBe(0);
   });

   it("a binding for a source this model does not define does not break serving the model's own sources", async () => {
      // Bindings are pushed to EVERY model in the package, so a model can receive
      // a binding for a source it doesn't define (defined in a sibling model).
      // That source has no field list here, so its narrowed schema is empty — the
      // serve shape must simply omit it, not emit an empty `type: {}` that fails
      // the whole shape model and drops storage serving for X too.
      setStorageMode("on");
      const model = await buildModel();
      model.setServeBindings([
         BINDING,
         {
            sourceName: "OtherModelSource", // not defined in this model
            destinationName: "duckdb",
            virtualHandle: "h2",
            tablePath: "mz_real",
            schema: [{ name: "total", type: "BIGINT" }],
         },
      ]);
      expect(await runTotal(model)).toBe(60);
   });

   it("routes when the captured schema is in the warehouse's identifier case", async () => {
      // An upper-folding source warehouse (Snowflake, Oracle, Redshift, Teradata)
      // reports DESCRIBE names in ITS case, not the author's. Before the
      // case-insensitive narrowing this intersected to nothing, the binding was
      // dropped as empty-schema, and EVERY query on the source failed the shape
      // compile with "Reference to undefined object" and served live — silently,
      // since the rows are still correct. The physical column stays "TOTAL";
      // DuckDB resolves the author-cased declaration against it.
      setStorageMode("on");
      const model = await buildModel({
         storageSQL: 'CREATE OR REPLACE TABLE mz_real AS SELECT 60 AS "TOTAL"',
      });
      model.setServeBindings([
         { ...BINDING, schema: [{ name: "TOTAL", type: "BIGINT" }] },
      ]);
      expect(await runTotal(model)).toBe(60);
   });

   it("routes a query that references no field when the case differs", async () => {
      // The aggregate-only case, kept separate because it is the one that made the
      // bug unintuitive: `count()` names no column, so nothing about the QUERY is
      // case-sensitive — yet it fell back too, because the whole SOURCE had left
      // the shape. A fix that only reconciled field references would pass the test
      // above and still fail this one.
      setStorageMode("on");
      const model = await buildModel({
         storageSQL:
            'CREATE OR REPLACE TABLE mz_real AS SELECT 60 AS "TOTAL" UNION ALL SELECT 7',
      });
      model.setServeBindings([
         { ...BINDING, schema: [{ name: "TOTAL", type: "BIGINT" }] },
      ]);
      // 2 rows in storage vs the original source's 1 — so the count proves which
      // path ran without referencing a column.
      const res = await model.runQuery(
         unrecordedAudit(),
         { query: "run: X -> { aggregate: c is count() }", filterParams: {} },
         QueryPrivileges.of({ bypassFilters: true }),
      );
      expect((res.compactResult as unknown as { c: number }[])[0].c).toBe(2);
      expect(res.servedFrom).toBe("storage");
   });

   it("does not serve an except:-hidden column from storage when the case differs", async () => {
      // Vector A again, with the captured names upper-folded: the case-insensitive
      // match must widen which PHYSICAL column can match an author field without
      // widening the set of author fields. `secret` is except:-ed, so "SECRET" has
      // nothing public to match and stays out of the shape.
      setStorageMode("on");
      const model = await buildModel({
         originalText:
            `source: X is duckdb.sql("SELECT 0 AS total, 'live' AS secret") ` +
            `extend { except: secret }`,
         storageSQL:
            "CREATE OR REPLACE TABLE mz_real AS " +
            `SELECT 60 AS "TOTAL", 'from_storage' AS "SECRET"`,
      });
      model.setServeBindings([
         {
            ...BINDING,
            schema: [
               { name: "TOTAL", type: "BIGINT" },
               { name: "SECRET", type: "VARCHAR" },
            ],
         },
      ]);
      // The served copy keeps a stored column no field names under its
      // captured spelling, so the author's `except: secret` does not compile
      // over `SECRET`: the source is left as written and answers live
      // (known limitation), never from the table...
      expect(await runTotal(model)).toBe(0);
      expect(model.servedCopyProblems()).toEqual([
         expect.objectContaining({
            sourceName: "X",
            reason: "reroot_not_compiled",
         }),
      ]);
      // ...and the hidden one is still refused, not served as 'from_storage'.
      await expect(
         model.runQuery(
            unrecordedAudit(),
            { query: "run: X -> { group_by: secret }", filterParams: {} },
            QueryPrivileges.of({ bypassFilters: true }),
         ),
      ).rejects.toThrow();
   });

   it("serves a computed dimension, not the column its name folds onto", async () => {
      // Malloy permits two fields differing only in case. Here `Total` is a
      // dimension over `other` (2 * 10 = 20) and `total` is a physical column (1).
      // A fold-first narrowing declared the STORED column under the dimension's
      // name, the duplicate failed the shape down to base-only, and this query
      // returned 1 while reporting `servedFrom: storage` — wrong rows, no warning,
      // which is worse than any fallback. The assertion is the value, because the
      // shape compiling is not the property that matters.
      setStorageMode("on");
      const model = await buildModel({
         originalText:
            `source: X is duckdb.sql("SELECT 1 AS total, 2 AS other") extend {\n` +
            `  dimension: Total is other * 10\n}`,
         storageSQL:
            "CREATE OR REPLACE TABLE mz_real AS SELECT 1 AS total, 2 AS other",
      });
      model.setServeBindings([
         {
            ...BINDING,
            schema: [
               { name: "total", type: "BIGINT" },
               { name: "other", type: "BIGINT" },
            ],
         },
      ]);
      const computed = await model.runQuery(
         unrecordedAudit(),
         { query: "run: X -> { group_by: Total }", filterParams: {} },
         QueryPrivileges.of({ bypassFilters: true }),
      );
      expect(
         (computed.compactResult as unknown as { Total: number }[])[0].Total,
      ).toBe(20);
      expect(computed.servedFrom).toBe("storage");
      // And the physical column it collides with still resolves to itself.
      const raw = await model.runQuery(
         unrecordedAudit(),
         { query: "run: X -> { group_by: total }", filterParams: {} },
         QueryPrivileges.of({ bypassFilters: true }),
      );
      expect(
         (raw.compactResult as unknown as { total: number }[])[0].total,
      ).toBe(1);
      expect(raw.servedFrom).toBe("storage");
   });

   it("does not serve an except:-hidden column from storage (vector A)", async () => {
      setStorageMode("on");
      // X hides `secret` via except:, but the built table still carries it (the
      // build's getSQL projected it) and the binding schema captured it.
      const model = await buildModel({
         originalText:
            `source: X is duckdb.sql("SELECT 0 AS total, 'live' AS secret") ` +
            `extend { except: secret }`,
         storageSQL:
            "CREATE OR REPLACE TABLE mz_real AS " +
            "SELECT 60 AS total, 'from_storage' AS secret",
      });
      model.setServeBindings([
         {
            ...BINDING,
            schema: [
               { name: "total", type: "BIGINT" },
               { name: "secret", type: "VARCHAR" },
            ],
         },
      ]);
      // The public column still serves from storage (narrowing didn't break it).
      expect(await runTotal(model)).toBe(60);
      // A query on the hidden column must NOT be served from storage: the shape
      // no longer declares `secret`, so it falls back to live, where X's
      // `except:` makes `secret` undefined → the query is refused. (Before the
      // fix this returned 'from_storage' — the leak.)
      await expect(
         model.runQuery(
            unrecordedAudit(),
            { query: "run: X -> { group_by: secret }", filterParams: {} },
            QueryPrivileges.of({ bypassFilters: true }),
         ),
      ).rejects.toThrow();
   });
});

// malloydata/publisher#1001: bindings are pushed to every model by name, and
// names are not unique in a package. A binding carries the sourceIDs of the
// sources it materializes, and a model keeps it only for its own source of
// that name.
describe("storage= serve bindings match on source identity", () => {
   it("routes when the binding names the model's own source", async () => {
      setStorageMode("on");
      const model = await buildModel();
      model.setServeBindings([
         { ...BINDING, sourceIDs: [sourceIDIn("X", "m.malloy")] },
      ]);
      expect(await runTotal(model)).toBe(60);
   });

   it("serves live when the model's source of that name is another one", async () => {
      setStorageMode("on");
      const model = await buildModel();
      model.setServeBindings([
         { ...BINDING, sourceIDs: [sourceIDIn("X", "other.malloy")] },
      ]);
      expect(await runTotal(model)).toBe(0);
   });

   it("still routes a binding with no sourceIDs, by name, as before", async () => {
      setStorageMode("on");
      const model = await buildModel();
      model.setServeBindings([BINDING]);
      expect(await runTotal(model)).toBe(60);
   });

   it("routes the materialized source through a model that imports it", async () => {
      setStorageMode("on");
      const model = await buildModel({
         files: { "importer.malloy": `import "m.malloy"` },
         entry: "importer.malloy",
      });
      // An imported source keeps its declaring model's sourceID.
      model.setServeBindings([
         { ...BINDING, sourceIDs: [sourceIDIn("X", "m.malloy")] },
      ]);
      expect(await runTotal(model)).toBe(60);
   });

   it("serves a same-named source in another model live, even with overlapping columns", async () => {
      setStorageMode("on");
      // `other.malloy` declares its own X with a `total` column, so schema
      // narrowing alone would serve it from m.malloy's table.
      const model = await buildModel({
         files: {
            "other.malloy": `source: X is duckdb.sql("SELECT 0 AS total, 1 AS extra")`,
         },
         entry: "other.malloy",
      });
      model.setServeBindings([
         { ...BINDING, sourceIDs: [sourceIDIn("X", "m.malloy")] },
      ]);
      expect(await runTotal(model)).toBe(0);
   });

   it("serves live a same-named source the model reaches only through an import", async () => {
      setStorageMode("on");
      // `report.malloy` imports only `weekly`, so `X` (other.malloy's own, not
      // the persisted one) is a hidden dependency: in `sourceRegistry`, not in
      // `contents`. The serve shape rebinds hidden dependencies too.
      const model = await buildModel({
         files: {
            "other.malloy": `source: X is duckdb.sql("SELECT 0 AS total, 1 AS extra")
source: weekly is X -> { aggregate: total is total.sum() }`,
            "report.malloy": `import { weekly } from "other.malloy"`,
         },
         entry: "report.malloy",
      });
      model.setServeBindings([
         { ...BINDING, sourceIDs: [sourceIDIn("X", "m.malloy")] },
      ]);
      const res = await model.runQuery(
         unrecordedAudit(),
         {
            query: "run: weekly -> { aggregate: t is total.sum() }",
            filterParams: {},
         },
         QueryPrivileges.of({ bypassFilters: true }),
      );
      const rows = res.compactResult as unknown as { t: number }[];
      expect(Number(rows[0].t)).toBe(0);
   });

   it("without sourceIDs, the served copy still reads no same-named source from another's table", async () => {
      // The serve shape matched such a binding by name and served the wrong
      // table (the defect sourceIDs fix). The served copy resolves it to this
      // model's own X, whose `extra` the stored table lacks, so it declines.
      setStorageMode("on");
      const model = await buildModel({
         files: {
            "other.malloy": `source: X is duckdb.sql("SELECT 0 AS total, 1 AS extra")`,
         },
         entry: "other.malloy",
      });
      model.setServeBindings([BINDING]);
      expect(await runTotal(model)).toBe(0);
   });
});
