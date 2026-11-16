// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A served copy's pass on the load worker, through a real worker: taken only
 * by a worker that is ready and idle now, compiled over the pass's files as
 * the inline pass compiles them, failing as the inline pass fails, and
 * hydrated into a model that runs on the copy's own connections.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { pathToFileURL } from "url";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   MalloyConfig,
   MalloyError,
} from "@malloydata/malloy";
import { PackageLoadPool } from "./package_load_pool";
import {
   compileServedCopyOnWorker,
   servedCopyCompileError,
} from "./served_copy_compile";
import { serializeError } from "./error_wire";
import { packageFiles } from "../service/materialization/text/package_files";
import { overlayKeyOfPath } from "../service/overlay_key";
import { writeTempPackage } from "../test_helpers/temp_package";

const MODEL = `source: nums is duckdb.sql("select 1 as a")`;

describe("a served copy's pass on the load worker", () => {
   let pool: PackageLoadPool;
   let dir: string;
   let duckdb: DuckDBConnection;
   let config: MalloyConfig;

   beforeAll(async () => {
      pool = new PackageLoadPool(1);
      dir = writeTempPackage(
         { "m.malloy": MODEL },
         { prefix: "publisher-scw-" },
      ).dir;
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL(
         "CREATE TABLE stored AS SELECT 60 AS a UNION ALL SELECT 2",
      );
      config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
   });

   afterAll(async () => {
      await pool.shutdown();
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const job = (text: string) => ({
      packagePath: dir,
      modelPath: "m.malloy",
      config,
      files: new Map([[overlayKeyOfPath(path.join(dir, "m.malloy")), text]]),
   });

   it("is not taken while no worker is ready, and never spawns one", async () => {
      expect(await compileServedCopyOnWorker(null, job(MODEL))).toBeUndefined();
      expect(await compileServedCopyOnWorker(pool, job(MODEL))).toBeUndefined();
      expect(pool.size).toBe(0);
   });

   it("is not taken while the worker is busy", async () => {
      const load = pool.loadPackage({
         packagePath: dir,
         packageName: "pkg",
         environmentName: "env",
         malloyConfig: config,
         defaultConnectionName: "duckdb",
      });
      expect(await compileServedCopyOnWorker(pool, job(MODEL))).toBeUndefined();
      await load;
   });

   it("compiles the pass's files over the package's, as the inline pass does", async () => {
      const text = `source: nums is duckdb.table("stored")`;
      const onWorker = await compileServedCopyOnWorker(pool, job(text));
      expect(onWorker).toBeDefined();
      const inline = await packageFiles(dir, "m.malloy", config).loadModel(
         job(text).files,
      );
      const inlineDef = (
         (await inline.materializer.getModel()) as unknown as {
            _modelDef: unknown;
         }
      )._modelDef;
      expect(JSON.stringify(onWorker!.def)).toBe(JSON.stringify(inlineDef));
      // Hydrated on the copy's connections: it runs, and so does a model
      // reloaded through its runtime (what a row-level graft does).
      const query = "run: nums -> { aggregate: t is a.sum() }";
      const rows = (await onWorker!.materializer.loadQuery(query).run()).data
         .value;
      expect(rows).toEqual([{ t: 62 }]);
      const regrafted = onWorker!.runtime._loadModelFromModelDef(
         onWorker!.def as never,
      );
      expect((await regrafted.loadQuery(query).run()).data.value).toEqual([
         { t: 62 },
      ]);
   });

   it("fails as the inline pass fails: a MalloyError with the same problems", async () => {
      const text = `source: nums is duckdb.sql("select 1 as a") extend { except: nope }`;
      const onWorker = await compileServedCopyOnWorker(pool, job(text)).then(
         () => undefined,
         (e: unknown) => e,
      );
      const inline = await packageFiles(dir, "m.malloy", config)
         .loadModel(job(text).files)
         .then((l) => l.materializer.getModel())
         .then(
            () => undefined,
            (e: unknown) => e,
         );
      expect(onWorker).toBeInstanceOf(MalloyError);
      expect(inline).toBeInstanceOf(MalloyError);
      expect((onWorker as MalloyError).message).toBe(
         (inline as MalloyError).message,
      );
      expect(JSON.stringify((onWorker as MalloyError).problems)).toBe(
         JSON.stringify((inline as MalloyError).problems),
      );
   });

   it("is not taken once the pool shuts down", async () => {
      const other = new PackageLoadPool(1);
      await other.shutdown();
      expect(
         await compileServedCopyOnWorker(other, job(MODEL)),
      ).toBeUndefined();
   });
});

describe("servedCopyCompileError", () => {
   it("restores a MalloyError with its problems, and any other error by name", () => {
      const problems = [
         {
            message: "`nope` is not defined",
            severity: "error",
            at: { url: pathToFileURL("/p/m.malloy").href },
         },
      ];
      const malloy = servedCopyCompileError(
         serializeError(new MalloyError("bad", problems as never)),
      );
      expect(malloy).toBeInstanceOf(MalloyError);
      expect(malloy.message).toBe("bad");
      expect((malloy as MalloyError).problems).toEqual(problems as never);

      const plain = new TypeError("no table");
      const restored = servedCopyCompileError(serializeError(plain));
      expect(restored).not.toBeInstanceOf(MalloyError);
      expect(restored.name).toBe("TypeError");
      expect(restored.message).toBe("no table");
   });
});
