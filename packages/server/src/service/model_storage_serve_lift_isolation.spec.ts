// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A lifted wrapper that passes the static lift checks but does not compile must
// cost only itself. The shape is probed with every lift first; when that fails
// but the shape compiles without them, the lifts are re-added one at a time and
// each that compiles is kept. Without the re-add, one bad wrapper sends every
// wrapper in the model live.
//
// The LIVE relation behind the fact is empty, so a wrapper answering with rows
// is provably reading the stored table, and one answering with none fell back.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import type { ServeBinding } from "./materialization_serve_transform";
import { Model } from "./model";

/**
 * `good` reads only the fact. `bad` reads through `j`, the fact's own join to a
 * source nothing materializes: its pipeline names no other source, so the static
 * checks pass it, and it fails only when the shape is compiled without `j`.
 */
const MODEL = `##! experimental.persistence
source: other is duckdb.sql("SELECT 1 AS id, 'x' AS label")

#@ persist name="mz_fact" storage=duckdb
source: fact is duckdb.sql("SELECT 0 AS id, 0 AS amount WHERE false") -> { select: * } extend {
  join_one: j is other on id = j.id
}

source: good is fact -> { select: id, amount }
source: bad is fact -> { group_by: j.label; aggregate: n is count() }
`;

const STORED = `CREATE OR REPLACE TABLE mz_fact AS
   SELECT * FROM (VALUES (1, 10), (1, 20)) AS t(id, amount)`;

const BINDING: ServeBinding = {
   sourceName: "fact",
   destinationName: "duckdb",
   virtualHandle: "h_fact",
   tablePath: "mz_fact",
   schema: [
      { name: "id", type: "BIGINT" },
      { name: "amount", type: "BIGINT" },
   ],
};

let dir: string;
let modelUrl: URL;

beforeAll(async () => {
   // On disk rather than in memory only: a lift is carried as the author's
   // declaration text, which the serve path reads from the model file.
   dir = await fs.mkdtemp(path.join(os.tmpdir(), "lift-isolation-"));
   await fs.writeFile(path.join(dir, "m.malloy"), MODEL);
   modelUrl = pathToFileURL(path.join(dir, "m.malloy"));
});

afterAll(async () => {
   await fs.rm(dir, { recursive: true, force: true });
});

async function buildModel(): Promise<Model> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   await duckdb.runSQL(STORED);
   const connMap = new Map<string, DuckDBConnection>([["duckdb", duckdb]]);
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[modelUrl.href, MODEL]])),
      connections: new FixedConnectionMap(connMap, "duckdb"),
   });
   const mm = runtime.loadModel(modelUrl);
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
      [],
      modelDefToModelInfo(modelDef),
   );
   const serveConfig = new MalloyConfig({ connections: {} });
   serveConfig.wrapConnections(() => new FixedConnectionMap(connMap, "duckdb"));
   model.setServeDestinationConfig(() => serveConfig);
   model.setServeBindings([BINDING]);
   return model;
}

async function rows(model: Model, query: string): Promise<unknown[]> {
   const res = await model.getQueryResults(
      undefined,
      undefined,
      query,
      {},
      true,
   );
   return res.compactResult as unknown as unknown[];
}

describe("a lifted wrapper that does not compile costs only itself", () => {
   afterEach(() => {
      delete process.env.PERSIST_STORAGE_MODE;
   });

   it("keeps serving the clean wrapper from storage beside a broken one", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();

      // Two stored rows. Live is empty, so two rows means the wrapper was on
      // the shape; zero means one bad sibling took it off.
      expect(
         await rows(model, "run: good -> { aggregate: s is amount.sum() }"),
      ).toEqual([{ s: 30 }]);
      // The broken wrapper still answers — live, from the empty relation, so
      // its count is of nothing. The point is that it answers at all rather
      // than failing the query.
      expect(
         await rows(model, "run: bad -> { aggregate: t is n.sum() }"),
      ).toEqual([{ t: 0 }]);
   });
});
