// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   isSourceDef,
   type ModelDef,
   Runtime,
   type SourceDef,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { structsReachedFrom } from "../../governance/ir/source_graph";
import {
   closeCorpus,
   fixtureModels,
   hammerModels,
   releasingEach,
} from "../../../test_helpers/persist_corpus";
import { reachableSourceIDs, SourceMap, sourceChain } from "./source_map";

// File-backed: db-duckdb's in-memory database is shared across spec files in
// one run, so a fixed table name would collide.
const dbDir = fs.mkdtempSync(path.join(os.tmpdir(), "source-map-spec-"));
const duckdb = new DuckDBConnection("duckdb", path.join(dbDir, "db.duckdb"));

async function compile(
   files: Record<string, string>,
   entry: string,
): Promise<ModelDef> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map(
            Object.entries(files).map(([name, text]) => [
               `file:///pkg/${name}`,
               text,
            ]),
         ),
      ),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const model = await runtime
      .loadModel(new URL(`file:///pkg/${entry}`))
      .getModel();
   return model._modelDef;
}

beforeAll(async () => {
   await duckdb.runSQL(
      "CREATE TABLE orders AS SELECT 1 AS id, 'east' AS region, 10.0 AS amount",
   );
   await duckdb.runSQL(
      "CREATE TABLE regions AS SELECT 'east' AS region, 'East' AS label",
   );
});

afterAll(async () => {
   await duckdb.close();
   await closeCorpus();
   fs.rmSync(dbDir, { recursive: true, force: true });
});

const MODEL = `##! experimental.persistence
source: orders is duckdb.table('orders') extend {
  primary_key: id
  measure: total is amount.sum()
}
source: regions is duckdb.table('regions') extend { primary_key: region }

#@ persist name="daily"
source: daily is orders -> { group_by: region; aggregate: total }

source: daily_ext is daily extend {
  join_one: r is regions on region = r.region
}
source: renamed is daily_ext
`;

describe("SourceMap", () => {
   it("keys each declaration by sourceID, with the name only an index", async () => {
      const modelDef = await compile({ "model.malloy": MODEL }, "model.malloy");
      const map = SourceMap.of(modelDef);
      const dailyID = map.sourceIDOf("daily");
      expect(dailyID).toMatch(/^daily@file:\/\/\/pkg\/model\.malloy$/);
      const daily = map.node(dailyID!)!;
      expect(daily.kind).toBe("query");
      expect(daily.base?.hop).toBe("query");
      expect(daily.base?.sourceID).toBe(map.sourceIDOf("orders"));
      const ext = map.node(map.sourceIDOf("daily_ext")!)!;
      expect(ext.kind).toBe("extend");
      expect(ext.base?.sourceID).toBe(dailyID);
      // `table(...) extend { … }` is a table with fields: no base edge.
      expect(map.node(map.sourceIDOf("orders")!)!.kind).toBe("table");
      // One map per compiled model.
      expect(SourceMap.of(modelDef)).toBe(map);
   });

   it("walks a chain through an extend and a query-shaped base", async () => {
      const modelDef = await compile({ "model.malloy": MODEL }, "model.malloy");
      const map = SourceMap.of(modelDef);
      expect(sourceChain(modelDef.contents["daily_ext"], modelDef)).toEqual([
         map.sourceIDOf("daily_ext")!,
         map.sourceIDOf("daily")!,
         map.sourceIDOf("orders")!,
      ]);
      // A plain alias records its target as `referenceID`, and a copy of the
      // target's own `extends` (the grandparent): the chain goes through the
      // target, so every table on it stands for the alias.
      expect(sourceChain(modelDef.contents["renamed"], modelDef)).toEqual([
         map.sourceIDOf("renamed")!,
         map.sourceIDOf("daily_ext")!,
         map.sourceIDOf("daily")!,
         map.sourceIDOf("orders")!,
      ]);
   });

   it("gives a source re-exported through index.malloy the declaring file's identity and chain", async () => {
      const files = {
         "model.malloy": MODEL,
         "index.malloy": `import { daily, daily_ext, orders } from "model.malloy"\n`,
      };
      const declared = SourceMap.of(await compile(files, "model.malloy"));
      const indexDef = await compile(files, "index.malloy");
      const reexported = SourceMap.of(indexDef);
      for (const name of ["daily", "daily_ext", "orders"]) {
         expect(reexported.sourceIDOf(name)).toBe(declared.sourceIDOf(name));
      }
      expect(sourceChain(indexDef.contents["daily_ext"], indexDef)).toEqual([
         declared.sourceIDOf("daily_ext")!,
         declared.sourceIDOf("daily")!,
         declared.sourceIDOf("orders")!,
      ]);
   });
});

describe("reachableSourceIDs", () => {
   it("reaches a published source's base, query base and joined sources", async () => {
      const modelDef = await compile({ "model.malloy": MODEL }, "model.malloy");
      const map = SourceMap.of(modelDef);
      const root = modelDef.contents["daily_ext"] as SourceDef;
      const ids = reachableSourceIDs(
         structsReachedFrom([root], modelDef),
         modelDef,
      );
      for (const name of ["daily_ext", "daily", "orders", "regions"]) {
         expect(ids.has(map.sourceIDOf(name)!)).toBe(true);
      }
      expect(ids.has(map.sourceIDOf("renamed")!)).toBe(false);
   });
});

describe("composites", () => {
   const COMPOSITE = `##! experimental { persistence composite_sources }
source: base is duckdb.table('orders')
#@ persist name="by_region"
source: by_region is base -> { group_by: region }
source: plain is duckdb.table('orders')
source: comp is compose(by_region, plain)
source: over_comp is comp -> { group_by: region }
`;

   it("reaches every member of a published composite, as the scan before the map did", async () => {
      const modelDef = await compile(
         { "model.malloy": COMPOSITE },
         "model.malloy",
      );
      const map = SourceMap.of(modelDef);
      for (const root of ["comp", "over_comp"]) {
         const def = modelDef.contents[root] as SourceDef;
         const ids = reachableSourceIDs(
            structsReachedFrom([def], modelDef),
            modelDef,
         );
         for (const name of ["by_region", "plain", "base"]) {
            expect(ids.has(map.sourceIDOf(name)!)).toBe(true);
         }
         // And never less than the legacy scan.
         for (const id of legacyDeepScan([def])) expect(ids.has(id)).toBe(true);
      }
   });

   it("records a composite as a composite with its members, and a query over one as a query", async () => {
      const modelDef = await compile(
         { "model.malloy": COMPOSITE },
         "model.malloy",
      );
      const map = SourceMap.of(modelDef);
      const comp = map.node(map.sourceIDOf("comp")!)!;
      expect(comp.kind).toBe("composite");
      expect(comp.base).toBeUndefined();
      expect(comp.members).toEqual([
         map.sourceIDOf("by_region")!,
         map.sourceIDOf("plain")!,
      ]);
      const over = map.node(map.sourceIDOf("over_comp")!)!;
      expect(over.kind).toBe("query");
      expect(over.base?.hop).toBe("query");
      expect(map.node(map.sourceIDOf("plain")!)!.members).toEqual([]);
   });
});

describe("chain fall-through", () => {
   it("continues past an extends the model cannot resolve, as the walk before the map did", () => {
      // A definition whose `extends` names an id the registry lacks, with a
      // rename that resolves: the walk still reaches the renamed source.
      const target = { type: "table", sourceID: "target@m" };
      const modelDef = {
         contents: { target },
         sourceRegistry: {
            "target@m": {
               entry: { type: "source_registry_reference", name: "target" },
            },
         },
      };
      const struct = {
         type: "table",
         sourceID: "child@m",
         extends: "missing@m",
         referenceID: "target@m",
      };
      expect(sourceChain(struct, modelDef)).toEqual(["child@m", "target@m"]);
   });
});

/**
 * What the unreachable verdict used before the source map: every
 * `sourceID`/`referenceID`/`extends` string anywhere under the roots. The map's
 * walk follows declared edges instead, and must reach at least as much, or a
 * package gains a false `unreachable` warning.
 */
function legacyDeepScan(roots: readonly unknown[]): Set<string> {
   const ids = new Set<string>();
   const seen = new Set<unknown>();
   const visit = (node: unknown, depth: number): void => {
      if (depth > 64 || node === null || typeof node !== "object") return;
      if (seen.has(node)) return;
      seen.add(node);
      if (Array.isArray(node)) {
         for (const item of node) visit(item, depth + 1);
         return;
      }
      const record = node as Record<string, unknown>;
      for (const key of ["sourceID", "referenceID", "extends"]) {
         const value = record[key];
         if (typeof value === "string") ids.add(value);
      }
      for (const value of Object.values(record)) visit(value, depth + 1);
   };
   for (const root of roots) visit(root, 0);
   return ids;
}

describe("reachableSourceIDs over the repo's persist corpus", () => {
   it(
      "reaches at least every sourceID the old deep scan did, for every source and named query",
      async () => {
         const hammer = await hammerModels();
         const corpus = [...fixtureModels(), ...hammer.models];
         const missed: string[] = [];
         let roots = 0;
         for await (const model of releasingEach(corpus)) {
            let modelDef: ModelDef;
            try {
               const runtime = new Runtime({
                  urlReader: new InMemoryURLReader(model.files),
                  connections: new FixedConnectionMap(
                     new Map([...model.connections]),
                     [...model.connections.keys()][0],
                  ),
               });
               modelDef = (
                  await runtime.loadModel(new URL(model.entry)).getModel()
               )._modelDef;
            } catch {
               continue;
            }
            for (const [name, entry] of Object.entries(modelDef.contents)) {
               let oldIds: Set<string>;
               let newIds: Set<string>;
               if (isSourceDef(entry)) {
                  const reached = structsReachedFrom([entry], modelDef);
                  oldIds = legacyDeepScan(reached);
                  newIds = reachableSourceIDs(reached, modelDef);
               } else if ((entry as { type?: string }).type === "query") {
                  oldIds = legacyDeepScan([entry]);
                  newIds = reachableSourceIDs([entry], modelDef);
               } else {
                  continue;
               }
               roots++;
               for (const id of oldIds) {
                  if (!newIds.has(id))
                     missed.push(`${model.label} ${name}: ${id}`);
               }
            }
         }
         expect(roots).toBeGreaterThan(200);
         expect(missed).toEqual([]);
      },
      { timeout: 240000 },
   );
});
