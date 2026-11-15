// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type ModelDef,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { confirmedAncestors, keptAncestorChain } from "./kept_ancestors";
import { SourceMap } from "./source_map";

// File-backed: db-duckdb's in-memory database is shared across spec files.
const dbDir = fs.mkdtempSync(path.join(os.tmpdir(), "kept-ancestors-spec-"));
const duckdb = new DuckDBConnection("duckdb", path.join(dbDir, "db.duckdb"));
const URL_ = "file:///pkg/m.malloy";

async function compile(text: string): Promise<ModelDef> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[URL_, text]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   return (await runtime.loadModel(new URL(URL_)).getModel())._modelDef;
}

beforeAll(async () => {
   await duckdb.runSQL(
      "CREATE TABLE ev AS SELECT 1 AS id, 'acme' AS org, 1.0 AS amt",
   );
});

afterAll(async () => {
   await duckdb.close();
   fs.rmSync(dbDir, { recursive: true, force: true });
});

// Over a table, Malloy records no base edge for an extension; the walk
// infers it, through renames and past an extension's own `where:` or join.
const TEXT = `source: ev is duckdb.table('ev')
source: base is ev extend { where: org = 'acme'; dimension: a is 1 }
source: b2 is base
source: b3 is b2
source: renamed_ext is b3 extend { measure: total is amt.sum() }
source: mid is b2 extend { where: amt > 0; join_one: p is ev on p.id = id }
source: mid_ext is mid extend { measure: total is amt.sum() }
`;

describe("the kept walk over a table-backed base", () => {
   let model: ModelDef;
   const names = (chain: { sourceID: string }[]) =>
      chain.map((a) => a.sourceID.split("@")[0]);
   const idOf = (name: string) =>
      (model.contents[name] as unknown as { sourceID: string }).sourceID;
   const own = (name: string) =>
      (model.contents[name] as unknown as { location: { range: never } })
         .location.range;
   beforeAll(async () => {
      model = await compile(TEXT);
   });

   for (const [label, read] of [
      // The materialize planner reads a model's contents alone.
      ["its contents alone", () => ({ contents: model.contents })],
      ["the whole model", () => model],
   ] as const) {
      it(`walks through renames and past an own where or join, reading ${label}`, () => {
         const map = SourceMap.of(read() as never);
         const chain = keptAncestorChain(map, idOf("renamed_ext"), URL_);
         expect(names(chain)).toEqual(["base", "ev"]);
         expect(chain[0].aliases?.map((a) => a.sourceID.split("@")[0])).toEqual(
            ["b2", "b3"],
         );
         expect(
            names(confirmedAncestors(TEXT, own("renamed_ext"), chain)),
         ).toEqual(["base", "ev"]);
         const mid = keptAncestorChain(map, idOf("mid_ext"), URL_);
         expect(names(mid)).toEqual(["mid", "base", "ev"]);
         expect(names(confirmedAncestors(TEXT, own("mid_ext"), mid))).toEqual([
            "mid",
            "base",
            "ev",
         ]);
      });
   }

   it("confirms a rename only where the text declares it bare, naming the next", () => {
      const map = SourceMap.of({ contents: model.contents } as never);
      const chain = keptAncestorChain(map, idOf("renamed_ext"), URL_);
      // An alias whose declaration is not a bare rename (it extends): never
      // confirmed, though it names the next alias.
      const at = (name: string) => ({
         sourceID: idOf(name),
         range: own(name),
      });
      const forged = [
         {
            ...at("base"),
            aliases: [at("mid"), at("b2")],
         },
      ];
      expect(confirmedAncestors(TEXT, own("mid_ext"), forged)).toEqual([]);
      // An alias naming a source that is not the one it renames.
      const misnamed = TEXT.replace("source: b3 is b2\n", "source: b3 is ev\n");
      expect(confirmedAncestors(misnamed, own("renamed_ext"), chain)).toEqual(
         [],
      );
      // A child naming a source that is no alias of the ancestor.
      const other = TEXT.replace(
         "source: renamed_ext is b3 extend",
         "source: renamed_ext is ev extend",
      );
      expect(confirmedAncestors(other, own("renamed_ext"), chain)).toEqual([]);
   });
});
