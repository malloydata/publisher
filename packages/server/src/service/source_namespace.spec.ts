/**
 * Compile-backed cover for the two facts `get_context` attribution rests on:
 * which sources a model file can actually resolve, and which of them it hides.
 *
 * Both are read off a real `ModelDef`, so a hand-built fixture would encode our
 * assumption about malloy's IR rather than test it. The `extend` cases in
 * particular exist because malloy represents an extension two different ways
 * (see `directlyDeclaredNoteTexts`), and only one of them is obvious.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { collectSourceInfos } from "./source_extraction";

const ROOT = "file:///probe/";
const TABLE = `duckdb.sql("select 'CA' as state, 1 as amt")`;

describe("model namespace (compiler contract)", () => {
   let connections: FixedConnectionMap;
   let duckdb: DuckDBConnection;

   beforeAll(() => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
   });
   afterAll(async () => {
      await duckdb.close();
   });

   async function defOf(
      files: Record<string, string>,
      entry: string,
   ): Promise<ModelDef> {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map(Object.entries(files).map(([n, t]) => [`${ROOT}${n}`, t])),
         ),
         connections,
      });
      const mm = runtime.loadModel(new URL(`${ROOT}${entry}`), {
         importBaseURL: new URL(ROOT),
      });
      /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
      return ((await mm.getModel()) as any)._modelDef as ModelDef;
   }

   const names = (def: ModelDef) =>
      collectSourceInfos(def)
         .map((s) => s.name)
         .sort();

   const DEFS = `source: a is ${TABLE}
source: b is ${TABLE}
source: c is ${TABLE}`;

   describe("collectSourceInfos", () => {
      it("reports a selective import's chosen name and nothing else from that file", async () => {
         const def = await defOf(
            {
               "defs.malloy": DEFS,
               "uses.malloy": `import { a } from "defs.malloy"
source: d is ${TABLE}`,
            },
            "uses.malloy",
         );
         // The bug this exists for: `b` and `c` were reported here too, and a
         // query naming either against uses.malloy fails to compile.
         expect(names(def)).toEqual(["a", "d"]);
      });

      it("reports a renamed import under the name that resolves", async () => {
         const def = await defOf(
            {
               "defs.malloy": DEFS,
               "uses.malloy": `import { renamed is a } from "defs.malloy"`,
            },
            "uses.malloy",
         );
         expect(names(def)).toEqual(["renamed"]);
      });

      it("reports the whole file for a non-selective import", async () => {
         const def = await defOf(
            {
               "defs.malloy": DEFS,
               "uses.malloy": `import "defs.malloy"
source: d is ${TABLE}`,
            },
            "uses.malloy",
         );
         expect(names(def)).toEqual(["a", "b", "c", "d"]);
      });

      it("reports a file's own sources when it imports nothing", async () => {
         const def = await defOf({ "defs.malloy": DEFS }, "defs.malloy");
         expect(names(def)).toEqual(["a", "b", "c"]);
      });
   });
});
