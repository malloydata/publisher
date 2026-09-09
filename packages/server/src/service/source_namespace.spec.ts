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
import {
   agentHiddenSourceNames,
   collectSourceInfos,
} from "./source_extraction";

const ROOT = "file:///probe/";
const TABLE = `duckdb.sql("select 'CA' as state, 1 as amt")`;

describe("model namespace and agent-hidden (compiler contract)", () => {
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

   describe("agentHiddenSourceNames", () => {
      const hidden = async (files: Record<string, string>, entry: string) =>
         [...agentHiddenSourceNames(await defOf(files, entry))].sort();

      it("hides a source that declares the tag", async () => {
         expect(
            await hidden(
               {
                  "m.malloy": `#(agent-hidden)
source: base is ${TABLE}
source: visible is ${TABLE}`,
               },
               "m.malloy",
            ),
         ).toEqual(["base"]);
      });

      it("accepts the paren-less and spaced spellings", async () => {
         expect(
            await hidden(
               {
                  "m.malloy": `# (agent-hidden)
source: spaced is ${TABLE}
#agent-hidden
source: bare is ${TABLE}`,
               },
               "m.malloy",
            ),
         ).toEqual(["bare", "spaced"]);
      });

      it("carries a source-level tag across an import", async () => {
         expect(
            await hidden(
               {
                  "defs.malloy": `#(agent-hidden)
source: a is ${TABLE}`,
                  "uses.malloy": `import { a } from "defs.malloy"
source: d is ${TABLE}`,
               },
               "uses.malloy",
            ),
         ).toEqual(["a"]);
      });

      it("does NOT hide an extension that declares its own annotation", async () => {
         // Malloy tucks the base's annotation under `inherits` here.
         expect(
            await hidden(
               {
                  "m.malloy": `#(agent-hidden)
source: base is ${TABLE}

#(doc) child
source: child is base extend { dimension: two is 2 }`,
               },
               "m.malloy",
            ),
         ).toEqual(["base"]);
      });

      it("does NOT hide an extension that declares no annotation of its own", async () => {
         // Malloy COPIES the base's annotation onto the child verbatim here,
         // so a naive read of blockNotes/notes hides the child too.
         expect(
            await hidden(
               {
                  "m.malloy": `#(agent-hidden)
source: base is ${TABLE}

source: child is base extend { dimension: two is 2 }

source: bare_child is base`,
               },
               "m.malloy",
            ),
         ).toEqual(["base"]);
      });

      it("does NOT hide a child declared immediately below the base", async () => {
         // The contiguity walk stops at the base's `source:` line, which is not
         // an annotation line, so the base's note never reaches the child.
         expect(
            await hidden(
               {
                  "m.malloy": `#(agent-hidden)
source: base is ${TABLE}
source: child is base extend { dimension: two is 2 }`,
               },
               "m.malloy",
            ),
         ).toEqual(["base"]);
      });

      it("treats a stack of annotations above a source as all declared there", async () => {
         expect(
            await hidden(
               {
                  "m.malloy": `#(doc) documented
#(agent-hidden)
source: stacked is ${TABLE}`,
               },
               "m.malloy",
            ),
         ).toEqual(["stacked"]);
      });

      it("applies a file-level tag to every source declared in that file", async () => {
         expect(
            await hidden(
               {
                  "m.malloy": `##(agent-hidden)
source: one is ${TABLE}
source: two is ${TABLE}`,
               },
               "m.malloy",
            ),
         ).toEqual(["one", "two"]);
      });

      it("does NOT let a file-level tag hide the sources of a file that imports it", async () => {
         // Otherwise one shared include hides every file importing it.
         expect(
            await hidden(
               {
                  "shared.malloy": `##(agent-hidden)
source: shared_src is ${TABLE}`,
                  "uses.malloy": `import { shared_src } from "shared.malloy"
source: mine is ${TABLE}`,
               },
               "uses.malloy",
            ),
         ).toEqual([]);
      });

      it("hides nothing when the file carries no tag", async () => {
         expect(await hidden({ "m.malloy": DEFS }, "m.malloy")).toEqual([]);
      });
   });
});
