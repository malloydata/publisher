// Contract test for the Malloy property entry-point visibility relies on:
// `ModelDef.exports`. The within-file curation in service/model.ts lists only
// the names in `exports`, so if a Malloy upgrade changes this behavior the
// curation silently breaks — pin it here against the real compiler (not a
// re-implementation). See entry_point_visibility.spec.ts for the end-to-end
// wiring through Package/Model.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";

const ROOT = "file:///probe/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

async function modelDefOf(
   files: Record<string, string>,
   entry: string,
): Promise<{ exports: string[]; contents: string[] }> {
   const urlReader = new InMemoryURLReader(
      new Map(
         Object.entries(files).map(([name, text]) => [`${ROOT}${name}`, text]),
      ),
   );
   const runtime = new Runtime({ urlReader, connections });
   const model = await runtime
      .loadModel(new URL(`${ROOT}${entry}`), { importBaseURL: new URL(ROOT) })
      .getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const def = (model as any)._modelDef;
   return {
      exports: [...(def.exports as string[])],
      contents: Object.keys(def.contents),
   };
}

describe("Malloy ModelDef.exports contract (curation depends on this)", () => {
   it("no export{}: exports = all locally-declared top-level sources", async () => {
      const { exports } = await modelDefOf(
         {
            "m.malloy": `source: a is duckdb.sql("select 1 as x")
                         source: b is duckdb.sql("select 2 as y")`,
         },
         "m.malloy",
      );
      expect(exports.sort()).toEqual(["a", "b"]);
   });

   it("export{a}: exports narrows to the listed name", async () => {
      const { exports } = await modelDefOf(
         {
            "m.malloy": `source: a is duckdb.sql("select 1 as x")
                         source: b is duckdb.sql("select 2 as y")
                         export { a }`,
         },
         "m.malloy",
      );
      expect(exports).toEqual(["a"]);
   });

   it("imported sources are in contents but NOT in exports unless re-exported", async () => {
      const { exports, contents } = await modelDefOf(
         {
            "base.malloy": `source: base_source is duckdb.sql("select 1 as x")`,
            "index.malloy": `import "base.malloy"
               source: customers is base_source extend { dimension: two is 2 }
               export { customers }`,
         },
         "index.malloy",
      );
      // The imported base_source is resolvable (in contents) — so joins work —
      // but it is not part of the re-export closure, so it must not be listed.
      expect(contents).toContain("base_source");
      expect(exports).toEqual(["customers"]);
   });

   it("import without export{}: imported name is excluded from exports", async () => {
      const { exports } = await modelDefOf(
         {
            "base.malloy": `source: base_source is duckdb.sql("select 1 as x")`,
            "user.malloy": `import "base.malloy"
               source: local_one is duckdb.sql("select 2 as y")`,
         },
         "user.malloy",
      );
      // Even with no explicit export{}, `exports` lists only locally-declared
      // names — the imported `base_source` is omitted. Curation is applied
      // unconditionally (Model.curateForDiscovery) precisely so publisher's
      // listed sources mirror what Malloy's modelDefToModelInfo exposes via
      // modelInfo/sourceInfos: an imported-but-not-re-exported source is
      // unlisted, while staying resolvable for joins and fully queryable.
      expect(exports).toEqual(["local_one"]);
   });
});
