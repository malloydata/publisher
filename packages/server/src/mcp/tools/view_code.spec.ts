/**
 * A view's `code`, and the gate that returns it.
 *
 * Malloy fills `code` on a field def for a scalar EXPRESSION only, so a view
 * arrives with none -- indistinguishable in the IR from a physical column.
 * Its `location` does cover the definition, so the text is sliced from the
 * model file instead. That makes the slice the load-bearing part, and a
 * hand-built fixture would only test our idea of what malloy emits: these
 * compile real Malloy and slice a real file.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, Runtime, type ModelDef } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { sliceRange } from "./get_context_tool";

const MODEL = `source: orders is duckdb.sql("select 'CA' as state, 1 as amt") extend {
  dimension: shouty is upper(state)
  measure: total is amt.sum()

  #(doc) Orders by state.
  view: by_state is {
    group_by: state
    aggregate: total
  }
}`;

describe("sliceRange", () => {
   const text = "alpha\nbravo\ncharlie";
   it("takes a single-line range", () => {
      expect(
         sliceRange(text, {
            start: { line: 1, character: 1 },
            end: { line: 1, character: 4 },
         }),
      ).toBe("rav");
   });
   it("spans lines, keeping the newlines", () => {
      expect(
         sliceRange(text, {
            start: { line: 0, character: 1 },
            end: { line: 2, character: 3 },
         }),
      ).toBe("lpha\nbravo\ncha");
   });
   it("returns undefined for a range past the end", () => {
      expect(
         sliceRange(text, {
            start: { line: 0, character: 0 },
            end: { line: 9, character: 0 },
         }),
      ).toBeUndefined();
   });
});

describe("a view's definition (compiler contract)", () => {
   let duckdb: DuckDBConnection;
   let dir: string;
   let modelPath: string;
   let def: ModelDef;

   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "viewcode-"));
      modelPath = path.join(dir, "m.malloy");
      fs.writeFileSync(modelPath, MODEL);
      const runtime = new Runtime({
         // The default reader does not resolve file: URLs here, and the file
         // has to be real: the slice is taken from it.
         urlReader: {
            // fileURLToPath, not url.pathname: on Windows the pathname of a
            // file: URL keeps a leading slash ("/D:/Temp/..."), which does not
            // open.
            readURL: async (url: URL) =>
               fs.readFileSync(fileURLToPath(url), "utf8"),
         },
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      const url = pathToFileURL(modelPath);
      const model = await runtime
         .loadModel(url, { importBaseURL: new URL(".", url) })
         .getModel();
      /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
      def = (model as any)._modelDef as ModelDef;
   });
   afterAll(async () => {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const fieldNamed = (name: string) => {
      /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
      const src = (def.contents["orders"] as any)!;
      /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
      return src.fields.find((f: any) => (f.as || f.name) === name);
   };

   it("is absent from the IR, which is why it is sliced", () => {
      // The premise. If malloy ever fills this in, the slice becomes dead
      // code and this test says so.
      expect(fieldNamed("by_state").code).toBeUndefined();
      expect(fieldNamed("by_state").type).toBe("turtle");
   });

   it("carries a location covering its own definition", () => {
      const sliced = sliceRange(
         fs.readFileSync(modelPath, "utf8"),
         fieldNamed("by_state").location.range,
      );
      // The view definition, not the enclosing source, and not the #(doc)
      // line above it.
      expect(sliced).toContain("group_by: state");
      expect(sliced).toContain("aggregate: total");
      expect(sliced).not.toContain("source: orders");
      expect(sliced).not.toContain("#(doc)");
   });

   it("still fills code from the IR for a dimension and a measure", () => {
      // Unchanged behaviour, pinned so the view path cannot displace it.
      expect(fieldNamed("shouty").code).toBe("upper(state)");
      expect(fieldNamed("total").code).toBe("amt.sum()");
   });
});
