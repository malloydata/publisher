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
import { registerGetContextTool, sliceRange } from "./get_context_tool";

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

/**
 * The same view, but reaching a response card.
 *
 * The block above pins the two pieces -- the compiler's contract and the
 * slice. Neither pins the wiring between them: the turtle branch in
 * `readFieldProvenance`, the `sourceTextFor` reader, and the `include_code`
 * gate could each break with both of those still green.
 *
 * The package here is a stand-in for the accessors `collectEntities` reads,
 * over a REAL compiled model, so the IR and its coordinates are malloy's own.
 */
describe("a view's definition on the card", () => {
   let duckdb: DuckDBConnection;
   let dir: string;
   let modelPath: string;
   let def: ModelDef;
   let sourceInfos: unknown[];

   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "viewcard-"));
      modelPath = path.join(dir, "m.malloy");
      fs.writeFileSync(modelPath, MODEL);
      const runtime = new Runtime({
         urlReader: {
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
      sourceInfos = [
         {
            name: "orders",
            annotations: [],
            schema: {
               fields: [
                  { kind: "dimension", name: "shouty", annotations: [] },
                  { kind: "measure", name: "total", annotations: [] },
                  { kind: "view", name: "by_state", annotations: [] },
               ],
            },
         },
      ];
   });
   afterAll(async () => {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   /**
    * `compiledSourceText` is what the loader captured for this compile. The
    * default is the text that was compiled; a test passes something else to
    * stand for a package whose file has moved on since.
    */
   const askFor = async (compiledSourceText: string | undefined) => {
      const pkg = {
         getPackagePath: () => dir,
         listModels: async () => [{ path: "m.malloy" }],
         getModel: (p: string) =>
            p === "m.malloy"
               ? {
                    getSourceInfos: () => sourceInfos,
                    getQueries: () => [],
                    getModelDef: () => def,
                    getCompiledSourceText: () => compiledSourceText,
                 }
               : undefined,
      };
      let handler:
         | ((params: Record<string, unknown>) => Promise<{
              content: Array<{ resource?: { text: string } }>;
           }>)
         | undefined;
      registerGetContextTool(
         {
            tool: (_n: string, _d: string, _s: unknown, h: typeof handler) => {
               handler = h;
            },
         } as never,
         {
            getEnvironment: async () =>
               ({
                  getPackage: async () => pkg,
                  getStaleCompileErrors: () => new Map(),
               }) as never,
         } as never,
      );
      if (!handler) throw new Error("handler was not registered");
      const payload = JSON.parse(
         (
            await handler({
               environmentName: "e",
               packageName: "p",
               search_targets: [{ target_type: "view" }],
               scopes: [{ environment: "e", package: "p" }],
               include_code: true,
            })
         ).content[0].resource!.text,
      );
      const entities = (payload.sources ?? []).flatMap(
         (s: { entities?: Array<{ name: string; code?: string }> }) =>
            s.entities ?? [],
      );
      return entities.find((e: { name: string }) => e.name === "by_state");
   };

   it("returns the view's own definition", async () => {
      const view = await askFor(MODEL);
      expect(view?.code).toContain("group_by: state");
      expect(view?.code).toContain("aggregate: total");
      // The definition, not the source around it or the #(doc) above it --
      // the same boundary the slice test pins, now through the response.
      expect(view?.code).not.toContain("source: orders");
      expect(view?.code).not.toContain("#(doc)");
   });

   it("slices the text the model compiled, never the file as it is now", async () => {
      // A package whose reload failed keeps serving the model it compiled
      // BEFORE the save, while the file on disk has moved on (get_status
      // reports it as stale: true). Reading the file at index time would cut
      // post-edit bytes at pre-edit coordinates and hand back text that is not
      // the view. Here the file is edited so that the old range lands
      // mid-token, and the compiled snapshot is what the card must use.
      const edited = "// a line added at the top\n" + MODEL;
      fs.writeFileSync(modelPath, edited);
      const view = await askFor(MODEL);
      expect(view?.code).toContain("group_by: state");
      // What reading the file now would have produced, so this fails loudly
      // if the reader ever falls back to disk.
      const fromDisk = sliceRange(
         edited,
         /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
         ((def.contents["orders"] as any).fields as any[]).find(
            (f) => (f.as || f.name) === "by_state",
         ).location.range,
      );
      expect(view?.code).not.toBe(fromDisk);
      fs.writeFileSync(modelPath, MODEL);
   });

   it("omits code when the loader captured no snapshot", async () => {
      // A notebook, a compile failure, or a Model built in process. The card
      // still comes back; only the definition is missing.
      const view = await askFor(undefined);
      expect(view).toBeDefined();
      expect(view?.code).toBeUndefined();
   });
});
