// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compile-backed cover for model-annotation ownership.
 *
 * `annotations.spec.ts` exercises `modelAnnotations` / `ownModelNotes` against a
 * hand-built registry, which pins the fold but also encodes our assumption about
 * how malloy names compilation nodes. Both functions exist only because malloy
 * does not export `getModelAnnotations`, so that assumption is the load-bearing
 * part: if an upgrade renames the `internal://` scheme or stops giving a
 * notebook's cells their own nodes, a unit test over a fixture keeps passing and
 * every notebook silently reads the wrong tags.
 *
 * Two halves, and they cover different things:
 *
 *  1. The compiler contract: a real `Runtime`, asserting the node scheme and
 *     that an import's `##` is reachable through the folded reader but not
 *     through the own-notes reader.
 *  2. The wiring: a real package through the load worker, asserting the
 *     notebook API response carries the notebook's own `##` and not its
 *     include's. This is the half that fails if the `model.ts` call site
 *     reverts to the folded reader.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
} from "@malloydata/malloy";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import {
   annotationTexts,
   modelAnnotations,
   ownModelNotes,
} from "./annotations";
import { Package } from "./package";

/**
 * A shared include carrying a model-level `##(filters)`. This is the tag behind
 * the bug: its only consumer is the SDK's `parseNotebookFilterAnnotation`
 * (`packages/sdk/src/components/filter/utils.ts`, via `Notebook.tsx`), which
 * matches the literal `##(filters)` prefix on the notebook response's
 * `annotations`, so folding it across imports configured the filter panel of
 * every notebook that imported this file.
 */
const SHARED_MALLOY = `##(filters) ["orders.state"]
## title="Shared include"
source: orders is duckdb.sql("select 'CA' as state, 1 as amt")`;

/** The predicate the SDK consumer actually applies. */
const isFilterTag = (text: string) => text.startsWith("##(filters)");

/**
 * Every `##` in the folded lineage, ancestral-first. Note this is NOT how
 * `source_extraction.ts`'s file-level `##(authorize)` detection reads the
 * fold: that takes `.notes`, the top link only. This is the complete set,
 * used here only to prove an import's tag really is reachable.
 */
const foldedTexts = (def: ModelDef): string[] =>
   annotationTexts(modelAnnotations(def)) ?? [];

describe("malloy model-annotation node scheme (compiler contract)", () => {
   const ROOT = "file:///probe/";
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

   function runtimeFor(files: Record<string, string>): Runtime {
      return new Runtime({
         urlReader: new InMemoryURLReader(
            new Map(
               Object.entries(files).map(([name, text]) => [
                  `${ROOT}${name}`,
                  text,
               ]),
            ),
         ),
         connections,
      });
   }

   /* eslint-disable @typescript-eslint/no-explicit-any */
   const defOf = async (mm: {
      getModel: () => Promise<unknown>;
   }): Promise<ModelDef> =>
      ((await mm.getModel()) as any)._modelDef as ModelDef;
   /* eslint-enable @typescript-eslint/no-explicit-any */

   it("names an imported file by its URL, so its `##` is inherited but not owned", async () => {
      const runtime = runtimeFor({
         "shared.malloy": SHARED_MALLOY,
         // The importer declares a `##` of its own, so the assertions below
         // distinguish "reads this file's tags and excludes the import's" from
         // "returns nothing at all".
         "importer.malloy": `## title="Importer"
import "shared.malloy"
source: local is orders extend { dimension: two is 2 }`,
      });
      const def = await defOf(
         runtime.loadModel(new URL(`${ROOT}importer.malloy`), {
            importBaseURL: new URL(ROOT),
         }),
      );

      // A `.malloy` model compiled from a URL is named by that URL, and so is
      // each file it imports. One node per document, so `modelID` alone would
      // have been enough here.
      expect(def.modelID).toBe(`${ROOT}importer.malloy`);
      expect(Object.keys(def.modelAnnotations ?? {}).sort()).toEqual([
         `${ROOT}importer.malloy`,
         `${ROOT}shared.malloy`,
      ]);

      // The include's `##(filters)` is genuinely reachable, which is what made
      // the bug real rather than theoretical, and the own-notes reader is what
      // keeps it out while still reading this file's own tag.
      expect(foldedTexts(def).filter(isFilterTag)).toHaveLength(1);
      expect(ownModelNotes(def).map((t) => t.trim())).toEqual([
         '## title="Importer"',
      ]);
      expect(ownModelNotes(def).filter(isFilterTag)).toEqual([]);
   });

   it("gives a notebook's cells `internal://` nodes that the file import is not part of", async () => {
      const runtime = runtimeFor({ "shared.malloy": SHARED_MALLOY });

      // The shape `Model.getNotebookModelMaterializer` builds: cell one is an
      // inline `loadModel`, every later cell an `extendModel` on top of it.
      let mm = runtime.loadModel(
         `import "shared.malloy"\n## title="Sales report"`,
         { importBaseURL: new URL(ROOT) },
      );
      mm = mm.extendModel(`run: orders -> { select: * }`, {
         importBaseURL: new URL(ROOT),
      });
      const def = await defOf(mm);

      // `modelID` is the LAST cell, which declares nothing of its own: reading
      // it alone would find no tags on any notebook. The scheme is what tells
      // "another cell of this document" from "a file it imported".
      expect(def.modelID).toStartWith("internal://extendModel/");
      const nodes = Object.keys(def.modelAnnotations ?? {});
      expect(
         nodes.filter((id) => id.startsWith("internal://loadModel/")).length,
      ).toBe(1);
      expect(nodes).toContain(`${ROOT}shared.malloy`);

      // The document's own tag is read; the include's `##(filters)` is not.
      expect(ownModelNotes(def).map((t) => t.trim())).toEqual([
         '## title="Sales report"',
      ]);
      expect(ownModelNotes(def).filter(isFilterTag)).toEqual([]);
      // …and the folded reader, which file-level `##(authorize)` DETECTION
      // uses (to refuse a stray tag declared in an import too), still sees
      // it. That asymmetry is the whole point of the pair.
      expect(foldedTexts(def).filter(isFilterTag)).toHaveLength(1);
   });
});

describe("notebook API response reports its own `##`, not its include's", () => {
   const ORIGINAL_WORKERS = process.env.PACKAGE_LOAD_WORKERS;
   let pool: PackageLoadPool;
   let tempDir = "";

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_WORKERS === undefined)
         delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_WORKERS;
   });

   afterEach(() => {
      if (tempDir) {
         try {
            fs.rmSync(tempDir, { recursive: true, force: true });
         } catch {
            /* already gone */
         }
         tempDir = "";
      }
   });

   it("keeps a shared include's `##(filters)` out of the notebook response", async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-own-notes-"));
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package" }),
      );
      fs.writeFileSync(path.join(tempDir, "shared.malloy"), SHARED_MALLOY);
      fs.writeFileSync(
         path.join(tempDir, "report.malloynb"),
         `>>>malloy
import "shared.malloy"
## title="Sales report"

>>>malloy
run: orders -> { select: * }`,
      );

      const { MalloyConfig } = await import("@malloydata/malloy");
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );

      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const notebook = (await pkg
            .getModel("report.malloynb")!
            .getNotebook()) as { type: string; annotations?: string[] };

         expect(notebook.type).toBe("notebook");
         const annotations = notebook.annotations ?? [];

         // Non-vacuous: the notebook's own model-level tag IS reported, so an
         // empty list would not pass this test by accident.
         expect(annotations.map((t) => t.trim())).toContain(
            '## title="Sales report"',
         );

         // The bug. `shared.malloy` declares `##(filters)`; the notebook only
         // imports it. Folding the lineage handed that tag to the SDK's filter
         // parser and configured a filter panel this notebook never asked for.
         expect(annotations.filter(isFilterTag)).toEqual([]);
         expect(annotations.map((t) => t.trim())).not.toContain(
            '## title="Shared include"',
         );
      } finally {
         await duckdb.close();
      }
   }, 120000);
});
