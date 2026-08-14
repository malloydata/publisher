import { describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import {
   annotationTexts,
   isReservedRoute,
   modelAnnotations,
   ownModelNotes,
} from "./annotations";

// Minimal `ModelDef` carrying only what `modelAnnotations` reads: `modelID`
// and the `modelAnnotations` registry (modelID → { ownNotes, inheritsFrom }).
const makeModelDef = (
   modelID: string,
   registry: Record<
      string,
      {
         ownNotes: { notes?: string[]; blockNotes?: string[] };
         inheritsFrom: string[];
      }
   >,
): ModelDef =>
   ({
      modelID,
      modelAnnotations: Object.fromEntries(
         Object.entries(registry).map(([id, { ownNotes, inheritsFrom }]) => [
            id,
            {
               ownNotes: {
                  notes: ownNotes.notes?.map((text) => ({ text, at: {} })),
                  blockNotes: ownNotes.blockNotes?.map((text) => ({
                     text,
                     at: {},
                  })),
               },
               inheritsFrom,
            },
         ]),
      ),
   }) as unknown as ModelDef;

const noteTexts = (def: ModelDef): string[] =>
   (modelAnnotations(def).notes ?? []).map((note) => note.text);

describe("isReservedRoute", () => {
   it("treats the empty route (MOTLY / render config) as reserved", () => {
      expect(isReservedRoute("")).toBe(true);
   });

   it("treats Malloy's punctuation sigils as reserved", () => {
      // Form 2 reserves the entire punct-only namespace, not just the
      // currently-claimed sigils (`!` `@` `"` `:`).
      for (const route of ["!", "@", '"', ":", "%", "-", "::"]) {
         expect(isReservedRoute(route)).toBe(true);
      }
   });

   it("treats bracketed app routes as not reserved", () => {
      for (const route of [
         "doc",
         "label",
         "filter",
         "bar-chart",
         "https://example.com/ns",
         "123",
         "══SECURITY══",
      ]) {
         expect(isReservedRoute(route)).toBe(false);
      }
   });
});

describe("modelAnnotations", () => {
   it("returns the local model's own `##` notes", () => {
      const def = makeModelDef("local", {
         local: { ownNotes: { notes: ["## local"] }, inheritsFrom: [] },
      });
      expect(noteTexts(def)).toEqual(["## local"]);
   });

   it("falls back to an imported model's notes when the local model has no own `##` (authorize must not fail open)", () => {
      // Regression: a `##(authorize)` declared in an imported file must still
      // be detected (and refused — file-level `##(authorize)` is deprecated)
      // even when the importing file declares no `##` of its own. An earlier
      // fold kept an empty local link at the top, so `.notes` returned [] and
      // the stray annotation escaped detection entirely.
      const def = makeModelDef("local", {
         local: { ownNotes: {}, inheritsFrom: ["base"] },
         base: {
            ownNotes: { notes: ['## (authorize) request.user == "admin"'] },
            inheritsFrom: [],
         },
      });
      expect(noteTexts(def)).toEqual([
         '## (authorize) request.user == "admin"',
      ]);
   });

   it("surfaces the local model's notes at the top when both local and import have `##`", () => {
      const def = makeModelDef("local", {
         local: { ownNotes: { notes: ["## local"] }, inheritsFrom: ["base"] },
         base: { ownNotes: { notes: ["## base"] }, inheritsFrom: [] },
      });
      // `.notes` is the top (local); `.texts()` flattens the whole lineage
      // ancestral-first.
      expect(noteTexts(def)).toEqual(["## local"]);
      expect(annotationTexts(modelAnnotations(def))).toEqual([
         "## base",
         "## local",
      ]);
   });

   it("is cycle-safe and returns an empty bundle for a model with no annotations", () => {
      const def = makeModelDef("local", {
         local: { ownNotes: {}, inheritsFrom: ["base"] },
         base: { ownNotes: {}, inheritsFrom: ["local"] }, // back-edge cycle
      });
      expect(modelAnnotations(def)).toEqual({});
      expect(noteTexts(def)).toEqual([]);
   });
});

describe("ownModelNotes", () => {
   it("returns the model's own `##`, block notes first", () => {
      const def = makeModelDef("local", {
         local: {
            ownNotes: { notes: ['## title="Sales"'], blockNotes: ["##! x"] },
            inheritsFrom: [],
         },
      });
      expect(ownModelNotes(def)).toEqual(["##! x", '## title="Sales"']);
   });

   it("does not inherit an imported model's `##`", () => {
      // The point of the function. `modelAnnotations` folds the lineage on
      // purpose, so that an imported `##(authorize)` is still DETECTED (and
      // refused) through the importer; every other model-level tag describes
      // one document. A shared include
      // carrying `## artifact` must not turn its importers into dashboards,
      // and an imported `## title=` must not retitle every notebook.
      const def = makeModelDef("file:///pkg/local.malloy", {
         "file:///pkg/local.malloy": {
            ownNotes: {},
            inheritsFrom: ["file:///pkg/shared.malloy"],
         },
         "file:///pkg/shared.malloy": {
            ownNotes: {
               notes: ['## artifact { title="Shared" }', '## title="Shared"'],
            },
            inheritsFrom: [],
         },
      });
      expect(ownModelNotes(def)).toEqual([]);
      // The inheriting reader still sees it, which is what authorize needs.
      expect(noteTexts(def)).toEqual([
         '## artifact { title="Shared" }',
         '## title="Shared"',
      ]);
   });

   it("does not inherit an import reached over a non-`file:` scheme", () => {
      // The walk is an allowlist (`modelID` or `internal://`) rather than a
      // denylist on `file:`. The worker's URL reader proxies any non-`file:`
      // URL to the main thread (`package_load_worker.ts`, makeWorkerUrlReader),
      // so a `gs://` or `https://` import is reachable today; under a
      // `!startsWith("file:")` test its `##` would have folded back in with
      // nothing failing.
      const def = makeModelDef("file:///pkg/local.malloy", {
         "file:///pkg/local.malloy": {
            ownNotes: { notes: ["## local"] },
            inheritsFrom: ["gs://bucket/shared.malloy"],
         },
         "gs://bucket/shared.malloy": {
            ownNotes: { notes: ['## title="Remote"'] },
            inheritsFrom: [],
         },
      });
      expect(ownModelNotes(def)).toEqual(["## local"]);
      // Still inherited by the folded reader, which is what authorize needs.
      expect(annotationTexts(modelAnnotations(def))).toEqual([
         '## title="Remote"',
         "## local",
      ]);
   });

   it("returns the local notes only when both the model and its import have `##`", () => {
      const def = makeModelDef("file:///pkg/local.malloy", {
         "file:///pkg/local.malloy": {
            ownNotes: { notes: ["## local"] },
            inheritsFrom: ["file:///pkg/base.malloy"],
         },
         "file:///pkg/base.malloy": {
            ownNotes: { notes: ["## base"] },
            inheritsFrom: [],
         },
      });
      expect(ownModelNotes(def)).toEqual(["## local"]);
   });

   it("collects a notebook's cells, which are internal nodes rather than files", () => {
      // A `.malloynb` compiles a cell at a time: its `##` lands on an
      // `internal://loadModel` node, and the `internal://extendModel` node that
      // `modelID` names inherits it with no notes of its own. Reading only
      // `modelID` would find nothing on every notebook.
      const def = makeModelDef("internal://extendModel/2", {
         "internal://extendModel/2": {
            ownNotes: {},
            inheritsFrom: ["internal://loadModel/1"],
         },
         "internal://loadModel/1": {
            ownNotes: { notes: ["## autorun=false", '## title="Orders"'] },
            inheritsFrom: ["file:///pkg/orders.malloy"],
         },
         "file:///pkg/orders.malloy": {
            ownNotes: { notes: ['## title="Not this one"'] },
            inheritsFrom: [],
         },
      });
      expect(ownModelNotes(def)).toEqual([
         "## autorun=false",
         '## title="Orders"',
      ]);
   });

   it("returns an empty list for a model with no annotation registry", () => {
      expect(ownModelNotes({ modelID: "x" } as unknown as ModelDef)).toEqual(
         [],
      );
   });
});
