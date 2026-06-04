import { describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import {
   annotationTexts,
   isReservedRoute,
   modelAnnotations,
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
      // flow into the importing file's file-level gate even when the importing
      // file declares no `##` of its own. An earlier fold kept an empty local
      // link at the top, so `.notes` returned [] and the gate failed open.
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
