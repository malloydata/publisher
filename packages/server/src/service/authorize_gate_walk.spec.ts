import { type ModelDef, type SourceDef } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";

import { Model } from "./model";

/**
 * Fail-closed branches of the entry-point gate walk, driven from synthetic
 * `ModelDef`s.
 *
 * These cover IR the Malloy compiler is not expected to emit: a `query_source`
 * whose base cannot be resolved, and a `sourceRegistry` entry that points at
 * nothing. They are reachable only through malformed / unexpected IR, which is
 * exactly why they need pinning — the natural way to write either site is to skip
 * what you cannot read, and skipping means "no gate", which is a silent allow on
 * a source whose base may be locked. Real Malloy input cannot express them, so
 * the integration spec cannot reach them; a synthetic `ModelDef` can (same idiom
 * as `annotations.spec.ts`).
 *
 * The third test is the control that keeps the other two honest: an ordinary
 * source resolves to its own declaration, which must stay "no gate". Denying that
 * case would deny every ungated source in every model.
 */

/** The private walk under test, exposed by shape rather than by `any`. */
interface GateEntry {
   label: string;
   exprs: string[];
   selfContained: boolean;
}
interface GateWalker {
   collectEntryPointGates(
      struct: SourceDef | undefined,
      modelDef: ModelDef | undefined,
      seen?: Set<SourceDef>,
      treatAsOwnGate?: boolean,
   ): GateEntry[];
}

function tableSource(name: string, extra: object = {}): SourceDef {
   return {
      type: "table",
      name,
      dialect: "duckdb",
      tablePath: name,
      connection: "duckdb",
      fields: [],
      ...extra,
   } as unknown as SourceDef;
}

/** A Model wrapping `modelDef`, with the private gate walk reachable. */
function walkerFor(modelDef: ModelDef): GateWalker {
   const model = new Model(
      "test-pkg",
      "synthetic.malloy",
      {},
      "model",
      undefined,
      modelDef,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      // Supplied so the constructor does not derive it from this partial def.
      {} as never,
   );
   return model as unknown as GateWalker;
}

describe("gate walk fail-closed branches", () => {
   it("denies a query_source whose derivation base cannot be resolved", () => {
      // `type: "query_source"` means it derives from something by construction,
      // and `structRef` names a source that is not in `contents`. Reading no gate
      // here would launder away whatever gates the real base.
      const orphan = {
         type: "query_source",
         name: "orphan",
         dialect: "duckdb",
         fields: [],
         query: { structRef: "base_that_is_not_here" },
      } as unknown as SourceDef;
      const modelDef = {
         name: "synthetic",
         contents: { orphan },
         annotations: {},
      } as unknown as ModelDef;

      const gates = walkerFor(modelDef).collectEntryPointGates(
         orphan,
         modelDef,
         new Set(),
         true,
      );
      expect(gates.map((g) => g.exprs)).toEqual([["false"]]);
   });

   it("denies when a sourceRegistry entry resolves to nothing", () => {
      // An id IS present and the registry DOES have an entry for it — the entry
      // just names something absent from `contents`. That is a link to a base we
      // failed to follow, not an absence of one.
      const dangling = tableSource("dangling", { sourceID: "sid-1" });
      const modelDef = {
         name: "synthetic",
         contents: { dangling },
         annotations: {},
         sourceRegistry: {
            "sid-1": {
               entry: { type: "source_registry_reference", name: "gone" },
            },
         },
      } as unknown as ModelDef;

      const gates = walkerFor(modelDef).collectEntryPointGates(
         dangling,
         modelDef,
         new Set(),
         true,
      );
      expect(gates.map((g) => g.exprs)).toEqual([["false"]]);
   });

   it("reports no gate for an ordinary source that is its own declaration", () => {
      // The control. `resolveDeclaredSource` returns `none` (not `unresolvable`)
      // for a registry entry that resolves back to the struct itself, and for a
      // struct with no ids at all. Collapsing that into the deny above would lock
      // every ungated source out of every model.
      const plain = tableSource("plain", { sourceID: "sid-self" });
      const modelDef = {
         name: "synthetic",
         contents: { plain },
         annotations: {},
         sourceRegistry: { "sid-self": { entry: plain } },
      } as unknown as ModelDef;

      expect(
         walkerFor(modelDef).collectEntryPointGates(
            plain,
            modelDef,
            new Set(),
            true,
         ),
      ).toEqual([]);

      const noIds = tableSource("no_ids");
      const bareDef = {
         name: "synthetic",
         contents: { no_ids: noIds },
         annotations: {},
      } as unknown as ModelDef;
      expect(
         walkerFor(bareDef).collectEntryPointGates(
            noIds,
            bareDef,
            new Set(),
            true,
         ),
      ).toEqual([]);
   });
});
