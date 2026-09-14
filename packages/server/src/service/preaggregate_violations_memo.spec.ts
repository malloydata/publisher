// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type ModelDef, type SourceDef } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";

import { Model } from "./model";

/**
 * `Model.preaggregateViolations()` is memoized, and this pins that.
 *
 * It is not a micro-optimization. `Package.preaggregateAccessWarnings()` puts
 * this walk on `getPackageMetadata()`, which `/status` reaches for every package
 * in every environment on every poll — and the walk re-parses the annotations on
 * every field of every source. Unmemoized, a worker holding real packages spent
 * seconds of main-thread CPU per poll, which blocked the event loop and pushed
 * multi-second latency onto every unrelated request.
 *
 * Identity rather than equality is the assertion, because equality passes
 * whether or not the memo exists.
 */

// Same shape the other synthetic-ModelDef specs build (authorize_gate_walk,
// partition_resolution); each keeps its own copy rather than sharing a fixture.
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

function sourceWithPreaggregateNote(name: string): SourceDef {
   return tableSource(name, {
      // A source-level declaration, which is a rejection: the grain would have
      // no measure to apply to. Used here only because it is the cheapest
      // annotation that makes the walk produce a finding.
      annotations: {
         notes: [
            {
               text: '#@ preaggregate grain="category"\n',
               at: {
                  url: "synthetic.malloy",
                  range: {
                     start: { line: 0, character: 0 },
                     end: { line: 0, character: 0 },
                  },
               },
            },
         ],
      },
   });
}

function modelWith(
   contents: Record<string, SourceDef>,
   onContentsRead?: () => void,
): Model {
   const modelDef = {
      name: "synthetic.malloy",
      exports: [],
      get contents() {
         onContentsRead?.();
         return contents;
      },
   };
   return new Model(
      "test-pkg",
      "synthetic.malloy",
      {},
      "model",
      undefined,
      modelDef as unknown as ModelDef,
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
}

describe("Model.preaggregateViolations memoization", () => {
   // Guards the fixture rather than the validator, which has its own spec: if
   // the synthetic annotation ever stops parsing, the walk returns [] and the
   // identity assertions below would still pass while measuring nothing.
   it("computes the violations", () => {
      const model = modelWith({ orders: sourceWithPreaggregateNote("orders") });
      const violations = model.preaggregateViolations();
      expect(violations.map((v) => v.code)).toEqual(["misplaced_on_source"]);
   });

   it("returns the same array on a second call rather than re-walking", () => {
      const model = modelWith({ orders: sourceWithPreaggregateNote("orders") });
      const first = model.preaggregateViolations();
      const second = model.preaggregateViolations();
      // Reference identity: an unmemoized walk builds a fresh array each call.
      expect(second).toBe(first);
   });

   it("memoizes a clean model too, so the common case is not the slow path", () => {
      // The hot path in production is a package with nothing wrong with it, so
      // an empty result must be memoized as well as a non-empty one.
      const model = modelWith({ orders: tableSource("orders") });
      const first = model.preaggregateViolations();
      expect(first).toEqual([]);
      expect(model.preaggregateViolations()).toBe(first);
   });

   it("does not re-walk when the underlying contents change", () => {
      // Identity alone would also hold for an implementation that recomputed and
      // refilled a retained array, so this pins the walk itself as not re-run.
      // Reaching into `contents` is safe to rely on only because nothing in the
      // server ever writes to it -- that immutability is what the memo rests on.
      const source = sourceWithPreaggregateNote("orders");
      const contents: Record<string, SourceDef> = { orders: source };
      const model = modelWith(contents);
      expect(model.preaggregateViolations()).toHaveLength(1);

      delete (source as unknown as { annotations?: unknown }).annotations;
      contents.extra = sourceWithPreaggregateNote("extra");

      expect(model.preaggregateViolations()).toHaveLength(1);
      expect(model.preaggregateViolations()[0].sourceName).toBe("orders");
   });

   it("reads the model contents once, however many times it is called", () => {
      // The assertions above pin the returned VALUE as cached, which a
      // recompute-and-discard implementation would also satisfy while burning
      // exactly the CPU this change exists to remove. Counting reads of
      // `contents` -- the walk's only entry into the model -- pins the work.
      const contents = { orders: sourceWithPreaggregateNote("orders") };
      let reads = 0;
      const model = modelWith(contents, () => {
         reads += 1;
      });
      reads = 0; // Ignore whatever construction itself touched.

      model.preaggregateViolations();
      model.preaggregateViolations();
      model.preaggregateViolations();

      expect(reads).toBe(1);
   });

   it("keeps each model's memo to itself", () => {
      // A memo parked on the wrong scope (module-level, or a shared static)
      // would leak one model's findings into another's.
      const a = modelWith({ orders: sourceWithPreaggregateNote("orders") });
      const b = modelWith({ orders: sourceWithPreaggregateNote("orders") });
      expect(a.preaggregateViolations()).not.toBe(b.preaggregateViolations());
   });
});
