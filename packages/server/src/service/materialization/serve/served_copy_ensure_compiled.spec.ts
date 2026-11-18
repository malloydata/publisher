// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of the served copy's compile cache
 * (`ServedCopyServe.ensureCompiled`): which compile a pin reuses, joins,
 * rebinds or starts, and which compiled copy a lane keeps when requests and
 * invalidations interleave. The compile itself is replaced by one the spec
 * settles by hand (the one seam replaced, in {@link servedCopy}), so the
 * ordering is exact and nothing is compiled for real.
 */

import { describe, expect, it } from "bun:test";
import { type Binding, makeSnapshot, type Pinned } from "../domain/binding";
import { ServedCopyServe } from "./served_copy_serve";

/** A database table binding; `fresh` varies only what the compile ignores. */
function binding(name: string, fresh = "2026-11-01T00:00:00Z"): Binding {
   return {
      kind: "persist",
      tier: "database",
      sourceName: name,
      sourceIDs: [`${name}@m.malloy`],
      destinationName: "duckdb",
      virtualHandle: `addr-${name}`,
      tablePath: `t_${name}`,
      schema: [{ name: "id", type: "BIGINT" }],
      freshAsOf: fresh,
   } as Binding;
}

/** A pin that reads every one of `bindings`. */
function pinned(bindings: readonly Binding[]): Pinned {
   return {
      snapshot: makeSnapshot(bindings),
      asOf: new Date(0),
      fresh: bindings,
   } as unknown as Pinned;
}

interface CompileCall {
   key: string;
   generation: number;
   bindings: Binding[];
   lane: number;
   finish(tag: string): void;
   fail(err: Error): void;
}

/**
 * A served copy whose compile waits for the spec: each call is recorded and
 * settled with {@link CompileCall.finish}, which reports `tag` as the
 * compiled copy's model and its one problem.
 */
function servedCopy() {
   const serve = new ServedCopyServe({
      modelPath: () => "m.malloy",
      modelDef: () => ({ contents: {} }),
      serveDestinationConfig: () => undefined,
   } as never);
   const calls: CompileCall[] = [];
   (serve as unknown as { compile: unknown }).compile = (
      key: string,
      generation: number,
      bindings: Binding[],
      lane = 0,
   ) =>
      new Promise((resolve, reject) => {
         calls.push({
            key,
            generation,
            bindings,
            lane,
            finish: (tag) =>
               resolve({
                  key,
                  generation,
                  over: bindings,
                  lane,
                  rerooted: [],
                  aliased: [],
                  modelDef: { tag },
                  problems: [{ reason: tag, detail: tag }],
               }),
            fail: reject,
         });
      });
   /** The compiled copy a pin is answered from, on `lane`. */
   const compiled = (pin: Pinned, lane = 0) =>
      (
         serve as unknown as {
            ensureCompiled(
               p: Pinned,
               lane?: number,
            ): Promise<{ modelDef: { tag: string } }>;
         }
      )
         .ensureCompiled(pin, lane)
         .then((c) => c.modelDef.tag);
   /** The copy lane 0 keeps, as the warnings read it. */
   const kept = () => serve.problems().map((p) => String(p.detail));
   return { serve, calls, compiled, kept };
}

/** Let the queued microtasks run. */
async function ticks(): Promise<void> {
   for (let i = 0; i < 20; i++) await Promise.resolve();
}

describe("ServedCopyServe.ensureCompiled", () => {
   it("reuses the kept copy for the same bindings, without compiling again", async () => {
      const { calls, compiled, kept } = servedCopy();
      const pin = pinned([binding("a")]);
      const first = compiled(pin);
      await ticks();
      calls[0].finish("one");
      expect(await first).toBe("one");
      expect(await compiled(pin)).toBe("one");
      expect(calls).toHaveLength(1);
      expect(kept()).toEqual(["one"]);
   });

   it("rebinds the kept copy to a pin whose bindings differ only in freshness", async () => {
      const { calls, compiled } = servedCopy();
      const first = compiled(pinned([binding("a", "2026-11-01T00:00:00Z")]));
      await ticks();
      calls[0].finish("one");
      await first;
      expect(
         await compiled(pinned([binding("a", "2026-11-02T00:00:00Z")])),
      ).toBe("one");
      expect(calls).toHaveLength(1);
   });

   it("compiles again for a pin two of whose bindings read alike, since a rebind cannot tell them apart", async () => {
      const { calls, compiled } = servedCopy();
      const twin = () => [binding("a"), binding("a")];
      const first = compiled(pinned(twin()));
      await ticks();
      calls[0].finish("one");
      await first;
      const second = compiled(pinned(twin()));
      await ticks();
      expect(calls).toHaveLength(2);
      calls[1].finish("two");
      expect(await second).toBe("two");
   });

   it("lets concurrent requests for the same tables share one compile", async () => {
      const { calls, compiled, kept } = servedCopy();
      const pin = pinned([binding("a")]);
      const one = compiled(pin);
      const two = compiled(pinned([binding("a")]));
      await ticks();
      expect(calls).toHaveLength(1);
      calls[0].finish("shared");
      expect([await one, await two]).toEqual(["shared", "shared"]);
      expect(kept()).toEqual(["shared"]);
   });

   it("compiles again when a concurrent caller joins a compile over twin bindings it cannot rebind", async () => {
      // Two requests with the same compile key, each pin holding two
      // bindings that read alike: the second joins the first's compile, then
      // cannot rebind its result, so it compiles its own beside it.
      const { calls, compiled } = servedCopy();
      const one = compiled(pinned([binding("a"), binding("a")]));
      const two = compiled(pinned([binding("a"), binding("a")]));
      await ticks();
      expect(calls).toHaveLength(1);
      calls[0].finish("first");
      expect(await one).toBe("first");
      await ticks();
      expect(calls).toHaveLength(2);
      calls[1].finish("second");
      expect(await two).toBe("second");
   });

   it("never lets an older request's compile, finishing last, replace a newer request's copy", async () => {
      const { calls, compiled, kept } = servedCopy();
      const older = compiled(pinned([binding("a")]));
      const newer = compiled(pinned([binding("b")]));
      await ticks();
      expect(calls).toHaveLength(2);
      calls[1].finish("newer");
      expect(await newer).toBe("newer");
      calls[0].finish("older");
      // The older request is still answered from its own compile...
      expect(await older).toBe("older");
      // ...but the lane keeps the newer one.
      expect(kept()).toEqual(["newer"]);
   });

   it("keeps a newer request's reuse over an older request's compile", async () => {
      const { calls, compiled, kept } = servedCopy();
      const base = compiled(pinned([binding("a")]));
      await ticks();
      calls[0].finish("a");
      await base;
      const older = compiled(pinned([binding("b")]));
      await ticks();
      // A newer request answered from the kept copy, by identity.
      expect(await compiled(pinned([binding("a")]))).toBe("a");
      calls[1].finish("b");
      expect(await older).toBe("b");
      expect(kept()).toEqual(["a"]);
   });

   it("keeps no compile that an invalidation overtook, and starts a fresh one after it", async () => {
      const { serve, calls, compiled, kept } = servedCopy();
      const pin = pinned([binding("a")]);
      const before = compiled(pin);
      await ticks();
      serve.invalidate();
      // The same tables after the invalidation do not join the stale compile.
      const after = compiled(pin);
      await ticks();
      expect(calls).toHaveLength(2);
      expect(calls[1].generation).toBe(calls[0].generation + 1);
      calls[0].finish("stale");
      expect(await before).toBe("stale");
      expect(kept()).toEqual([]);
      calls[1].finish("fresh");
      expect(await after).toBe("fresh");
      expect(kept()).toEqual(["fresh"]);
   });

   it("invalidates on a new context, so the next request compiles", async () => {
      const { serve, calls, compiled } = servedCopy();
      const pin = pinned([binding("a")]);
      const first = compiled(pin);
      await ticks();
      calls[0].finish("one");
      await first;
      serve.setContext("/pkg", {} as never);
      const second = compiled(pin);
      await ticks();
      expect(calls).toHaveLength(2);
      calls[1].finish("two");
      expect(await second).toBe("two");
   });

   it("keeps each lane's copy apart: a pair table's other block compiles once, beside lane 0", async () => {
      const { calls, compiled, kept } = servedCopy();
      const pin = pinned([binding("a")]);
      const zero = compiled(pin, 0);
      const one = compiled(pin, 1);
      await ticks();
      expect(calls.map((c) => c.lane)).toEqual([0, 1]);
      calls[0].finish("lane 0");
      calls[1].finish("lane 1");
      expect([await zero, await one]).toEqual(["lane 0", "lane 1"]);
      expect(await compiled(pin, 1)).toBe("lane 1");
      expect(calls).toHaveLength(2);
      expect(kept()).toEqual(["lane 0"]);
   });

   // Not pinned: a compile that REJECTS. `ensureCompiled` tracks its in-flight
   // promise with `void promise.finally(...)`, whose derived promise rejects
   // unhandled, so a rejected compile raises an unhandled rejection beside
   // the caller's own. Reported for a decision, not pinned.
});
