// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's compile cache (`CompiledCopyLanes`, which
 * `ServedCopyServe.ensureCompiled` asks): which compile a pin reuses, joins,
 * rebinds or starts, and which compiled copy a lane keeps when requests and
 * invalidations interleave. The compile is one the spec settles by hand
 * (handed to the cache, as the served copy hands its own), so the ordering
 * is exact and nothing is compiled for real.
 */

import { describe, expect, it, spyOn } from "bun:test";
import { type Binding, makeSnapshot, type Pinned } from "../domain/binding";
import { CompiledCopyLanes, type LaneCompiled } from "./served_copy_lanes";
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

type Copy = LaneCompiled & { lane: number; modelDef: { tag: string } };

/**
 * A compile cache whose compile waits for the spec: each call is recorded
 * and settled with {@link CompileCall.finish}, which reports `tag` as the
 * compiled copy's model.
 */
function servedCopy() {
   const calls: CompileCall[] = [];
   const lanes = new CompiledCopyLanes<Copy>(
      (key, generation, bindings, lane) =>
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
                  }),
               fail: reject,
            });
         }),
   );
   /** The compiled copy a pin is answered from, on `lane`. */
   const compiled = (pin: Pinned, lane = 0) =>
      lanes
         .ensure([...pin.fresh], lane, pin.asOf.getTime())
         .then((c) => c.modelDef.tag);
   /** The copy lane 0 keeps, as the served copy's warnings read it. */
   const kept = () => {
      const copy = lanes.kept(0);
      return copy ? [copy.modelDef.tag] : [];
   };
   return { serve: lanes, calls, compiled, kept };
}

/** A pin at `asOfMs` that reads `bindings`. */
function pinnedAt(bindings: readonly Binding[], asOfMs: number): Pinned {
   return {
      snapshot: makeSnapshot(bindings),
      asOf: new Date(asOfMs),
      fresh: bindings,
   } as unknown as Pinned;
}

/** Let the queued microtasks run. */
async function ticks(): Promise<void> {
   for (let i = 0; i < 20; i++) await Promise.resolve();
}

describe("CompiledCopyLanes", () => {
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

   describe("across a freshness edge", () => {
      const T0 = Date.parse("2026-11-01T00:00:00Z");
      // Read until T0 + 60 s; `b` has no window.
      const lapsing = {
         ...binding("a", "2026-11-01T00:00:00Z"),
         freshnessWindowSeconds: 60,
         freshnessFallback: "live",
      } as Binding;
      const steady = binding("b");
      const before = [lapsing, steady];
      const after = [steady];
      const PRE = T0 + 50_000;
      const POST = T0 + 61_000;

      it("keeps the copy compiled at the edge when a query pinned before it re-keeps the old copy meanwhile", async () => {
         const { calls, compiled, kept } = servedCopy();
         const first = compiled(pinnedAt(before, T0 + 10_000));
         await ticks();
         calls[0].finish("old");
         await first;
         // The commit at the edge starts its compile...
         const lapse = compiled(pinnedAt(after, POST));
         await ticks();
         expect(calls).toHaveLength(2);
         // ...then a query pinned just before the edge asks, with a later
         // turn, and is answered from the old copy by identity.
         expect(await compiled(pinnedAt(before, PRE))).toBe("old");
         calls[1].finish("lapsed");
         expect(await lapse).toBe("lapsed");
         expect(kept()).toEqual(["lapsed"]);
      });

      it("never lets a query pinned before the edge replace the copy compiled after it", async () => {
         const { calls, compiled, kept } = servedCopy();
         const first = compiled(pinnedAt(before, T0 + 10_000));
         await ticks();
         calls[0].finish("old");
         await first;
         const lapse = compiled(pinnedAt(after, POST));
         await ticks();
         calls[1].finish("lapsed");
         await lapse;
         // A query pinned before the edge, asking after the commit kept its
         // copy, compiles its own over the lapsing table.
         const late = compiled(pinnedAt(before, PRE));
         await ticks();
         expect(calls).toHaveLength(3);
         calls[2].finish("pre-edge");
         expect(await late).toBe("pre-edge");
         expect(kept()).toEqual(["lapsed"]);
      });
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

   it("says the kept copy is current only for the bindings it would answer a request over", async () => {
      const { serve, calls, compiled } = servedCopy();
      const a = [binding("a", "2026-11-01T00:00:00Z")];
      expect(serve.keepsFor(0, a)).toBe(false);
      const first = compiled(pinned(a));
      await ticks();
      calls[0].finish("a");
      await first;
      expect(serve.keepsFor(0, a)).toBe(true);
      // Freshness alone moved: the copy is reused, so it is current.
      expect(serve.keepsFor(0, [binding("a", "2026-11-02T00:00:00Z")])).toBe(
         true,
      );
      // A bind added a table: the kept copy is the previous bindings'.
      expect(serve.keepsFor(0, [...a, binding("b")])).toBe(false);
      expect(serve.keepsFor(0, [])).toBe(false);
      serve.invalidate();
      expect(serve.keepsFor(0, a)).toBe(false);
   });

   it("keeps a compile that threw for the verdict, but never reuses it, and is not current once its backoff passes", async () => {
      const calls: Array<() => void> = [];
      let escaped = true;
      const lanes = new CompiledCopyLanes<LaneCompiled>(
         (key, generation, bindings) =>
            new Promise((resolve) => {
               calls.push(() =>
                  resolve({
                     key,
                     generation,
                     over: bindings,
                     rerooted: [],
                     aliased: [],
                     ...(escaped ? { escaped: true } : {}),
                  }),
               );
            }),
      );
      const a = [binding("a")];
      const first = lanes.ensure(a, 0, 0);
      await ticks();
      calls[0]();
      const failed = await first;
      expect(lanes.keepsFor(0, a)).toBe(true);
      escaped = false;
      const second = lanes.ensure(a, 0, 0);
      await ticks();
      expect(calls).toHaveLength(2);
      calls[1]();
      expect((await second).escaped).toBeUndefined();
      // A failed copy whose backoff passed is not current.
      failed.retryDue = true;
      expect(lanes.kept(0)).not.toBe(failed);
      const third = lanes.ensure([binding("b")], 0, 0);
      await ticks();
      escaped = true;
      calls[2]();
      const again = await third;
      expect(lanes.keepsFor(0, [binding("b")])).toBe(true);
      again.retryDue = true;
      expect(lanes.keepsFor(0, [binding("b")])).toBe(false);
   });

   it("drops a failed compile from the in-flight set, so the next request compiles again", async () => {
      const { calls, compiled } = servedCopy();
      const pin = pinned([binding("a")]);
      const first = compiled(pin);
      await ticks();
      calls[0].fail(new Error("compile failed"));
      expect(await first.catch((e: Error) => e.message)).toBe("compile failed");
      const second = compiled(pin);
      await ticks();
      expect(calls).toHaveLength(2);
      calls[1].finish("two");
      expect(await second).toBe("two");
   });
});

describe("ServedCopyServe", () => {
   it("invalidates its compiled copies on a new context", () => {
      const serve = new ServedCopyServe({
         modelPath: () => "m.malloy",
         modelDef: () => ({ contents: {} }),
         serveDestinationConfig: () => undefined,
      } as never);
      const invalidate = spyOn(serve, "invalidate");
      serve.setContext("/pkg", {} as never);
      expect(invalidate).toHaveBeenCalledTimes(1);
   });
});
