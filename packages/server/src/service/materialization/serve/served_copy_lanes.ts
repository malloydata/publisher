// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's compile cache: which compiled copy a request is answered
 * from, per lane (the block every multi-path pair table is read for; 0 for
 * the copy compiled at bind). A request reuses the kept copy for the same
 * bindings, rebinds it to bindings that differ only in what the compile does
 * not read, joins a compile in flight for the same tables, or starts one.
 * Each request has a turn, so an older request's compile, finishing last,
 * never replaces the copy a newer one kept; an invalidation starts a new
 * generation, so a compile it overtook is never kept.
 *
 * The compile itself is the served copy's, handed in.
 */

import type { Binding } from "../domain/binding";

/** What the cache reads of a compiled copy. */
export interface LaneCompiled {
   key: string;
   /** The invalidation generation it was compiled under. */
   generation: number;
   /** The bindings it was compiled over, by identity. */
   over: readonly Binding[];
   /** Bindings actually re-rooted, with their destination under its alias. */
   rerooted: Binding[];
   aliased: Binding[];
}

/** Compile a copy of `bindings` for `lane`, under `generation`. */
type CompileCopy<C> = (
   key: string,
   generation: number,
   bindings: Binding[],
   lane: number,
   asOfMs: number,
) => Promise<C>;

/**
 * What a served copy's compile reads of a binding: everything but how fresh
 * the table is (`freshAsOf`, its window and fallback, which decide only
 * whether the pin reads it) and what the router reads of it beside the copy
 * (`ownGateKept`, `holdsOnlyUngatedRows`, `retypedColumns`, a rollup's key
 * check). A field added to `Binding` is read
 * here until it is listed, so a new input recompiles rather than reuses.
 * Memoized per binding, which is replaced, never edited.
 */
const compileInputs = new WeakMap<Binding, string>();
export function compileInputOf(b: Binding): string {
   let input = compileInputs.get(b);
   if (input === undefined) {
      const {
         freshAsOf: _freshAsOf,
         freshnessWindowSeconds: _window,
         freshnessFallback: _fallback,
         ownGateKept: _gateKept,
         holdsOnlyUngatedRows: _ungated,
         retypedColumns: _retyped,
         rollupKey: _rollupKey,
         ...read
      } = b;
      input = JSON.stringify(read);
      compileInputs.set(b, input);
   }
   return input;
}

/** The compile key of the bindings a copy is compiled over, in any order. */
function compileKeyOf(bindings: readonly Binding[]): string {
   return bindings.map(compileInputOf).sort().join("\u0000");
}

/** Whether two binding lists are the same bindings, by identity, in order. */
function sameBindings(a: readonly Binding[], b: readonly Binding[]): boolean {
   return a.length === b.length && a.every((x, i) => x === b[i]);
}

/**
 * `compiled` answering for `bindings`, whose compile key is its own: the same
 * compile, with each binding it reports (re-rooted, aliased) replaced by the
 * one of `bindings` that reads alike, so an answer reports the pin's
 * freshness, never the compile's. Undefined when two of `bindings` read
 * alike, since which is which cannot be told.
 */
function reboundTo<C extends LaneCompiled>(
   compiled: C,
   bindings: readonly Binding[],
): C | undefined {
   const byInput = new Map<string, Binding>();
   for (const b of bindings) {
      const input = compileInputOf(b);
      if (byInput.has(input)) return undefined;
      byInput.set(input, b);
   }
   const now = (b: Binding): Binding => byInput.get(compileInputOf(b)) ?? b;
   const rerooted = compiled.rerooted.map(now);
   return {
      ...compiled,
      over: bindings,
      rerooted,
      aliased: rerooted.map((b, i) => ({
         ...b,
         destinationName: compiled.aliased[i].destinationName,
      })),
   };
}

/** The compiled copy each lane keeps, and the compiles in flight. */
export class CompiledCopyLanes<C extends LaneCompiled> {
   private compiled = new Map<number, C>();
   private inFlight = new Map<
      number,
      { key: string; generation: number; promise: Promise<C> }
   >();
   /** Bumped on every invalidation, so a compile started before it is never kept. */
   private generation = 0;
   /** Requests for a compiled copy so far, and the latest whose copy each lane keeps. */
   private turns = 0;
   private keptTurn = new Map<number, number>();

   constructor(private readonly compile: CompileCopy<C>) {}

   /** The copy `lane` keeps, if any. */
   kept(lane: number): C | undefined {
      return this.compiled.get(lane);
   }

   /**
    * Whether `lane`'s kept copy is the one a request over `bindings` reads
    * now ({@link ensure}'s reuse rule): compiled under this generation over
    * the same bindings, or over ones that read alike. A copy compiled over a
    * previous bind's bindings is not, until a compile over these replaces it.
    */
   keepsFor(lane: number, bindings: readonly Binding[]): boolean {
      const current = this.compiled.get(lane);
      return (
         !!current &&
         current.generation === this.generation &&
         (sameBindings(current.over, bindings) ||
            current.key === compileKeyOf(bindings))
      );
   }

   /** Drop every kept copy and every compile in flight. */
   invalidate(): void {
      this.generation++;
      this.compiled.clear();
      this.inFlight.clear();
   }

   /**
    * The compiled copy for `lane` over `bindings`. Lane 0 is the copy a
    * query is first compiled in; another is compiled only when a query reads
    * that block, and kept like lane 0 until the next invalidation.
    */
   async ensure(bindings: Binding[], lane: number, asOfMs: number): Promise<C> {
      // Each request's turn: a compile an older request awaited never
      // replaces the copy a newer one kept.
      const turn = ++this.turns;
      const keep = (compiled: C): void => {
         if ((this.keptTurn.get(lane) ?? 0) > turn) return;
         this.keptTurn.set(lane, turn);
         this.compiled.set(lane, compiled);
      };
      const generation = this.generation;
      const current = this.compiled.get(lane);
      // The same bindings as the copy compiled over, by identity: no key to
      // read (a query's usual case).
      if (
         current &&
         current.generation === generation &&
         sameBindings(current.over, bindings)
      ) {
         // Kept again under this turn: an older request still compiling
         // must not replace the copy this one answered from.
         keep(current);
         return current;
      }
      const key = compileKeyOf(bindings);
      if (current && current.key === key && current.generation === generation) {
         const reused = reboundTo(current, bindings);
         if (reused) {
            keep(reused);
            return reused;
         }
      }
      let inFlight = this.inFlight.get(lane);
      if (
         !inFlight ||
         inFlight.key !== key ||
         inFlight.generation !== generation
      ) {
         const started = {
            key,
            generation,
            promise: this.compile(key, generation, bindings, lane, asOfMs),
         };
         this.inFlight.set(lane, started);
         // The callers that await `started.promise` see its rejection; this
         // derived promise only clears the slot, so its own is dropped.
         started.promise
            .finally(() => {
               if (this.inFlight.get(lane) === started) {
                  this.inFlight.delete(lane);
               }
            })
            .catch(() => {});
         inFlight = started;
      }
      const awaited = await inFlight.promise;
      // A compile this pin joined over bindings that differ only in what the
      // compile does not read answers with this pin's bindings.
      const rebound = sameBindings(awaited.over, bindings)
         ? awaited
         : reboundTo(awaited, bindings);
      // Two of this pin's bindings read alike, so which is which cannot be
      // told: compile over them as they are.
      const compiled =
         rebound ??
         (await this.compile(key, generation, bindings, lane, asOfMs));
      // An invalidation while it compiled makes it stale: keep it only when
      // it is still the current generation's. A copy over another snapshot
      // is kept too, unless a later request kept its own: the latest
      // request's copy is what the warnings report.
      if (compiled.generation === this.generation) keep(compiled);
      return compiled;
   }
}
