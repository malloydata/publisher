// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One leg's compiled variants, by key: the view, query and rollup legs each
 * hold one. A compile is started once per key and shared by every caller
 * that asks while it runs; a failed compile is kept as the value
 * `failed(message)` returns, so it is not retried until the cache is
 * cleared. The cache is cleared when the leg's context changes
 * ({@link invalidate}) or when a query is pinned to other bindings
 * ({@link over}), and each clear starts a new {@link generation}, which the
 * legs put in their graft cache scopes.
 *
 * Bounded: a key can carry what varies per query (the rollup leg's views),
 * so past `limit` entries the least recently used is dropped.
 */

import type { Binding, Pinned } from "../domain/binding";

/** The most compiled variants one leg keeps. */
const DEFAULT_LIMIT = 500;

export class VariantCompileCache<V> {
   private generationNo = 0;
   private readonly entries = new Map<string, Promise<V>>();
   private compiledFor: readonly Binding[] | undefined;

   constructor(private readonly limit = DEFAULT_LIMIT) {}

   /** Bumped by every clear: a graft cache scope. */
   get generation(): number {
      return this.generationNo;
   }

   /** Drop every compiled variant. */
   invalidate(): void {
      this.generationNo++;
      this.entries.clear();
   }

   /** Start over when `pinned`'s bindings are not the ones compiled over. */
   over(pinned: Pinned): void {
      if (this.compiledFor === pinned.snapshot.bindings) return;
      if (this.compiledFor !== undefined) this.invalidate();
      this.compiledFor = pinned.snapshot.bindings;
   }

   /**
    * The variant for `key`: the one compiled or compiling, else `compile()`'s,
    * with a rejection kept as `failed(message)`.
    */
   get(
      key: string,
      compile: () => Promise<V>,
      failed: (message: string) => V,
   ): Promise<V> {
      let compiled = this.entries.get(key);
      if (compiled !== undefined) {
         // Most recently used last.
         this.entries.delete(key);
         this.entries.set(key, compiled);
         return compiled;
      }
      compiled = compile().catch((error: unknown) =>
         failed(error instanceof Error ? error.message : String(error)),
      );
      if (this.entries.size >= this.limit) {
         const oldest = this.entries.keys().next().value;
         if (oldest !== undefined) this.entries.delete(oldest);
      }
      this.entries.set(key, compiled);
      return compiled;
   }

   /** How many variants are held. */
   get size(): number {
      return this.entries.size;
   }

   /** The variants held, compiled or compiling, least recently used first. */
   values(): IterableIterator<Promise<V>> {
      return this.entries.values();
   }
}
