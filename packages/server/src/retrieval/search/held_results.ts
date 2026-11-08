// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The complete get_context results held for their later pages. Its own module
 * so the RetrievalRegistry can build a HeldResults without loading the paging
 * code (./pages.ts), whose imports read the registry.
 */

import type { SourceCard } from "./response";

/** How long a complete result is held for its later pages. */
export const FCJ_RESULT_TTL_MS = 10 * 60_000;
/** Most results held at once; the oldest goes first. */
const MAX_HELD_RESULTS = 256;
/** Most characters of cards held at once, over every held result. */
const MAX_HELD_CHARS = 16_000_000;

/** One complete result, as the response layer pages it. */
export interface HeldResult {
   cards: SourceCard[];
   /**
    * `semantic` when the targets were matched by embedding closeness as well
    * as by keyword and stored value; `lexical` when no embedding was used (no
    * provider, or an index built without vectors).
    */
   retrieval: "semantic" | "lexical";
   clusters: unknown[];
   coverage?: unknown[];
   unresolved_targets: unknown[];
   /** The engine's own warnings about the result, repeated on every page. */
   warnings: string[];
}

interface Entry {
   /** The index the result was computed on; a different one is a miss. */
   index: object;
   result: HeldResult;
   chars: number;
   at: number;
}

/**
 * Complete results held for their later pages: at most
 * {@link MAX_HELD_RESULTS} of them and {@link MAX_HELD_CHARS} characters of
 * cards, each for {@link FCJ_RESULT_TTL_MS}. In memory, per server process.
 */
export class HeldResults {
   private readonly entries = new Map<string, Entry>();
   private chars = 0;

   constructor(
      private readonly ttlMs = FCJ_RESULT_TTL_MS,
      private readonly maxResults = MAX_HELD_RESULTS,
      private readonly maxChars = MAX_HELD_CHARS,
      private readonly now: () => number = Date.now,
   ) {}

   get(key: string, index: object): HeldResult | undefined {
      const entry = this.entries.get(key);
      if (!entry) return undefined;
      if (entry.index !== index || this.now() - entry.at > this.ttlMs) {
         this.remove(key);
         return undefined;
      }
      return entry.result;
   }

   set(key: string, index: object, result: HeldResult): void {
      this.remove(key);
      const chars = JSON.stringify(result.cards).length;
      // A result bigger than the whole cap is answered, never held.
      if (chars > this.maxChars) return;
      this.entries.set(key, { index, result, chars, at: this.now() });
      this.chars += chars;
      for (const [k] of this.entries) {
         if (
            this.entries.size <= this.maxResults &&
            this.chars <= this.maxChars
         ) {
            break;
         }
         this.remove(k);
      }
   }

   get size(): number {
      return this.entries.size;
   }

   clear(): void {
      this.entries.clear();
      this.chars = 0;
   }

   private remove(key: string): void {
      const entry = this.entries.get(key);
      if (!entry) return;
      this.chars -= entry.chars;
      this.entries.delete(key);
   }
}
