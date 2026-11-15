// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createRequire } from "module";

/** The installed `@malloydata/render-validator`, part of every key. */
export const RENDER_VALIDATOR_VERSION = readValidatorVersion();

/**
 * The validator's version from its package.json. A bundled `dist` may ship
 * without it in node_modules, and importing `model.ts` must not throw for
 * that: the version then reads "unknown". The cache lives in one process,
 * which runs one validator, so the key loses nothing that could change.
 */
function readValidatorVersion(): string {
   try {
      const pkg = createRequire(import.meta.url)(
         "@malloydata/render-validator/package.json",
      ) as { version?: unknown };
      return typeof pkg.version === "string" ? pkg.version : "unknown";
   } catch {
      return "unknown";
   }
}

/**
 * The key for one model's findings: the digest of the results the check read
 * (SerializedModel.renderTagResultsDigest) and the validator that read them.
 */
export function renderTagFindingsKey(
   digest: string,
   validatorVersion: string = RENDER_VALIDATOR_VERSION,
): string {
   return `${validatorVersion}\u0000${digest}`;
}

/**
 * Render-tag findings by {@link renderTagFindingsKey}. The digest and the
 * validator are the check's whole input, so a model loaded again unchanged (a
 * reload, a re-placement, another environment's copy) is not judged again on
 * the main thread (ms2data/service#7795). Bounded: the least recently used
 * entry goes first.
 */
/**
 * A finding as the cache holds it: Model's `RenderTagWarning`, kept
 * structural here so this module does not import `model.ts`.
 */
export interface CachedRenderTagFinding {
   subject: string;
   message: string;
   severity: "error" | "warn";
}

export class RenderTagFindingsCache {
   private readonly entries = new Map<
      string,
      readonly CachedRenderTagFinding[]
   >();

   constructor(private readonly maxEntries: number) {}

   /** A copy of the findings, so a caller's edit cannot reach the cache. */
   public get(key: string): CachedRenderTagFinding[] | undefined {
      const hit = this.entries.get(key);
      if (hit === undefined) return undefined;
      this.entries.delete(key);
      this.entries.set(key, hit);
      return hit.map((f) => ({ ...f }));
   }

   public set(key: string, findings: readonly CachedRenderTagFinding[]): void {
      this.entries.delete(key);
      this.entries.set(
         key,
         findings.map((f) => ({ ...f })),
      );
      while (this.entries.size > this.maxEntries) {
         const oldest = this.entries.keys().next().value as string;
         this.entries.delete(oldest);
      }
   }

   public get size(): number {
      return this.entries.size;
   }

   public clear(): void {
      this.entries.clear();
   }
}

/** A finding is a few short strings; 1024 models' worth is well under 1 MB. */
export const renderTagFindingsCache = new RenderTagFindingsCache(1024);
