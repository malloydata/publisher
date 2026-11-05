// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Parses the persistence-policy keys carried on a `#(index ... )` annotation
 * into the structured {@link IndexPolicy} reported on a dimension `Entity`.
 *
 *   #(index [freshness.window="24h"] [freshness.fallback=stale_ok]
 *           [refresh=full])
 *   #(index_values n=200 freshness.window="1h")
 *
 * The dimension-grain analog of the source-side
 * `PersistSourcePlan.{refresh,freshness}`, but on the entities channel (indexed
 * dimensions never travel on the `buildPlan`). Unlike `#(filter)`
 * parsing, this is a *lenient metadata* extractor: unknown keys (e.g.
 * `index_values`'s `n=`) are ignored and invalid enum values are dropped
 * (reported absent), never thrown — a malformed policy key must not fail an
 * otherwise-valid publish. Index freshness is opt-in per dimension, so a bare
 * `#(index)` with no policy keys yields `null` (no policy), and a host should
 * apply no proactive cadence.
 *
 * Scope is now a package-level mode (`Package.scope`), so per-dimension
 * `sharing` and `schedule` are no longer part of the policy. They are not
 * silently ignored: {@link findForbiddenIndexPolicyKeys} detects them so the
 * discovery layer can reject the publish.
 */

import type { Freshness, IndexPolicy } from "./internal_types";
import { tokenizeAnnotationBody } from "./filter_parsing";

export type { IndexPolicy };

type RefreshValue = NonNullable<IndexPolicy["refresh"]>;
type FallbackValue = NonNullable<Freshness["fallback"]>;

const VALID_REFRESH = new Set<RefreshValue>(["full", "incremental"]);
const VALID_FALLBACK = new Set<FallbackValue>(["live", "stale_ok", "fail"]);

/**
 * Per-dimension policy keys that are no longer supported now that scope is a
 * package-level mode. Their presence on a `#(index)` is a publish-time rejection
 * (see {@link findForbiddenIndexPolicyKeys}), not a silent drop.
 */
const FORBIDDEN_KEYS = new Set(["sharing", "schedule"]);

// Captures the body between the `index`/`index_values` keyword and the closing
// paren: `#( index <body> )`. Mirrors the presence detector in
// dim_index_discovery.ts (`INDEX_TAG_PATTERN`) but also grabs the inner args.
const INDEX_TAG_BODY = /^#\s*\(\s*index(?:_values)?\b(.*)\)\s*$/;

/**
 * Extracts the {@link IndexPolicy} from a field's taglines. Returns `null` when
 * no `#(index ... )` tagline is present or none of the policy keys are declared
 * (a bare `#(index)` — no policy). Only the first matching `#(index)` tagline is
 * read (a field carries at most one).
 */
export function parseIndexPolicy(taglines: string[]): IndexPolicy | null {
   for (const tagline of taglines) {
      const match = INDEX_TAG_BODY.exec(tagline.trim());
      if (!match) continue;
      return policyFromBody(match[1] ?? "");
   }
   return null;
}

/**
 * Detects any no-longer-supported per-dimension policy keys (`sharing`,
 * `schedule`) declared on a field's `#(index ... )` annotation. Returns the set
 * of forbidden keys found (empty when none). The discovery layer turns a
 * non-empty result into a publish-time rejection so authors get a clear error
 * instead of a silently-ignored knob. Only the first matching `#(index)` tagline
 * is inspected (a field carries at most one).
 */
export function findForbiddenIndexPolicyKeys(taglines: string[]): string[] {
   for (const tagline of taglines) {
      const match = INDEX_TAG_BODY.exec(tagline.trim());
      if (!match) continue;
      const tokens = tokenizeAnnotationBody((match[1] ?? "").trim());
      const found: string[] = [];
      for (const token of tokens) {
         const eq = token.indexOf("=");
         if (eq < 0) continue;
         const key = token.slice(0, eq).toLowerCase();
         if (FORBIDDEN_KEYS.has(key) && !found.includes(key)) {
            found.push(key);
         }
      }
      return found;
   }
   return [];
}

function policyFromBody(body: string): IndexPolicy | null {
   const tokens = tokenizeAnnotationBody(body.trim());
   const policy: IndexPolicy = {};
   let freshnessWindow: string | undefined;
   let freshnessFallback: FallbackValue | undefined;

   for (const token of tokens) {
      const eq = token.indexOf("=");
      if (eq < 0) continue; // bare flags / the `index` keyword — not policy
      const key = token.slice(0, eq).toLowerCase();
      const value = token.slice(eq + 1);
      switch (key) {
         case "refresh":
            if (VALID_REFRESH.has(value as RefreshValue)) {
               policy.refresh = value as RefreshValue;
            }
            break;
         case "freshness.window":
            if (value) freshnessWindow = value;
            break;
         case "freshness.fallback":
            if (VALID_FALLBACK.has(value as FallbackValue)) {
               freshnessFallback = value as FallbackValue;
            }
            break;
         default:
            // Unknown key (e.g. `index_values`'s `n=`, `name=`) — ignore.
            break;
      }
   }

   if (freshnessWindow !== undefined || freshnessFallback !== undefined) {
      const freshness: Freshness = {};
      if (freshnessWindow !== undefined) freshness.window = freshnessWindow;
      if (freshnessFallback !== undefined)
         freshness.fallback = freshnessFallback;
      policy.freshness = freshness;
   }

   // Omit an all-empty policy so a bare `#(index)` reports nothing (opt-in).
   return Object.keys(policy).length > 0 ? policy : null;
}
