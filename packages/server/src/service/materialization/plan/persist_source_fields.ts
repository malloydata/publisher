// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Leaf readers of one compiled persist source: its output columns, its
 * `#@ persist` annotation fields, and the shape of its entry-point gate
 * classification. They live apart from build_plan.ts so the modules build_plan
 * composes (incremental_declaration, materialization_eligibility) can read a
 * source without importing build_plan back.
 */

import type { AtomicField, PersistSource } from "@malloydata/malloy";
import { parseRouteTag } from "../../motly";
import { components } from "../../../api";
import { logger } from "../../../logger";
import { errMessage } from "../../../utils";

type WireColumn = components["schemas"]["Column"];

/** Output columns of a persist source, degrading to [] if unavailable. */
export function deriveColumns(persistSource: PersistSource): WireColumn[] {
   try {
      return persistSource._explore.intrinsicFields
         .filter((f) => f.isAtomicField())
         .map((f) => ({
            name: f.name,
            type: String((f as AtomicField).type),
         }));
   } catch (err) {
      logger.warn("Failed to derive columns for persist source", {
         sourceID: persistSource.sourceID,
         error: errMessage(err),
      });
      return [];
   }
}

/**
 * All scalar key=value fields of a source's `#@ persist` annotation, or {} if
 * absent or unparseable. The control plane uses `name` as the table name (it may
 * carry a container path like `dataset.table`). The full set is returned so new
 * directives flow through without a publisher change.
 */
export function deriveAnnotationFields(
   persistSource: PersistSource,
): Record<string, string> {
   const out: Record<string, string> = {};
   try {
      const tag = parseRouteTag(persistSource.annotations, "@").tag;
      for (const [key, value] of tag.entries()) {
         const text = value.text();
         if (text !== undefined) out[key] = text;
      }
   } catch {
      // Degrade to {}.
   }
   return out;
}

/**
 * Whether every gate on the entry point RESOLVED, not which route it was written
 * on. `"row_level"` is kept as the name so stored plans still read; an
 * `#(authorize)` lock lands there too, deliberately — this decides whether a
 * source may be BUILT, and `Model.queryEntryPointHasRowLevelGate` blocks routing
 * to frozen rows per request on ANY collected gate.
 */
export type PersistSourceGateClassification = "row_level" | "rejected";

/**
 * `classification`: whether the entry point's gates resolved to something
 * enforceable. `attributed`: `false` when `isAuthorizeAttributedToEntryPoint`
 * (materialization_eligibility.ts) finds a note reachable only through a join —
 * checked independently because `collectEntryPointGates` does not trace joins,
 * so `classification` cannot see a join-carried gate.
 */
export interface PersistSourceGateOutcome {
   classification: PersistSourceGateClassification;
   attributed: boolean;
}

/** The `storage=` destination a persist source declares, trimmed; undefined for none. */
export function declaredStorage(
   persistSource: PersistSource,
): string | undefined {
   return deriveAnnotationFields(persistSource).storage?.trim() || undefined;
}
