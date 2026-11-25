// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Leaf readers of one compiled persist source: its output columns, its
 * `#@ persist` annotation fields, and the shape of its entry-point gate
 * classification. They live apart from build_plan.ts so the modules build_plan
 * composes (incremental_declaration, materialization_eligibility) can read a
 * source without importing build_plan back.
 */

import { exploreOf } from "../../governance/ir/compiled_model";
import { errorCodeOf } from "../../../error_code";
import type { AtomicField, PersistSource } from "@malloydata/malloy";
import {
   destinationOf,
   persistAnnotationFields,
   placementOf,
   readRouteIfAny,
} from "./declaration";
import { components } from "../../../api";
import { logger } from "../../../logger";

type WireColumn = components["schemas"]["Column"];

/** Output columns of a persist source, degrading to [] if unavailable. */
export function deriveColumns(persistSource: PersistSource): WireColumn[] {
   try {
      return exploreOf(persistSource)
         .intrinsicFields.filter((f) => f.isAtomicField())
         .map((f) => ({
            name: f.name,
            type: String((f as AtomicField).type),
         }));
   } catch (err) {
      logger.warn("Failed to derive columns for persist source", {
         sourceID: persistSource.sourceID,
         code: errorCodeOf(err),
      });
      return [];
   }
}

/**
 * All scalar key=value fields of a source's `#@ persist` annotation, with
 * `storage=` and `name=` read by the placement rule; {} if absent or
 * unparseable. The control plane uses `name` as the table name (it may carry
 * a container path like `dataset.table`). The full set is returned so new
 * directives flow through without a publisher change.
 */
export function deriveAnnotationFields(
   persistSource: PersistSource,
): Record<string, string> {
   return persistAnnotationFields(persistSource.annotations);
}

/**
 * The `storage=` destination a persist source declares, trimmed; undefined
 * for none, and for one that names nothing (refused at eligibility).
 */
export function declaredStorage(
   persistSource: PersistSource,
): string | undefined {
   const tag = readRouteIfAny(persistSource.annotations, "@");
   return tag ? destinationOf(tag, "persist") : undefined;
}

/** Whether a persist source's `storage=` names nothing, which refuses it. */
export function declaresEmptyStorage(persistSource: PersistSource): boolean {
   const tag = readRouteIfAny(persistSource.annotations, "@");
   return tag !== undefined && "refused" in placementOf(tag, "persist");
}
