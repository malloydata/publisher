// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The colocated tables bound with no captured warehouse type for a column
 * whose read depends on one (`stored-native-schema-missing`): a column whose
 * field Malloy types as a big integer is read through a cast to the type the
 * build recorded on its manifest entry (`nativeSchema`), and an entry written
 * before that capture, or one whose capture missed the column, records none.
 * On BigQuery the column is read through INT64, its only integer type, and
 * still serves; on any other dialect it is left out, and a query that reads
 * it answers live. Either way the next full rebuild captures the type.
 *
 * Decided by the same read the served copy makes (`bigIntegerReads`), from
 * the bindings bound now and the author's compiled source, so it follows the
 * manifest rather than the query. Pure.
 */

import { type Binding, isColocated } from "../domain/binding";
import type { MaterializePlan } from "../domain/table_plans";
import { SourceMap } from "../text/source_map";
import { bigIntegerReads, nativeTypeLookup } from "../stored_column_types";

/** The warning code a column read without its captured type is reported with. */
export const NATIVE_SCHEMA_MISSING_CODE = "stored-native-schema-missing";

/** One bound source's big-integer columns with no captured warehouse type. */
export interface NativeSchemaNotice {
   sourceName: string;
   /** The author's sourceID, so a source several models import reports once. */
   sourceID: string;
   /** Read through a type the dialect fixes (BigQuery's INT64): served. */
   inferred: { column: string; as: string }[];
   /** Read through no type: left out, so a query reading one answers live. */
   leftOut: string[];
}

/**
 * Each colocated binding of a source `modelDef` declares whose big-integer
 * columns have no captured warehouse type. A `#(materialize)` binding is the
 * generated build source's; its plan names the author's source.
 */
export function nativeSchemaNotices(
   bindings: readonly Binding[],
   modelDef: unknown,
   materializePlans: Readonly<Record<string, MaterializePlan>>,
): NativeSchemaNotice[] {
   const map = SourceMap.of(modelDef);
   const out: NativeSchemaNotice[] = [];
   for (const binding of bindings) {
      if (!isColocated(binding)) continue;
      const plan = binding.sourceIDs
         ?.map((id) => materializePlans[id])
         .find((p) => p !== undefined);
      const authorIDs = plan
         ? [plan.sourceID]
         : (binding.sourceIDs ??
           [map.sourceIDOf(binding.sourceName)].filter(
              (id): id is string => id !== undefined,
           ));
      const node = authorIDs
         .map((id) => map.node(id))
         .find((n) => n !== undefined);
      if (!node) continue;
      const def = node.def as { fields?: unknown[]; dialect?: unknown };
      const captured = nativeTypeLookup(binding.nativeSchema);
      const uncaptured = bigIntegerReads(binding.schema, def.fields, {
         dialect: def.dialect,
         colocated: true,
         nativeTypeOf: captured,
         // A v1 table stores its query's output names; a `#(materialize)`
         // table, each field's own name after its renames.
         storesOutputNames: !plan,
      }).filter((r) => captured(r.column) === undefined);
      if (uncaptured.length === 0) continue;
      out.push({
         sourceName: plan?.sourceName ?? binding.sourceName,
         sourceID: node.sourceID,
         inferred: uncaptured.flatMap((r) =>
            r.castTo ? [{ column: r.column, as: r.castTo }] : [],
         ),
         leftOut: uncaptured.filter((r) => !r.castTo).map((r) => r.column),
      });
   }
   return out;
}
