// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What one model reads of a snapshot's `storage=` tables of its own
 * sources: the bindings it keeps (a same-named source declared elsewhere is
 * not this model's), for the path report and the package's served-source
 * list; and the stored columns whose type does not serve as declared
 * (`storage-column-type-mismatch`), each read through a cast or answered
 * live. The same warning's colocated findings: a table altered outside a
 * build, whose drift the bind read ({@link colocatedDriftFindings}).
 *
 * Called by `Model` once per snapshot, which logs and counts what this
 * finds. Reads the author's model files for the lift; no other I/O.
 */

import { queryTimezoneOf } from "../../governance/ir/compiled_model";
import { type ModelDef } from "@malloydata/malloy";
import { compiledTextReader } from "../../../utils";
import { v1StoredColumns } from "./served_copy_serve";
import { type Binding, bindingServesModelSource } from "../domain/binding";
import {
   authorModelLiftContext,
   type DerivedSourceDef,
} from "../text/author_model";
import {
   declaredColumnsOf,
   type ReadCast,
   serveTypePlan,
   storedColumnReadPlan,
   storedColumnTypeMismatches,
   type StoredColumnTypeMismatch,
   storedMalloyType,
   storedTypeServesAs,
} from "../stored_column_types";
import type { ObservedDrift } from "./bind";
import type { BuildPlan } from "../domain/types";
import type { MaterializePlan } from "../domain/table_plans";

/** The warning code a stored column type mismatch is logged and reported with. */
export { STORED_COLUMN_TYPE_MISMATCH_CODE } from "../stored_column_types";

/** One bound source's stored columns that do not serve as their declared type. */
export interface SourceTypeMismatches {
   sourceName: string;
   /** The model's own sourceID for it, so two models' same-named sources stay apart. */
   sourceID?: string;
   /** Each mismatch, and whether it is read through a cast or answered live. */
   mismatches: (StoredColumnTypeMismatch & { handling: "cast" | "live" })[];
}

/**
 * How a stored column whose type does not serve as declared is read: through
 * a faithful `cast`, or not at all, so queries that read it are answered
 * `live`.
 */
export type MismatchHandling = "cast" | "live";

/**
 * A `#(materialize)`, persisted view or query, or rollup `storage=` table's
 * stored columns that do not serve as the types its build source declares.
 */
export interface BoundTableTypeFinding {
   binding: Binding;
   sourceID: string;
   modelPath?: string;
   /** The name the author wrote: the source, view or query, or a rollup's base. */
   subject: string;
   mismatches: (StoredColumnTypeMismatch & { handling: MismatchHandling })[];
}

/**
 * The type findings of every `storage=` table that is not a `#@ persist`
 * source's (whose findings each model makes from its own source's fields,
 * {@link modelStorageTables}), against the columns the plan's build source
 * declares: the same declaration the build casts to and checks after it
 * writes, so build and serve report one drift.
 */
export function boundTableTypeFindings(
   bindings: readonly Binding[],
   plan: BuildPlan | null,
   readers: {
      /**
       * The query time zone the reader of a table's source reads it in (a
       * date cast from an instant is not faithful under one); undefined when
       * it reads in none or the source is not found.
       */
      queryTimezone?: (
         sourceID: string,
         source: NonNullable<BuildPlan["sources"]>[string],
      ) => string | undefined;
      /** The `#(materialize)` plans, by the author's sourceID. */
      materializePlans?: Readonly<Record<string, MaterializePlan>>;
   } = {},
): BoundTableTypeFinding[] {
   const sources = Object.entries(plan?.sources ?? {});
   const findings: BoundTableTypeFinding[] = [];
   for (const binding of bindings) {
      if (binding.tier !== "storage" || binding.kind === "persist") continue;
      const atAddress = sources.filter(
         ([, s]) => s.sourceEntityId === binding.virtualHandle,
      );
      const [sourceID, source] =
         atAddress.find(([id]) => binding.sourceIDs?.includes(id)) ??
         atAddress[0] ??
         [];
      if (!sourceID || !source) continue;
      const declared = (source.columns ?? []).flatMap((c) =>
         c.name && c.type ? [{ name: c.name, type: c.type }] : [],
      );
      const mismatches = storedColumnTypeMismatches(binding.schema, declared);
      if (mismatches.length === 0) continue;
      // A joined field stored in a `#(materialize)` table's record (an
      // inlined join_one, a pair table) has no cast inside the record, so
      // the served copy leaves its whole source live.
      const recordColumns = new Set(
         binding.kind === "materialize"
            ? (readers.materializePlans?.[sourceID]?.columns ?? [])
                 .filter((c) => c.join !== undefined)
                 .map((c) => c.column)
            : [],
      );
      const handled = handlingOf(mismatches, {
         queryTimezone: readers.queryTimezone?.(sourceID, source),
         recordColumns,
      });
      findings.push({
         binding,
         sourceID,
         ...(source.modelPath ? { modelPath: source.modelPath } : {}),
         subject: source.preaggregate?.baseSourceName ?? source.name,
         mismatches: mismatches.map((m) => ({
            ...m,
            handling: handled(m),
         })),
      });
   }
   return findings;
}

/**
 * One bound colocated table whose warehouse columns moved since its build,
 * as the bind read them ({@link ObservedDrift}), for the same warning a
 * `storage=` table's mismatch raises.
 */
export interface ColocatedDriftFinding {
   sourceID: string;
   modelPath?: string;
   /** The name the author wrote: the source, view or query, or a rollup's base. */
   subject: string;
   /**
    * Each moved column against the type its source declares: `storedType` is
    * absent for a column that is gone. `table` when the table is answered
    * live as a whole (a rollup, persisted view or query, which no cast
    * reaches).
    */
   mismatches: (Omit<StoredColumnTypeMismatch, "storedType"> & {
      storedType?: string;
      handling: MismatchHandling | "table";
   })[];
}

/**
 * The warning findings of the colocated tables whose columns moved outside a
 * build ({@link ObservedDrift}), against the columns the plan's build source
 * declares, handled as the bind handles them: a served-copy table
 * (`#(materialize)`, `#@ persist`) reads each moved column through a
 * faithful cast or leaves it out, and skips one whose new type still serves
 * as declared; any other table is answered live whole.
 */
export function colocatedDriftFindings(
   drifted: readonly ObservedDrift[],
   plan: BuildPlan | null,
   readers: {
      /** Whether the table is read through the served copy, which casts per column. */
      servedCopy: (sourceEntityId: string) => boolean;
      /** As {@link boundTableTypeFindings}'s. */
      queryTimezone?: (
         sourceID: string,
         source: NonNullable<BuildPlan["sources"]>[string],
      ) => string | undefined;
   },
): ColocatedDriftFinding[] {
   const sources = Object.entries(plan?.sources ?? {});
   return drifted.flatMap((drift): ColocatedDriftFinding[] => {
      const [sourceID, source] =
         sources.find(([, s]) => s.sourceEntityId === drift.sourceEntityId) ??
         [];
      if (!sourceID || !source) return [];
      const declaredOf = (column: string, builtType: string) =>
         source.columns?.find((c) => c.name === column)?.type ??
         storedMalloyType(builtType) ??
         builtType;
      const servedCopy = readers.servedCopy(drift.sourceEntityId);
      const queryTimezone = readers.queryTimezone?.(sourceID, source);
      const mismatches = drift.columns.flatMap(
         (c): ColocatedDriftFinding["mismatches"] => {
            const declaredType = declaredOf(c.column, c.builtType);
            const moved = {
               column: c.column,
               declaredType,
               ...(c.observedType ? { storedType: c.observedType } : {}),
            };
            if (!servedCopy) return [{ ...moved, handling: "table" }];
            if (!c.observedType) return [{ ...moved, handling: "live" }];
            if (storedTypeServesAs(c.observedType, declaredType) !== false) {
               return [];
            }
            const { casts } = serveTypePlan(
               [{ ...moved, storedType: c.observedType }],
               { queryTimezone },
            );
            return [{ ...moved, handling: casts.length > 0 ? "cast" : "live" }];
         },
      );
      if (mismatches.length === 0) return [];
      return [
         {
            sourceID,
            ...(source.modelPath ? { modelPath: source.modelPath } : {}),
            subject: source.preaggregate?.baseSourceName ?? source.name,
            mismatches,
         },
      ];
   });
}

/**
 * How each reader treats a mismatch, the one rule every `storage=` reader
 * applies ({@link serveTypePlan}): read through a faithful cast where one
 * gives live's values, else not read, so a query reading it answers live. A
 * persisted result (`storedColumnCasts`) and a rollup (`storedRollupReads`)
 * decline the table for such a column; a `#(materialize)` table's served copy
 * leaves the column out (`materializedColumnsOf`).
 */
function handlingOf(
   mismatches: readonly StoredColumnTypeMismatch[],
   options: { queryTimezone?: string; recordColumns: ReadonlySet<string> },
): (m: StoredColumnTypeMismatch) => MismatchHandling {
   const cast = new Set(
      serveTypePlan(mismatches, { queryTimezone: options.queryTimezone })
         .casts.map((c) => c.column)
         .filter((c) => !options.recordColumns.has(c)),
   );
   return (m) => (cast.has(m.column) ? "cast" : "live");
}

/** One binding's mismatches, with how each is handled, for the model's log. */
interface BindingTypeFinding {
   binding: Binding;
   casts: ReadCast[];
   unservable: StoredColumnTypeMismatch[];
   mismatches: StoredColumnTypeMismatch[];
}

/** A model's own `storage=` tables in a snapshot, and its type findings. */
interface ModelStorageTables {
   /** The `#@ persist` storage bindings of this model's own sources. */
   kept: readonly Binding[];
   /** Source names whose binding stores a same-named source declared elsewhere. */
   shadowed: readonly string[];
   mismatches: readonly SourceTypeMismatches[];
   findings: readonly BindingTypeFinding[];
   /** Why the type check could not run, for the model's debug log. */
   checkFailed?: string;
}

/**
 * The `#@ persist` `storage=` bindings of `bindings` this model may serve,
 * and their stored columns' types against the model's declared ones. A
 * binding whose sourceIDs do not include this model's own source of that
 * name is for another model's source (model_storage_serve.spec.ts), so that
 * source serves live here. A type check that cannot run reports nothing but
 * why (`checkFailed`): the served copy decides each column on its own.
 */
export function modelStorageTables(
   bindings: readonly Binding[],
   modelDef: ModelDef | undefined,
   /** The package's root, which the model's files are read inside. */
   packageRoot?: string,
): ModelStorageTables {
   const stored = bindings.filter(
      (b) => b.kind === "persist" && b.tier === "storage",
   );
   if (stored.length === 0) {
      return { kept: [], shadowed: [], mismatches: [], findings: [] };
   }
   const { contents } = authorModelLift(modelDef, packageRoot);
   const { kept, shadowed } = ownBindingsIn(stored, contents);
   const mismatches: SourceTypeMismatches[] = [];
   const findings: BindingTypeFinding[] = [];
   try {
      for (const binding of kept) {
         const plan = typePlanFor(binding, contents);
         if (plan.mismatches.length === 0) continue;
         findings.push({ binding, ...plan });
         const unservable = new Set(plan.unservable.map((m) => m.column));
         const def = contents?.[binding.sourceName] as
            | { sourceID?: unknown }
            | undefined;
         mismatches.push({
            sourceName: binding.sourceName,
            ...(typeof def?.sourceID === "string"
               ? { sourceID: def.sourceID }
               : {}),
            mismatches: plan.mismatches.map((m) => ({
               ...m,
               handling: unservable.has(m.column) ? "live" : "cast",
            })),
         });
      }
   } catch (err) {
      return {
         kept,
         shadowed,
         mismatches: [],
         findings: [],
         checkFailed: err instanceof Error ? err.message : String(err),
      };
   }
   return { kept, shadowed, mismatches, findings };
}

/**
 * The bindings that store this model's own source of their name, and the
 * names of those that store a same-named source declared elsewhere.
 */
export function ownBindings(
   bindings: readonly Binding[],
   modelDef: ModelDef | undefined,
   packageRoot?: string,
): { kept: Binding[]; shadowed: string[] } {
   if (!bindings.some((b) => b.sourceIDs)) {
      return { kept: [...bindings], shadowed: [] };
   }
   return ownBindingsIn(
      bindings,
      authorModelLift(modelDef, packageRoot).contents,
   );
}

function ownBindingsIn(
   bindings: readonly Binding[],
   contents: Record<string, { sourceID?: unknown } | undefined>,
): { kept: Binding[]; shadowed: string[] } {
   const kept: Binding[] = [];
   const shadowed: string[] = [];
   for (const binding of bindings) {
      if (bindingServesModelSource(binding, contents)) kept.push(binding);
      else shadowed.push(binding.sourceName);
   }
   return { kept, shadowed };
}

/**
 * The author model's sources and file text, read once per call. The lift's
 * contents, not the model's: they also hold the hidden dependencies from
 * `sourceRegistry`, which `contents` leaves out.
 */
function authorModelLift(
   modelDef: ModelDef | undefined,
   packageRoot?: string,
): {
   contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>;
} {
   return authorModelLiftContext(
      modelDef,
      compiledTextReader(modelDef, packageRoot),
   );
}

/**
 * A binding's stored columns against the source's declared types, hidden
 * ones included, as the served copy binds them: which columns read through a
 * cast, and which no cast makes faithful.
 */
function typePlanFor(
   binding: Binding,
   contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>,
): Omit<BindingTypeFinding, "binding"> {
   const def = contents?.[binding.sourceName];
   const plan = storedColumnReadPlan(
      v1StoredColumns(binding.schema, def?.fields),
      declaredColumnsOf(def?.fields),
      { queryTimezone: queryTimezoneOf(def) },
   );
   return {
      casts: plan.casts,
      unservable: plan.leftOut,
      mismatches: plan.mismatches,
   };
}
