// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What one model reads of a snapshot's `storage=` tables of its own
 * sources: the bindings it keeps (a same-named source declared elsewhere is
 * not this model's), for the path report and the package's served-source
 * list; and the stored columns whose type does not serve as declared
 * (`storage-column-type-mismatch`), each read through a cast or answered
 * live.
 *
 * Called by `Model` once per snapshot, which logs and counts what this
 * finds. Reads the author's model files for the lift; no other I/O.
 */

import { queryTimezoneOf } from "../../governance/ir/compiled_model";
import { type ModelDef } from "@malloydata/malloy";
import { compiledTextReader } from "../../../utils";
import { narrowSchemaToPublic } from "./bindings";
import { type Binding, bindingServesModelSource } from "../domain/binding";
import {
   authorModelLiftContext,
   type DerivedSourceDef,
} from "../text/author_model";
import {
   declaredColumnsOf,
   type ReadCast,
   serveTypePlan,
   storedColumnTypeMismatches,
   type StoredColumnTypeMismatch,
} from "../stored_column_types";

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
 * name is for another model's source (malloydata/publisher#1001), so that
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
 * A binding's public schema against the source's declared types: which
 * columns read through a cast, and which no cast makes faithful.
 */
function typePlanFor(
   binding: Binding,
   contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>,
): Omit<BindingTypeFinding, "binding"> {
   const def = contents?.[binding.sourceName];
   const fields = def?.fields;
   const publicSchema = narrowSchemaToPublic(binding.schema, fields);
   const mismatches = storedColumnTypeMismatches(
      publicSchema,
      declaredColumnsOf(fields),
   );
   const { casts, unservable } = serveTypePlan(mismatches, {
      queryTimezone: queryTimezoneOf(def),
   });
   return { casts, unservable, mismatches };
}
