// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A model's `storage=` serve bindings and the destinations they resolve
 * against: what the package pushed, kept for the served copy, the rollup and
 * view legs and the path report. It reports the stored columns whose type does
 * not serve as declared (`storage-column-type-mismatch`).
 *
 * Moved out of the storage serve shape when the v1 cutover retired it (6g):
 * the binding filter and the mismatch report are the shape's own code,
 * unchanged, so a package warns and binds exactly as before.
 */

import { type MalloyConfig, type ModelDef } from "@malloydata/malloy";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { logger } from "../../../logger";
import { recordStoredColumnTypeMismatch } from "../../../materialization_metrics";
import { evaluateManifestFreshness } from "../domain/freshness";
import {
   bindingServesModelSource,
   narrowSchemaToPublic,
   type ServeBinding,
} from "./materialization_serve_transform";
import {
   authorModelLiftContext,
   type DerivedSourceDef,
} from "../text/author_model";
import {
   declaredColumnsOf,
   queryTimezoneOf,
   type ReadCast,
   serveTypePlan,
   STORED_COLUMN_TYPE_MISMATCH_CODE,
   storedColumnTypeMismatches,
   type StoredColumnTypeMismatch,
} from "../stored_column_types";

/** One bound source's stored columns that do not serve as their declared type. */
export interface SourceTypeMismatches {
   sourceName: string;
   /** The model's own sourceID for it, so two models' same-named sources stay apart. */
   sourceID?: string;
   /** Each mismatch, and whether it is read through a cast or answered live. */
   mismatches: (StoredColumnTypeMismatch & { handling: "cast" | "live" })[];
}

/** One binding's stored columns against its declared types, decided once per bind. */
interface BindingTypePlan {
   casts: ReadCast[];
   unservable: StoredColumnTypeMismatch[];
   mismatches: StoredColumnTypeMismatch[];
}

/** What the bindings read from the model they are bound to. */
interface StorageBindingsHost {
   modelPath(): string;
   modelDef(): ModelDef | undefined;
   /**
    * A dry run's throwaway set: it logs at debug what a model's own logs at
    * info or warn, and records no metric, since nothing is served.
    */
   quiet?: boolean;
}

export class StorageBindings {
   /**
    * The connections a stored table resolves against: this environment's
    * storage destinations. Deliberately NOT the config this model itself
    * compiles against, so a destination is never resolvable from the author's
    * model, a notebook cell or a query. Absent: nothing serves from storage.
    */
   serveDestinationConfig?: () => MalloyConfig;
   /**
    * The package's `storage=` serve bindings this model keeps (see
    * {@link setServeBindings}). Empty: no serve routing (the common case).
    */
   serveBindings: ServeBinding[] = [];
   /** See {@link storedColumnTypeMismatches}. */
   private typeMismatches: SourceTypeMismatches[] = [];
   /** Tables already reported for a stored column type mismatch. */
   private readonly typeMismatchesLogged = new Set<string>();
   /** Sources already reported as served live for a name clash. */
   private readonly shadowedSourcesLogged = new Set<string>();

   constructor(private readonly host: StorageBindingsHost) {}

   /**
    * Set the destination connections. Pushed by the owning Package from the
    * environment's destination list, resolved on each use so a list change
    * takes effect without a reload.
    */
   public setServeDestinationConfig(provider: () => MalloyConfig): void {
      this.serveDestinationConfig = provider;
   }

   /**
    * Set (or clear) this model's `storage=` serve bindings.
    *
    * The package pushes the same set to every model, keyed by source name, and
    * names are not unique in a package. A binding whose sourceIDs do not include
    * this model's own source of that name is for another model's source, so it is
    * dropped here and that source serves live (malloydata/publisher#1001).
    */
   public setServeBindings(bindings: ServeBinding[]): void {
      // Read only when some binding carries an identity to check. The lift's
      // contents, not the model's: they also hold the hidden dependencies
      // from `sourceRegistry`, which `contents` leaves out.
      const contents = bindings.some((binding) => binding.sourceIDs)
         ? this.authorModelLift().contents
         : undefined;
      const kept: ServeBinding[] = [];
      for (const binding of bindings) {
         if (bindingServesModelSource(binding, contents)) {
            kept.push(binding);
         } else if (!this.shadowedSourcesLogged.has(binding.sourceName)) {
            // Once per source per model: bindings are re-pushed on every bind.
            this.shadowedSourcesLogged.add(binding.sourceName);
            logger[this.host.quiet ? "debug" : "info"](
               "Serving a source live: its storage table materializes a same-named source declared elsewhere",
               {
                  modelPath: this.host.modelPath(),
                  source: binding.sourceName,
               },
            );
         }
      }
      this.serveBindings = kept;
      try {
         this.typeMismatches = this.findTypeMismatches(kept);
      } catch (err) {
         // Reporting only: the served copy decides each column on its own.
         this.typeMismatches = [];
         logger.debug("Could not check stored column types", {
            modelPath: this.host.modelPath?.(),
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }

   /**
    * The stored columns of this model's bindings whose type does not serve as
    * the type the source declares, found when the bindings are set so the
    * package can report them before any query runs.
    */
   public storedColumnTypeMismatches(): readonly SourceTypeMismatches[] {
      return this.typeMismatches;
   }

   /**
    * The serve bindings that may serve from their table right NOW: fresh,
    * un-gated, or stale-under-`stale_ok`. A stale binding with a `live` or
    * `fail` fallback is dropped (`evaluateManifestFreshness`).
    */
   freshServeBindings(now: number): ServeBinding[] {
      const at = new Date(now);
      return this.serveBindings.filter(
         (b) =>
            evaluateManifestFreshness(
               {
                  tableName: b.tablePath,
                  dataAsOf: b.freshAsOf,
                  freshnessWindowSeconds: b.freshnessWindowSeconds,
                  freshnessFallback: b.freshnessFallback,
               },
               at,
            ) === "serve_table",
      );
   }

   private findTypeMismatches(
      bindings: readonly ServeBinding[],
   ): SourceTypeMismatches[] {
      // A rollup's name resolves to nothing in the author's model, and its
      // columns are measures the planner typed, so it is not checked here.
      const ordinary = bindings.filter((b) => b.origin !== "preaggregate");
      if (ordinary.length === 0) return [];
      const { contents } = this.authorModelLift();
      const out: SourceTypeMismatches[] = [];
      for (const binding of ordinary) {
         const plan = typePlanFor(binding, contents);
         if (plan.mismatches.length === 0) continue;
         const unservable = new Set(plan.unservable.map((m) => m.column));
         const def = contents?.[binding.sourceName] as
            | { sourceID?: unknown }
            | undefined;
         out.push({
            sourceName: binding.sourceName,
            ...(typeof def?.sourceID === "string"
               ? { sourceID: def.sourceID }
               : {}),
            mismatches: plan.mismatches.map((m) => ({
               ...m,
               handling: unservable.has(m.column) ? "live" : "cast",
            })),
         });
         const key = `${binding.sourceName}\u0000${binding.virtualHandle}`;
         if (this.typeMismatchesLogged.has(key)) continue;
         // Once per table per model: bindings are re-pushed on every bind.
         this.typeMismatchesLogged.add(key);
         for (const m of plan.mismatches) {
            if (!this.host.quiet) {
               recordStoredColumnTypeMismatch("serve", m.declaredType);
            }
         }
         logger[this.host.quiet ? "debug" : "warn"](
            "Stored column types do not match the declared Malloy types; casting where faithful, serving the rest live",
            {
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               modelPath: this.host.modelPath(),
               source: binding.sourceName,
               cast: plan.casts,
               live: plan.unservable,
            },
         );
      }
      return out;
   }

   /** The author model's sources and file text, read once per call. */
   private authorModelLift(): {
      contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>;
   } {
      return authorModelLiftContext(this.host.modelDef(), (url) => {
         try {
            return readFileSync(fileURLToPath(url), "utf8");
         } catch {
            return undefined;
         }
      });
   }
}

/**
 * A binding's public schema against the source's declared types: which
 * columns read through a cast, and which no cast makes faithful.
 */
function typePlanFor(
   binding: ServeBinding,
   contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>,
): BindingTypePlan {
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
