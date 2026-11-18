// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Persisted-view variants (P14): the router's view leg for `#@ persist` on a
 * view.
 *
 * A persisted view's table holds the view's result, so it answers a query
 * only when the query calls that view exactly, or starts from it and goes on:
 *
 * - `run: ops -> by_carrier`: an exact call, recognized from its text
 *   (`extractRunViewCall`) and confirmed by the public result schema
 *   (`resultExplore.name` is the view's name; a hand-written look-alike with
 *   identical SQL is `__stage0`, plan Evidence 4);
 * - `run: ops -> by_carrier -> { … }`: a query that starts from the view,
 *   which only its text shows; its later stages read the stored result;
 * - never `ops -> by_carrier + { … }` (a refinement changes the view's own
 *   query: skipped `MISSING_FIELD`), `ops -> { nest: by_carrier }` or a
 *   hand-written look-alike (neither reads as a call, so the leg does not
 *   apply), nor a call on another source, an extension or a caller's own
 *   redefinition (the run target's `sourceID` must be the view's source's).
 *
 * The caller's text compiles unchanged, in a model generated beside the
 * author's where the source's name means the view's table:
 *
 *     import { customers, REGION } from "flights.malloy"
 *     #(access_filter) region = $REGION        // the source's gates
 *     source: ops is <the view's table> extend {
 *       # bar_chart                            // the view's own notes
 *       view: by_carrier is { select: carrier, flight_count; order_by: flight_count desc }
 *     }
 *
 * so the result keeps the view's name, its columns in their order, its
 * ordering and its render tags. The table is read where it was built: a
 * `storage=` view through `virtual()` on the destination, a database one by
 * importing its build source from the companion and letting the build
 * manifest substitute its table.
 *
 * Route, then govern: the source's row-level gates are written on the
 * variant's source, so `govern()` grafts and proves them on the variant as on
 * live (R3). Only a view whose gate fields it groups by is ever built
 * (question 3, `persisted_view_compile.ts`), and a gated entry point is tried
 * only with `PUBLISHER_MATERIALIZE_GATED_SOURCES` on.
 *
 * Behind `materialize.enabled`.
 */

import type {
   MalloyConfig,
   ModelDef,
   ModelMaterializer,
   QueryMaterializer,
} from "@malloydata/malloy";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../../logger";
import type { GraftRuntime, GraftScope } from "../../gate_classification";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import {
   doubleQuotedString,
   identifier,
   stringLiteral,
} from "../../malloy_text";
import { queryTimezoneOf } from "../../governance/ir/compiled_model";
import type { ModelConnectionInput } from "../../model_runtime";
import { extractRunViewCall, type RunViewCall } from "../../query_text";
import { quoteManifestTablePath } from "../../quoting";
import type { BuildManifest } from "../domain/types";
import {
   generatePersistedResultFile,
   persistedViewCompanionHref,
   viewNoteTexts,
} from "../text/read_text";
import {
   isPersistedQuerySourceID,
   splitSourceID,
   type PersistedQueryPlan,
   type PersistedViewPlan,
} from "../domain/table_plans";
import { SourceMap } from "../text/source_map";
import { compileDeclaringFileVariant } from "./declaring_file_variant";
import {
   type Binding,
   heldReadBinding,
   isDatabase,
   type Pinned,
   storageLookupOf,
} from "../domain/binding";
import type { MaterializedTableCatalog } from "./query_path";
import { fieldNotes, noteSource } from "../text/field_notes";
import { TIMEFRAMES } from "./served_copy";
import { type ColumnCast, storedTypedSource } from "./stored_typed_source";
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";
import { LegDecline } from "../domain/leg_decline";
import {
   canonicalStoredSchema,
   nativeTypeLookup,
   storedColumnReadPlan,
} from "../stored_column_types";
import { STORAGE_DIALECT } from "../domain/dialect";

type VirtualMap = Map<string, Map<string, string>>;

/** What the leg reads from the model that owns it. */
interface ViewVariantHost {
   modelPath(): string;
   modelDef(): ModelDef | undefined;
   modelMaterializer(): ModelMaterializer | undefined;
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
   /** The package's planned tables, to find each view's address. */
   catalog(): MaterializedTableCatalog | undefined;
   /**
    * Every persisted view a compiled model can see, as the planner reads them
    * (`plan/persisted_view.ts`): passed in by the model, since serving never
    * imports the planner.
    */
   planModelPersistedViews(modelDef: unknown): { plans: PersistedViewPlan[] };
   /** The persisted named queries likewise, for the companion they share. */
   planModelPersistedQueries(modelDef: unknown): {
      plans: PersistedQueryPlan[];
   };
}

/** A persisted view's answer, for the router. */
export interface ViewAnswer {
   runnable: QueryMaterializer;
   /** The view's table: its address, tier and as-of. */
   table: ViewTable;
   /** For a `storage=` view: how its `virtual()` handle resolves. */
   virtualMap?: VirtualMap;
   /** For a `storage=` view: its binding. */
   binding?: Binding;
   /**
    * For a database view: its table, for the check that a later stage's
    * filter compares its columns as live compares them
    * (`gate_collation.ts`): its binding, or its path as the manifest
    * substitutes it.
    */
   databaseTableRead?: {
      tier: "database";
      tablePath?: string;
      destinationName?: string;
   };
   /** Where `govern()` grafts the source's gates on the variant (R3). */
   graftScope: GraftScope;
}

/** A persisted view's table, as the path report names it. */
export interface ViewTable {
   /** The table's `sourceEntityId`, when the plan knows it. */
   tableId?: string;
   tier: "storage" | "database";
   /** Bound to the package at all. */
   built: boolean;
   /** Bound, but past its freshness window. */
   stale: boolean;
   dataAsOf?: string;
}

export interface ResolvedTable extends ViewTable {
   virtualMap?: VirtualMap;
   quotedPath?: string;
   binding?: Binding;
   /** A database table's name as the manifest substitutes it. */
   databaseTable?: string;
   /**
    * A database table's stored columns no faithful cast reads as declared
    * since it was altered outside a build (the bind's `leftOutColumns`):
    * every query on a persisted result reads all its columns, so the leg
    * declines while one is.
    */
   leftOut?: readonly string[];
}

export interface CompiledVariant {
   materializer?: ModelMaterializer;
   modelDef?: ModelDef;
   runtime?: GraftRuntime;
   error?: string;
   /** Why the stored rows cannot answer the view with live's columns, by design. */
   refusal?: string;
}

/** One model's persisted views. */
export class PersistedViewServe {
   private packagePath?: string;
   private packageConfig?: ModelConnectionInput;
   /** Bumped when the context, the destinations or the bindings change: a graft cache scope. */
   private generation = 0;
   private compiled = new Map<string, Promise<CompiledVariant>>();
   /** The snapshot bindings {@link compiled} was compiled over. */
   private compiledFor: readonly Binding[] | undefined;
   private planMemo?: { modelDef: unknown; plans: PersistedViewPlan[] };

   constructor(private readonly host: ViewVariantHost) {}

   setContext(packagePath: string, packageConfig: ModelConnectionInput): void {
      this.packagePath = packagePath;
      this.packageConfig = packageConfig;
      this.invalidate();
   }

   invalidate(): void {
      this.generation++;
      this.compiled.clear();
   }

   /** Start a new compile cache when the snapshot's bindings changed. */
   private compiledOver(pinned: Pinned): void {
      if (this.compiledFor === pinned.snapshot.bindings) return;
      if (this.compiledFor !== undefined) this.invalidate();
      this.compiledFor = pinned.snapshot.bindings;
   }

   /** Every persisted view a source this model can see declares. */
   plans(): PersistedViewPlan[] {
      const modelDef = this.host.modelDef();
      const memo = this.planMemo;
      if (memo && memo.modelDef === modelDef) return memo.plans;
      const plans = modelDef
         ? this.host.planModelPersistedViews(modelDef).plans
         : [];
      this.planMemo = { modelDef, plans };
      return plans;
   }

   hasViews(): boolean {
      return this.plans().length > 0;
   }

   /**
    * The `storage=` view tables of a pinned snapshot that store a view this
    * model can see: every one bound, or only those read at the pin.
    */
   viewBindings(pinned: Pinned, which: "bound" | "fresh"): Binding[] {
      const ids = new Set(this.plans().map((p) => p.buildSourceID));
      return (
         which === "bound" ? pinned.snapshot.bindings : pinned.fresh
      ).filter(
         (b) =>
            b.kind === "view" &&
            b.tier === "storage" &&
            (b.sourceIDs ?? []).some((id) => ids.has(id)),
      );
   }

   /** The view the query text calls, when it is a persisted view of this model. */
   calledView(
      queryString: string,
   ): { call: RunViewCall; plan: PersistedViewPlan } | undefined {
      if (!this.hasViews()) return undefined;
      const call = extractRunViewCall(queryString);
      if (!call) return undefined;
      const sourceID = SourceMap.of(this.host.modelDef()).sourceIDOf(
         call.source,
      );
      if (sourceID === undefined) return undefined;
      const plan = this.plans().find(
         (p) => p.sourceID === sourceID && p.viewName === call.view,
      );
      return plan ? { call, plan } : undefined;
   }

   /** The table a query calling a persisted view would read, for the path report. */
   reportedTable(queryString: string, pinned: Pinned): ViewTable | undefined {
      const called = this.calledView(queryString);
      if (!called) return undefined;
      const {
         virtualMap,
         quotedPath,
         binding,
         databaseTable,
         leftOut,
         ...table
      } = this.tableOf(called.plan, pinned, pinned.compilerManifest);
      void virtualMap;
      void quotedPath;
      void binding;
      void databaseTable;
      void leftOut;
      return table;
   }

   /**
    * Answer `queryString` from the view it calls, or throw a {@link LegDecline}.
    * Call only when {@link calledView} names a view.
    */
   async attempt(args: {
      queryString: string;
      givens: Record<string, unknown> | undefined;
      /** The caller's own compile against the author's model. */
      authorRunnable: QueryMaterializer;
      /** The query's pinned tables. */
      pinned: Pinned;
      /** The pinned build manifest without the rollups' entries. */
      buildManifest: BuildManifest | undefined;
   }): Promise<ViewAnswer> {
      this.compiledOver(args.pinned);
      const called = this.calledView(args.queryString);
      if (!called) {
         throw new LegDecline("view_shape_mismatch");
      }
      const { call, plan } = called;
      const refined =
         refinedCallDecline(call) ??
         callerTextDecline(
            call.otherNames,
            [call.source, plan.sourceName],
            call.view,
            {
               contents: this.host.modelDef()?.contents as
                  | Record<string, unknown>
                  | undefined,
               declaringURL: splitSourceID(plan.sourceID)?.url,
            },
         );
      if (refined) throw refined;
      // The run target must be the view's own source, by sourceID: not an
      // extension, and not a caller's redefinition of the name.
      const prepared = await readPreparedQuery(args.authorRunnable);
      const struct = resolveStructRefSource<{ sourceID?: unknown }>(
         prepared.structRef,
         prepared.modelDef ?? this.host.modelDef(),
      );
      const localName =
         typeof prepared.structRef === "string"
            ? prepared.structRef
            : undefined;
      const offTarget = callTargetDecline(
         call,
         plan,
         localName,
         struct?.sourceID,
      );
      // An unnamed run target is always declined (`offTarget` is set).
      if (offTarget || !localName) throw offTarget;
      // An exact call shows in the public result schema; a query that goes on
      // past the view shows only in its text (Evidence 4).
      if (call.laterStages === 0) {
         const result = await args.authorRunnable.getPreparedResult({
            ...(args.givens ? { givens: args.givens as never } : {}),
         } as never);
         const inexact = exactCallDecline(
            call,
            plan,
            result.resultExplore.name,
         );
         if (inexact) throw inexact;
      }
      const table = this.tableOf(plan, args.pinned, args.buildManifest);
      if (!table.built) {
         throw new LegDecline("view_not_built");
      }
      if (table.stale || (!table.virtualMap && !table.databaseTable)) {
         throw new LegDecline("view_stale");
      }
      const altered = alteredColumnsRefusal(table);
      if (altered) {
         throw new LegDecline(
            "view_missing_field",
            `the stored result cannot answer \`${call.view}\` with live's columns: ${altered}`,
         );
      }
      const variant = await this.compiledVariant(plan, localName, table);
      if (variant.refusal) {
         throw new LegDecline(
            "view_missing_field",
            `the stored result cannot answer \`${call.view}\` with live's columns: ${variant.refusal}`,
         );
      }
      return readStoredVariant({
         ...args,
         variant,
         table,
         modelPath: this.host.modelPath(),
         subject: `${plan.sourceName} -> ${plan.viewName}`,
         cacheScope: `persisted_view\u0000${this.generation}\u0000${plan.buildSourceID}\u0000${localName}`,
         compilerKey: this.compilerKeyOf(plan.buildSourceID),
      });
   }

   /**
    * The key Malloy substitutes `buildSourceID`'s database table under,
    * where it is not the table's address (see PlannedTable.compilerKey).
    */
   compilerKeyOf(buildSourceID: string): string | undefined {
      return this.host.catalog()?.planned.get(buildSourceID)?.compilerKey;
   }

   /** A persisted result's table: a `storage=` binding, or a database manifest entry. */
   tableOf(
      plan: Pick<PersistedViewPlan, "buildSourceID" | "storage">,
      pinned: Pinned,
      freshManifest: BuildManifest | undefined,
   ): ResolvedTable {
      const planned = this.host.catalog()?.planned.get(plan.buildSourceID);
      const tableId = planned?.sourceEntityId;
      const withId = tableId !== undefined ? { tableId } : {};
      const lookup = storageLookupOf(
         pinned,
         plan.buildSourceID,
         isPersistedQuerySourceID(plan.buildSourceID) ? "query" : "view",
         plan.storage !== undefined,
      );
      if (lookup) {
         if (lookup.state === "not_built") {
            return { ...withId, tier: "storage", built: false, stale: false };
         }
         const { binding } = lookup;
         const dataAsOf = binding.freshAsOf
            ? { dataAsOf: binding.freshAsOf }
            : {};
         if (lookup.state === "stale" || !this.host.serveDestinationConfig()) {
            return {
               ...withId,
               ...dataAsOf,
               tier: "storage",
               built: true,
               stale: true,
            };
         }
         const quotedPath = quoteManifestTablePath(
            binding.tablePath,
            STORAGE_DIALECT,
         );
         return {
            ...withId,
            ...dataAsOf,
            tier: "storage",
            built: true,
            stale: false,
            binding,
            quotedPath,
            virtualMap: new Map([
               [
                  servedCopyAlias(binding.destinationName),
                  new Map([[binding.virtualHandle, quotedPath]]),
               ],
            ]),
         };
      }
      const bound =
         tableId !== undefined ? pinned.snapshot.entries[tableId] : undefined;
      if (!bound || tableId === undefined) {
         return { ...withId, tier: "database", built: false, stale: false };
      }
      const dataAsOf = bound.dataAsOf ? { dataAsOf: bound.dataAsOf } : {};
      const fresh = freshManifest?.entries[tableId]?.tableName;
      const leftOut = bound.leftOutColumns?.length
         ? { leftOut: bound.leftOutColumns }
         : {};
      // Altered outside its build: read as a `storage=` table is, through a
      // typed source on its own connection, never by the substitution.
      const held =
         fresh !== undefined
            ? heldReadBinding(
                 isPersistedQuerySourceID(plan.buildSourceID)
                    ? "query"
                    : "view",
                 tableId,
                 bound,
              )
            : undefined;
      if (held) {
         return {
            ...withId,
            ...dataAsOf,
            tier: "database",
            built: true,
            stale: false,
            binding: held,
            quotedPath: held.tablePath,
            virtualMap: new Map([
               [held.destinationName, new Map([[tableId, held.tablePath]])],
            ]),
            ...leftOut,
         };
      }
      return fresh
         ? {
              ...withId,
              ...dataAsOf,
              tier: "database",
              built: true,
              stale: false,
              databaseTable: fresh,
              ...leftOut,
           }
         : {
              ...withId,
              ...dataAsOf,
              tier: "database",
              built: true,
              stale: true,
           };
   }

   private compiledVariant(
      plan: PersistedViewPlan,
      localName: string,
      table: ResolvedTable,
   ): Promise<CompiledVariant> {
      const key = [
         plan.buildSourceID,
         localName,
         table.binding
            ? `${table.binding.destinationName}/${table.binding.virtualHandle}`
            : "database",
      ].join("\u0000");
      let compiled = this.compiled.get(key);
      if (!compiled) {
         compiled = this.compile(plan, localName, table).catch((error) => ({
            error: error instanceof Error ? error.message : String(error),
         }));
         this.compiled.set(key, compiled);
      }
      return compiled;
   }

   /**
    * Compile the variant. The view's source is redeclared IN AN OVERLAY OF
    * ITS OWN DECLARING FILE, so it keeps the author's `sourceID`: every gate
    * the governor keys on that source (by name, then by `sourceID`) finds its
    * graft target in the variant, exactly as the served copy keeps it by
    * re-rooting the author's own file. The file's other names come from a
    * copy of it (`<file>.view-author.malloy`), and the query model imports
    * every name of the author's model from its declaring file as before.
    */
   private async compile(
      plan: PersistedViewPlan,
      localName: string,
      table: ResolvedTable,
   ): Promise<CompiledVariant> {
      const packagePath = this.packagePath;
      const packageConfig = this.packageConfig;
      const modelDef = this.host.modelDef();
      const author = this.host.modelMaterializer();
      const declared = splitSourceID(plan.sourceID);
      if (!packagePath || !packageConfig || !modelDef || !author || !declared) {
         return { error: "no context" };
      }
      const shape = await viewShape(author, modelDef, plan, localName);
      const view = viewDeclaration(plan.viewName, shape);
      if ("refusal" in view) return { refusal: view.refusal };
      // malloy-text: viewDeclaration's text, every value through identifier or noteSource
      const viewBody = view.text;
      return compilePersistedResultVariant({
         packagePath,
         packageConfig,
         modelDef,
         modelPath: this.host.modelPath(),
         destinations: this.host.serveDestinationConfig(),
         companionPlans: () => ({
            views: this.host.planModelPersistedViews(modelDef).plans,
            queries: this.host.planModelPersistedQueries(modelDef).plans,
         }),
         plan,
         table,
         shape,
         redeclare: { sourceID: plan.sourceID },
         databaseAlias: `${plan.buildSourceName}__built`,
         emit: (read) => {
            // malloy-text: the source's own gate expressions, as the author wrote them, one per line (see gateLines)
            const gates = read.gates;
            // malloy-text: the source's own `timezone:` statement, its zone through stringLiteral
            const zone = read.sourceTimezone;
            const declared = `${gates}source: ${identifier(read.sourceName)} is ${identifier(read.stored.name)}`;
            if ("typeText" in read.stored) {
               // malloy-text: storedTypedSource's text, names through identifier
               const typeText = read.stored.typeText;
               // malloy-text: the served copy's timeframe and cast emitter, the author's notes, names through identifier
               const timeframes = read.stored.timeframes;
               return `${typeText}
${declared} extend {${zone}${timeframes}
${viewBody}
}`;
            }
            // malloy-text: the companion import, names through identifier and its file through doubleQuotedString
            const importLine = read.stored.importLine;
            return `${importLine}
${declared} extend {${zone}
${viewBody}
}`;
         },
      });
   }
}

/**
 * Compile a persisted result's variant, the view leg's and the query leg's
 * alike: the source it reads redeclared over its table in an overlay of its
 * own declaring file, so it keeps the author's `sourceID` and every gate the
 * governor keys on it finds its graft target, as the served copy keeps it by
 * re-rooting the author's own file. A `storage=` table is read through a
 * typed source, each stored column whose type or value does not serve as
 * live's through the served copy's faithful cast (or the variant is refused);
 * a database one through its build source, imported from the companion over
 * the copy so its address is the one the build plan computed. `emit` writes
 * the leg's own declarations over that stored source.
 */
export async function compilePersistedResultVariant(args: {
   packagePath: string;
   packageConfig: ModelConnectionInput;
   modelDef: ModelDef;
   modelPath: string;
   destinations: (() => MalloyConfig) | undefined;
   /** Every persisted view and named query the model sees: the companion's. */
   companionPlans(): {
      views: readonly PersistedViewPlan[];
      queries: readonly PersistedQueryPlan[];
   };
   plan: { buildSourceName: string; declaringURL: string };
   table: ResolvedTable;
   shape: ViewShape;
   /** What the variant redeclares: a source by `sourceID`, or its declaring file. */
   redeclare:
      | { sourceID: string; alsoRedeclared?: string[] }
      | {
           declaringURL: string;
           alsoRedeclared: string[];
           alsoExported?: string[];
        };
   /** The name a database build source is imported under. */
   databaseAlias: string;
   emit(read: {
      gates: string;
      sourceName: string;
      /** The source's own `timezone:` statement, or "". */
      sourceTimezone: string;
      stored:
         | { name: string; typeText: string; timeframes: string }
         | { name: string; importLine: string };
   }): string;
}): Promise<CompiledVariant> {
   const { packagePath, packageConfig, plan, table, shape } = args;
   const read = table.binding
      ? storedColumnCasts(
           // Matched by a unique case fold too, as the served copy matches:
           // a warehouse that folded the build's quoted aliases (P3-06..09).
           canonicalStoredSchema(
              table.binding.schema,
              shape.columns.map((c) => c.name),
           ),
           shape,
           isDatabase(table.binding)
              ? {
                   database: true,
                   nativeTypeOf: nativeTypeLookup(table.binding.nativeSchema),
                }
              : {},
        )
      : { casts: new Map<string, ColumnCast>() };
   if ("refusal" in read) return { refusal: read.refusal };
   // The source's own time zone: a `storage=` read truncates each stored
   // truncation again, and a later stage truncates a stored timestamp, each
   // in the zone live computes it in, never UTC.
   const sourceTimezone = shape.sourceTimezone
      ? `\n  timezone: ${stringLiteral(shape.sourceTimezone)}`
      : "";
   const modelFile = path.join(packagePath, args.modelPath);
   const config =
      table.binding && !isDatabase(table.binding) && args.destinations
         ? servedCopyConfig(
              packageConfig,
              args.destinations(),
              new Set([table.binding.destinationName]),
           )
         : packageConfig;
   return compileDeclaringFileVariant({
      packagePath,
      modelPath: args.modelPath,
      packageConfig,
      config,
      modelDef: args.modelDef,
      ...args.redeclare,
      copySuffix: ".view-author.malloy",
      queryModelURL: pathToFileURL(`${modelFile}.view.malloy`),
      flags: VARIANT_FLAGS,
      declare: ({ gates, sourceName, copyName, declaringURL, addFile }) => {
         if (table.binding) {
            const name = `${plan.buildSourceName}__stored`;
            const { typeText, timeframes } = storedTypedSource({
               columns: shape.columns,
               casts: read.casts,
               binding: table.binding,
               typeName: `${plan.buildSourceName}__type`,
               stored: name,
            });
            return args.emit({
               gates,
               sourceName,
               sourceTimezone,
               stored: { name, typeText, timeframes },
            });
         }
         const ownFile = (p: { declaringURL: string }) =>
            p.declaringURL === plan.declaringURL;
         const { views, queries } = args.companionPlans();
         const text = generatePersistedResultFile(
            views.filter(ownFile),
            queries.filter(ownFile),
            copyName,
         );
         if (!text) return { error: "no companion" };
         const companion = persistedViewCompanionHref(declaringURL.href);
         addFile(new URL(companion), text);
         const name = args.databaseAlias;
         return args.emit({
            gates,
            sourceName,
            sourceTimezone,
            stored: {
               name,
               importLine: `import { ${identifier(name)} is ${identifier(plan.buildSourceName)} } from ${doubleQuotedString(path.basename(fileURLToPath(companion)))}`,
            },
         });
      },
   });
}

/**
 * Compile the caller's text on a compiled variant and check that its SQL
 * reads the stored table, or throw a {@link LegDecline}: the tail every
 * persisted result's attempt shares (a view's, a named query's).
 */
export async function readStoredVariant(args: {
   queryString: string;
   givens: Record<string, unknown> | undefined;
   buildManifest: BuildManifest | undefined;
   variant: CompiledVariant;
   table: ResolvedTable;
   modelPath: string;
   /** What the variant answers, for the log. */
   subject: string;
   /** The graft cache scope of this variant. */
   cacheScope: string;
   /** The key Malloy substitutes a database table under, when not its address. */
   compilerKey?: string;
}): Promise<ViewAnswer> {
   const { variant, table } = args;
   // Sources folded apart on one build share Malloy's key: under it, this
   // variant reads its own table.
   const own =
      args.compilerKey && table.tableId
         ? args.buildManifest?.entries[table.tableId]
         : undefined;
   const buildManifest =
      own && args.buildManifest
         ? {
              ...args.buildManifest,
              entries: {
                 ...args.buildManifest.entries,
                 [args.compilerKey!]: own,
              },
           }
         : args.buildManifest;
   if (!variant.materializer || !variant.modelDef) {
      logger.warn("A persisted-view variant did not compile", {
         modelPath: args.modelPath,
         view: args.subject,
         error: variant.error,
      });
      throw new LegDecline(
         "view_missing_field",
         "the query does not compile against the stored view",
      );
   }
   const runnable = variant.materializer.loadRestrictedQuery(args.queryString);
   let sql: string;
   try {
      const result = await runnable.getPreparedResult({
         ...(table.virtualMap ? { virtualMap: table.virtualMap } : {}),
         ...(args.givens ? { givens: args.givens as never } : {}),
         ...(table.virtualMap ? {} : { buildManifest }),
      } as never);
      sql = result.sql;
   } catch {
      throw new LegDecline(
         "view_missing_field",
         "the query does not compile against the stored view",
      );
   }
   // The compiled SQL must read the table, never recompute the result.
   const read = table.virtualMap ? table.quotedPath : table.databaseTable;
   if (!read || !sql.includes(read)) {
      throw new LegDecline("view_stale");
   }
   const {
      virtualMap,
      quotedPath,
      binding,
      databaseTable,
      leftOut,
      ...reported
   } = table;
   void quotedPath;
   void leftOut;
   return {
      runnable,
      table: reported,
      ...(virtualMap ? { virtualMap } : {}),
      // A database table read through its typed source is still a database
      // table to the path report and the gate policy, as through the
      // substitution: only a `storage=` table's binding is reported.
      ...(binding && !isDatabase(binding) ? { binding } : {}),
      ...(binding && isDatabase(binding)
         ? {
              databaseTableRead: {
                 tier: "database" as const,
                 tablePath: binding.tablePath,
                 destinationName: binding.destinationName,
              },
           }
         : !virtualMap && databaseTable
           ? {
                databaseTableRead: {
                   tier: "database" as const,
                   tablePath: databaseTable,
                },
             }
           : {}),
      graftScope: {
         modelDef: variant.modelDef,
         materializer: variant.materializer,
         cacheScope: args.cacheScope,
         ...(variant.runtime ? { runtime: variant.runtime } : {}),
      },
   };
}

/** The flags every generated variant file declares. */
const VARIANT_FLAGS =
   "##! experimental { access_modifiers givens persistence virtual_source }";

/** What the variant reproduces of the view's result. */
export interface ViewShape {
   /**
    * Output columns, in order, with a timeframe where the field has one and
    * the annotation lines live's result column carries.
    */
   columns: ViewColumn[];
   orderBy: { name: string; desc: boolean }[];
   notes: string[];
   /** The last stage's kind (`reduce`, `project`, …), as compiled. */
   stageType?: string;
   /** The query time zone the view's rows were computed in (the source's or the view's own). */
   queryTimezone?: string;
   /** The source's own query time zone (`timezone:` on the source). */
   sourceTimezone?: string;
   /** The view's own query time zone (`timezone:` in one of its stages). */
   viewTimezone?: string;
   /** The source's dialect, the one live answers on (as compiled, unchecked). */
   dialect?: unknown;
}

/** One output column of the view, as live reports it. */
interface ViewColumn {
   name: string;
   /** The Malloy type of the stored value (`number`, `string`, `array`, …). */
   type: string;
   timeframe?: string;
   notes: string[];
   /**
    * What live's result reports it as: a `measure` (its `#(malloy)` carries
    * `calculation`), a `dimension`, or a `nest` (a nested result).
    */
   kind: "measure" | "dimension" | "nest";
   /** The compiled output field, for what its type alone does not say (a `bigint` number). */
   field: unknown;
}

/** A compiled query stage, as far as the shape reads it. */
export interface StageIR {
   type?: unknown;
   outputStruct?: { fields?: unknown[] };
   orderBy?: { field?: unknown; dir?: unknown }[];
}

/** What live's result explore says of the columns: which are measures, which nest. */
interface ResultKinds {
   measures: Set<string>;
   nests: Set<string>;
}

/** The kinds live's result reports, read from Malloy's public result explore. */
export function resultKinds(explore: {
   allFields: readonly unknown[];
}): ResultKinds {
   const out: ResultKinds = { measures: new Set(), nests: new Set() };
   for (const raw of explore.allFields) {
      const f = raw as {
         name: string;
         isAtomicField(): boolean;
         isExploreField(): boolean;
         sourceWasMeasure?(): boolean;
      };
      if (f.isAtomicField() && f.sourceWasMeasure?.()) {
         out.measures.add(f.name);
      } else if (f.isExploreField()) {
         out.nests.add(f.name);
      }
   }
   return out;
}

/** The last stage's output columns and ordering. */
export function stageShape(
   stage: StageIR | undefined,
   kinds: ResultKinds,
): Pick<ViewShape, "columns" | "orderBy"> {
   const columns = (stage?.outputStruct?.fields ?? []).flatMap(
      (raw): ViewColumn[] => {
         const f = raw as {
            name?: unknown;
            as?: unknown;
            type?: unknown;
            timeframe?: unknown;
         };
         const name =
            typeof f.as === "string"
               ? f.as
               : typeof f.name === "string"
                 ? f.name
                 : undefined;
         if (name === undefined) return [];
         return [
            {
               name,
               type: typeof f.type === "string" ? f.type : "",
               ...(typeof f.timeframe === "string" &&
               TIMEFRAMES.has(f.timeframe)
                  ? { timeframe: f.timeframe }
                  : {}),
               notes: fieldNotes(raw),
               kind: kinds.nests.has(name)
                  ? "nest"
                  : kinds.measures.has(name)
                    ? "measure"
                    : "dimension",
               field: raw,
            },
         ];
      },
   );
   const orderBy = (stage?.orderBy ?? []).flatMap((o) => {
      const name =
         typeof o.field === "number"
            ? columns[o.field - 1]?.name
            : typeof o.field === "string"
              ? o.field
              : undefined;
      return name === undefined ? [] : [{ name, desc: o.dir === "desc" }];
   });
   return { columns, orderBy };
}

/** The view's output columns, ordering and notes, from a compile of the bare call. */
async function viewShape(
   author: ModelMaterializer,
   modelDef: ModelDef,
   plan: PersistedViewPlan,
   localName: string,
): Promise<ViewShape> {
   const call = author.loadQuery(
      `run: ${identifier(localName)} -> ${identifier(plan.viewName)}`,
   );
   const prepared = await readPreparedQuery(call);
   const stages = (
      Array.isArray(prepared.pipeline) ? prepared.pipeline : []
   ) as StageIR[];
   // Live's result explore says which columns are measures and which nest.
   const result = await call.getPreparedResult();
   const last = stages[stages.length - 1];
   const { columns, orderBy } = stageShape(
      last,
      resultKinds(result.resultExplore),
   );
   const def = SourceMap.of(modelDef).node(plan.sourceID)?.def as
      | {
           fields?: {
              type?: unknown;
              name?: unknown;
              as?: unknown;
              annotations?: never;
           }[];
        }
      | undefined;
   const view = def?.fields?.find(
      (f) => f.type === "turtle" && (f.as ?? f.name) === plan.viewName,
   );
   const { queryTimezone: ownZone, dialect } = (def ?? {}) as {
      queryTimezone?: unknown;
      dialect?: unknown;
   };
   const sourceTimezone = queryTimezoneOf({ queryTimezone: ownZone });
   const viewTimezone = queryTimezoneOf({ query: { pipeline: stages } });
   const queryTimezone = viewTimezone ?? sourceTimezone;
   return {
      columns,
      orderBy,
      notes: viewNoteTexts(view?.annotations),
      ...(typeof last?.type === "string" ? { stageType: last.type } : {}),
      ...(queryTimezone ? { queryTimezone } : {}),
      ...(sourceTimezone ? { sourceTimezone } : {}),
      ...(viewTimezone ? { viewTimezone } : {}),
      ...(dialect !== undefined ? { dialect } : {}),
   };
}

/**
 * Why a database persisted result cannot answer with live's columns: a
 * stored column altered outside its build that no faithful cast reads as
 * declared (the bind's `leftOutColumns`), which every query on the result
 * reads. Undefined when none is.
 */
export function alteredColumnsRefusal(
   table: Pick<ResolvedTable, "leftOut">,
): string | undefined {
   return table.leftOut?.length
      ? alteredColumnsDetail(table.leftOut)
      : undefined;
}

/** Why a stored table's columns the bind left out cannot be read, for a decline. */
export function alteredColumnsDetail(leftOut: readonly string[]): string {
   const detail = `its table was altered outside a build, and no cast reads ${leftOut.map((c) => `\`${c}\``).join(", ")} as declared`;
   return detail;
}

/**
 * The stored columns of a `storage=` view's table read through a cast, by
 * name ({@link storedColumnReadPlan}: a faithful `DATE` or `BOOLEAN`, or a big
 * integer's native read), or why the stored table cannot give live's values:
 * a column no faithful cast reads as live's type, a big integer stored as a
 * float (its digits already lost), or a column live answers as a
 * warehouse-native value (`sql native`) or JSON, which no `type:` reads back
 * as live does.
 */
export function storedColumnCasts(
   schema: readonly { name: string; type: string }[],
   shape: Pick<ViewShape, "columns" | "queryTimezone" | "dialect">,
   /**
    * Where the table is: a database table (`database`, read where it was
    * built, each big integer through the type its catalog names, `nativeTypeOf`),
    * else a `storage=` table in DuckLake.
    */
   site: {
      database?: boolean;
      nativeTypeOf?: (column: string) => string | undefined;
   } = {},
): { casts: Map<string, ColumnCast> } | { refusal: string } {
   const native = shape.columns.find(
      (c) => c.type === "sql native" || c.type === "json",
   );
   if (native) {
      return {
         refusal: `live answers \`${native.name}\` as a ${native.type === "json" ? "JSON" : "warehouse-native"} value, which no stored read gives exactly`,
      };
   }
   const declared = shape.columns
      .filter((c) => c.kind !== "nest" && c.type !== "")
      .map((c) => ({ name: c.name, type: c.type, field: c.field }));
   const plan = storedColumnReadPlan(schema, declared, {
      queryTimezone: shape.queryTimezone,
      dialect: shape.dialect,
      ...(site.database
         ? { database: true, nativeTypeOf: site.nativeTypeOf }
         : // A storage destination is DuckLake.
           { ducklake: true }),
   });
   const [unservable] = plan.leftOut;
   if (unservable) {
      return {
         refusal: `the stored \`${unservable.column}\` (${unservable.storedType}) cannot be read as live's ${unservable.declaredType}`,
      };
   }
   const [lost] = plan.refusals;
   if (lost) {
      return {
         refusal: `the stored \`${lost.column}\` (${lost.storedType}) cannot hold live's integer exactly`,
      };
   }
   return {
      casts: new Map(
         plan.casts.map((c): [string, ColumnCast] => [
            c.column,
            c.native
               ? { nativeCast: c.castTo }
               : { castTo: c.castTo === "BOOLEAN" ? "BOOLEAN" : "DATE" },
         ]),
      ),
   };
}

/**
 * Whether the stored result must be grouped again for its columns to keep
 * live's kinds: a measure or a nest is a column a `select:` would read as a
 * dimension.
 */
function regroups(columns: readonly ViewColumn[]): boolean {
   return columns.some((c) => c.kind !== "dimension");
}

/** The types `max()` takes, which re-aggregates a measure's one stored value. */
const MAX_TYPES = new Set([
   "number",
   "string",
   "date",
   "timestamp",
   "timestamptz",
]);

/**
 * Why the stored rows cannot be grouped again into exactly themselves, or
 * undefined when they can. Regrouping is exact only when the view's last
 * stage groups by at least one dimension: its stored rows are then keyed by
 * those dimensions, one row per group.
 *
 * - A stage that is not a grouping (`select:`) has no key; grouping by its
 *   columns would merge duplicate rows.
 * - A stage with no dimension groups everything into one row, which a
 *   `having:` may have filtered away; grouping an empty table again still
 *   answers one row.
 * - A nest's rows are an array per stored row; rebuilding it would merge a
 *   `select:` nest's duplicates and turn an empty nest into one NULL row.
 */
function regroupRefusal(shape: ViewShape): string | undefined {
   if (shape.stageType !== "reduce") {
      return `its last stage is a ${shape.stageType ?? "stage"} that does not group, so its rows have no key`;
   }
   const nest = shape.columns.find((c) => c.kind === "nest");
   if (nest) {
      return `it nests \`${nest.name}\`, whose stored rows cannot be rebuilt exactly`;
   }
   if (!shape.columns.some((c) => c.kind === "dimension")) {
      return "it groups by no dimension, so an empty result would come back as one row";
   }
   return undefined;
}

/**
 * The view, redeclared over the stored result: its notes, columns and
 * ordering.
 *
 * A view whose result holds only dimensions selects its stored columns. One
 * with a measure groups its stored rows again, by its dimensions, which the
 * stored result holds once per row, so each group is one stored row and each
 * measure re-aggregates one value (`total is total.max()`): the rows are the
 * stored rows, and each measure is a measure again, its `#(malloy)`
 * annotation carrying `calculation` as live's does. A boolean measure
 * re-aggregates through the number it picks, NULL kept NULL. Where that
 * regrouping is not provably exact ({@link regroupRefusal}), or a measure's
 * type is one `max()` does not take (a JSON value, an array), it returns the
 * refusal instead, and the query answers live. `head` opens the statement
 * (a named query's is `query: q is <source> ->`).
 */
export function viewDeclaration(
   viewName: string,
   shape: ViewShape,
   // malloy-text: a statement head its callers write through identifier
   head = `view: ${identifier(viewName)} is`,
): { text: string } | { refusal: string } {
   // An index's result is not the view's rows as a query writes them: live
   // annotates none of its columns, and a stored read annotates every one.
   if (shape.stageType === "index") {
      return {
         refusal:
            "it is an `index:`, whose columns live writes without the annotations a stored read gives them",
      };
   }
   // malloy-text: the author's own view notes (single lines, no `#@`), names through identifier
   const notes = shape.notes.map((n) => `  ${n}\n`).join("");
   // The view's own time zone, so a timeframe a `storage=` read truncates
   // again, and a later stage, compute in the zone live does.
   const zone = shape.viewTimezone
      ? `timezone: ${stringLiteral(shape.viewTimezone)}`
      : "";
   const select = shape.columns.map((c) => identifier(c.name)).join(", ");
   const order =
      shape.orderBy.length > 0
         ? `order_by: ${shape.orderBy
              .map((o) => `${identifier(o.name)} ${o.desc ? "desc" : "asc"}`)
              .join(", ")}`
         : "";
   if (!regroups(shape.columns)) {
      return {
         text: `${notes}  ${head} { ${zone ? `${zone}; ` : ""}select: ${select}${order ? `; ${order}` : ""} }`,
      };
   }
   const refusal =
      regroupRefusal(shape) ??
      shape.columns
         .filter(
            (c) =>
               c.kind === "measure" &&
               c.type !== "boolean" &&
               !MAX_TYPES.has(c.type),
         )
         .map(
            (c) =>
               `the measure \`${c.name}\` is a ${c.type || "value"} that max() cannot re-aggregate`,
         )[0];
   if (refusal) return { refusal };
   const lines = shape.columns.map(groupedColumn).join("");
   return {
      text: `${notes}  ${head} {\n${zone ? `    ${zone}\n` : ""}${lines}${order ? `    ${order}\n` : ""}  }`,
   };
}

/** One column of the regrouped view, as its statement. */
function groupedColumn(c: ViewColumn): string {
   const name = identifier(c.name);
   if (c.kind === "dimension") return `    group_by: ${name}\n`;
   // malloy-text: the author's own notes through noteSource
   const notes = c.notes.map((n) => noteSource(n, "      ")).join("");
   // A boolean through the number it picks; `not b` holds for a NULL `b`, so
   // a NULL is picked as NULL first.
   if (c.type === "boolean") {
      return `    aggregate:\n${notes}      ${name} is max(pick null when ${name} is null pick 1 when ${name} else 0) = 1\n`;
   }
   return `    aggregate:\n${notes}      ${name} is ${name}.max()\n`;
}

/**
 * A call that refines the view (`+ { … }`): the stored result holds only the
 * view as written, so the leg declines it.
 */
export function refinedCallDecline(call: RunViewCall): LegDecline | undefined {
   if (call.refined) {
      return new LegDecline(
         "view_shape_mismatch",
         `the query refines \`${call.view}\` (\`+ { … }\`), and the stored result holds only the view as written`,
      );
   }
   return undefined;
}

/**
 * A caller's text that names, outside the call's head, a name that may read
 * the stored rows where live reads the source, or undefined: fails closed.
 *
 * - A name the variant redeclares (`redeclared`: the run target's, and for a
 *   named query its own): the variant gives it to the stored result, so a
 *   join back to the source, or a preamble source built over it, reads the
 *   stored rows.
 * - A named query of the model (`contents`): a named query keeps its source
 *   as a bare name, resolved where the query is used, which in the variant
 *   is the stored result.
 * - A model name declared in a file other than the redeclared one
 *   (`declaringURL`): that file's own import of the declaring file resolves
 *   to the variant's overlay.
 *
 * A caller's own preamble names (a report's definition cells), the declaring
 * file's other sources (read from its copy), a dimension-only `extend:` and
 * a join to one of those sources read as live does.
 */
export function callerTextDecline(
   otherNames: ReadonlySet<string>,
   redeclared: readonly (string | undefined)[],
   name: string,
   model: {
      contents: Record<string, unknown> | undefined;
      declaringURL: string | undefined;
   },
   /** The leg that asks, which the decline's code is attributed to. */
   leg: "view" | "query" = "view",
): LegDecline | undefined {
   const code =
      leg === "query" ? "query_shape_mismatch" : "view_shape_mismatch";
   const named = redeclared.find((n) => n !== undefined && otherNames.has(n));
   if (named !== undefined) {
      return new LegDecline(
         code,
         `the query text names \`${named}\` beside the call of \`${name}\`, a name the stored result's variant gives to the stored rows`,
      );
   }
   for (const other of otherNames) {
      if (!model.contents || !Object.hasOwn(model.contents, other)) continue;
      const entry = model.contents[other] as
         | {
              type?: unknown;
              sourceID?: unknown;
              location?: { url?: unknown };
           }
         | undefined;
      if (entry?.type === "query") {
         return new LegDecline(
            code,
            `the query text names the named query \`${other}\` beside the call of \`${name}\`, which may read the stored rows where live reads its source`,
         );
      }
      const declaredIn =
         (typeof entry?.sourceID === "string"
            ? splitSourceID(entry.sourceID)?.url
            : undefined) ??
         (typeof entry?.location?.url === "string"
            ? entry.location.url
            : undefined);
      if (declaredIn === undefined || declaredIn !== model.declaringURL) {
         return new LegDecline(
            code,
            `the query text names \`${other}\`, declared in another file than the stored result's, beside the call of \`${name}\`, which may read the stored rows where live reads its source`,
         );
      }
   }
   return undefined;
}

/**
 * The run target must be the view's own source, by sourceID: not an
 * extension, and not a caller's redefinition of the name. `localName` is the
 * run target's name when the query names one; `sourceID` its compiled source's.
 */
export function callTargetDecline(
   call: RunViewCall,
   plan: Pick<PersistedViewPlan, "sourceID">,
   localName: string | undefined,
   sourceID: unknown,
): LegDecline | undefined {
   if (!localName || sourceID !== plan.sourceID) {
      return new LegDecline(
         "view_shape_mismatch",
         `the query does not call \`${call.view}\` on the source that declares it`,
      );
   }
   return undefined;
}

/**
 * Exact-call recognition: a query with no stage past the view's own is a
 * call of the persisted view only when its public result schema is named for
 * that view (`resultName`, the compiled `resultExplore.name`).
 */
export function exactCallDecline(
   call: RunViewCall,
   plan: Pick<PersistedViewPlan, "viewName">,
   resultName: string,
): LegDecline | undefined {
   if (resultName !== plan.viewName) {
      return new LegDecline(
         "view_shape_mismatch",
         `the query is not a call of \`${call.view}\``,
      );
   }
   return undefined;
}
