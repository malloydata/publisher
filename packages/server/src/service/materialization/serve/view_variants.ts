// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Persisted-view variants (materialization v2, phase 4, P14): the router's
 * view leg for `#@ persist` on a view.
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
 * `storage=` view through `virtual()` on the destination, a colocated one by
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
   generatePersistedViewFile,
   persistedViewCompanionHref,
   viewNoteTexts,
} from "../text/read_text";
import { splitSourceID, type PersistedViewPlan } from "../domain/table_plans";
import { SourceMap } from "../text/source_map";
import { compileDeclaringFileVariant } from "./declaring_file_variant";
import type { Binding, Pinned } from "../domain/binding";
import type { MaterializedTableCatalog } from "./query_path";
import { fieldNotes, noteSource } from "../text/field_notes";
import {
   TIMEFRAMES,
   timeframeRestatements,
   typeDeclaration,
} from "./served_copy";
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";
import { LegDecline } from "../domain/leg_decline";
import {
   bigIntegerReadOf,
   serveTypePlan,
   storedColumnTypeMismatches,
} from "../stored_column_types";

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
   /** Where `govern()` grafts the source's gates on the variant (R3). */
   graftScope: GraftScope;
}

/** A persisted view's table, as the path report names it. */
export interface ViewTable {
   /** The table's `sourceEntityId`, when the plan knows it. */
   tableId?: string;
   tier: "storage" | "colocated";
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
   /** A colocated table's name as the manifest substitutes it. */
   colocatedTable?: string;
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
      const { virtualMap, quotedPath, binding, colocatedTable, ...table } =
         this.tableOf(called.plan, pinned, pinned.compilerManifest);
      void virtualMap;
      void quotedPath;
      void binding;
      void colocatedTable;
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
         throw new LegDecline("view_missing_field");
      }
      const { call, plan } = called;
      const refined =
         refinedCallDecline(call) ??
         callerTextDecline(
            call.otherNames,
            [call.source, plan.sourceName],
            call.view,
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
      if (table.stale || (!table.virtualMap && !table.colocatedTable)) {
         throw new LegDecline("view_stale");
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
      });
   }

   /** A persisted result's table: a `storage=` binding, or a colocated manifest entry. */
   tableOf(
      plan: Pick<PersistedViewPlan, "buildSourceID" | "storage">,
      pinned: Pinned,
      freshManifest: BuildManifest | undefined,
   ): ResolvedTable {
      const planned = this.host.catalog()?.planned.get(plan.buildSourceID);
      const tableId = planned?.sourceEntityId;
      const withId = tableId !== undefined ? { tableId } : {};
      if (plan.storage !== undefined) {
         const lookup = pinned.tableFor(plan.buildSourceID, "view");
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
         const quotedPath = quoteManifestTablePath(binding.tablePath, "duckdb");
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
         return { ...withId, tier: "colocated", built: false, stale: false };
      }
      const dataAsOf = bound.dataAsOf ? { dataAsOf: bound.dataAsOf } : {};
      const fresh = freshManifest?.entries[tableId]?.tableName;
      return fresh
         ? {
              ...withId,
              ...dataAsOf,
              tier: "colocated",
              built: true,
              stale: false,
              colocatedTable: fresh,
           }
         : {
              ...withId,
              ...dataAsOf,
              tier: "colocated",
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
            : "colocated",
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
      const modelPath = this.host.modelPath();
      const modelFile = path.join(packagePath, modelPath);
      const destinations = this.host.serveDestinationConfig();
      const config =
         table.binding && destinations
            ? servedCopyConfig(
                 packageConfig,
                 destinations(),
                 new Set([table.binding.destinationName]),
              )
            : packageConfig;
      const view = viewDeclaration(plan.viewName, shape);
      if ("refusal" in view) return { refusal: view.refusal };
      // A stored column whose type or value does not serve as live's is read
      // through the served copy's faithful cast (a date truncation a build
      // stored as a TIMESTAMP, a big integer as HUGEINT), or refuses the
      // variant.
      const read = table.binding
         ? storedColumnCasts(table.binding.schema, shape)
         : { casts: new Map<string, ColumnCast>() };
      if ("refusal" in read) return { refusal: read.refusal };
      const { casts } = read;
      // malloy-text: viewDeclaration's text, every value through identifier or noteSource
      const viewBody = view.text;
      // The source's own time zone, on the variant's source: a `storage=`
      // read truncates each stored truncation again, and a later stage
      // truncates a stored timestamp, each in the zone live computes it in,
      // never UTC.
      const sourceTimezone = shape.sourceTimezone
         ? `\n  timezone: ${stringLiteral(shape.sourceTimezone)}`
         : "";
      return compileDeclaringFileVariant({
         packagePath,
         modelPath,
         packageConfig,
         config,
         modelDef,
         sourceID: plan.sourceID,
         copySuffix: ".view-author.malloy",
         queryModelURL: pathToFileURL(`${modelFile}.view.malloy`),
         flags: VARIANT_FLAGS,
         declare: ({
            gates: authorGates,
            sourceName: source,
            copyName,
            declaringURL,
            addFile,
         }) => {
            // malloy-text: the source's own gate expressions, as the author wrote them, one per line (see gateLines)
            const gates = authorGates;
            if (table.binding) {
               const typeName = `${plan.buildSourceName}__type`;
               const stored = `${plan.buildSourceName}__stored`;
               // A stored column carries the notes live's output column does
               // (its field's, and the view's own on it): a `type:` field
               // keeps its annotations, so the result column reads them as
               // live's does. A timeframe column's go on its redefined field
               // (timeframeRestatements), and a measure's on the view's
               // re-aggregation of it (viewDeclaration), instead.
               // A cast column's go on its redefined field too
               // (storedColumnCasts).
               const columnNotes = new Map(
                  shape.columns.map((c) => [
                     c.name,
                     c.timeframe || casts.has(c.name) || c.kind === "measure"
                        ? []
                        : c.notes,
                  ]),
               );
               const columns = table.binding.schema.map((c) => {
                  const notes = columnNotes.get(c.name);
                  return {
                     name: c.name,
                     type: c.type,
                     ...(notes && notes.length > 0 ? { notes } : {}),
                  };
               });
               // malloy-text: the served copy's timeframe and cast emitter, the author's notes, names through identifier
               const timeframes = timeframeRestatements(
                  shape.columns.map((c) => {
                     const cast = casts.get(c.name);
                     if (!cast) return c;
                     // A measure's notes go on its re-aggregation.
                     const notes = c.kind === "measure" ? [] : c.notes;
                     return { ...c, notes, ...cast };
                  }),
                  table.binding.schema.map((c) => c.name),
               );
               // malloy-text: the type emitter, the author's gate and view notes (single lines), and names through identifier
               return `${typeDeclaration({ typeName, columns })}
source: ${identifier(stored)} is ${identifier(servedCopyAlias(table.binding.destinationName))}.virtual(${stringLiteral(table.binding.virtualHandle)})::${identifier(typeName)}
${gates}source: ${identifier(source)} is ${identifier(stored)} extend {${sourceTimezone}${timeframes}
${viewBody}
}`;
            }
            // The companion over the copy, so its build SQL (and so its
            // address) is the one the build plan computed.
            const siblings = this.host
               .planModelPersistedViews(modelDef)
               .plans.filter((p) => p.declaringURL === declared.url);
            const text = generatePersistedViewFile(siblings, copyName);
            if (!text) return { error: "no companion" };
            const companion = persistedViewCompanionHref(declaringURL.href);
            addFile(new URL(companion), text);
            const built = `${plan.buildSourceName}__built`;
            return `import { ${identifier(built)} is ${identifier(plan.buildSourceName)} } from ${doubleQuotedString(path.basename(fileURLToPath(companion)))}
${gates}source: ${identifier(source)} is ${identifier(built)} extend {${sourceTimezone}
${viewBody}
}`;
         },
      });
   }
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
}): Promise<ViewAnswer> {
   const { variant, table } = args;
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
         ...(table.virtualMap ? {} : { buildManifest: args.buildManifest }),
      } as never);
      sql = result.sql;
   } catch {
      throw new LegDecline(
         "view_missing_field",
         "the query does not compile against the stored view",
      );
   }
   // The compiled SQL must read the table, never recompute the result.
   const read = table.virtualMap ? table.quotedPath : table.colocatedTable;
   if (!read || !sql.includes(read)) {
      throw new LegDecline("view_stale");
   }
   const { virtualMap, quotedPath, binding, colocatedTable, ...reported } =
      table;
   void quotedPath;
   void colocatedTable;
   return {
      runnable,
      table: reported,
      ...(virtualMap ? { virtualMap } : {}),
      ...(binding ? { binding } : {}),
      graftScope: {
         modelDef: variant.modelDef,
         materializer: variant.materializer,
         cacheScope: args.cacheScope,
         ...(variant.runtime ? { runtime: variant.runtime } : {}),
      },
   };
}

/** The flags every generated variant file declares. */
export const VARIANT_FLAGS =
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

/** How a stored column is read: through a faithful cast, or a big integer's native type. */
export type ColumnCast =
   | { castTo: "DATE" | "BOOLEAN" }
   | { nativeCast: string };

/**
 * The stored columns of a `storage=` view's table read through a cast, by
 * name, or why the stored table cannot give live's values:
 *
 * - `DATE` or `BOOLEAN`, the served copy's faithful casts
 *   ({@link serveTypePlan}). A build stores each column as its declared type
 *   where it can, but one whose cast it declined (a view or source with a
 *   query time zone) or a build whose casts failed and stored what the
 *   warehouse returned holds a date truncation as a TIMESTAMP, which the
 *   `type:` would declare a timestamp where live types it a date.
 * - `HUGEINT` for a column live types as a `bigint` number (a BIGINT, a max
 *   over one), the served copy's big-integer read ({@link bigIntegerReadOf}):
 *   a `type:` declares it only as a `number`, which Malloy reads through a
 *   JavaScript number and rounds above 2^53.
 *
 * The refusal, so the query answers live, for a column no faithful cast reads
 * as live's type, a big integer stored as a float (its digits already lost),
 * and a column live answers as a warehouse-native value (`sql native`: a
 * TIMESTAMP_NS, whose digits past the millisecond a `timestamp` drops) or
 * JSON, which no `type:` reads back as live does.
 */
export function storedColumnCasts(
   schema: readonly { name: string; type: string }[],
   shape: Pick<ViewShape, "columns" | "queryTimezone" | "dialect">,
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
      .map((c) => ({ name: c.name, type: c.type }));
   const mismatches = storedColumnTypeMismatches(schema, declared);
   const plan = serveTypePlan(mismatches, {
      queryTimezone: shape.queryTimezone,
   });
   const [unservable] = plan.unservable;
   if (unservable) {
      return {
         refusal: `the stored \`${unservable.column}\` (${unservable.storedType}) cannot be read as live's ${unservable.declaredType}`,
      };
   }
   const casts = new Map<string, ColumnCast>(
      plan.casts.map((c) => [
         c.column,
         { castTo: c.castTo === "BOOLEAN" ? "BOOLEAN" : "DATE" },
      ]),
   );
   for (const c of shape.columns) {
      const stored = schema.find((s) => s.name === c.name);
      if (!stored || casts.has(c.name)) continue;
      const big = bigIntegerReadOf(c.field, stored, { dialect: shape.dialect });
      if (!big) continue;
      if (!big.castTo) {
         return {
            refusal: `the stored \`${c.name}\` (${stored.type}) cannot hold live's integer exactly`,
         };
      }
      casts.set(c.name, { nativeCast: big.castTo });
   }
   return { casts };
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
         "view_missing_field",
         `the query refines \`${call.view}\` (\`+ { … }\`), and the stored result holds only the view as written`,
      );
   }
   return undefined;
}

/**
 * A caller's text that names, outside the call's head, a name the variant
 * redeclares (`redeclared`: the run target's, and for a named query its own).
 * The variant gives that name to the stored result, so a join back to the
 * source, or a preamble source built over it, would read the stored rows
 * where live reads the source: fails closed. Any other preamble (a report's
 * definition cells), a dimension-only `extend:` and a join to another source
 * read as live does.
 */
export function callerTextDecline(
   otherNames: ReadonlySet<string>,
   redeclared: readonly (string | undefined)[],
   name: string,
): LegDecline | undefined {
   const named = redeclared.find((n) => n !== undefined && otherNames.has(n));
   if (named === undefined) return undefined;
   return new LegDecline(
      "view_missing_field",
      `the query text names \`${named}\` beside the call of \`${name}\`, a name the stored result's variant gives to the stored rows`,
   );
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
         "view_missing_field",
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
         "view_missing_field",
         `the query is not a call of \`${call.view}\``,
      );
   }
   return undefined;
}
