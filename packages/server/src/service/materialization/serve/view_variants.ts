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
import type { ModelConnectionInput } from "../../model_runtime";
import { extractRunViewCall, type RunViewCall } from "../../query_text";
import { quoteManifestTablePath } from "../../quoting";
import { evaluateManifestFreshness } from "../domain/freshness";
import type { BuildManifest } from "../domain/types";
import {
   generatePersistedViewFile,
   persistedViewCompanionHref,
   planModelPersistedViews,
   splitSourceID,
   type PersistedViewPlan,
} from "../plan/persisted_view";
import { viewNoteTexts } from "../plan/persisted_view_compile";
import { SourceMap } from "../text/source_map";
import { compileDeclaringFileVariant } from "./declaring_file_variant";
import type { ServeBinding } from "../domain/serve_binding";
import type { MaterializedTableCatalog } from "./query_path";
import { fieldNotes, noteSource } from "../text/field_notes";
import {
   TIMEFRAMES,
   timeframeKeyword,
   timeframeRestatements,
   typeDeclaration,
} from "./served_copy";
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";
import { LegDecline } from "../domain/leg_decline";

type VirtualMap = Map<string, Map<string, string>>;

/** What the leg reads from the model that owns it. */
interface ViewVariantHost {
   modelPath(): string;
   modelDef(): ModelDef | undefined;
   modelMaterializer(): ModelMaterializer | undefined;
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
   /** The package's planned tables, to find each view's address. */
   catalog(): MaterializedTableCatalog | undefined;
}

/** A persisted view's answer, for the router. */
interface ViewAnswer {
   runnable: QueryMaterializer;
   /** The view's table: its address, tier and as-of. */
   table: ViewTable;
   /** For a `storage=` view: how its `virtual()` handle resolves. */
   virtualMap?: VirtualMap;
   /** For a `storage=` view: its binding. */
   binding?: ServeBinding;
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

interface ResolvedTable extends ViewTable {
   virtualMap?: VirtualMap;
   quotedPath?: string;
   binding?: ServeBinding;
   /** A colocated table's name as the manifest substitutes it. */
   colocatedTable?: string;
}

interface CompiledVariant {
   materializer?: ModelMaterializer;
   modelDef?: ModelDef;
   runtime?: GraftRuntime;
   error?: string;
}

/** One model's persisted views. */
export class PersistedViewServe {
   private packagePath?: string;
   private packageConfig?: ModelConnectionInput;
   private bindings: ServeBinding[] = [];
   private generation = 0;
   private compiled = new Map<string, Promise<CompiledVariant>>();
   private planMemo?: { modelDef: unknown; plans: PersistedViewPlan[] };

   constructor(private readonly host: ViewVariantHost) {}

   setContext(packagePath: string, packageConfig: ModelConnectionInput): void {
      this.packagePath = packagePath;
      this.packageConfig = packageConfig;
      this.invalidate();
   }

   /** Keep the bindings that store a persisted view this model can see. */
   setBindings(bindings: readonly ServeBinding[]): void {
      const ids = this.buildSourceIDs();
      this.bindings = bindings.filter((b) =>
         (b.sourceIDs ?? []).some((id) => ids.has(id)),
      );
      this.invalidate();
   }

   invalidate(): void {
      this.generation++;
      this.compiled.clear();
   }

   /** Every persisted view a source this model can see declares. */
   plans(): PersistedViewPlan[] {
      const modelDef = this.host.modelDef();
      const memo = this.planMemo;
      if (memo && memo.modelDef === modelDef) return memo.plans;
      const plans = modelDef ? planModelPersistedViews(modelDef).plans : [];
      this.planMemo = { modelDef, plans };
      return plans;
   }

   hasViews(): boolean {
      return this.plans().length > 0;
   }

   /** The generated build sources' `sourceID`s, to keep their bindings off the other legs. */
   buildSourceIDs(): Set<string> {
      return new Set(this.plans().map((p) => p.buildSourceID));
   }

   /** The `storage=` view tables bound now; with `now`, only the fresh ones. */
   viewBindings(now?: number): ServeBinding[] {
      if (now === undefined) return this.bindings;
      const at = new Date(now);
      return this.bindings.filter((b) => isFresh(b, at));
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
   reportedTable(
      queryString: string,
      freshManifest: BuildManifest | undefined,
   ): ViewTable | undefined {
      const called = this.calledView(queryString);
      if (!called) return undefined;
      const { virtualMap, quotedPath, binding, colocatedTable, ...table } =
         this.tableOf(called.plan, freshManifest);
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
      /** The fresh build manifest. */
      buildManifest: BuildManifest | undefined;
   }): Promise<ViewAnswer> {
      const called = this.calledView(args.queryString);
      if (!called) {
         throw new LegDecline("view_missing_field");
      }
      const { call, plan } = called;
      if (call.refined) {
         throw new LegDecline(
            "view_missing_field",
            `the query refines \`${call.view}\` (\`+ { … }\`), and the stored result holds only the view as written`,
         );
      }
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
      if (!localName || struct?.sourceID !== plan.sourceID) {
         throw new LegDecline(
            "view_missing_field",
            `the query does not call \`${call.view}\` on the source that declares it`,
         );
      }
      // An exact call shows in the public result schema; a query that goes on
      // past the view shows only in its text (Evidence 4).
      if (call.laterStages === 0) {
         const result = await args.authorRunnable.getPreparedResult({
            ...(args.givens ? { givens: args.givens as never } : {}),
         } as never);
         if (result.resultExplore.name !== plan.viewName) {
            throw new LegDecline(
               "view_missing_field",
               `the query is not a call of \`${call.view}\``,
            );
         }
      }
      const table = this.tableOf(plan, args.buildManifest);
      if (!table.built) {
         throw new LegDecline("view_not_built");
      }
      if (table.stale || (!table.virtualMap && !table.colocatedTable)) {
         throw new LegDecline("view_stale");
      }
      const variant = await this.compiledVariant(plan, localName, table);
      if (!variant.materializer || !variant.modelDef) {
         logger.warn("A persisted-view variant did not compile", {
            modelPath: this.host.modelPath(),
            view: `${plan.sourceName} -> ${plan.viewName}`,
            error: variant.error,
         });
         throw new LegDecline(
            "view_missing_field",
            "the query does not compile against the stored view",
         );
      }
      const runnable = variant.materializer.loadRestrictedQuery(
         args.queryString,
      );
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
      // The compiled SQL must read the table, never recompute the view.
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
            cacheScope: `persisted_view\u0000${this.generation}\u0000${plan.buildSourceID}\u0000${localName}`,
            ...(variant.runtime ? { runtime: variant.runtime } : {}),
         },
      };
   }

   /** The view's table: a `storage=` binding, or a colocated manifest entry. */
   private tableOf(
      plan: PersistedViewPlan,
      freshManifest: BuildManifest | undefined,
   ): ResolvedTable {
      const planned = this.host.catalog()?.planned.get(plan.buildSourceID);
      const tableId = planned?.sourceEntityId;
      const withId = tableId !== undefined ? { tableId } : {};
      if (plan.storage !== undefined) {
         const binding = this.bindings.find((b) =>
            (b.sourceIDs ?? []).includes(plan.buildSourceID),
         );
         if (!binding) {
            return { ...withId, tier: "storage", built: false, stale: false };
         }
         const dataAsOf = binding.freshAsOf
            ? { dataAsOf: binding.freshAsOf }
            : {};
         if (
            !isFresh(binding, new Date()) ||
            !this.host.serveDestinationConfig()
         ) {
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
         tableId !== undefined
            ? this.host.catalog()?.bound[tableId]
            : undefined;
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
      const viewBody = viewDeclaration(plan.viewName, shape);
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
               const columnNotes = new Map(
                  shape.columns.map((c) => [
                     c.name,
                     c.timeframe || c.kind === "measure" ? [] : c.notes,
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
               // malloy-text: the served copy's timeframe emitter, the author's notes, names through identifier
               const timeframes = timeframeRestatements(shape.columns);
               // malloy-text: the type emitter, the author's gate and view notes (single lines), and names through identifier
               return `${typeDeclaration({ typeName, columns })}
source: ${identifier(stored)} is ${identifier(servedCopyAlias(table.binding.destinationName))}.virtual(${stringLiteral(table.binding.virtualHandle)})::${identifier(typeName)}
${gates}source: ${identifier(source)} is ${identifier(stored)} extend {${timeframes}
${viewBody}
}`;
            }
            // The companion over the copy, so its build SQL (and so its
            // address) is the one the build plan computed.
            const siblings = planModelPersistedViews(modelDef).plans.filter(
               (p) => p.declaringURL === declared.url,
            );
            const text = generatePersistedViewFile(siblings, copyName);
            if (!text) return { error: "no companion" };
            const companion = persistedViewCompanionHref(declaringURL.href);
            addFile(new URL(companion), text);
            const built = `${plan.buildSourceName}__built`;
            return `import { ${identifier(built)} is ${identifier(plan.buildSourceName)} } from ${doubleQuotedString(path.basename(fileURLToPath(companion)))}
${gates}source: ${identifier(source)} is ${identifier(built)} extend {
${viewBody}
}`;
         },
      });
   }
}

/** The flags every generated variant file declares. */
const VARIANT_FLAGS =
   "##! experimental { access_modifiers givens persistence virtual_source }";

/** Whether a binding is inside its freshness window at `now`. */
function isFresh(b: ServeBinding, now: Date): boolean {
   return (
      evaluateManifestFreshness(
         {
            tableName: b.tablePath,
            dataAsOf: b.freshAsOf,
            freshnessWindowSeconds: b.freshnessWindowSeconds,
            freshnessFallback: b.freshnessFallback,
         },
         now,
      ) === "serve_table"
   );
}

/** What the variant reproduces of the view's result. */
interface ViewShape {
   /**
    * Output columns, in order, with a timeframe where the field has one and
    * the annotation lines live's result column carries.
    */
   columns: ViewColumn[];
   orderBy: { name: string; desc: boolean }[];
   notes: string[];
}

/** One output column of the view (or of a nest in it), as live reports it. */
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
   /** A nest's own columns, ordering and limit. */
   nest?: NestShape;
}

interface NestShape {
   columns: ViewColumn[];
   orderBy: { name: string; desc: boolean }[];
   limit?: number;
}

/** A compiled query stage, as far as the shape reads it. */
interface StageIR {
   outputStruct?: { fields?: unknown[] };
   orderBy?: { field?: unknown; dir?: unknown }[];
   limit?: unknown;
   queryFields?: unknown[];
}

/** What live's result explore says of a field: its kind, and a nest's fields. */
interface ResultKinds {
   measures: Set<string>;
   nests: Map<string, ResultKinds>;
}

/** The kinds live's result reports, read from Malloy's public result explore. */
function resultKinds(explore: { allFields: readonly unknown[] }): ResultKinds {
   const out: ResultKinds = { measures: new Set(), nests: new Map() };
   for (const raw of explore.allFields) {
      const f = raw as {
         name: string;
         isAtomicField(): boolean;
         isExploreField(): boolean;
         sourceWasMeasure?(): boolean;
         allFields?: readonly unknown[];
      };
      if (f.isAtomicField() && f.sourceWasMeasure?.()) {
         out.measures.add(f.name);
      } else if (f.isExploreField() && f.allFields) {
         out.nests.set(f.name, resultKinds({ allFields: f.allFields }));
      }
   }
   return out;
}

/** A stage's output columns, ordering and limit, a nest's included. */
function stageShape(stage: StageIR | undefined, kinds: ResultKinds): NestShape {
   const turtles = new Map<string, StageIR[]>();
   for (const q of stage?.queryFields ?? []) {
      const t = q as {
         type?: unknown;
         name?: unknown;
         as?: unknown;
         pipeline?: unknown;
      };
      const name = typeof t.as === "string" ? t.as : t.name;
      if (
         t.type === "turtle" &&
         typeof name === "string" &&
         Array.isArray(t.pipeline)
      ) {
         turtles.set(name, t.pipeline as StageIR[]);
      }
   }
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
         const nestKinds = kinds.nests.get(name);
         const pipeline = turtles.get(name);
         const nest =
            nestKinds && pipeline
               ? stageShape(pipeline[pipeline.length - 1], nestKinds)
               : undefined;
         return [
            {
               name,
               type: typeof f.type === "string" ? f.type : "",
               ...(typeof f.timeframe === "string" &&
               TIMEFRAMES.has(f.timeframe)
                  ? { timeframe: f.timeframe }
                  : {}),
               notes: fieldNotes(raw),
               kind: nest
                  ? "nest"
                  : kinds.measures.has(name)
                    ? "measure"
                    : "dimension",
               ...(nest ? { nest } : {}),
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
   return {
      columns,
      orderBy,
      ...(typeof stage?.limit === "number" ? { limit: stage.limit } : {}),
   };
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
   const { columns, orderBy } = stageShape(
      stages[stages.length - 1],
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
   return { columns, orderBy, notes: viewNoteTexts(view?.annotations) };
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
 * The view, redeclared over the stored result: its notes, columns and
 * ordering.
 *
 * A view whose result holds only dimensions selects its stored columns. One
 * with a measure or a nest groups its stored rows again, by its dimensions,
 * which the stored result holds once per row, so each group is one stored row
 * and each measure re-aggregates one value (`total is total.max()`): the rows
 * are the stored rows, and each measure is a measure again, its `#(malloy)`
 * annotation carrying `calculation` as live's does. A nest is rebuilt the same
 * way from its stored rows, with its ordering and limit, so it is drillable
 * and its own fields keep their kinds. A boolean measure re-aggregates through
 * the number it picks. A measure of any other type `max()` does not take (a
 * JSON value, an array) cannot be re-aggregated; the variant is refused, and
 * the query answers live.
 */
function viewDeclaration(viewName: string, shape: ViewShape): string {
   // malloy-text: the author's own view notes (single lines, no `#@`), names through identifier
   const notes = shape.notes.map((n) => `  ${n}\n`).join("");
   if (!regroups(shape.columns)) {
      const select = shape.columns.map((c) => identifier(c.name)).join(", ");
      return `${notes}  view: ${identifier(viewName)} is { select: ${select}${orderByClause(shape.orderBy, "; ")} }`;
   }
   // malloy-text: groupedBody's statements, every value through identifier or noteSource (recursive, so the ratchet cannot follow it)
   return `${notes}  view: ${identifier(viewName)} is {\n${groupedBody(shape, [], "    ")}  }`;
}

/** `order_by: …`, after `separator`; empty when there is none. */
function orderByClause(
   orderBy: readonly { name: string; desc: boolean }[],
   // malloy-text: a literal separator ("; " or an indent), never a value
   separator: string,
): string {
   return orderBy.length > 0
      ? `${separator}order_by: ${orderBy
           .map((o) => `${identifier(o.name)} ${o.desc ? "desc" : "asc"}`)
           .join(", ")}`
      : "";
}

/**
 * A grouping stage's statements over the stored result, one per column in
 * order. `path` is the stored nest the columns are read through (empty at the
 * top, where each column is the source's own field).
 */
function groupedBody(
   shape: NestShape,
   path: readonly string[],
   // malloy-text: whitespace this file builds, never a value
   indent: string,
): string {
   const lines = shape.columns.map((c) => {
      const stored = [...path, c.name].map(identifier).join(".");
      // malloy-text: the author's own notes through noteSource, names through identifier
      const notes = c.notes.map((n) => noteSource(n, `${indent}  `)).join("");
      const name = identifier(c.name);
      switch (c.kind) {
         case "measure":
            // A boolean through the number it picks, so a NULL stays NULL.
            if (c.type === "boolean") {
               return `${indent}aggregate:\n${notes}${indent}  ${name} is max(pick 1 when ${stored} pick 0 when not ${stored} else null) = 1\n`;
            }
            if (!MAX_TYPES.has(c.type)) {
               throw new Error(
                  `the measure \`${c.name}\` is a ${c.type || "value"} that max() cannot re-aggregate`,
               );
            }
            return `${indent}aggregate:\n${notes}${indent}  ${name} is ${stored}.max()\n`;
         case "nest":
            // malloy-text: this function's own output for the nest, every value through identifier or noteSource (recursion the ratchet cannot follow)
            return `${indent}nest:\n${notes}${indent}  ${name} is {\n${groupedBody(c.nest!, [...path, c.name], `${indent}    `)}${indent}  }\n`;
         case "dimension": {
            // At the top a dimension is the source's own field, timeframe and
            // notes included; in a nest it is read from the stored record.
            if (path.length === 0) return `${indent}group_by: ${name}\n`;
            // malloy-text: the stored path through identifier, truncated by a timeframe keyword from TIMEFRAMES
            const read = c.timeframe
               ? `${stored}.${timeframeKeyword(c.timeframe)}`
               : stored;
            return `${indent}group_by:\n${notes}${indent}  ${name} is ${read}\n`;
         }
      }
   });
   const order = orderByClause(shape.orderBy, indent);
   return `${lines.join("")}${order ? `${order}\n` : ""}${shape.limit !== undefined ? `${indent}limit: ${shape.limit}\n` : ""}`;
}
