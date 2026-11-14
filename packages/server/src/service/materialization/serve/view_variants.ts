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
import { evaluateManifestFreshness } from "../run/freshness";
import type { BuildManifest } from "../types";
import {
   generatePersistedViewFile,
   persistedViewCompanionHref,
   planModelPersistedViews,
   splitSourceID,
   type PersistedViewPlan,
} from "../plan/persisted_view";
import { viewNoteTexts } from "../plan/persisted_view_compile";
import { SourceMap } from "../plan/source_map";
import { compileDeclaringFileVariant } from "./declaring_file_variant";
import type { ServeBinding } from "./materialization_serve_transform";
import type { MaterializedTableCatalog } from "./query_path";
import { fieldNotes } from "../field_notes";
import {
   TIMEFRAMES,
   timeframeRestatements,
   typeDeclaration,
} from "./served_copy";
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";
import { LegDecline } from "./leg_decline";

type VirtualMap = Map<string, Map<string, string>>;

/** What the leg reads from the model that owns it. */
export interface ViewVariantHost {
   modelPath(): string;
   modelDef(): ModelDef | undefined;
   modelMaterializer(): ModelMaterializer | undefined;
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
   /** The package's planned tables, to find each view's address. */
   catalog(): MaterializedTableCatalog | undefined;
}

/** A persisted view's answer, for the router. */
export interface ViewAnswer {
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
               // (timeframeRestatements) instead.
               const columnNotes = new Map(
                  shape.columns.map((c) => [
                     c.name,
                     c.timeframe ? [] : c.notes,
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
   columns: { name: string; timeframe?: string; notes: string[] }[];
   orderBy: { name: string; desc: boolean }[];
   notes: string[];
}

/** The view's output columns, ordering and notes, from a compile of the bare call. */
async function viewShape(
   author: ModelMaterializer,
   modelDef: ModelDef,
   plan: PersistedViewPlan,
   localName: string,
): Promise<ViewShape> {
   const prepared = await readPreparedQuery(
      author.loadQuery(
         `run: ${identifier(localName)} -> ${identifier(plan.viewName)}`,
      ),
   );
   const stages = (
      Array.isArray(prepared.pipeline) ? prepared.pipeline : []
   ) as {
      outputStruct?: { fields?: unknown[] };
      orderBy?: { field?: unknown; dir?: unknown }[];
   }[];
   const last = stages[stages.length - 1];
   const columns = (last?.outputStruct?.fields ?? []).flatMap((raw) => {
      const f = raw as { name?: unknown; as?: unknown; timeframe?: unknown };
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
            ...(typeof f.timeframe === "string" && TIMEFRAMES.has(f.timeframe)
               ? { timeframe: f.timeframe }
               : {}),
            notes: fieldNotes(raw),
         },
      ];
   });
   const orderBy = (last?.orderBy ?? []).flatMap((o) => {
      const name =
         typeof o.field === "number"
            ? columns[o.field - 1]?.name
            : typeof o.field === "string"
              ? o.field
              : undefined;
      return name === undefined ? [] : [{ name, desc: o.dir === "desc" }];
   });
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

/** The view, redeclared over the stored result: its notes, columns and ordering. */
function viewDeclaration(viewName: string, shape: ViewShape): string {
   const select = shape.columns.map((c) => identifier(c.name)).join(", ");
   const order =
      shape.orderBy.length > 0
         ? `; order_by: ${shape.orderBy
              .map((o) => `${identifier(o.name)} ${o.desc ? "desc" : "asc"}`)
              .join(", ")}`
         : "";
   // malloy-text: the author's own view notes (single lines, no `#@`), names through identifier
   const notes = shape.notes.map((n) => `  ${n}\n`).join("");
   return `${notes}  view: ${identifier(viewName)} is { select: ${select}${order} }`;
}
