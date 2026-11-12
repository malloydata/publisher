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
import { collectEntryPointGates } from "../../gate_classification";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import {
   doubleQuotedString,
   identifier,
   stringLiteral,
} from "../../malloy_text";
import {
   createModelRuntime,
   type ModelConnectionInput,
} from "../../model_runtime";
import { overlayKey } from "../../overlay_key";
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
import {
   blankedPersistFiles,
   viewNoteTexts,
} from "../plan/persisted_view_compile";
import { SourceMap } from "../plan/source_map";
import type { ServeBinding } from "./materialization_serve_transform";
import type { MaterializedTableCatalog } from "./query_path";
import { typeDeclaration } from "./served_copy";
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";

type VirtualMap = Map<string, Map<string, string>>;

/** Why a persisted view did not answer, as the path report maps it. */
export type ViewSkipCode =
   | "view_missing_field"
   | "view_not_built"
   | "view_stale";

/** A persisted view that did not answer one query: expected, logged quietly. */
export class ViewSkip extends Error {
   constructor(
      readonly code: ViewSkipCode,
      /** In the caller's own terms, safe to show: never a table name. */
      readonly callerDetail?: string,
   ) {
      super(callerDetail ?? code);
   }
}

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
    * Answer `queryString` from the view it calls, or throw a {@link ViewSkip}.
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
         throw new ViewSkip("view_missing_field");
      }
      const { call, plan } = called;
      if (call.refined) {
         throw new ViewSkip(
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
         throw new ViewSkip(
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
            throw new ViewSkip(
               "view_missing_field",
               `the query is not a call of \`${call.view}\``,
            );
         }
      }
      const table = this.tableOf(plan, args.buildManifest);
      if (!table.built) {
         throw new ViewSkip("view_not_built");
      }
      if (table.stale || (!table.virtualMap && !table.colocatedTable)) {
         throw new ViewSkip("view_stale");
      }
      const variant = await this.compiledVariant(plan, localName, table);
      if (!variant.materializer || !variant.modelDef) {
         logger.warn("A persisted-view variant did not compile", {
            modelPath: this.host.modelPath(),
            view: `${plan.sourceName} -> ${plan.viewName}`,
            error: variant.error,
         });
         throw new ViewSkip(
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
         throw new ViewSkip(
            "view_missing_field",
            "the query does not compile against the stored view",
         );
      }
      // The compiled SQL must read the table, never recompute the view.
      const read = table.virtualMap ? table.quotedPath : table.colocatedTable;
      if (!read || !sql.includes(read)) {
         throw new ViewSkip("view_stale");
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
      let declaringFile: string;
      try {
         declaringFile = fileURLToPath(declared.url);
      } catch {
         return { error: "not a local file" };
      }
      const shape = await viewShape(author, modelDef, plan, localName);
      const modelPath = this.host.modelPath();
      const modelFile = path.join(packagePath, modelPath);
      const queryModelURL = pathToFileURL(`${modelFile}.view.malloy`);
      const declaringURL = pathToFileURL(declaringFile);
      const copyURL = pathToFileURL(`${declaringFile}.view-author.malloy`);
      const copyName = path.basename(fileURLToPath(copyURL));
      const { runtime: reader } = await createModelRuntime(
         packagePath,
         modelPath,
         packageConfig,
      );
      const readText = async (url: string) => {
         try {
            const read = await reader.urlReader.readURL(new URL(url));
            return typeof read === "string" ? read : read.contents;
         } catch {
            return undefined;
         }
      };
      // The author's files with source-level `#@` lines blanked (a view never
      // chains onto its source's table), the declaring file's under the copy.
      const blanked = await blankedPersistFiles(modelDef, readText);
      const overlay = new Map<string, string>();
      for (const [url, text] of blanked) {
         if (url !== overlayKey(declaringURL)) overlay.set(url, text);
      }
      const declaringText =
         blanked.get(overlayKey(declaringURL)) ??
         (await readText(declaringURL.href));
      if (declaringText === undefined) return { error: "unreadable source" };
      overlay.set(overlayKey(copyURL), declaringText);
      // The copy's own names, to import into the declaring file's overlay.
      const copyNames = await namesDeclaredIn(
         packagePath,
         modelPath,
         packageConfig,
         overlay,
         copyURL,
      );
      // malloy-text: the source's own gate expressions, as the author wrote them, one per line (see gateLines)
      const gates = gateLines(modelDef, plan.sourceID);
      const viewBody = viewDeclaration(plan.viewName, shape);
      const source = declared.name;
      let declaration: string;
      if (table.binding) {
         const typeName = `${plan.buildSourceName}__type`;
         const stored = `${plan.buildSourceName}__stored`;
         const columns = table.binding.schema.map((c) => ({
            name: c.name,
            type: c.type,
         }));
         // malloy-text: the type emitter, the author's gate and view notes (single lines), and names through identifier
         declaration = `${typeDeclaration({ typeName, columns })}
source: ${identifier(stored)} is ${identifier(servedCopyAlias(table.binding.destinationName))}.virtual(${stringLiteral(table.binding.virtualHandle)})::${identifier(typeName)}
${gates}source: ${identifier(source)} is ${identifier(stored)} extend {${timeframeStatements(shape)}
${viewBody}
}`;
      } else {
         // The companion over the copy, so its build SQL (and so its address)
         // is the one the build plan computed.
         const siblings = planModelPersistedViews(modelDef).plans.filter(
            (p) => p.declaringURL === declared.url,
         );
         const text = generatePersistedViewFile(siblings, copyName);
         if (!text) return { error: "no companion" };
         const companion = persistedViewCompanionHref(declaringURL.href);
         overlay.set(overlayKey(companion), text);
         const built = `${plan.buildSourceName}__built`;
         declaration = `import { ${identifier(built)} is ${identifier(plan.buildSourceName)} } from ${doubleQuotedString(path.basename(fileURLToPath(companion)))}
${gates}source: ${identifier(source)} is ${identifier(built)} extend {
${viewBody}
}`;
      }
      const others = copyNames.filter((n) => n !== source);
      const importOthers =
         others.length > 0
            ? `import { ${others.map((n) => identifier(n)).join(", ")} } from ${doubleQuotedString(copyName)}\n`
            : "";
      overlay.set(
         overlayKey(declaringURL),
         `${VARIANT_FLAGS}
${importOthers}
${declaration}
`,
      );
      overlay.set(
         overlayKey(queryModelURL.href),
         queryModelText(
            modelDef,
            queryModelURL,
            pathToFileURL(modelFile).href,
            {
               from: overlayKey(declaringURL),
               to: copyURL.href,
               keep: source,
            },
         ),
      );
      const destinations = this.host.serveDestinationConfig();
      const config =
         table.binding && destinations
            ? servedCopyConfig(
                 packageConfig,
                 destinations(),
                 new Set([table.binding.destinationName]),
              )
            : packageConfig;
      const { runtime, importBaseURL } = await createModelRuntime(
         packagePath,
         modelPath,
         config,
         { overlay },
      );
      const materializer = runtime.loadModel(queryModelURL, { importBaseURL });
      const model = await materializer.getModel();
      return {
         materializer,
         modelDef: (model as unknown as { _modelDef: ModelDef })._modelDef,
         runtime: runtime as unknown as GraftRuntime,
      };
   }
}

/** The flags every generated variant file declares. */
const VARIANT_FLAGS =
   "##! experimental { access_modifiers givens persistence virtual_source }";

/** The names a file declares (its sources, queries and givens), compiled on its own. */
async function namesDeclaredIn(
   packagePath: string,
   modelPath: string,
   config: ModelConnectionInput,
   overlay: ReadonlyMap<string, string>,
   url: URL,
): Promise<string[]> {
   const { runtime, importBaseURL } = await createModelRuntime(
      packagePath,
      modelPath,
      config,
      { overlay },
   );
   const model = await runtime.loadModel(url, { importBaseURL }).getModel();
   const def = (model as unknown as { _modelDef: ModelDef })._modelDef;
   const names = new Set<string>();
   for (const [name, entry] of Object.entries(def.contents ?? {})) {
      const located = entry as { location?: { url?: unknown } };
      // Declared in the file itself, not imported into it.
      if (located.location?.url === url.href) names.add(name);
   }
   for (const given of Object.values(def.givens ?? {})) {
      const g = given as { name?: unknown; location?: { url?: unknown } };
      if (typeof g.name === "string" && g.location?.url === url.href) {
         names.add(g.name);
      }
   }
   return [...names].sort();
}

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
   /** Output columns, in order, with a timeframe where the field has one. */
   columns: { name: string; timeframe?: string }[];
   orderBy: { name: string; desc: boolean }[];
   notes: string[];
}

const TIMEFRAMES = new Set([
   "second",
   "minute",
   "hour",
   "day",
   "week",
   "month",
   "quarter",
   "year",
]);

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

function timeframeText(timeframe: string): string {
   switch (timeframe) {
      case "second":
         return "second";
      case "minute":
         return "minute";
      case "hour":
         return "hour";
      case "day":
         return "day";
      case "week":
         return "week";
      case "month":
         return "month";
      case "quarter":
         return "quarter";
      default:
         return "year";
   }
}

/**
 * A stored timeframe column read back through `virtual()` has no timeframe;
 * the view's field does. Declared under an internal name and redefined as the
 * column truncated again (the identity on a truncated value).
 */
function timeframeStatements(shape: ViewShape): string {
   const truncated = shape.columns.filter((c) => c.timeframe);
   if (truncated.length === 0) return "";
   const renames = truncated
      .map((c) => `    ${identifier(`${c.name}__tf`)} is ${identifier(c.name)}`)
      .join("\n");
   const dims = truncated
      .map(
         (c) =>
            `    ${identifier(c.name)} is ${identifier(`${c.name}__tf`)}.${timeframeText(c.timeframe ?? "")}`,
      )
      .join("\n");
   return `\n  internal rename:\n${renames}\n  dimension:\n${dims}`;
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

/** The routes a row-level gate is written on. */
const GATE_ROUTES = new Set(["authorize", "access_filter"]);

/**
 * The view's source's gates, as note lines for the variant's source, so
 * `govern()` finds them there (R3). Throws when one cannot be written as a
 * single line: the variant is then not compiled and the view not served.
 */
function gateLines(modelDef: ModelDef, sourceID: string): string {
   const def = SourceMap.of(modelDef).node(sourceID)?.def;
   if (!def) return "";
   const lines: string[] = [];
   for (const entry of collectEntryPointGates(def as never, modelDef)) {
      if (!GATE_ROUTES.has(entry.route)) {
         throw new Error(`unknown gate route ${entry.route}`);
      }
      for (const expr of entry.exprs) {
         if (expr.includes("\n")) throw new Error("a multi-line gate");
         lines.push(
            entry.route === "authorize"
               ? `#(authorize) ${expr}`
               : `#(access_filter) ${expr}`,
         );
      }
   }
   return lines.map((l) => `${l}\n`).join("");
}

/** `url` relative to the query model's directory, for an `import`. */
function relativeImport(from: URL, url: string): string | undefined {
   try {
      return path
         .relative(path.dirname(fileURLToPath(from)), fileURLToPath(url))
         .split(path.sep)
         .join("/");
   } catch {
      return undefined;
   }
}

/**
 * The query model: the author's model's names, each imported from the file
 * that declares it (the view's source from its declaring file's overlay).
 */
function queryModelText(
   modelDef: unknown,
   queryModelURL: URL,
   modelURL: string,
   /**
    * Names declared in `from` (the view's declaring file) other than `keep`
    * are imported from `to` (its copy): a name a file imports is not exported
    * again, so only the view's source comes from the declaring file's overlay.
    */
   redirect: { from: string; to: string; keep: string },
): string {
   const model = modelDef as {
      contents?: Record<string, unknown>;
      exports?: string[];
      givens?: Record<string, { name?: unknown; location?: { url?: unknown } }>;
   };
   const exported = new Set(model.exports ?? []);
   const byFile = new Map<string, string[]>();
   const add = (url: string, item: string, declaredName?: string) => {
      const target =
         overlayKey(url) === redirect.from && declaredName !== redirect.keep
            ? redirect.to
            : url;
      byFile.set(target, [...(byFile.get(target) ?? []), item]);
   };
   for (const [name, entry] of Object.entries(model.contents ?? {})) {
      if (exported.has(name)) {
         add(modelURL, identifier(name), name);
         continue;
      }
      const e = entry as {
         sourceID?: unknown;
         name?: unknown;
         location?: { url?: unknown };
      };
      const declared =
         typeof e.sourceID === "string" ? splitSourceID(e.sourceID) : undefined;
      const url =
         declared?.url ??
         (typeof e.location?.url === "string" ? e.location.url : undefined);
      const declaredName =
         declared?.name ?? (typeof e.name === "string" ? e.name : name);
      if (!url) continue;
      add(
         url,
         declaredName === name
            ? identifier(name)
            : `${identifier(name)} is ${identifier(declaredName)}`,
         declaredName,
      );
   }
   // Givens are not in `contents`; each comes from the file declaring it.
   for (const given of Object.values(model.givens ?? {})) {
      const url = given.location?.url;
      if (typeof given.name === "string" && typeof url === "string") {
         add(url, identifier(given.name), given.name);
      }
   }
   const imports = [...byFile.entries()]
      .map(([url, items]) => {
         const from = relativeImport(queryModelURL, url);
         if (from === undefined) return "";
         // malloy-text: each item is identifier(name) or identifier(name) is identifier(declared)
         return `import { ${items.join(", ")} } from ${doubleQuotedString(from)}`;
      })
      .filter((line) => line !== "")
      .join("\n");
   return `${VARIANT_FLAGS}
${imports}
`;
}
