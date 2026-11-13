// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rollup variants (materialization v2, phase 4): the router's rollup leg for
 * `#(preaggregate)`.
 *
 * A rollup answers a query on its base by compiling the caller's own text,
 * unchanged, in a model where the base's name means the rollup. That model is
 * generated per rollup beside the author's: it imports every other name the
 * author's model has (sources, queries and givens, each from the file that
 * declares it), and declares the base's name itself, over the rollup's table:
 *
 *     import { customers, by_month, REGION } from "orders.malloy"
 *     source: orders is <the rollup's table> extend {
 *       measure: total is total__partial.sum()
 *       internal: total__partial
 *     }
 *
 * so a query naming `orders` reads the rollup, and anything else resolves as
 * the author wrote it. Givens cross a selective import by name, so a query
 * that supplies one compiles; the author's files are never rewritten.
 *
 * The table is read where it was built: a `storage=` rollup through
 * `virtual()` on the destination (as the served copy reads a stored source),
 * a colocated one by importing its build source from the rollup companion and
 * letting Malloy's build manifest substitute its table.
 *
 * A rollup answers only a query it reproduces exactly (R2, skip never bend):
 *
 * - the query compiles against it: every grouped and filtered field is in the
 *   grain (else `GRAIN_TOO_COARSE`), every measure is one it stores (else
 *   `MISSING_FIELD`);
 * - every aggregate the query computes is a stored measure's merge, read from
 *   the compiled query's IR (governance/ir/prepared_query.ts,
 *   `queryLevelFacts`): an aggregate written in the query (`count()`,
 *   `x.sum()`, `stddev(…)`), in a field or a `having:`, would count the
 *   rollup's rows, and a `select:` or `index:` stage reads rows the rollup
 *   does not have (`COUNT_SEMANTICS`);
 * - a fan-out grain dimension (A6) is fixed at every aggregating level, by a
 *   `group_by:` there or above, or a `where:` to one value
 *   (`GRAIN_TOO_COARSE`);
 * - it sets no `timezone:` (A5: the build truncated in the model's default
 *   time zone, `TIME_ZONE`);
 * - its table is built and fresh, and the compiled SQL reads it.
 *
 * Rollups are offered coarsest first; the first that answers is used. Behind
 * `materialize.enabled`.
 */

import type {
   MalloyConfig,
   ModelMaterializer,
   QueryMaterializer,
} from "@malloydata/malloy";
import * as path from "path";
import { logger } from "../../../logger";
import { fileURLToPath, pathToFileURL } from "url";
import { Annotations } from "@malloydata/malloy";
import {
   createModelRuntime,
   type ModelConnectionInput,
} from "../../model_runtime";
import { overlayKey } from "../../overlay_key";
import { quoteManifestTablePath } from "../../quoting";
import { ownModelNoteObjects } from "../../annotations";
import { routeOfNote } from "../../annotation_reader";
import { parseRouteTag } from "../../motly";
import {
   doubleQuotedString,
   identifier,
   stringLiteral,
} from "../../malloy_text";
import {
   queryLevelFacts,
   readPreparedQuery,
   resolveStructRefSource,
   type QueryLevelFacts,
} from "../../governance/ir/prepared_query";
import type { BuildManifest } from "../types";
import { evaluateManifestFreshness } from "../run/freshness";
import { persistBlankedFiles } from "../persist_blanking";
import {
   packageRelativeSourceID,
   rollupCompanionURLOf,
} from "../plan/rollup_compile";
import { emitMeasureExpr } from "../plan/rollup_measure_grammar";
import { splitSourceID } from "../plan/persisted_view";
import {
   generateRollupBuildFile,
   planSourceRollups,
   type RollupVariantPlan,
} from "../plan/rollup_plan";
import { SourceMap } from "../plan/source_map";
import { authorNameImports, relativeImport } from "./author_imports";
import { typeDeclaration } from "./served_copy";
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";
import type { ServeBinding } from "./materialization_serve_transform";
import type { MaterializedTableCatalog } from "./query_path";
import { queryShapeOf } from "./query_path";

type VirtualMap = Map<string, Map<string, string>>;

/** Why a rollup did not answer, as the path report maps it. */
export type RollupSkipCode =
   | "rollup_grain_too_coarse"
   | "rollup_missing_field"
   | "rollup_count_semantics"
   | "rollup_time_zone"
   | "rollup_not_built";

/** A rollup that did not answer one query: expected, so logged quietly. */
export class RollupSkip extends Error {
   constructor(
      readonly code: RollupSkipCode,
      /** In the caller's own terms, safe to show: never a table or rollup name. */
      readonly callerDetail: string,
      /** Decided from the query's shape alone, before compiling against it. */
      readonly shapeCheck = false,
   ) {
      super(callerDetail);
   }
}

/** The query was not on a source with rollups: the leg does not apply. */
export class NoRollupForQuery extends Error {}

/** What the variant reads from the model that owns it. */
export interface RollupVariantHost {
   modelPath(): string;
   modelDef(): unknown;
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
   /** The package's planned tables, to find each rollup's address. */
   catalog(): MaterializedTableCatalog | undefined;
}

/** A rollup's answer, for the router. */
export interface RollupAnswer {
   runnable: QueryMaterializer;
   /** The rollup that answered: its name, for the path report. */
   rollupSourceName: string;
   /** For a `storage=` rollup: how its `virtual()` handle resolves. */
   virtualMap?: VirtualMap;
   /** For a `storage=` rollup: its binding. */
   binding?: ServeBinding;
}

interface CompiledVariant {
   materializer?: ModelMaterializer;
   error?: string;
}

/** One model's rollup variants. */
export class RollupVariantServe {
   private packagePath?: string;
   private packageConfig?: ModelConnectionInput;
   private bindings: ServeBinding[] = [];
   private generation = 0;
   private compiled = new Map<string, Promise<CompiledVariant>>();
   private planMemo?: {
      modelDef: unknown;
      byBase: Map<string, RollupVariantPlan[]>;
   };

   constructor(private readonly host: RollupVariantHost) {}

   setContext(packagePath: string, packageConfig: ModelConnectionInput): void {
      this.packagePath = packagePath;
      this.packageConfig = packageConfig;
      this.planMemo = undefined;
      this.invalidate();
   }

   setBindings(bindings: ServeBinding[]): void {
      this.bindings = bindings.filter((b) => b.origin === "preaggregate");
      this.invalidate();
   }

   invalidate(): void {
      this.generation++;
      this.compiled.clear();
   }

   /** Every rollup a source this model can see declares, by base `sourceID`. */
   private plansByBase(): Map<string, RollupVariantPlan[]> {
      const modelDef = this.host.modelDef();
      const memo = this.planMemo;
      if (memo && memo.modelDef === modelDef) return memo.byBase;
      const byBase = new Map<string, RollupVariantPlan[]>();
      const packagePath = this.packagePath;
      if (modelDef && packagePath) {
         const map = SourceMap.of(modelDef);
         for (const node of map.nodes()) {
            const { plans } = planSourceRollups(map, node, {
               packageRelativeSourceID: (id) =>
                  packageRelativeSourceID(packagePath, id),
            });
            if (plans.length > 0) byBase.set(node.sourceID, plans);
         }
      }
      this.planMemo = { modelDef, byBase };
      return byBase;
   }

   /**
    * The `storage=` rollup tables bound to the package, for the path report;
    * with `now`, only those inside their freshness window.
    */
   rollupBindings(now?: number): ServeBinding[] {
      if (now === undefined) return this.bindings;
      const at = new Date(now);
      return this.bindings.filter((b) => isFresh(b, at));
   }

   /** Whether any source this model can see declares a rollup. */
   hasRollups(): boolean {
      return this.plansByBase().size > 0;
   }

   /** The rollups, with their bases' names in this model, for the path report. */
   rollups(): {
      rollupSourceName: string;
      baseSourceName: string;
      baseSourceID: string;
   }[] {
      const map = SourceMap.of(this.host.modelDef());
      return [...this.plansByBase().values()].flat().map((p) => ({
         rollupSourceName: p.rollupSourceName,
         baseSourceName: map.node(p.baseSourceID)?.name ?? p.baseSourceName,
         baseSourceID: p.baseSourceID,
      }));
   }

   /**
    * Answer `queryString` from the coarsest rollup that reproduces it, or throw
    * a {@link RollupSkip} (the last rollup's reason) or
    * {@link NoRollupForQuery}.
    */
   async attempt(args: {
      queryString: string;
      givens: Record<string, unknown> | undefined;
      /** The caller's own compile against the author's model. */
      authorRunnable: QueryMaterializer;
      /** The fresh build manifest, rollup entries included. */
      buildManifest: BuildManifest | undefined;
   }): Promise<RollupAnswer> {
      const modelDef = this.host.modelDef();
      const prepared = await readPreparedQuery(args.authorRunnable);
      // The run target must be a source this model names, by name: an inline
      // extension (`orders extend { … } -> …`) adds fields a rollup lacks.
      const localName =
         typeof prepared.structRef === "string"
            ? prepared.structRef
            : undefined;
      const struct = resolveStructRefSource<{ sourceID?: unknown }>(
         prepared.structRef,
         prepared.modelDef ?? (modelDef as never),
      );
      const sourceID =
         typeof struct?.sourceID === "string" ? struct.sourceID : undefined;
      const plans = sourceID ? this.plansByBase().get(sourceID) : undefined;
      if (!localName || !sourceID || !plans || plans.length === 0) {
         throw new NoRollupForQuery("the query's source declares no rollup");
      }
      const shape = queryShapeOf(prepared.query, struct, {
         sourceName: localName,
      });
      if (shape.timeZone !== undefined) {
         throw new RollupSkip(
            "rollup_time_zone",
            "the query sets its own time zone, and the rollup was built in the model's",
         );
      }
      // The reason reported is the first rollup's that got past the grain
      // and measure check (the one closest to answering), else the first's.
      let first: RollupSkip | undefined;
      let closest: RollupSkip | undefined;
      for (const plan of plans) {
         try {
            return await this.attemptOne(plan, localName, shape, args);
         } catch (error) {
            if (!(error instanceof RollupSkip)) throw error;
            first ??= error;
            if (!error.shapeCheck) closest ??= error;
         }
      }
      throw closest ?? first ?? new NoRollupForQuery("no rollup");
   }

   private async attemptOne(
      plan: RollupVariantPlan,
      localName: string,
      shape: ReturnType<typeof queryShapeOf>,
      args: Parameters<RollupVariantServe["attempt"]>[0],
   ): Promise<RollupAnswer> {
      const grain = new Set(plan.grainDimensions);
      const measures = new Set(plan.rollupMeasures.map((m) => m.name));
      // Cheap first, from the caller's own compile: what it groups and filters
      // by must be in the grain, what it aggregates must be stored.
      const coarse = [...shape.groupedBy, ...shape.filteredBy].find(
         (f) => !grain.has(f) && !measures.has(f),
      );
      if (coarse !== undefined) {
         throw new RollupSkip(
            "rollup_grain_too_coarse",
            `the query reads \`${coarse}\`, which the rollup's grain does not keep`,
            true,
         );
      }
      const missing = shape.measures.find((m) => !measures.has(m));
      if (missing !== undefined) {
         throw new RollupSkip(
            "rollup_missing_field",
            `the query aggregates \`${missing}\`, which the rollup does not store`,
            true,
         );
      }
      const table = this.tableOf(plan);
      if (!table) {
         throw new RollupSkip(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      const variant = await this.compiledVariant(plan, localName, table);
      if (!variant.materializer) {
         // A generated model that does not compile is a publisher bug.
         logger.warn("A #(preaggregate) rollup variant did not compile", {
            modelPath: this.host.modelPath(),
            rollup: plan.rollupSourceName,
            error: variant.error,
         });
         throw new RollupSkip(
            "rollup_missing_field",
            "the query does not compile against the rollup",
         );
      }
      const runnable = variant.materializer.loadRestrictedQuery(
         args.queryString,
      );
      // A colocated rollup is read through the build manifest: built and
      // fresh only when the manifest holds its entry.
      const colocatedTable =
         table.sourceEntityId !== undefined
            ? args.buildManifest?.entries[table.sourceEntityId]?.tableName
            : undefined;
      if (!table.virtualMap && !colocatedTable) {
         throw new RollupSkip(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      let result;
      try {
         result = await runnable.getPreparedResult({
            ...(table.virtualMap ? { virtualMap: table.virtualMap } : {}),
            ...(args.givens ? { givens: args.givens as never } : {}),
            ...(table.virtualMap ? {} : { buildManifest: args.buildManifest }),
         } as never);
      } catch {
         throw new RollupSkip(
            "rollup_missing_field",
            "the query does not compile against the rollup",
         );
      }
      checkCoverage(plan, queryLevelFacts(await readPreparedQuery(runnable)));
      // The compiled SQL must read the table, never recompute the rollup.
      const readsTable = result.sql.includes(
         (table.virtualMap ? table.quotedPath : colocatedTable) ?? "\u0000",
      );
      if (!readsTable) {
         throw new RollupSkip(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      return {
         runnable,
         rollupSourceName: plan.rollupSourceName,
         ...(table.virtualMap ? { virtualMap: table.virtualMap } : {}),
         ...(table.binding ? { binding: table.binding } : {}),
      };
   }

   /** The rollup's fresh table: a storage binding, or a bound colocated entry. */
   private tableOf(plan: RollupVariantPlan):
      | {
           virtualMap?: VirtualMap;
           quotedPath?: string;
           binding?: ServeBinding;
           sourceEntityId?: string;
        }
      | undefined {
      const sourceID = rollupSourceID(plan);
      if (plan.storage !== undefined) {
         const now = new Date();
         // Keyed by the rollup's own `sourceID`, never by its name.
         const binding =
            sourceID === undefined
               ? undefined
               : this.bindings.find(
                    (b) => b.sourceIDs?.includes(sourceID) && isFresh(b, now),
                 );
         if (!binding || !this.host.serveDestinationConfig()) return undefined;
         const alias = servedCopyAlias(binding.destinationName);
         return {
            virtualMap: new Map([
               [
                  alias,
                  new Map([
                     [
                        binding.virtualHandle,
                        quoteManifestTablePath(binding.tablePath, "duckdb"),
                     ],
                  ]),
               ],
            ]),
            quotedPath: quoteManifestTablePath(binding.tablePath, "duckdb"),
            binding,
         };
      }
      const planned = sourceID
         ? this.host.catalog()?.planned.get(sourceID)
         : undefined;
      return planned ? { sourceEntityId: planned.sourceEntityId } : undefined;
   }

   private compiledVariant(
      plan: RollupVariantPlan,
      localName: string,
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
   ): Promise<CompiledVariant> {
      const key = [
         plan.rollupSourceName,
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

   private async compile(
      plan: RollupVariantPlan,
      localName: string,
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
   ): Promise<CompiledVariant> {
      const packagePath = this.packagePath;
      const packageConfig = this.packageConfig;
      if (!packagePath || !packageConfig) return { error: "no context" };
      const modelPath = this.host.modelPath();
      const modelFile = path.join(packagePath, modelPath);
      const queryModelURL = pathToFileURL(`${modelFile}.rollup.malloy`);
      const overlay = new Map<string, string>();
      // The rollup with its merged measures, then the base's name over it with
      // the partial columns internal: readable by the measures, by no query.
      const servedName = `${plan.rollupSourceName}__served`;
      let declaration: string;
      if (table.binding) {
         const typeName = `${plan.rollupSourceName}__type`;
         const columns = table.binding.schema.map((c) => ({
            name: c.name,
            type: c.type,
         }));
         // malloy-text: the served copy's type emitter; rollupStatements re-emits the grammar's parse through malloy_text
         declaration = `${typeDeclaration({ typeName, columns })}
source: ${identifier(servedName)} is ${identifier(servedCopyAlias(table.binding.destinationName))}.virtual(${stringLiteral(table.binding.virtualHandle)})::${identifier(typeName)} extend {${timeframeStatements(plan, this.host.modelDef())}${rollupStatements(plan)}
}
source: ${identifier(localName)} is ${identifier(servedName)}${internalPartials(plan)}`;
      } else {
         // Colocated: import the build source from the declaring model's
         // companion, so the build manifest substitutes its table.
         const declared = splitSourceID(plan.baseSourceID);
         const companion = declared
            ? rollupCompanionURLOf(declared.url)
            : undefined;
         let declaringFile: string | undefined;
         try {
            declaringFile = declared ? fileURLToPath(declared.url) : undefined;
         } catch {
            declaringFile = undefined;
         }
         const text =
            companion && declaringFile
               ? generateRollupBuildFile([plan], path.basename(declaringFile))
               : undefined;
         if (!companion || !text) return { error: "no companion" };
         overlay.set(overlayKey(companion.href), text);
         // The shared, checked blanking (persist_blanking.ts), read through
         // the package's own reader, as the build companion reads them.
         const { runtime: reader } = await createModelRuntime(
            packagePath,
            modelPath,
            packageConfig,
         );
         const blanked = await persistBlankedFiles(
            this.host.modelDef(),
            async (url) => {
               try {
                  const read = await reader.urlReader.readURL(new URL(url));
                  return typeof read === "string" ? read : read.contents;
               } catch {
                  return undefined;
               }
            },
         );
         if (!blanked) return { error: "a `#@` line could not be left out" };
         for (const [url, file] of blanked) overlay.set(url, file);
         const built = `${plan.rollupSourceName}__built`;
         declaration = `import { ${identifier(built)} is ${identifier(plan.rollupSourceName)} } from ${doubleQuotedString(relativeImport(queryModelURL, companion.href) ?? "")}
source: ${identifier(servedName)} is ${identifier(built)} extend {${rollupStatements(plan)}
}
source: ${identifier(localName)} is ${identifier(servedName)}${internalPartials(plan)}`;
      }
      const text = queryModelText(
         this.host.modelDef(),
         queryModelURL,
         pathToFileURL(modelFile).href,
         localName,
         declaration,
      );
      overlay.set(overlayKey(queryModelURL.href), text);
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
      await materializer.getModel();
      return { materializer };
   }
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

/** The rollup's own `sourceID`, as its build companion declares it. */
export function rollupSourceID(plan: RollupVariantPlan): string | undefined {
   const declared = splitSourceID(plan.baseSourceID);
   const companion = declared ? rollupCompanionURLOf(declared.url) : undefined;
   return companion ? `${plan.rollupSourceName}@${companion.href}` : undefined;
}

/** The rollup's merged measures. */
function rollupStatements(plan: RollupVariantPlan): string {
   const measures = plan.rollupMeasures
      .map((m) => `    ${identifier(m.name)} is ${emitMeasureExpr(m.merged)}`)
      .join("\n");
   // malloy-text: each merged measure re-emitted from the grammar's parse through malloy_text
   return `\n  measure:\n${measures}`;
}

/**
 * ` include { internal: … public: * }`: the partial columns, readable by the
 * merged measures and by no query, so a caller cannot aggregate them.
 */
function internalPartials(plan: RollupVariantPlan): string {
   const partials = plan.rollupMeasures
      .flatMap((m) => m.partials.map((p) => identifier(p.column)))
      .join(", ");
   return ` include { internal: ${partials} public: * }`;
}

/** The timeframes a stored grain column can carry, the only ones emitted. */
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
 * A stored grain column read back through `virtual()` has no timeframe; the
 * author's field does. Renamed internally and redefined as the column
 * truncated again (the identity on a truncated value), as the served copy does.
 */
function timeframeStatements(
   plan: RollupVariantPlan,
   modelDef: unknown,
): string {
   const base = SourceMap.of(modelDef).node(plan.baseSourceID)?.def as
      | { fields?: { name?: unknown; as?: unknown; timeframe?: unknown }[] }
      | undefined;
   const timeframes = new Map<string, string>();
   for (const f of base?.fields ?? []) {
      const name = String(f.as ?? f.name);
      if (typeof f.timeframe === "string" && TIMEFRAMES.has(f.timeframe)) {
         timeframes.set(name, f.timeframe);
      }
   }
   const truncated = plan.grainDimensions.filter((g) => timeframes.has(g));
   if (truncated.length === 0) return "";
   const renames = truncated
      .map((g) => `    ${identifier(`${g}__stored`)} is ${identifier(g)}`)
      .join("\n");
   const dims = truncated
      .map(
         (g) =>
            `    ${identifier(g)} is ${identifier(`${g}__stored`)}.${timeframeText(timeframes.get(g) ?? "")}`,
      )
      .join("\n");
   return `\n  internal rename:\n${renames}\n  dimension:\n${dims}`;
}

/**
 * The query model: the author's model's names, imported from the files that
 * declare them, and `declaration`, which declares `localName` itself.
 */
function queryModelText(
   modelDef: unknown,
   queryModelURL: URL,
   modelURL: string,
   localName: string,
   declaration: string,
): string {
   // Every name but the one the variant declares, givens included, so a query
   // that supplies or reads a given compiles against the rollup.
   const imports = authorNameImports(modelDef, queryModelURL, modelURL, {
      skip: new Set([localName]),
   });
   return `${flagLine(modelDef)}
${imports}

${declaration}
`;
}

/** The author's experimental flags, and the two the query model needs. */
function flagLine(modelDef: unknown): string {
   const flags = new Set(["persistence", "virtual_source", "access_modifiers"]);
   try {
      const notes = ownModelNoteObjects(modelDef as never).filter(
         (n) => routeOfNote(n.text) === "!",
      );
      if (notes.length > 0) {
         const { tag } = parseRouteTag(
            new Annotations({ notes } as ConstructorParameters<
               typeof Annotations
            >[0]),
            "!",
         );
         for (const [key] of tag.tag("experimental")?.entries() ?? []) {
            if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) flags.add(key);
         }
      }
   } catch {
      // The author's model compiled with its flags; the query model keeps the
      // two it needs either way.
   }
   return `##! experimental { ${[...flags]
      .sort()
      .map((f) => identifier(f))
      .join(" ")} }`;
}

/**
 * Throw a {@link RollupSkip} unless every aggregate the compiled query computes
 * is a stored measure's merge and every fan-out grain dimension is fixed.
 * Read from the compiled query's IR (`queryLevelFacts`), never its text: the
 * variant source holds only the grain dimensions and the merged measures (the
 * partials are internal), so an aggregate the query computes itself counts the
 * rollup's rows, and any other read of a stored measure is its merge.
 *
 * Two kinds of grain dimension must be grouped or fixed at every level that
 * aggregates: the plan's own fan-out dimensions (A6, read through a
 * join_many), and those a measure read at that level fans out across (a
 * join-level sum or count re-summed across groups that share its key, A2).
 * A level that reads a measure other than by its bare name (an expression, a
 * filtered measure, a `having:`) is held to every measure's.
 */
export function checkCoverage(
   plan: RollupVariantPlan,
   levels: QueryLevelFacts | undefined,
): void {
   if (!levels) {
      throw new RollupSkip(
         "rollup_count_semantics",
         "the query has no stage the rollup can answer",
      );
   }
   const fanOut = new Set(plan.fanOutDimensions);
   const measureFanOut = new Map(
      plan.rollupMeasures.map((m) => [m.name, m.fanOutDimensions]),
   );
   const anyMeasureFanOut = new Set(
      plan.rollupMeasures.flatMap((m) => m.fanOutDimensions),
   );
   const visit = (level: QueryLevelFacts, fixedAbove: ReadonlySet<string>) => {
      if (level.kind !== "reduce") {
         throw new RollupSkip(
            "rollup_count_semantics",
            `a ${level.kind === "project" ? "select:" : level.kind === "index" ? "index:" : `${level.kind}`} stage reads the source's rows, which the rollup does not keep`,
         );
      }
      if (level.havings.some((h) => h.computesAggregate)) {
         throw new RollupSkip(
            "rollup_count_semantics",
            "a having: filter aggregates the rollup's rows, not a stored measure",
         );
      }
      // The grain dimensions this level must group or fix.
      const required = new Set<string>();
      let aggregates = false;
      if (level.havings.length > 0) {
         aggregates = true;
         for (const d of anyMeasureFanOut) required.add(d);
      }
      const grouped = new Set<string>();
      for (const f of level.fields) {
         if (f.isNest) continue;
         if (f.computesAggregate) {
            throw new RollupSkip(
               "rollup_count_semantics",
               `\`${f.name}\` aggregates the rollup's rows, not a stored measure`,
            );
         }
         if (f.ungrouped && (fanOut.size > 0 || anyMeasureFanOut.size > 0)) {
            throw new RollupSkip(
               "rollup_grain_too_coarse",
               `\`${f.name}\` re-sums across a fan-out dimension of the rollup's grain`,
            );
         }
         const ref = f.ref?.length === 1 ? f.ref[0] : undefined;
         const named = ref !== undefined ? measureFanOut.get(ref) : undefined;
         if (ref !== undefined && named === undefined) grouped.add(ref);
         else if (named !== undefined) {
            aggregates = true;
            for (const d of named) required.add(d);
         } else if (
            f.expressionType === "aggregate" ||
            f.expressionType === "ungrouped_aggregate"
         ) {
            aggregates = true;
            for (const d of anyMeasureFanOut) required.add(d);
         }
      }
      const fixed = new Set(fixedAbove);
      for (const d of [...fanOut, ...anyMeasureFanOut]) {
         if (grouped.has(d) || level.fixedByWhere.has(d)) fixed.add(d);
      }
      if (aggregates) {
         for (const d of fanOut) required.add(d);
         const loose = [...required].find((d) => !fixed.has(d));
         if (loose !== undefined) {
            throw new RollupSkip(
               "rollup_grain_too_coarse",
               fanOut.has(loose)
                  ? `the query does not group by or fix \`${loose}\`, a fan-out dimension of the rollup's grain, so its measures would be counted once per value`
                  : `the query does not group by or fix \`${loose}\`, so a measure taken through a join would count each joined row once per value of it`,
            );
         }
      }
      for (const f of level.fields) {
         if (!f.isNest) continue;
         if (!f.nest) {
            throw new RollupSkip(
               "rollup_count_semantics",
               `the nest \`${f.name}\` could not be read`,
            );
         }
         visit(f.nest, fixed);
      }
   };
   visit(levels, new Set());
}
