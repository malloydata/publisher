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
 * - it sets no `timezone:`, at any stage or in any nest (A5: the build
 *   truncated in the model's default time zone, `TIME_ZONE`);
 * - its table is built and fresh, and the compiled SQL reads it.
 *
 * The queried source's name in that model may also carry what the query
 * reads beyond the rollup's fields, re-declared in an `extend`:
 *
 * - a named view the query runs, refines, starts from or nests (row 11), as
 *   the author wrote it (read at its compiled location), so the view's own
 *   query is what compiles and is checked, with each view its definition
 *   builds on (`view: a is b + { … }`, at any depth); one that no longer
 *   compiles to the view the author's model holds (its file edited since) or
 *   that reads raw SQL declines (`MISSING_FIELD`); a persisted view of the
 *   name is the router's earlier leg (R1);
 * - for a query on an extension or rename of the rolled-up source (row 22,
 *   found through the source map's base edges), the extension's dimensions
 *   over the grain alone. One that adds a `where:` (`GRAIN_TOO_COARSE`), a
 *   `timezone:` (`TIME_ZONE`), or binds a base field's name to another field
 *   (`MISSING_FIELD`) declines.
 *
 * Route, then govern (R3 for rollups, known limitation 1): a base variant
 * redeclares the run target over the rollup IN AN OVERLAY OF ITS OWN
 * DECLARING FILE (`declaring_file_variant.ts`), so it keeps the author's
 * `sourceID` and carries the run target's entry-point gates as note lines.
 * With `materialize.gatedSources` on, the router lets the leg past a gated
 * entry point; a rollup then answers only when it was built for its gate
 * (`rollupGateEnforcedAtRead`) and the pre-proof holds (`proveGates`): the
 * variant's run target is the author's `sourceID`, its gates equal the live
 * entry point's (`sameGateSet`, the rule `govern()` re-proves with) and each
 * field they read is a grain dimension. Else it is skipped `CONTRACT`, with a
 * detail that never names the gate. `govern()` then grafts the gates on the
 * variant's own scope; a denial there is a defect, not a routing outcome. A
 * join leg (A7) carries no gate, so it declines past one.
 *
 * Rollups are offered coarsest first; the first that answers is used. Behind
 * `materialize.enabled`.
 */

import type {
   MalloyConfig,
   ModelDef,
   SourceDef,
   ModelMaterializer,
   QueryMaterializer,
} from "@malloydata/malloy";
import * as path from "path";
import { logger } from "../../../logger";
import { fileURLToPath, pathToFileURL } from "url";
import { Annotations, EMPTY_BUILD_MANIFEST } from "@malloydata/malloy";
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
   fieldNameReads,
   sourceDelta,
   type SourceDelta,
} from "../../governance/ir/source_graph";
import {
   definitionTextAt,
   extractRunViewCall,
   malloyIdentifiers,
   type TextRange,
} from "../../query_text";
import { viewNoteTexts } from "../plan/persisted_view_compile";
import {
   aggregateOutsideJoin,
   namedViewsRead,
   aggregateWrittenThroughJoin,
   queryFieldUsagePaths,
   queryLevelFacts,
   readPreparedQuery,
   readsRawSql,
   resolveStructRefSource,
   setsTimezone,
   sourceFieldJoinReads,
   type QueryLevelFacts,
} from "../../governance/ir/prepared_query";
import type { BuildManifest } from "../domain/types";
import { evaluateManifestFreshness } from "../domain/freshness";
import { persistBlankedFiles } from "../text/persist_blanking";
import {
   packageRelativeSourceID,
   rollupCompanionURLOf,
} from "../plan/rollup_compile";
import { emitMeasureExpr } from "../plan/rollup_measure_grammar";
import { splitSourceID } from "../plan/persisted_view";
import {
   generateRollupBuildFile,
   planSourceRollups,
   rollupGateEnforcedAtRead,
   type RollupVariantPlan,
} from "../plan/rollup_plan";
import { SourceMap } from "../text/source_map";
import {
   authorNameImports,
   relativeImport,
   type AuthorImportOptions,
} from "./author_imports";
import {
   checkJoinCoverage,
   joinedKeyProblem,
   joinRollupLegs,
   parentKeyProblem,
   unmatchedRowProblem,
   type JoinRollupLeg,
} from "./join_rollup";
import { compileDeclaringFileVariant } from "./declaring_file_variant";
import {
   collectEntryPointGates,
   type GraftRuntime,
   type GraftScope,
} from "../../gate_classification";
import { enforcedGatesOf, type EnforcedGates } from "../../access_record";
import { sameGateSet } from "../../governance/query_governor";
import { fieldNotes, noteSource } from "../text/field_notes";
import { timeframeRestatements, typeDeclaration } from "./served_copy";
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";
import {
   getMaterializeEnabled,
   getMaterializeGatedSources,
} from "../../../config";
import {
   boundTablePath,
   buildVirtualMap,
   sqlReadsTable,
   type ServeBinding,
} from "./materialization_serve_transform";
import type { ServedCopyJoinBasis } from "./served_copy_serve";
import { isDeclineOf, LegDecline } from "../domain/leg_decline";
import type { MaterializedTableCatalog } from "./query_path";
import { queryShapeOf } from "./query_path";

type VirtualMap = Map<string, Map<string, string>>;

/** The query was not on a source with rollups: the leg does not apply. */
class NoRollupForQuery extends Error {}

/** What the variant reads from the model that owns it. */
interface RollupVariantHost {
   modelPath(): string;
   modelDef(): unknown;
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
   /** The package's planned tables, to find each rollup's address. */
   catalog(): MaterializedTableCatalog | undefined;
   /**
    * The served copy's compiled basis, while the served copy serves this
    * model: the join leg answers a query on a source stored in a storage
    * destination there, through a joined source's rollup in the same one
    * (known limitation 26).
    */
   joinRollupBasis?(): Promise<ServedCopyJoinBasis | undefined>;
}

/** A rollup's answer, for the router. */
interface RollupAnswer {
   runnable: QueryMaterializer;
   /** The rollup that answered: its name, for the path report. */
   rollupSourceName: string;
   /** For a `storage=` rollup: how its `virtual()` handle resolves. */
   virtualMap?: VirtualMap;
   /** For a `storage=` rollup: its binding. */
   binding?: ServeBinding;
   /**
    * Every stored table the answer reads, when that is more than the
    * rollup's: a join leg answered in a storage destination reads the query
    * source's table there too, whose binding decides whether a failed run
    * may be retried live.
    */
   bindings?: ServeBinding[];
   /**
    * Where `govern()` grafts the run target's gates on the variant (R3): set
    * for every base variant, and the one an answer past an entry-point gate
    * is governed on.
    */
   graftScope?: GraftScope;
}

/**
 * How a variant model declares the query's source: as the rollup itself
 * (`base`, the source's own rollup), or as the author's source with one join
 * re-pointed at a joined source's rollup (`join`, A7).
 */
type VariantUse =
   | ({ kind: "base" } & VariantTarget)
   | {
        kind: "join";
        localName: string;
        leg: JoinRollupLeg;
        /**
         * Set when the query's source is read from the rollup's storage
         * destination through the served copy (known limitation 26): the
         * variant is compiled over the copy's files, and `keepsJoin` says
         * whether the copy's source still declares the join (it leaves a
         * join to a source not stored there out across connections).
         */
        destination?: { basis: ServedCopyJoinBasis; keepsJoin: boolean };
     };

/** A join leg that does not apply to this query: it reads nothing through the join. */
class JoinNotRead extends Error {}

interface CompiledVariant {
   materializer?: ModelMaterializer;
   /** A base variant's compiled model, where `govern()` grafts (R3). */
   modelDef?: ModelDef;
   /** The runtime a base variant compiled in, which a graft reloads through. */
   runtime?: GraftRuntime;
   /** The compile's cache key and generation: its graft cache scope. */
   cacheScope?: string;
   error?: string;
}

/** One query, as every rollup it is offered to reads it. */
interface RollupQuery {
   /** The run target's name in the author's model. */
   localName: string;
   /** The run target's `sourceID`. */
   sourceID: string;
   /**
    * The live entry point's gates (the governor's walk), when it has any:
    * the router let the rollup leg past them (R3), so a rollup answers only
    * when the same gates are written on its variant and each field they read
    * is a grain column (`proveGates`).
    */
   gates?: EnforcedGates;
   /** The run target's compiled definition. */
   struct: object;
   shape: ReturnType<typeof queryShapeOf>;
   /** The views of the run target the query reads by name (row 11). */
   views: string[];
   /**
    * Whether the query, its views expanded, holds raw SQL (`sql_*`): its text
    * names columns the compiler does not record, which a rollup may lack.
    */
   rawSql: boolean;
   /**
    * Set when the run target extends or renames the source the rollups
    * aggregate (row 22): that base's definition, and what the run target
    * adds to it.
    */
   extension?: { baseDef: unknown; delta?: SourceDelta };
}

/** An extension's dimension, re-declared on the variant. */
interface ExtensionDimension {
   name: string;
   /** The compiler's record of the author's expression. */
   code: string;
   notes: string[];
}

/** What a variant declares for the queried source beyond the rollup's fields. */
interface VariantTarget {
   localName: string;
   /** The run target's `sourceID`: the variant redeclares it in its declaring file. */
   sourceID: string;
   struct: object;
   views: readonly string[];
   dimensions: readonly ExtensionDimension[];
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
      /**
       * The switches the plans were read under ({@link planSwitches}): a
       * plan read with one is not the plan with the other.
       */
      switches: string;
      byBase: Map<string, RollupVariantPlan[]>;
      /** A7: each source's join legs, by its `sourceID`. */
      joinLegs?: Map<string, ReturnType<typeof joinRollupLegs>>;
   };

   constructor(private readonly host: RollupVariantHost) {}

   setContext(packagePath: string, packageConfig: ModelConnectionInput): void {
      this.packagePath = packagePath;
      this.packageConfig = packageConfig;
      this.planMemo = undefined;
      this.invalidate();
   }

   /** Keep the bindings that store a rollup this model can see. */
   setBindings(bindings: readonly ServeBinding[]): void {
      const ids = this.buildSourceIDs(bindings);
      this.bindings = bindings.filter((b) =>
         (b.sourceIDs ?? []).some((id) => ids.has(id)),
      );
      this.invalidate();
   }

   /**
    * The rollups' build `sourceID`s, to keep their bindings off the other
    * legs (the serve shape and the served copy cannot read a rollup's table).
    * Plans are read only when `bindings` holds a `preaggregate` one.
    */
   buildSourceIDs(bindings: readonly ServeBinding[]): Set<string> {
      if (!bindings.some((b) => b.origin === "preaggregate")) return new Set();
      const ids = new Set<string>();
      for (const plan of [...this.plansByBase().values()].flat()) {
         const id = rollupSourceID(plan);
         if (id !== undefined) ids.add(id);
      }
      return ids;
   }

   invalidate(): void {
      this.generation++;
      this.compiled.clear();
   }

   /** Every rollup a source this model can see declares, by base `sourceID`. */
   private plansByBase(): Map<string, RollupVariantPlan[]> {
      const modelDef = this.host.modelDef();
      const switches = planSwitches();
      const memo = this.planMemo;
      if (memo && memo.modelDef === modelDef && memo.switches === switches) {
         return memo.byBase;
      }
      const byBase = new Map<string, RollupVariantPlan[]>();
      const packagePath = this.packagePath;
      if (modelDef && packagePath) {
         const map = SourceMap.of(modelDef);
         for (const node of map.nodes()) {
            const { plans } = planSourceRollups(map, node, {
               packageRelativeSourceID: (id) =>
                  packageRelativeSourceID(packagePath, id),
               // Routed under the v1 cutover too: v2's route only with its
               // switch.
               readsV2: getMaterializeEnabled(),
            });
            if (plans.length > 0) byBase.set(node.sourceID, plans);
         }
      }
      this.planMemo = { modelDef, switches, byBase };
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
    * a {@link LegDecline} (the last rollup's reason) or
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
            : importedSourceName(
                 prepared.structRef,
                 prepared.modelDef ?? (modelDef as never),
              );
      const struct = resolveStructRefSource<{ sourceID?: unknown }>(
         prepared.structRef,
         prepared.modelDef ?? (modelDef as never),
      );
      const sourceID =
         typeof struct?.sourceID === "string" ? struct.sourceID : undefined;
      const target =
         localName && sourceID ? this.rollupTarget(sourceID) : undefined;
      // A7: the query source's joins whose joined source has rollups.
      const legs = sourceID && localName ? this.joinLegsOf(sourceID) : [];
      if (
         !localName ||
         !sourceID ||
         !struct ||
         (!target && legs.length === 0)
      ) {
         throw new NoRollupForQuery("the query's source declares no rollup");
      }
      const plans = target?.plans ?? [];
      const extension = target?.extension;
      // The live entry point's gates, as the governor walks them: a query
      // reaches here past them only when the router let the leg through.
      const gates = liveEntryPointGates(
         struct,
         prepared.modelDef ?? (modelDef as never),
      );
      const shape = queryShapeOf(prepared.query, struct, {
         sourceName: localName,
      });
      // Anywhere in the query, a nest's own `timezone:` included: the build
      // truncated the grain's times in the model's zone.
      if (setsTimezone(prepared.query)) {
         throw new LegDecline(
            "rollup_time_zone",
            "the query sets its own time zone, and the rollup was built in the model's",
         );
      }
      // The reason reported is the first rollup's that got past the grain
      // and measure check (the one closest to answering), else the first's.
      let first: LegDecline | undefined;
      let closest: LegDecline | undefined;
      // An extension that adds more than fields declines its base's rollups
      // (row 22); its own joins' rollups (A7) are still tried.
      let ownPlans = plans;
      if (extension) {
         try {
            extension.delta = checkExtension(struct, extension.baseDef);
         } catch (error) {
            if (!isDeclineOf(error, "rollup")) throw error;
            first = error;
            ownPlans = [];
         }
      }
      // The views the query reads by name (row 11): the one its text runs,
      // and each it nests, carried onto the variant as the author wrote them,
      // so the view's own query is what compiles against the rollup and is
      // checked for coverage.
      const call = extractRunViewCall(args.queryString);
      const views = namedViewsRead(
         prepared,
         struct,
         call && call.source === localName ? call.view : undefined,
      );
      const query: RollupQuery = {
         localName,
         sourceID,
         struct,
         shape,
         views,
         rawSql: readsRawSql(prepared.query),
         ...(gates ? { gates } : {}),
         ...(extension ? { extension } : {}),
      };
      for (const plan of ownPlans) {
         try {
            return await this.attemptOne(plan, query, args);
         } catch (error) {
            if (!isDeclineOf(error, "rollup")) throw error;
            first ??= error;
            if (!error.shapeCheck) closest ??= error;
         }
      }
      // Then a joined source's rollup, read through the join (A7): R1's
      // join legs, inside the rollup step.
      const usage = legs.length > 0 ? queryFieldUsagePaths(prepared) : [];
      for (const { leg, refusal } of legs) {
         // A join leg redeclares the author's source around the join, not
         // over its declaring file, so it carries no gate to prove: past an
         // entry-point gate it declines (CONTRACT).
         if (gates) {
            first ??= contractSkip();
            continue;
         }
         try {
            return await this.attemptJoin(leg, refusal, {
               localName,
               sourceID,
               shape,
               prepared,
               struct,
               usage,
               args,
            });
         } catch (error) {
            if (error instanceof JoinNotRead) continue;
            if (!isDeclineOf(error, "rollup")) throw error;
            first ??= error;
            if (!error.shapeCheck) closest ??= error;
         }
      }
      throw closest ?? first ?? new NoRollupForQuery("no rollup");
   }

   /**
    * The rollups that can answer a query on `sourceID`: its own, or (row 22)
    * those of the nearest source it extends or renames, through `extend` and
    * rename hops only, among the model's own declarations (a caller's
    * `source:` in the query text is never one). A query-shaped hop starts new
    * rows, so it ends the walk.
    */
   private rollupTarget(sourceID: string):
      | {
           plans: RollupVariantPlan[];
           extension?: RollupQuery["extension"];
        }
      | undefined {
      const byBase = this.plansByBase();
      const own = byBase.get(sourceID);
      if (own && own.length > 0) return { plans: own };
      const map = SourceMap.of(this.host.modelDef());
      let node = map.node(sourceID);
      for (let hops = 0; node && hops < 64; hops++) {
         const base = node.base;
         if (
            !base ||
            (base.hop !== "extends" && base.hop !== "rename") ||
            base.sourceID === undefined
         ) {
            return undefined;
         }
         const plans = byBase.get(base.sourceID);
         const baseNode = map.node(base.sourceID);
         if (plans && plans.length > 0 && baseNode) {
            return { plans, extension: { baseDef: baseNode.def } };
         }
         node = baseNode;
      }
      return undefined;
   }

   /**
    * The legs the joins of source `sourceID` offer (A7), each with why it
    * cannot be used, if it cannot. Read through the source map's join
    * readers; memoised with the plans.
    */
   private joinLegsOf(sourceID: string): ReturnType<typeof joinRollupLegs> {
      const byBase = this.plansByBase();
      const memo = this.planMemo;
      const cached = memo?.joinLegs?.get(sourceID);
      if (cached) return cached;
      const map = SourceMap.of(this.host.modelDef());
      const filter = map.filterJoinsOf(sourceID);
      const joins = map.joinsOf(sourceID);
      const legs = joinRollupLegs(
         joins,
         byBase,
         // Unknown reads count as reading every join.
         filter === undefined || filter.nested
            ? new Set(joins.map((j) => j.name))
            : filter.joins,
         primaryKeyOf(map.node(sourceID)?.def),
      );
      if (memo) {
         memo.joinLegs ??= new Map();
         memo.joinLegs.set(sourceID, legs);
      }
      return legs;
   }

   /**
    * The base `sourceID`s of the rollups a query on `struct` could read
    * through one of its joins (A7), for the path report.
    */
   joinedRollupBaseIDs(
      struct: unknown,
      prepared: Parameters<typeof queryFieldUsagePaths>[0],
   ): Set<string> {
      const sourceID = (struct as { sourceID?: unknown } | undefined)?.sourceID;
      if (typeof sourceID !== "string" || !this.hasRollups()) return new Set();
      const legs = this.joinLegsOf(sourceID);
      if (legs.length === 0) return new Set();
      // Only the joins the query reads (every one, when its reads are
      // unreadable): the leg never tries a join the query does not read.
      const usage = queryFieldUsagePaths(prepared);
      return new Set(
         legs
            .filter(
               ({ leg }) =>
                  usage === undefined ||
                  usage.some((p) => p[0] === leg.join.name),
            )
            .map(({ leg }) => leg.plan.baseSourceID),
      );
   }

   /**
    * Answer from a joined source's rollup, read at its grain through the
    * re-pointed join (A7, see join_rollup.ts), or throw a {@link LegDecline}
    * ({@link JoinNotRead} when the query reads nothing through the join).
    */
   private async attemptJoin(
      leg: JoinRollupLeg,
      refusal: ReturnType<typeof joinRollupLegs>[number]["refusal"],
      q: {
         localName: string;
         sourceID: string;
         shape: ReturnType<typeof queryShapeOf>;
         prepared: Awaited<ReturnType<typeof readPreparedQuery>>;
         struct: unknown;
         usage: string[][] | undefined;
         args: Parameters<RollupVariantServe["attempt"]>[0];
      },
   ): Promise<RollupAnswer> {
      const { plan, join } = leg;
      const name = join.name;
      // Only a query that reads through the join (an unreadable one might).
      if (q.usage && !q.usage.some((p) => p[0] === name)) {
         throw new JoinNotRead(name);
      }
      if (refusal) {
         throw new LegDecline(refusal.code, refusal.callerDetail, {
            shapeCheck: true,
         });
      }
      // A rollup in a storage destination is joined there, so only to a
      // query source the served copy reads from that destination too; Malloy
      // joins within one connection (malloy#3032).
      // The destination is the rollup's table's, when it has a fresh one.
      const inDestination =
         plan.storage !== undefined
            ? await this.destinationOf(
                 this.tableOf(plan)?.binding?.destinationName ?? plan.storage,
                 q.struct,
                 name,
              )
            : undefined;
      // The build truncated and filtered times in the joined source's time
      // zone; live reads the joined rows in the query source's.
      const childDef = SourceMap.of(this.host.modelDef()).node(
         plan.baseSourceID,
      )?.def as
         | {
              queryTimezone?: unknown;
              fields?: {
                 name?: unknown;
                 as?: unknown;
                 e?: unknown;
                 join?: unknown;
              }[];
           }
         | undefined;
      const parentZone = (q.struct as { queryTimezone?: unknown } | undefined)
         ?.queryTimezone;
      if ((childDef?.queryTimezone ?? "") !== (parentZone ?? "")) {
         throw new LegDecline(
            "rollup_time_zone",
            `\`${name}\` is read in the query source's time zone, and its rollup was built in its own`,
            { shapeCheck: true },
         );
      }
      const grain = new Set(plan.grainDimensions);
      const measures = new Set(plan.rollupMeasures.map((m) => m.name));
      const through = (paths: readonly string[]) =>
         paths
            .filter((p) => p.startsWith(`${name}.`))
            .map((p) => p.slice(name.length + 1));
      const coarse = through([
         ...q.shape.groupedBy,
         ...q.shape.filteredBy,
      ]).find((f) => !grain.has(f) && !measures.has(f));
      if (coarse !== undefined) {
         throw new LegDecline(
            "rollup_grain_too_coarse",
            `the query reads \`${name}.${coarse}\`, which the rollup's grain does not keep`,
            { shapeCheck: true },
         );
      }
      const missing = through(q.shape.measures).find(
         (m) => !measures.has(m) && !grain.has(m),
      );
      if (missing !== undefined) {
         throw new LegDecline(
            "rollup_missing_field",
            `the query aggregates \`${name}.${missing}\`, which the rollup does not store`,
            { shapeCheck: true },
         );
      }
      // Every level reads rows by groups: a `select:` or `index:` stage would
      // read the rollup's rows, one per group, not the joined rows.
      const levels = queryLevelFacts(q.prepared);
      const rowLevel = levels ? nonReduceLevel(levels) : "unreadable";
      if (rowLevel !== undefined) {
         throw new LegDecline(
            "rollup_count_semantics",
            rowLevel === "unreadable"
               ? "the query has no stage the rollup can answer"
               : `a ${rowLevel} stage reads the joined rows, which the rollup does not keep`,
         );
      }
      const written = aggregateWrittenThroughJoin(q.prepared, q.struct, name);
      if (written !== undefined) {
         throw new LegDecline(
            "rollup_count_semantics",
            `\`${written}\` aggregates the rows of \`${name}\` itself, which its rollup does not keep`,
         );
      }
      if (q.usage === undefined) {
         throw new LegDecline(
            "rollup_count_semantics",
            "the query's reads could not be traced",
         );
      }
      // A6 and A2 through the join (row 27): a fan-out grain dimension, and
      // the other grain dimensions of a join-level sum or count, grouped or
      // fixed at every level that aggregates through the join.
      if (levels) {
         const loose = checkJoinCoverage(
            leg,
            levels,
            sourceFieldJoinReads(q.struct, name),
         );
         if (loose) throw new LegDecline(loose.code, loose.callerDetail);
      }
      const read = new Set(
         q.usage
            .filter((p) => p[0] === name && p.length === 2)
            .map((p) => p[1] as string),
      );
      // A join_one the data does not keep unique repeats each parent row
      // once per joined row live, and every aggregate not taken at the join
      // counts the repeats; the rollup holds one row per key. Served only
      // when the rollup's build checked the key unique over the rows it
      // grouped (known limitation 29): then live's rows and the variant's
      // are the same rows.
      if (join.relationship === "one") {
         const outside = aggregateOutsideJoin(q.prepared, q.struct, name);
         const unique =
            outside === undefined
               ? undefined
               : this.keyUniqueness(plan, join.keyColumn);
         if (outside !== undefined && unique !== true) {
            throw new LegDecline(
               "rollup_count_semantics",
               unique === false
                  ? `${outside} aggregates rows outside the join_one \`${name}\`, whose key the data holds more than once, so live repeats those rows; its rollup holds one row per key`
                  : `${outside} aggregates rows outside the join_one \`${name}\`, which live repeats for each of its rows, and its key was not checked unique when its rollup was built; its rollup holds one row per key`,
            );
         }
      }
      // A join read as a join_many: each rollup row is met by every parent
      // row sharing its key, so the query source's key must be unique and
      // never NULL: as the rollup's build found it, or, for a rollup in a
      // storage destination, as the build of the query source's stored
      // table there found it over the rows that table holds.
      const keyProblem = parentKeyProblem(
         leg,
         inDestination
            ? this.storedKeyUniqueness(inDestination.source, join.baseKey)
            : this.parentKeyUniqueness(plan, q.struct),
      );
      if (keyProblem !== undefined) {
         throw new LegDecline("rollup_count_semantics", keyProblem);
      }
      // Another join_one the query reads, known not to keep its key unique:
      // live's rows repeat through it, and re-pointing this join changes how
      // Malloy aggregates over those repeats, so the answers part (known
      // limitation 29). Unknown is served, as before the check.
      const repeating = this.nonUniqueJoinOneRead(q.struct, name, q.usage);
      if (repeating !== undefined) {
         throw new LegDecline(
            "rollup_count_semantics",
            `the query reads the join_one \`${repeating}\`, whose key the data holds more than once, so live repeats rows through it`,
         );
      }
      const childFields = new Map(
         (childDef?.fields ?? []).map((f) => [String(f.as ?? f.name), f]),
      );
      const isBareColumn = (
         f: { e?: unknown; join?: unknown } | undefined,
      ): boolean =>
         f !== undefined && f.e === undefined && f.join === undefined;
      // A join of the joined source keyed on one of its own columns: that
      // column is NULL on an unmatched parent's row, which meets no row.
      const childJoins = SourceMap.of(this.host.modelDef()).joinsOf(
         plan.baseSourceID,
      );
      const innerMeetingNoRow = (joinName: string) =>
         childJoins.find(
            (j) =>
               j.name === joinName &&
               j.relationship !== "cross" &&
               j.baseKey !== undefined &&
               !!j.condition &&
               !j.condition.nested &&
               [...j.condition.joins].every((c) => c === j.name) &&
               isBareColumn(childFields.get(j.baseKey)),
         );
      const problem = unmatchedRowProblem(
         leg,
         read,
         (operand) => {
            if (operand.length === 1) {
               return isBareColumn(childFields.get(operand[0] as string));
            }
            const inner = innerMeetingNoRow(operand[0] as string);
            const innerFields = (inner?.fields ?? []) as {
               name?: unknown;
               as?: unknown;
               e?: unknown;
               join?: unknown;
            }[];
            return (
               inner !== undefined &&
               isBareColumn(
                  innerFields.find(
                     (f) => String(f.as ?? f.name) === operand[1],
                  ),
               )
            );
         },
         (joinName) => innerMeetingNoRow(joinName) !== undefined,
      );
      if (problem !== undefined) {
         throw new LegDecline("rollup_count_semantics", problem);
      }
      // The joined source's own key, when the join is keyed on another
      // column: live counts and sums its rows once per non-NULL key.
      const joinedKey = (childDef as { primaryKey?: unknown } | undefined)
         ?.primaryKey;
      const joinedProblem = joinedKeyProblem(
         leg,
         read,
         typeof joinedKey === "string" ? joinedKey : undefined,
         plan.storage !== undefined
            ? this.tableOf(plan)?.binding?.primaryKeys?.[plan.baseSourceID]
            : this.parentKeyUniqueness(plan, { sourceID: plan.baseSourceID }),
      );
      if (joinedProblem !== undefined) {
         throw new LegDecline("rollup_count_semantics", joinedProblem);
      }
      const table = this.tableOf(plan);
      if (!table) {
         throw new LegDecline(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      if (inDestination) {
         return this.answerJoinInDestination(leg, q, table, inDestination);
      }
      const variant = await this.compiledVariant(
         plan,
         { kind: "join", localName: q.localName, leg },
         table,
      );
      if (!variant.materializer) {
         logger.warn(
            "A #(preaggregate) join-to-rollup variant did not compile",
            {
               modelPath: this.host.modelPath(),
               rollup: plan.rollupSourceName,
               join: name,
               error: variant.error,
            },
         );
         throw new LegDecline(
            "rollup_missing_field",
            "the query does not compile against the rollup",
         );
      }
      const runnable = variant.materializer.loadRestrictedQuery(
         q.args.queryString,
      );
      const colocatedTable =
         table.sourceEntityId !== undefined
            ? q.args.buildManifest?.entries[table.sourceEntityId]?.tableName
            : undefined;
      if (!colocatedTable) {
         throw new LegDecline(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      let result;
      try {
         result = await runnable.getPreparedResult({
            ...(q.args.givens ? { givens: q.args.givens as never } : {}),
            buildManifest: q.args.buildManifest ?? EMPTY_BUILD_MANIFEST,
         } as never);
      } catch {
         throw new LegDecline(
            "rollup_missing_field",
            "the query does not compile against the rollup",
         );
      }
      if (!result.sql.includes(colocatedTable)) {
         throw new LegDecline(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      return { runnable, rollupSourceName: plan.rollupSourceName };
   }

   /**
    * The served copy's basis and the query source's table in `destination`,
    * when the served copy reads the query's source (or a source it is built
    * on) from that destination's table; else a `CROSS_CONNECTION` skip. A
    * table that is not fresh is not re-rooted, so a stale one declines too.
    */
   private async destinationOf(
      destination: string,
      struct: unknown,
      join: string,
   ): Promise<{ basis: ServedCopyJoinBasis; source: ServeBinding }> {
      const across = () =>
         new LegDecline(
            "rollup_cross_connection",
            `the rollup of \`${join}\` is in a storage destination, and the query's source is not read from that destination (it reads its own connection, or its table there is not built or not fresh)`,
            { shapeCheck: true },
         );
      let basis: ServedCopyJoinBasis | undefined;
      try {
         basis = await this.host.joinRollupBasis?.();
      } catch {
         basis = undefined;
      }
      if (!basis) throw across();
      const chain = new Set(
         SourceMap.of(this.host.modelDef()).chainOf(struct as never),
      );
      const source = basis.rerooted.find(
         (b) =>
            !b.colocated &&
            b.destinationName === destination &&
            (b.sourceIDs ?? []).some((id) => chain.has(id)),
      );
      if (!source) throw across();
      return { basis, source };
   }

   /**
    * Answer a join leg in a storage destination (known limitation 26): the
    * query's source as the served copy declares it, over its table there,
    * with the join re-pointed at the joined source's rollup in the same
    * destination, so the whole query runs on that one connection. Every
    * check of the leg ran before; this adds the served copy's own checks for
    * the tables the variant reads, and that its SQL reads both tables.
    */
   private async answerJoinInDestination(
      leg: JoinRollupLeg,
      q: Parameters<RollupVariantServe["attemptJoin"]>[2],
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
      at: { basis: ServedCopyJoinBasis; source: ServeBinding },
   ): Promise<RollupAnswer> {
      const { plan, join } = leg;
      const name = join.name;
      const { basis, source } = at;
      const across = (why: string) =>
         new LegDecline(
            "rollup_cross_connection",
            `the rollup of \`${name}\` is in a storage destination, and ${why}`,
         );
      if (
         !table.binding ||
         !table.virtualMap ||
         !table.quotedPath ||
         table.binding.destinationName !== source.destinationName
      ) {
         throw across("the query's source is stored in another one");
      }
      // The copy leaves out a join to a source not stored in its destination
      // (and every definition that reads it); the variant then adds the join
      // back, re-pointed, instead of replacing it.
      const servedSource = (
         basis.modelDef as {
            contents?: Record<string, { fields?: unknown[] }>;
         }
      )?.contents?.[q.localName];
      const keepsJoin = (
         (servedSource?.fields ?? []) as { name?: unknown; as?: unknown }[]
      ).some((f) => (f.as ?? f.name) === name);
      const variant = await this.compiledVariant(
         plan,
         {
            kind: "join",
            localName: q.localName,
            leg,
            destination: { basis, keepsJoin },
         },
         table,
      );
      // A definition the copy left out reads a join across connections: a
      // query that reads one does not compile over the copy.
      const leftOutHere = basis.leftOut.length > 0;
      if (!variant.materializer) {
         logger.warn(
            "A #(preaggregate) join-to-rollup variant in a storage destination did not compile",
            {
               modelPath: this.host.modelPath(),
               rollup: plan.rollupSourceName,
               join: name,
               error: variant.error,
            },
         );
         throw new LegDecline(
            "rollup_missing_field",
            "the query does not compile against the rollup",
         );
      }
      const runnable = variant.materializer.loadRestrictedQuery(
         q.args.queryString,
      );
      const virtualMap = buildVirtualMap([...basis.aliased]);
      for (const [connection, handles] of table.virtualMap) {
         const inner = virtualMap.get(connection) ?? new Map<string, string>();
         for (const [handle, tablePath] of handles)
            inner.set(handle, tablePath);
         virtualMap.set(connection, inner);
      }
      let sql: string;
      try {
         sql = (
            await runnable.getPreparedResult({
               virtualMap,
               ...(q.args.givens ? { givens: q.args.givens as never } : {}),
               // Explicit: an undefined manifest reads the Runtime's.
               buildManifest: EMPTY_BUILD_MANIFEST,
            } as never)
         ).sql;
      } catch {
         if (leftOutHere) {
            throw across(
               "the query reads a definition its source's stored copy leaves out, since it reads a join to a source on another connection",
            );
         }
         throw new LegDecline(
            "rollup_missing_field",
            "the query does not compile against the rollup",
         );
      }
      const tablesRead = basis.rerooted.filter((b) =>
         sqlReadsTable(sql, boundTablePath(b)),
      );
      if (!sqlReadsTable(sql, table.quotedPath)) {
         throw new LegDecline(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      if (!tablesRead.includes(source)) {
         throw across("the query's source is not read from it");
      }
      try {
         await basis.check({
            sourceID: q.sourceID,
            runnable,
            queryString: q.args.queryString,
            tablesRead,
         });
      } catch (error) {
         if (!isDeclineOf(error, "served_copy")) throw error;
         throw servedCopySkip(error, name);
      }
      return {
         runnable,
         rollupSourceName: plan.rollupSourceName,
         virtualMap,
         binding: table.binding,
         bindings: [...tablesRead, table.binding],
      };
   }

   private async attemptOne(
      plan: RollupVariantPlan,
      query: RollupQuery,
      args: Parameters<RollupVariantServe["attempt"]>[0],
   ): Promise<RollupAnswer> {
      const { localName, shape } = query;
      // Past an entry-point gate, only a rollup whose base gate is enforced
      // at read (built for it: rows 1 and 21) is offered.
      if (query.gates && !rollupGateEnforcedAtRead(plan)) {
         throw contractSkip();
      }
      // A source with its own `timezone:` truncated and compared its times in
      // that zone when the rollup was built; the variant redeclares it over
      // the rollup without one, so a time grain dimension would be read, and
      // a grafted gate or a caller's filter on it compared, in another zone:
      // live's answer would differ, and a truncation gate would admit groups
      // live refuses.
      const zone = (query.struct as { queryTimezone?: unknown }).queryTimezone;
      if (typeof zone === "string" && zone !== "") {
         throw new LegDecline(
            "rollup_time_zone",
            "the query's source sets its own time zone, which the rollup's stored times are not read in",
            { shapeCheck: true },
         );
      }
      // A view's `where: sql_boolean("${TABLE}.region = …")` reads a column
      // no field reference names, so no grain check sees it.
      if (query.rawSql) {
         throw new LegDecline(
            "rollup_missing_field",
            "the query reads raw SQL (`sql_*`), whose columns the rollup may not keep",
            { shapeCheck: true },
         );
      }
      const measures = new Set(plan.rollupMeasures.map((m) => m.name));
      // An extension's dimensions over the grain alone are re-declared on the
      // variant (row 22), and read as grain dimensions here.
      const dimensions = query.extension
         ? extensionDimensions(query.extension, plan, measures)
         : [];
      const grain = new Set([
         ...plan.grainDimensions,
         ...dimensions.map((d) => d.name),
      ]);
      // Cheap first, from the caller's own compile: what it groups and filters
      // by must be in the grain, what it aggregates must be stored.
      const coarse = [...shape.groupedBy, ...shape.filteredBy].find(
         (f) => !grain.has(f) && !measures.has(f),
      );
      if (coarse !== undefined) {
         throw new LegDecline(
            "rollup_grain_too_coarse",
            `the query reads \`${coarse}\`, which the rollup's grain does not keep`,
            { shapeCheck: true },
         );
      }
      const missing = shape.measures.find((m) => !measures.has(m));
      if (missing !== undefined) {
         throw new LegDecline(
            "rollup_missing_field",
            `the query aggregates \`${missing}\`, which the rollup does not store`,
            { shapeCheck: true },
         );
      }
      const table = this.tableOf(plan);
      if (!table) {
         throw new LegDecline(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      const variant = await this.compiledVariant(
         plan,
         {
            kind: "base",
            localName,
            sourceID: query.sourceID,
            struct: query.struct,
            views: query.views,
            dimensions,
         },
         table,
      );
      if (!variant.materializer) {
         // A generated model that does not compile is a publisher bug.
         logger.warn("A #(preaggregate) rollup variant did not compile", {
            modelPath: this.host.modelPath(),
            rollup: plan.rollupSourceName,
            error: variant.error,
         });
         throw new LegDecline(
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
         throw new LegDecline(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      let result;
      try {
         result = await runnable.getPreparedResult({
            ...(table.virtualMap ? { virtualMap: table.virtualMap } : {}),
            ...(args.givens ? { givens: args.givens as never } : {}),
            // Explicit even beside a virtualMap: an undefined manifest reads
            // the Runtime's, unfiltered by freshness.
            buildManifest: table.virtualMap
               ? EMPTY_BUILD_MANIFEST
               : (args.buildManifest ?? EMPTY_BUILD_MANIFEST),
         } as never);
      } catch {
         throw new LegDecline(
            "rollup_missing_field",
            "the query does not compile against the rollup",
         );
      }
      const routed = await readPreparedQuery(runnable);
      checkCoverage(plan, queryLevelFacts(routed));
      // Route, then govern: past an entry-point gate, the gate must be
      // provable on this rollup before it is used, so `govern()`'s re-proof
      // (graft, bind guard, gate-set equality) denies only on a defect.
      if (query.gates) proveGates(plan, query, routed, variant.modelDef);
      // The compiled SQL must read the table, never recompute the rollup.
      const readsTable = result.sql.includes(
         (table.virtualMap ? table.quotedPath : colocatedTable) ?? "\u0000",
      );
      if (!readsTable) {
         throw new LegDecline(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      return {
         runnable,
         rollupSourceName: plan.rollupSourceName,
         ...(table.virtualMap ? { virtualMap: table.virtualMap } : {}),
         ...(table.binding ? { binding: table.binding } : {}),
         ...(variant.modelDef && variant.cacheScope
            ? {
                 graftScope: {
                    modelDef: variant.modelDef,
                    materializer: variant.materializer,
                    cacheScope: variant.cacheScope,
                    ...(variant.runtime ? { runtime: variant.runtime } : {}),
                 },
              }
            : {}),
      };
   }

   /**
    * What the build of `plan`'s table found of its one grain dimension, when
    * that is `keyColumn` (known limitation 29): true when unique, false when
    * a value repeats, undefined when unknown (not checked, not built by a run
    * that checked it, or a table bound from a host's manifest).
    */
   private keyUniqueness(
      plan: RollupVariantPlan,
      keyColumn: string | undefined,
   ): boolean | undefined {
      if (
         keyColumn === undefined ||
         plan.grainDimensions.length !== 1 ||
         plan.grainDimensions[0] !== keyColumn
      ) {
         return undefined;
      }
      // The table's content address fixes its grain, so its check is of
      // this key.
      const id = this.tableOf(plan)?.sourceEntityId;
      return id === undefined
         ? undefined
         : this.host.catalog()?.bound[id]?.keyUniqueness?.unique;
   }

   /**
    * What the build of `plan`'s table found of the query source's primary key
    * (its parent key check): true when unique and never NULL, false when not,
    * undefined when unknown (not checked, or a table a host's manifest binds).
    */
   private parentKeyUniqueness(
      plan: RollupVariantPlan,
      struct: unknown,
   ): boolean | undefined {
      const sourceID = (struct as { sourceID?: unknown } | undefined)?.sourceID;
      const id = this.tableOf(plan)?.sourceEntityId;
      if (typeof sourceID !== "string" || id === undefined) return undefined;
      return this.host.catalog()?.bound[id]?.keyUniqueness?.parentKeys?.[
         sourceID
      ];
   }

   /**
    * What the build of a stored table the served copy reads for the query's
    * source found of `key`: true when a source it stores declares `key` its
    * primary key and the check found it unique and never NULL; false when one
    * found it repeated or NULL; undefined when unknown (not checked, or a
    * binding a host's manifest made).
    */
   private storedKeyUniqueness(
      binding: ServeBinding,
      key: string | undefined,
   ): boolean | undefined {
      if (key === undefined || !binding.primaryKeys) return undefined;
      const map = SourceMap.of(this.host.modelDef());
      let found: boolean | undefined;
      for (const id of binding.sourceIDs ?? []) {
         const unique = binding.primaryKeys[id];
         if (unique === undefined) continue;
         const declared = (map.node(id)?.def as { primaryKey?: unknown })
            ?.primaryKey;
         if (declared !== key) continue;
         if (!unique) return false;
         found = true;
      }
      return found;
   }

   /**
    * The first join_one of the query's source, other than `legJoin`, that the
    * query reads and whose key a rollup's build found repeated (see
    * {@link keyUniqueness}), or undefined.
    */
   private nonUniqueJoinOneRead(
      struct: unknown,
      legJoin: string,
      usage: readonly (readonly string[])[],
   ): string | undefined {
      const sourceID = (struct as { sourceID?: unknown } | undefined)?.sourceID;
      if (typeof sourceID !== "string") return undefined;
      const byBase = this.plansByBase();
      for (const j of SourceMap.of(this.host.modelDef()).joinsOf(sourceID)) {
         if (
            j.name === legJoin ||
            j.relationship !== "one" ||
            j.sourceID === undefined ||
            j.keyColumn === undefined ||
            !usage.some((p) => p[0] === j.name)
         ) {
            continue;
         }
         for (const plan of byBase.get(j.sourceID) ?? []) {
            if (this.keyUniqueness(plan, j.keyColumn) === false) return j.name;
         }
      }
      return undefined;
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
      use: VariantUse,
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
   ): Promise<CompiledVariant> {
      const key = [
         plan.rollupSourceName,
         use.localName,
         use.kind === "join"
            ? `join:${use.leg.join.name}${use.destination ? `\u0000${use.destination.basis.key}\u0000${use.destination.keepsJoin}` : ""}`
            : "base",
         table.binding
            ? `${table.binding.destinationName}/${table.binding.virtualHandle}`
            : "colocated",
         // The views vary by query; an extension's dimensions follow from
         // its name and the plan.
         ...(use.kind === "base" ? use.views : []),
      ].join("\u0000");
      let compiled = this.compiled.get(key);
      if (!compiled) {
         // One graft cache scope per compiled variant: two variants naming
         // the same source (other views, another rollup, a reload) never
         // share a grafted materializer.
         const cacheScope = `rollup\u0000${this.generation}\u0000${key}`;
         compiled = this.compile(plan, use, table).then(
            (v) => ({ ...v, cacheScope }),
            (error) => ({
               error: error instanceof Error ? error.message : String(error),
            }),
         );
         this.compiled.set(key, compiled);
      }
      return compiled;
   }

   private async compile(
      plan: RollupVariantPlan,
      use: VariantUse,
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
   ): Promise<CompiledVariant> {
      // A join leg reads a storage rollup only over the served copy
      // (answerJoinInDestination).
      if (use.kind === "join" && table.binding && !use.destination) {
         return {
            error: "a join leg reads a storage rollup over the served copy only",
         };
      }
      const packagePath = this.packagePath;
      const packageConfig = this.packageConfig;
      if (!packagePath || !packageConfig) return { error: "no context" };
      const modelPath = this.host.modelPath();
      const modelFile = path.join(packagePath, modelPath);
      const queryModelURL = pathToFileURL(`${modelFile}.rollup.malloy`);
      const destinations = this.host.serveDestinationConfig();
      const config =
         table.binding && destinations
            ? servedCopyConfig(
                 packageConfig,
                 destinations(),
                 new Set([table.binding.destinationName]),
              )
            : packageConfig;
      if (use.kind === "join" && use.destination && table.binding) {
         return this.compileJoinInDestination(
            plan,
            { ...use, destination: use.destination },
            table.binding,
            {
               packagePath,
               modelPath,
               queryModelURL,
               modelFile,
            },
         );
      }
      if (use.kind === "base") {
         return this.compileBase(plan, use, table, {
            packagePath,
            packageConfig,
            config,
            modelPath,
            queryModelURL,
         });
      }
      const { localName } = use;
      // The package's own reader, made once and only when a file is read.
      let reader: ReturnType<typeof createModelRuntime> | undefined;
      const readText = async (url: string) => {
         try {
            reader ??= createModelRuntime(
               packagePath,
               modelPath,
               packageConfig,
            );
            const { runtime } = await reader;
            const read = await runtime.urlReader.readURL(new URL(url));
            return typeof read === "string" ? read : read.contents;
         } catch {
            return undefined;
         }
      };
      const overlay = new Map<string, string>();
      // The rollup with its merged measures, then the author's source with
      // the join re-pointed at it (A7).
      const servedName = `${plan.rollupSourceName}__served`;
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
      const blanked = await persistBlankedFiles(this.host.modelDef(), readText);
      if (!blanked) return { error: "a `#@` line could not be left out" };
      for (const [url, file] of blanked) overlay.set(url, file);
      const built = `${plan.rollupSourceName}__built`;
      const declaration = `import { ${identifier(built)} is ${identifier(plan.rollupSourceName)} } from ${doubleQuotedString(relativeImport(queryModelURL, companion.href) ?? "")}
source: ${identifier(servedName)} is ${identifier(built)} extend {${rollupStatements(plan, this.host.modelDef())}
}
${sourceOverRollup(plan, use, servedName, "")}`;
      const modelText = queryModelText(
         this.host.modelDef(),
         queryModelURL,
         pathToFileURL(modelFile).href,
         { alias: new Map([[localName, parentAlias(plan)]]) },
         declaration,
      );
      overlay.set(overlayKey(queryModelURL.href), modelText);
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

   /**
    * Compile a join leg in a storage destination (known limitation 26): the
    * query model imports every name from the served copy's files (so the
    * query's source is the copy's, over its stored table), declares the
    * rollup over its table in the same destination, and declares the query's
    * source as the copy's with the join re-pointed at the rollup. Compiled
    * with the copy's own connections, so the source and the rollup read one.
    */
   private async compileJoinInDestination(
      plan: RollupVariantPlan,
      use: Extract<VariantUse, { kind: "join" }> & {
         destination: NonNullable<
            Extract<VariantUse, { kind: "join" }>["destination"]
         >;
      },
      binding: ServeBinding,
      ctx: {
         packagePath: string;
         modelPath: string;
         queryModelURL: URL;
         modelFile: string;
      },
   ): Promise<CompiledVariant> {
      const modelDef = this.host.modelDef();
      const { basis } = use.destination;
      const servedName = `${plan.rollupSourceName}__served`;
      const declaration = `${storedRollupDeclaration(plan, binding, servedName, modelDef)}
${sourceOverRollup(plan, use, servedName, "")}`;
      // The served copy's names, imported from its files: a definition it
      // left out across connections is not one of them.
      const imports = authorNameImports(
         basis.modelDef,
         ctx.queryModelURL,
         pathToFileURL(ctx.modelFile).href,
         { alias: new Map([[use.localName, parentAlias(plan)]]) },
      );
      const overlay = new Map(basis.files);
      overlay.set(
         overlayKey(ctx.queryModelURL.href),
         `${flagLine(modelDef)}
${imports}

${declaration}
`,
      );
      const { runtime, importBaseURL } = await createModelRuntime(
         ctx.packagePath,
         ctx.modelPath,
         basis.config,
         { overlay },
      );
      const materializer = runtime.loadModel(ctx.queryModelURL, {
         importBaseURL,
      });
      await materializer.getModel();
      return { materializer };
   }

   /**
    * Compile a base variant: the query's run target redeclared over the
    * rollup in an overlay of ITS OWN DECLARING FILE
    * (`declaring_file_variant.ts`), so it keeps the author's `sourceID` and
    * carries the run target's gates as note lines, which `govern()` grafts
    * and proves on the variant (R3) when the router lets it past an
    * entry-point gate. A colocated rollup's build source is imported from
    * the companion beside its base's declaring file, generated over the copy
    * of that file when it is the overlaid one, so its build SQL (and so its
    * address) is the build plan's.
    */
   private async compileBase(
      plan: RollupVariantPlan,
      use: Extract<VariantUse, { kind: "base" }>,
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
      ctx: {
         packagePath: string;
         packageConfig: ModelConnectionInput;
         config: ModelConnectionInput;
         modelPath: string;
         queryModelURL: URL;
      },
   ): Promise<CompiledVariant> {
      const modelDef = this.host.modelDef() as ModelDef | undefined;
      if (!modelDef) return { error: "no model" };
      const servedName = `${plan.rollupSourceName}__served`;
      const base = splitSourceID(plan.baseSourceID);
      const companion = base ? rollupCompanionURLOf(base.url) : undefined;
      // The views the variant declares: the query's, and each a refinement
      // among them builds on (variantAdditions).
      let declaredViews: readonly string[] = use.views;
      const variant = await compileDeclaringFileVariant({
         packagePath: ctx.packagePath,
         modelPath: ctx.modelPath,
         packageConfig: ctx.packageConfig,
         config: ctx.config,
         modelDef,
         sourceID: use.sourceID,
         copySuffix: ".rollup-author.malloy",
         queryModelURL: ctx.queryModelURL,
         flags: flagLine(modelDef),
         declare: async ({
            gates: authorGates,
            sourceName,
            copyName,
            declaringURL,
            addFile,
         }) => {
            // malloy-text: the source's own gate expressions, as the author wrote them, one per line (see gateLines)
            const gates = authorGates;
            // What the run target adds over the rollup's fields: an
            // extension's dimensions over the grain, and the views the
            // query reads, as the author wrote them.
            const added = await variantAdditions(use, (url) =>
               this.readAuthorText(ctx, url),
            );
            if ("error" in added) return added;
            declaredViews = added.views;
            const over = sourceOverRollup(
               plan,
               { ...use, localName: sourceName },
               servedName,
               added.text,
            );
            if (table.binding) {
               const stored = storedRollupDeclaration(
                  plan,
                  table.binding,
                  servedName,
                  modelDef,
               );
               return `${stored}
${gates}${over}`;
            }
            // Colocated: the build source from the base's companion, so the
            // build manifest substitutes its table.
            let baseFile: string | undefined;
            try {
               baseFile = base ? fileURLToPath(base.url) : undefined;
            } catch {
               baseFile = undefined;
            }
            if (!companion || !baseFile) return { error: "no companion" };
            const overlaid =
               overlayKey(pathToFileURL(baseFile)) === overlayKey(declaringURL);
            const text = generateRollupBuildFile(
               [plan],
               overlaid ? copyName : path.basename(baseFile),
            );
            if (!text) return { error: "no companion" };
            addFile(companion, text);
            const built = `${plan.rollupSourceName}__built`;
            return `import { ${identifier(built)} is ${identifier(plan.rollupSourceName)} } from ${doubleQuotedString(relativeImport(declaringURL, companion.href) ?? "")}
source: ${identifier(servedName)} is ${identifier(built)} extend {${rollupStatements(plan, modelDef)}
}
${gates}${over}`;
         },
      });
      // Each view read back from the author's file must compile to the view
      // the author's model holds: the file may have changed since it
      // compiled (an edit not yet reloaded), and the variant would then
      // answer a query the caller did not run.
      const variantDef = variant.modelDef?.contents?.[use.localName];
      // Every view declared, the ones a refinement builds on included.
      for (const view of declaredViews) {
         const theirs = turtleNamed(use.struct, view);
         const ours = turtleNamed(variantDef, view);
         if (
            !theirs ||
            !ours ||
            pipelineSignature(theirs.pipeline) !==
               pipelineSignature(ours.pipeline)
         ) {
            return {
               error: `view \`${view}\` does not read back as the model compiled it`,
            };
         }
      }
      return variant;
   }

   /** A file of the author's package, read through the package's own reader. */
   private async readAuthorText(
      ctx: {
         packagePath: string;
         packageConfig: ModelConnectionInput;
         modelPath: string;
      },
      url: string,
   ): Promise<string | undefined> {
      try {
         const { runtime } = await createModelRuntime(
            ctx.packagePath,
            ctx.modelPath,
            ctx.packageConfig,
         );
         const read = await runtime.urlReader.readURL(new URL(url));
         return typeof read === "string" ? read : read.contents;
      } catch {
         return undefined;
      }
   }
}

/** The view `name` of a compiled source, if it declares one. */
function turtleNamed(
   struct: unknown,
   name: string,
): { pipeline?: unknown } | undefined {
   const fields = (struct as { fields?: unknown[] } | undefined)?.fields;
   if (!Array.isArray(fields)) return undefined;
   return (fields as { type?: unknown; name?: unknown; as?: unknown }[]).find(
      (f) => f.type === "turtle" && (f.as ?? f.name) === name,
   ) as { pipeline?: unknown } | undefined;
}

/**
 * What a compiled pipeline records of its context rather than of the query:
 * where it was written, the notes its fields inherit, and the result's shape
 * over the source it ran on (a stored column's connection and number type).
 */
const PIPELINE_CONTEXT_KEYS = new Set([
   "location",
   "at",
   "referencedAt",
   "annotations",
   "outputStruct",
]);

/**
 * A view's compiled pipeline without where it was written: the same view
 * text compiled in two models differs only in its locations.
 */
function pipelineSignature(pipeline: unknown): string {
   return JSON.stringify(pipeline, (key, value) =>
      PIPELINE_CONTEXT_KEYS.has(key) ? undefined : value,
   );
}

/**
 * What an extension adds over the base its rollups aggregate (row 22), or a
 * {@link LegDecline} when that is more than fields: a `where:` of its own
 * (its rows are not the base's, `GRAIN_TOO_COARSE`), or a `timezone:` of its
 * own (`TIME_ZONE`). A gate of its own never reaches here: the router skips
 * the leg for a gated entry point (`CONTRACT`).
 */
function checkExtension(struct: object, baseDef: unknown): SourceDelta {
   const delta = sourceDelta(struct, baseDef);
   if (delta.addsFilter) {
      throw new LegDecline(
         "rollup_grain_too_coarse",
         "the query's source filters its rows (`where:`) beyond the source the rollup aggregates",
         { shapeCheck: true },
      );
   }
   const zone = (d: unknown) =>
      (d as { queryTimezone?: unknown } | undefined)?.queryTimezone;
   if (zone(struct) !== zone(baseDef)) {
      throw new LegDecline(
         "rollup_time_zone",
         "the query's source sets its own time zone, and the rollup was built in its base's",
         { shapeCheck: true },
      );
   }
   return delta;
}

/**
 * The extension's own dimensions the variant re-declares for `plan`: each
 * public scalar dimension it adds whose expression reads only the rollup's
 * grain dimensions and dimensions already taken (row 22), in an order where
 * each follows what it reads. Anything else it adds (a measure, a join, a
 * dimension over a field the rollup lacks) is not declared, so a query that
 * reads it does not compile against the variant (`MISSING_FIELD`) or fails
 * the grain check first. A grain dimension or stored measure the extension
 * holds differently from its base (a `rename:` onto its name) declines the
 * rollup: the variant's field would not be the one the query reads.
 */
function extensionDimensions(
   extension: NonNullable<RollupQuery["extension"]>,
   plan: RollupVariantPlan,
   measures: ReadonlySet<string>,
): ExtensionDimension[] {
   const delta = extension.delta;
   if (!delta) return [];
   const grain = new Set(plan.grainDimensions);
   // Any name the extension binds to another field, not only a grain
   // dimension or stored measure: those read their base's fields by name, so
   // `rename: reg is region; dimension: region is 'east'` changes what a
   // stored `amount.sum() { where: region = 'east' }` reads on the extension,
   // while the rollup stores it as the base defines it.
   const clash =
      delta.changed.find((n) => grain.has(n) || measures.has(n)) ??
      delta.changed[0];
   if (clash !== undefined) {
      throw new LegDecline(
         "rollup_missing_field",
         `the query's source redefines \`${clash}\`, which the rollup's fields read as its base defines it`,
         { shapeCheck: true },
      );
   }
   interface AddedField {
      name?: unknown;
      as?: unknown;
      type?: unknown;
      join?: unknown;
      code?: unknown;
      expressionType?: unknown;
      accessModifier?: unknown;
      annotations?: Parameters<typeof viewNoteTexts>[0];
      e?: unknown;
   }
   const candidates = (delta.added as AddedField[]).filter(
      (f) =>
         f.type !== "turtle" &&
         f.join === undefined &&
         typeof f.code === "string" &&
         // Raw SQL names columns the compiler records no read of.
         !readsRawSql(f.e) &&
         (f.expressionType === undefined || f.expressionType === "scalar") &&
         (f.accessModifier == null || f.accessModifier === "public"),
   );
   const known = new Set(grain);
   const out: ExtensionDimension[] = [];
   for (let grew = true; grew; ) {
      grew = false;
      for (const f of candidates) {
         const name =
            typeof f.as === "string"
               ? f.as
               : typeof f.name === "string"
                 ? f.name
                 : undefined;
         if (name === undefined || known.has(name)) continue;
         const reads = fieldNameReads(f);
         if (!reads || !reads.every((r) => known.has(r))) continue;
         known.add(name);
         out.push({
            name,
            code: f.code as string,
            notes: viewNoteTexts(f.annotations),
         });
         grew = true;
      }
   }
   return out;
}

/**
 * The ` extend { … }` that declares the target's extension dimensions and the
 * views the query reads on the variant (empty when there are none), with the
 * views it declares, or why it cannot. Each view is its definition as the
 * author wrote it, read at its compiled location ({@link definitionTextAt}),
 * so the view's own query is what compiles against the rollup and what
 * coverage reads; one that does not read back as its own definition leaves
 * the variant uncompiled. A view defined over another (`view: a is b + { … }`,
 * a refinement, at any depth) names it in its text, so each view of the
 * source its definition names is declared before it: the compiled pipeline
 * holds `b` expanded, without its name. A name that is not the base view is
 * declared too (one view more, never one fewer), and every view declared is
 * held to the compiled-pipeline check in {@link RollupVariantServe.compileBase}.
 */
async function variantAdditions(
   target: VariantTarget,
   readText: (url: string) => Promise<string | undefined>,
): Promise<{ text: string; views: string[] } | { error: string }> {
   const lines: string[] = [];
   for (const d of target.dimensions) {
      // malloy-text: the author's own notes on the field, single lines, `#@` left out (viewNoteTexts)
      const notes = d.notes.map((n) => `  ${n}`);
      lines.push(...notes);
      // malloy-text: the compiler's record of the author's own expression, over names the variant declares
      const expression = d.code;
      lines.push(`  dimension: ${identifier(d.name)} is ${expression}`);
   }
   const fields = ((target.struct as { fields?: unknown[] }).fields ?? []) as {
      type?: unknown;
      name?: unknown;
      as?: unknown;
      location?: { url?: unknown; range?: TextRange };
      annotations?: Parameters<typeof viewNoteTexts>[0];
   }[];
   const turtles = new Map<string, (typeof fields)[number]>();
   for (const f of fields) {
      const name = f.as ?? f.name;
      if (f.type === "turtle" && typeof name === "string") turtles.set(name, f);
   }
   const declared: string[] = [];
   const done = new Set<string>();
   const open = new Set<string>();
   // Each view after the views its definition names, so each is declared
   // before a view refines it.
   const declare = async (
      view: string,
      depth: number,
   ): Promise<{ error: string } | undefined> => {
      if (done.has(view)) return undefined;
      if (open.has(view) || depth > 64) {
         return { error: `the definition of view \`${view}\` names itself` };
      }
      open.add(view);
      const field = turtles.get(view);
      const url = field?.location?.url;
      const range = field?.location?.range;
      const text = typeof url === "string" ? await readText(url) : undefined;
      // malloy-text: the author's own view definition, read whole at its compiled location (definitionTextAt checks its name and brackets)
      const definition =
         text !== undefined && range
            ? definitionTextAt(text, range, view)
            : undefined;
      if (definition === undefined) {
         return { error: `the definition of view \`${view}\` was not read` };
      }
      for (const name of malloyIdentifiers(definition)) {
         if (name === view || !turtles.has(name)) continue;
         const problem = await declare(name, depth + 1);
         if (problem) return problem;
      }
      open.delete(view);
      done.add(view);
      declared.push(view);
      // malloy-text: the author's own notes on the view, single lines, `#@` left out (viewNoteTexts)
      const notes = viewNoteTexts(field?.annotations).map((n) => `  ${n}`);
      lines.push(...notes);
      lines.push(`  view: ${definition}`);
      return undefined;
   };
   for (const view of target.views) {
      const problem = await declare(view, 0);
      if (problem) return problem;
   }
   // malloy-text: each line above is a name through identifier, or the author's own text marked where it is read
   const text = lines.length > 0 ? ` extend {\n${lines.join("\n")}\n}` : "";
   return { text, views: declared };
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
function rollupSourceID(plan: RollupVariantPlan): string | undefined {
   const declared = splitSourceID(plan.baseSourceID);
   const companion = declared ? rollupCompanionURLOf(declared.url) : undefined;
   return companion ? `${plan.rollupSourceName}@${companion.href}` : undefined;
}

/**
 * A `storage=` rollup declared over its table in the destination, as
 * `servedName`: a `type:` of its stored columns (each grain column with its
 * dimension's notes, as the served copy's stored columns carry theirs), the
 * `virtual()` source on the destination's served-copy alias, its
 * timeframes restated and its merged measures.
 */
function storedRollupDeclaration(
   plan: RollupVariantPlan,
   binding: ServeBinding,
   servedName: string,
   modelDef: unknown,
): string {
   const typeName = `${plan.rollupSourceName}__type`;
   const grain = grainColumns(plan, modelDef);
   // A `type:` field keeps its annotations, so a result column reads them as
   // live's does. A timeframe column's go on its redefined field
   // (timeframeRestatements) instead.
   const grainNotes = new Map(
      grain.map((g) => [g.name, g.timeframe ? [] : g.notes]),
   );
   const columns = binding.schema.map((c) => {
      const notes = grainNotes.get(c.name);
      return {
         name: c.name,
         type: c.type,
         ...(notes && notes.length > 0 ? { notes } : {}),
      };
   });
   // malloy-text: the served copy's timeframe emitter, names through identifier
   const timeframes = timeframeRestatements(grain);
   // malloy-text: the served copy's type emitter; rollupStatements re-emits the grammar's parse through malloy_text
   return `${typeDeclaration({ typeName, columns })}
source: ${identifier(servedName)} is ${identifier(servedCopyAlias(binding.destinationName))}.virtual(${stringLiteral(binding.virtualHandle)})::${identifier(typeName)} extend {${timeframes}${rollupStatements(plan, modelDef)}
}`;
}

/**
 * The served copy's decline of a join leg's query in a storage destination,
 * as the rollup skip the path report reads: a query its rules send live
 * keeps their reason (`COUNT_SEMANTICS`, `MISSING_FIELD`); a
 * source it does not answer from the destination (opted out, or flagged by
 * the load-time checks) leaves the rollup across connections.
 */
function servedCopySkip(error: LegDecline, join: string): LegDecline {
   switch (error.code) {
      case "served_copy_count_semantics":
      case "served_copy_pair_shape":
         return new LegDecline(
            "rollup_count_semantics",
            `the query's source is read from a stored table that cannot answer it exactly with \`${join}\` joined to its rollup`,
         );
      case "served_copy_joined_annotations":
         return new LegDecline(
            "rollup_missing_field",
            "the query returns a field whose annotations the stored table cannot carry",
         );
      default:
         return new LegDecline(
            "rollup_cross_connection",
            `the rollup of \`${join}\` is in a storage destination, and the query's source is not answered from it`,
         );
   }
}

/** A source's declared primary key, if it has one. */
function primaryKeyOf(def: unknown): string | undefined {
   const key = (def as { primaryKey?: unknown } | undefined)?.primaryKey;
   return typeof key === "string" ? key : undefined;
}

/** The author's source, imported for a join leg under this name. */
function parentAlias(plan: RollupVariantPlan): string {
   return `${plan.rollupSourceName}__parent`;
}

/**
 * The variant's declaration of the query's source over `servedName`: the
 * rollup itself under the source's name, or (A7) the author's source with
 * the join re-pointed at the rollup on its key. `include { *; except: … }`
 * keeps every other field's access modifier and annotations as written.
 */
function sourceOverRollup(
   plan: RollupVariantPlan,
   use: VariantUse,
   servedName: string,
   /** For `base`: the ` extend { … }` of {@link variantAdditions}. */
   additions: string,
): string {
   if (use.kind === "base") {
      // malloy-text: variantAdditions emits names through identifier, and the author's own notes, expressions and view definitions (marked there)
      const extension = additions;
      return `source: ${identifier(use.localName)} is ${identifier(servedName)}${internalPartials(plan)}${extension}`;
   }
   const { join } = use.leg;
   const keyword = use.leg.relationship === "one" ? "join_one" : "join_many";
   const joined = `${plan.rollupSourceName}__joined`;
   const j = identifier(join.name);
   // Over the served copy, a join it left out across connections is not
   // there to leave out.
   const kept =
      use.destination && !use.destination.keepsJoin
         ? ""
         : ` include { *; except: ${j} }`;
   return `source: ${identifier(joined)} is ${identifier(servedName)}${internalPartials(plan)}
source: ${identifier(use.localName)} is ${identifier(parentAlias(plan))}${kept} extend {
  ${keyword}: ${j} is ${identifier(joined)} on ${j}.${identifier(join.keyColumn)} = ${identifier(join.baseKey)}
}`;
}

/** The first level of `levels` (or a nest in it) that is not a `reduce`, by its stage word. */
function nonReduceLevel(levels: QueryLevelFacts): string | undefined {
   if (levels.kind !== "reduce") {
      return levels.kind === "project"
         ? "select:"
         : levels.kind === "index"
           ? "index:"
           : levels.kind;
   }
   for (const f of levels.fields) {
      if (!f.isNest) continue;
      if (!f.nest) return "nest";
      const inner = nonReduceLevel(f.nest);
      if (inner !== undefined) return inner;
   }
   return undefined;
}

/**
 * The rollup's merged measures, each with the author's measure's annotation
 * lines (`#(classification)`, `#(doc)`, a render tag such as `# currency`):
 * a measure's notes are its own, kept on the result column, so a query's
 * result carries what live's does.
 */
function rollupStatements(plan: RollupVariantPlan, modelDef: unknown): string {
   const notes = baseFieldNotes(plan, modelDef);
   // malloy-text: the author's own notes, then each merged measure re-emitted from the grammar's parse through malloy_text
   const measures = plan.rollupMeasures
      .map(
         (m) =>
            (notes.get(m.name) ?? [])
               .map((n) => noteSource(n, "    "))
               .join("") +
            `    ${identifier(m.name)} is ${emitMeasureExpr(m.merged)}`,
      )
      .join("\n");
   return `\n  measure:\n${measures}`;
}

/** The base's fields' annotation lines, by name. */
function baseFieldNotes(
   plan: RollupVariantPlan,
   modelDef: unknown,
): Map<string, string[]> {
   const base = SourceMap.of(modelDef).node(plan.baseSourceID)?.def as
      | { fields?: { name?: unknown; as?: unknown }[] }
      | undefined;
   return new Map(
      (base?.fields ?? []).map((f) => [String(f.as ?? f.name), fieldNotes(f)]),
   );
}

/**
 * ` include { internal: … public: * }`: the partial columns, readable by the
 * merged measures and by no query, so a caller cannot aggregate them; and an
 * `internal` grain dimension a gate reads (A4), redeclared with the author's
 * modifier, readable by the grafted gate and by no query, as on live.
 */
function internalPartials(plan: RollupVariantPlan): string {
   const hidden = [
      ...plan.rollupMeasures.flatMap((m) => m.partials.map((p) => p.column)),
      ...(plan.internalGrain ?? []),
   ]
      .map((n) => identifier(n))
      .join(", ");
   return ` include { internal: ${hidden} public: * }`;
}

/**
 * The rollup's grain columns, each with the author's timeframe if it has one
 * and its dimension's annotation lines.
 */
function grainColumns(
   plan: RollupVariantPlan,
   modelDef: unknown,
): { name: string; timeframe?: string; notes: string[] }[] {
   const base = SourceMap.of(modelDef).node(plan.baseSourceID)?.def as
      | { fields?: { name?: unknown; as?: unknown; timeframe?: unknown }[] }
      | undefined;
   const timeframes = new Map<string, string>();
   for (const f of base?.fields ?? []) {
      if (typeof f.timeframe === "string") {
         timeframes.set(String(f.as ?? f.name), f.timeframe);
      }
   }
   const notes = baseFieldNotes(plan, modelDef);
   return plan.grainDimensions.map((name) => {
      const timeframe = timeframes.get(name);
      return {
         name,
         ...(timeframe === undefined ? {} : { timeframe }),
         notes: notes.get(name) ?? [],
      };
   });
}

/**
 * The query model: the author's model's names, imported from the files that
 * declare them, and `declaration`, which declares `localName` itself.
 */
function queryModelText(
   modelDef: unknown,
   queryModelURL: URL,
   modelURL: string,
   names: AuthorImportOptions,
   declaration: string,
): string {
   // Every name but the one the variant declares (or that name under an
   // alias, for a join leg), givens included, so a query that supplies or
   // reads a given compiles against the rollup.
   const imports = authorNameImports(modelDef, queryModelURL, modelURL, names);
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
 * A rollup skipped for the query's gate (`CONTRACT`). Its detail never names
 * the gate's field or text: a caller may not learn a gate's shape from why
 * its query was answered live.
 */
function contractSkip(): LegDecline {
   return new LegDecline(
      "rollup_contract",
      "the query's source has an access rule this rollup cannot apply",
      { shapeCheck: true },
   );
}

/**
 * The gates the live entry point carries (the governor's walk: own,
 * inherited, a derivation's base), or undefined when it has none. A walk
 * that cannot be read counts as a gate no rollup proves.
 */
function liveEntryPointGates(
   struct: unknown,
   modelDef: unknown,
): EnforcedGates | undefined {
   try {
      const gates = enforcedGatesOf(
         collectEntryPointGates(
            struct as SourceDef,
            modelDef as ModelDef,
            new Set(),
            true,
         ),
      );
      return gates.authorize.length + gates.accessFilter.length > 0
         ? gates
         : undefined;
   } catch {
      return { authorize: ["false"], accessFilter: [] };
   }
}

/**
 * The pre-proof for a query let past an entry-point gate (R3 for rollups):
 * throw `CONTRACT` unless the variant's run target is the author's source (its
 * `sourceID`), the gates written on it are exactly the live entry point's
 * (by route and expression, `sameGateSet`: one rule with `govern()`'s
 * re-proof), and every field those gates read is one of the rollup's grain
 * dimensions, so filtering its groups equals filtering the base's rows
 * (table 1).
 */
function proveGates(
   plan: RollupVariantPlan,
   query: Pick<RollupQuery, "gates" | "sourceID" | "struct">,
   routed: Awaited<ReturnType<typeof readPreparedQuery>>,
   variantModelDef: ModelDef | undefined,
): void {
   const live = query.gates;
   if (!live) return;
   const model = routed.modelDef ?? variantModelDef;
   const struct = model
      ? resolveStructRefSource<{ sourceID?: unknown }>(routed.structRef, model)
      : undefined;
   if (!struct || struct.sourceID !== query.sourceID) throw contractSkip();
   let written: EnforcedGates;
   try {
      written = enforcedGatesOf(
         collectEntryPointGates(
            struct as unknown as SourceDef,
            model as ModelDef,
            new Set(),
            true,
         ),
      );
   } catch {
      throw contractSkip();
   }
   if (!sameGateSet(live, written)) throw contractSkip();
   const fields = new Map(
      (
         ((query.struct as { fields?: unknown[] }).fields ?? []) as {
            name?: unknown;
            as?: unknown;
         }[]
      ).map((f) => [String(f.as ?? f.name), f]),
   );
   const grain = new Set(plan.grainDimensions);
   for (const expr of [...written.authorize, ...written.accessFilter]) {
      for (const id of malloyIdentifiers(expr)) {
         if (fields.has(id) && !grain.has(id)) throw contractSkip();
      }
   }
}

/**
 * Throw a {@link LegDecline} unless every aggregate the compiled query computes
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
 * filtered measure, a `calculate:`, a `having:`) is held to every measure's.
 */
export function checkCoverage(
   plan: RollupVariantPlan,
   levels: QueryLevelFacts | undefined,
): void {
   if (!levels) {
      throw new LegDecline(
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
         throw new LegDecline(
            "rollup_count_semantics",
            `a ${level.kind === "project" ? "select:" : level.kind === "index" ? "index:" : `${level.kind}`} stage reads the source's rows, which the rollup does not keep`,
         );
      }
      if (level.havings.some((h) => h.computesAggregate)) {
         throw new LegDecline(
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
            throw new LegDecline(
               "rollup_count_semantics",
               `\`${f.name}\` aggregates the rollup's rows, not a stored measure`,
            );
         }
         if (f.ungrouped && (fanOut.size > 0 || anyMeasureFanOut.size > 0)) {
            throw new LegDecline(
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
            f.expressionType !== "scalar" &&
            f.expressionType !== "scalar_analytic"
         ) {
            // Any field the query defines that may read a measure: an
            // aggregate expression, an ungrouped one, or a `calculate:` over
            // a measure (`lag(total)`, `aggregate_analytic`), which computes
            // that measure at this level even when no `aggregate:` names it.
            // An unrecorded type fails closed the same way.
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
            throw new LegDecline(
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
            throw new LegDecline(
               "rollup_count_semantics",
               `the nest \`${f.name}\` could not be read`,
            );
         }
         visit(f.nest, fixed);
      }
   };
   visit(levels, new Set());
}

/**
 * The switches {@link planSourceRollups} reads, as a memo key: `#(preaggregate)`
 * with `PUBLISHER_MATERIALIZE` (`readsV2`), and whether a gated base's rollup
 * is read.
 */
function planSwitches(): string {
   return [getMaterializeEnabled(), getMaterializeGatedSources()].join("/");
}

/**
 * The name a model gives the source `structRef` holds inline, when it is that
 * named source unchanged: Malloy inlines a source a model imports without
 * exporting it (#6646), so the query's struct is an object, not a name. It is
 * the named source only when the model's entry of the struct's `as` has the
 * same `sourceID`, fields and filters; an inline extension (`orders extend {
 * … } -> …`) changes one of them and stays unnamed.
 */
function importedSourceName(
   structRef: unknown,
   modelDef: { contents?: Record<string, unknown> } | undefined,
): string | undefined {
   if (structRef === null || typeof structRef !== "object") return undefined;
   const struct = structRef as {
      as?: unknown;
      sourceID?: unknown;
      fields?: unknown;
      filterList?: unknown;
   };
   if (typeof struct.as !== "string" || typeof struct.sourceID !== "string") {
      return undefined;
   }
   const named = modelDef?.contents?.[struct.as] as typeof struct | undefined;
   if (!named || named.sourceID !== struct.sourceID) return undefined;
   const same = (a: unknown, b: unknown) =>
      JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
   return same(named.fields, struct.fields) &&
      same(named.filterList, struct.filterList)
      ? struct.as
      : undefined;
}
