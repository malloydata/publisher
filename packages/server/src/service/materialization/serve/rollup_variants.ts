// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rollup variants: the router's rollup leg for `#(preaggregate)`.
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
 * letting Malloy's build manifest substitute its table, or, once its table
 * was altered outside its build, through `virtual()` on its own connection
 * as a `storage=` rollup is read. A stored column no read gives live's
 * values (`storedRollupReads`, and the bind's own left-out columns) is not
 * declared, with every merged measure over it, so only a query reading one
 * declines (`MISSING_FIELD`).
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
 * - a `timezone:` it sets, at any stage or in any nest, changes nothing it
 *   reads from the rollup (A5: the build truncated and compared in the
 *   model's default time zone): it reads no temporal field, directly or
 *   through a dimension, measure or the base's `where:`, and nothing through
 *   a join; else `TIME_ZONE`;
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
 * With `materialize.gatedSources` on, or under G3 with it off for a colocated
 * rollup or for an entry point whose gates are locks reading no column on
 * either tier (the gate policy, `domain/gate_policy.ts`), the router lets the leg
 * past a gated entry point; a rollup then answers only when it was built for
 * its gate
 * (`rollupGateEnforcedAtRead`) and the pre-proof holds (`proveGates`): the
 * variant's run target is the author's `sourceID`, its gates equal the live
 * entry point's (`sameGateSet`, the rule `govern()` re-proves with) and each
 * field they read is a grain dimension. Else it is skipped `CONTRACT`, with a
 * detail that never names the gate. `govern()` then grafts the gates on the
 * variant's own scope; a denial there is a defect, not a routing outcome. A
 * join branch (A7) carries no gate, so it declines past one.
 *
 * Rollups are offered in the plan's order, cheapest read first (coarsest,
 * then colocated before `storage=`); the first that answers is used. A
 * `#(preaggregate)` rollup is read only with `materialize.enabled` on; a v1
 * `#@ preaggregate` one is read either way.
 */

import { requireUnique, verdictOf, type KeyVerdict } from "../domain/key_check";
import {
   givenWhereCodes,
   primaryKeyOf,
   queryTimezoneOf,
   recordedTimezoneOf,
   timezoneOf,
} from "../../governance/ir/compiled_model";
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
import { EMPTY_BUILD_MANIFEST } from "@malloydata/malloy";
import type { ModelConnectionInput } from "../../model_runtime";
import { overlayKey } from "../../overlay_key";
import { quoteManifestTablePath } from "../../quoting";
import { ownModelNoteObjects } from "../../annotations";
import { routeOfNote } from "../../annotation_reader";
import { experimentalFlagKeys } from "../../motly";
import { doubleQuotedString, identifier } from "../../malloy_text";
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
import { packageFiles } from "../text/package_files";
import { fieldPathsIn, gateIdentifiers } from "./gate_reads";
import {
   emitMeasureExpr,
   generateRollupBuildFile,
   packageRelativeSourceID,
   readsBuildCopy,
   rollupBuildCopyName,
   rollupBuildCopyText,
   rollupCompanionURLOf,
   viewNoteTexts,
} from "../text/read_text";
import { splitSourceID, type RollupVariantPlan } from "../domain/table_plans";
import { SourceMap, type SourceNode } from "../text/source_map";
import {
   authorNameImports,
   relativeImport,
   type AuthorImportOptions,
} from "./author_imports";
import {
   checkJoinCoverage,
   joinedKeyProblem,
   joinRollupBranches,
   parentKeyProblem,
   unmatchedRowProblem,
   type JoinRollupBranch,
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
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";
import { type ColumnCast, storedTypedSource } from "./stored_typed_source";
import { alteredColumnsDetail } from "./view_variants";
import {
   canonicalStoredSchema,
   nativeTypeLookup,
   storedColumnReadPlan,
   storedTypeServesAs,
} from "../stored_column_types";
import { gateSwitches, getMaterializeEnabled } from "../../../config";
import { boundTablePath, buildVirtualMap, sqlReadsTable } from "./bindings";
import {
   type Binding,
   heldReadBinding,
   isColocated,
   type Pinned,
   storageLookupOf,
} from "../domain/binding";
import type { ServedCopyJoinBasis } from "./served_copy_serve";
import {
   isDeclineOf,
   LegDecline,
   withMemberDeclines,
} from "../domain/leg_decline";
import type { MaterializedTableCatalog } from "./query_path";
import { queryShapeOf } from "./query_path";
import { STORAGE_DIALECT } from "../domain/dialect";

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
    * model: the join branch answers a query on a source stored in a storage
    * destination there, through a joined source's rollup in the same one
    * (known limitation 26).
    */
   joinRollupBasis(pinned: Pinned): Promise<ServedCopyJoinBasis | undefined>;
   /**
    * The rollups one source declares, as the planner reads them
    * (`plan/rollup_plan.ts`): passed in by the model, since serving never
    * imports the planner.
    */
   planSourceRollups(
      map: SourceMap,
      node: SourceNode,
      ctx: {
         packageRelativeSourceID(sourceID: string): string;
         readsPreaggregateDeclarations?: boolean;
      },
   ): { plans: RollupVariantPlan[] };
   /**
    * Whether a planned rollup's base gate is enforced when it is read, by the
    * planner's one answer (`plan/rollup_plan.ts`), which the build plan and
    * the bind ask too.
    */
   rollupGateEnforcedAtRead(plan: RollupVariantPlan): boolean;
}

/** A rollup's answer, for the router. */
interface RollupAnswer {
   runnable: QueryMaterializer;
   /** The rollup that answered: its name, for the path report. */
   rollupSourceName: string;
   /** For a `storage=` rollup: how its `virtual()` handle resolves. */
   virtualMap?: VirtualMap;
   /** For a `storage=` rollup: its binding. */
   binding?: Binding;
   /**
    * Every stored table the answer reads, when that is more than the
    * rollup's: a join branch answered in a storage destination reads the query
    * source's table there too, whose binding decides whether a failed run
    * may be retried live.
    */
   bindings?: Binding[];
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
        branch: JoinRollupBranch;
        /**
         * Set when the query's source is read from the rollup's storage
         * destination through the served copy (known limitation 26): the
         * variant is compiled over the copy's files, and `keepsJoin` says
         * whether the copy's source still declares the join (it leaves a
         * join to a source not stored there out across connections).
         */
        destination?: { basis: ServedCopyJoinBasis; keepsJoin: boolean };
     };

/** A join branch that does not apply to this query: it reads nothing through the join. */
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
   /**
    * Why a `storage=` rollup's stored columns cannot read as live's
    * ({@link storedRollupReads}), so the query answers live.
    */
   refusal?: string;
   /**
    * Why a query that does not compile against the variant may not: the
    * stored columns it leaves out, which no read gives live's values.
    */
   unreadDetail?: string;
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
   /**
    * Set when the query sets its own `timezone:`: every field path it reads,
    * expanded (undefined when one could not be read), for `zoneDecline`.
    */
   zoned?: { usage: string[][] | undefined };
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
   /** Bumped when the context or the destinations change: a graft cache scope. */
   private generation = 0;
   private compiled = new Map<string, Promise<CompiledVariant>>();
   /**
    * Compiled variants whose failure was warned about: each is cached for its
    * binding, so every later attempt would repeat the same warning.
    */
   private warnedFailures = new WeakSet<CompiledVariant>();
   /** The snapshot bindings {@link compiled} was compiled over. */
   private compiledFor: readonly Binding[] | undefined;
   private planMemo?: {
      modelDef: unknown;
      /**
       * The switches the plans were read under ({@link planSwitches}): a
       * plan read with one is not the plan with the other.
       */
      switches: string;
      byBase: Map<string, RollupVariantPlan[]>;
      /** A7: each source's join branches, by its `sourceID`. */
      joinBranches?: Map<string, ReturnType<typeof joinRollupBranches>>;
   };

   constructor(private readonly host: RollupVariantHost) {}

   setContext(packagePath: string, packageConfig: ModelConnectionInput): void {
      this.packagePath = packagePath;
      this.packageConfig = packageConfig;
      this.planMemo = undefined;
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
            const { plans } = this.host.planSourceRollups(map, node, {
               packageRelativeSourceID: (id) =>
                  packageRelativeSourceID(packagePath, id),
               // v1 `#@ preaggregate` rollups are always routed;
               // `#(preaggregate)` ones only with the materialize switch on.
               readsPreaggregateDeclarations: getMaterializeEnabled(),
            });
            if (plans.length > 0) byBase.set(node.sourceID, plans);
         }
      }
      this.planMemo = { modelDef, switches, byBase };
      return byBase;
   }

   /**
    * The `storage=` rollup tables of a pinned snapshot, for the path report:
    * every one bound, or only those read at the pin.
    */
   rollupBindings(pinned: Pinned, which: "bound" | "fresh"): Binding[] {
      return (
         which === "bound" ? pinned.snapshot.bindings : pinned.fresh
      ).filter((b) => b.kind === "rollup" && b.tier === "storage");
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
      /** The query's pinned tables: its rollup tables and build manifest. */
      pinned: Pinned;
   }): Promise<RollupAnswer> {
      this.compiledOver(args.pinned);
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
      const branches =
         sourceID && localName ? this.joinBranchesOf(sourceID) : [];
      if (
         !localName ||
         !sourceID ||
         !struct ||
         (!target && branches.length === 0)
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
      // truncated the grain's times in the model's zone, so a zoned query is
      // read from a rollup only where it reads no time (`zoneDecline`).
      const zoned = setsTimezone(prepared.query)
         ? { usage: queryFieldUsagePaths(prepared) }
         : undefined;
      // The reason reported is the first rollup's that got past the grain
      // and measure check (the one closest to answering), else the first's.
      let first: LegDecline | undefined;
      let closest: LegDecline | undefined;
      // Each rollup's own decline, by its synthesized source name: the path
      // report gives each rollup its own reason (P2-32).
      const members = new Map<string, LegDecline>();
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
            for (const plan of plans) members.set(plan.rollupSourceName, error);
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
         ...(zoned ? { zoned } : {}),
         ...(extension ? { extension } : {}),
      };
      for (const plan of ownPlans) {
         try {
            return await this.attemptOne(plan, query, args);
         } catch (error) {
            if (!isDeclineOf(error, "rollup")) throw error;
            first ??= error;
            if (!error.shapeCheck) closest ??= error;
            members.set(plan.rollupSourceName, error);
         }
      }
      // Then a joined source's rollup, read through the join (A7): R1's
      // join branches, inside the rollup step.
      const usage = branches.length > 0 ? queryFieldUsagePaths(prepared) : [];
      for (const { branch, refusal } of branches) {
         // Through a join, a zoned query is read live (A5).
         if (zoned) {
            const decline = zonedQueryDecline();
            first ??= decline;
            members.set(branch.plan.rollupSourceName, decline);
            continue;
         }
         // A join branch redeclares the author's source around the join, not
         // over its declaring file, so it carries no gate to prove: past an
         // entry-point gate it declines (CONTRACT).
         if (gates) {
            const decline = contractSkip();
            first ??= decline;
            members.set(branch.plan.rollupSourceName, decline);
            continue;
         }
         try {
            return await this.attemptJoin(branch, refusal, {
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
            members.set(branch.plan.rollupSourceName, error);
         }
      }
      const summary = closest ?? first;
      if (!summary) throw new NoRollupForQuery("no rollup");
      throw withMemberDeclines(summary, members);
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
    * The join branches the joins of source `sourceID` offer (A7), each with why it
    * cannot be used, if it cannot. Read through the source map's join
    * readers; memoised with the plans.
    */
   private joinBranchesOf(
      sourceID: string,
   ): ReturnType<typeof joinRollupBranches> {
      const byBase = this.plansByBase();
      const memo = this.planMemo;
      const cached = memo?.joinBranches?.get(sourceID);
      if (cached) return cached;
      const map = SourceMap.of(this.host.modelDef());
      const filter = map.filterJoinsOf(sourceID);
      const joins = map.joinsOf(sourceID);
      const branches = joinRollupBranches(
         joins,
         byBase,
         // Unknown reads count as reading every join.
         filter === undefined || filter.nested
            ? new Set(joins.map((j) => j.name))
            : filter.joins,
         primaryKeyOf(map.node(sourceID)?.def),
      );
      if (memo) {
         memo.joinBranches ??= new Map();
         memo.joinBranches.set(sourceID, branches);
      }
      return branches;
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
      const branches = this.joinBranchesOf(sourceID);
      if (branches.length === 0) return new Set();
      // Only the joins the query reads (every one, when its reads are
      // unreadable): the leg never tries a join the query does not read.
      const usage = queryFieldUsagePaths(prepared);
      return new Set(
         branches
            .filter(
               ({ branch }) =>
                  usage === undefined ||
                  usage.some((p) => p[0] === branch.join.name),
            )
            .map(({ branch }) => branch.plan.baseSourceID),
      );
   }

   /**
    * Answer from a joined source's rollup, read at its grain through the
    * re-pointed join (A7, see join_rollup.ts), or throw a {@link LegDecline}
    * ({@link JoinNotRead} when the query reads nothing through the join).
    */
   private async attemptJoin(
      branch: JoinRollupBranch,
      refusal: ReturnType<typeof joinRollupBranches>[number]["refusal"],
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
      const { plan, join } = branch;
      const name = join.name;
      const declined = joinBranchDecline(name, q.usage, refusal);
      if (declined) throw declined;
      // A rollup in a storage destination is joined there, so only to a
      // query source the served copy reads from that destination too; Malloy
      // joins only within one connection.
      // The destination is the rollup's table's, when it has a fresh one.
      const stored = this.storedIn(plan, q.args.pinned);
      const inDestination =
         stored !== undefined
            ? await this.destinationOf(stored, q.struct, name, q.args.pinned)
            : undefined;
      // The build truncated and filtered times in the joined source's time
      // zone; live reads the joined rows in the query source's.
      const childDef = SourceMap.of(this.host.modelDef()).node(
         plan.baseSourceID,
      )?.def as JoinedSourceDef | undefined;
      const parentZone = recordedTimezoneOf(q.struct);
      if ((recordedTimezoneOf(childDef) ?? "") !== (parentZone ?? "")) {
         throw new LegDecline(
            "rollup_time_zone",
            `\`${name}\` is read in the query source's time zone, and its rollup was built in its own`,
            { shapeCheck: true },
         );
      }
      const grain = new Set(plan.grainDimensions);
      const measures = new Set(plan.rollupMeasures.map((m) => m.name));
      const coarse = joinGrainDecline(name, q.shape, grain, measures);
      if (coarse) throw coarse;
      const missing = joinCoverageDecline(name, q.shape, grain, measures);
      if (missing) throw missing;
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
            branch,
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
      this.checkJoinKeys(branch, q, q.usage, childDef, read, inDestination);
      const table = this.tableOf(plan, q.args.pinned);
      if (!table) {
         throw this.noFreshTable(plan, q.args.pinned);
      }
      if (inDestination) {
         return this.answerJoinInDestination(branch, q, table, inDestination);
      }
      return this.answerJoinColocated(branch, q, table);
   }

   /**
    * The join branch's key checks (known limitation 29): that live's rows and
    * the variant's are the same rows through the re-pointed join, else a
    * {@link LegDecline} (`rollup_count_semantics`). `read` are the joined
    * source's fields the query reads through the join.
    */
   private checkJoinKeys(
      branch: JoinRollupBranch,
      q: Parameters<RollupVariantServe["attemptJoin"]>[2],
      usage: readonly (readonly string[])[],
      childDef: JoinedSourceDef | undefined,
      read: ReadonlySet<string>,
      inDestination:
         | { basis: ServedCopyJoinBasis; source: Binding }
         | undefined,
   ): void {
      const { plan, join } = branch;
      const name = join.name;
      // A join_one the data does not keep unique repeats each parent row
      // once per joined row live, and every aggregate not taken at the join
      // counts the repeats; the rollup holds one row per key. Served only
      // when the rollup's build checked the key unique over the rows it
      // grouped (known limitation 29): then live's rows and the variant's
      // are the same rows.
      if (join.relationship === "one") {
         const outside = aggregateOutsideJoin(q.prepared, q.struct, name);
         const verdict: KeyVerdict =
            outside === undefined
               ? "unknown"
               : this.keyUniqueness(plan, join.keyColumn, q.args.pinned);
         if (outside !== undefined && !requireUnique(verdict)) {
            throw new LegDecline(
               "rollup_count_semantics",
               verdict === "repeated"
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
         branch,
         inDestination
            ? this.storedKeyUniqueness(inDestination.source, join.baseKey)
            : this.parentKeyUniqueness(plan, q.struct, q.args.pinned),
      );
      if (keyProblem !== undefined) {
         throw new LegDecline("rollup_count_semantics", keyProblem);
      }
      // Another join_one the query reads, known not to keep its key unique:
      // live's rows repeat through it, and re-pointing this join changes how
      // Malloy aggregates over those repeats, so the answers part (known
      // limitation 29). Unknown is served, as before the check.
      const repeating = this.nonUniqueJoinOneRead(
         q.struct,
         name,
         usage,
         q.args.pinned,
      );
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
         branch,
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
      const joinedProblem = joinedKeyProblem(
         branch,
         read,
         primaryKeyOf(childDef),
         this.storedIn(plan, q.args.pinned) !== undefined
            ? verdictOf(
                 this.tableOf(plan, q.args.pinned)?.binding?.primaryKeys?.[
                    plan.baseSourceID
                 ],
              )
            : this.parentKeyUniqueness(
                 plan,
                 { sourceID: plan.baseSourceID },
                 q.args.pinned,
              ),
      );
      if (joinedProblem !== undefined) {
         throw new LegDecline("rollup_count_semantics", joinedProblem);
      }
   }

   /**
    * Answer a join branch over a colocated rollup: the query compiled against
    * the variant, read through the build manifest, whose SQL must read the
    * rollup's table; else a {@link LegDecline}.
    */
   private async answerJoinColocated(
      branch: JoinRollupBranch,
      q: Parameters<RollupVariantServe["attemptJoin"]>[2],
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
   ): Promise<RollupAnswer> {
      const { plan, join } = branch;
      const name = join.name;
      // Altered outside its build: its table is read only through a typed
      // source, which a join branch over the substitution cannot take.
      if (table.binding) {
         throw new LegDecline(
            "rollup_missing_field",
            `the rollup of \`${name}\` was altered outside its build, and a join branch reads its table only as built`,
         );
      }
      const variant = await this.compiledVariant(
         plan,
         { kind: "join", localName: q.localName, branch },
         table,
      );
      if (!variant.materializer) {
         this.warnCompileFailure(
            variant,
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
            ? q.args.pinned.compilerManifest.entries[table.sourceEntityId]
                 ?.tableName
            : undefined;
      if (!colocatedTable) {
         throw this.noFreshTable(plan, q.args.pinned, table.sourceEntityId);
      }
      let result;
      try {
         result = await runnable.getPreparedResult({
            ...(q.args.givens ? { givens: q.args.givens as never } : {}),
            buildManifest: q.args.pinned.compilerManifest,
         } as never);
      } catch {
         throw new LegDecline(
            "rollup_missing_field",
            notCompiledDetail(variant),
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
      pinned: Pinned,
   ): Promise<{ basis: ServedCopyJoinBasis; source: Binding }> {
      const across = () =>
         new LegDecline(
            "rollup_cross_connection",
            `the rollup of \`${join}\` is in a storage destination, and the query's source is not read from that destination (it reads its own connection, or its table there is not built or not fresh)`,
            { shapeCheck: true },
         );
      let basis: ServedCopyJoinBasis | undefined;
      try {
         basis = await this.host.joinRollupBasis(pinned);
      } catch {
         basis = undefined;
      }
      if (!basis) throw across();
      const chain = new Set(
         SourceMap.of(this.host.modelDef()).chainOf(struct as never),
      );
      const source = basis.rerooted.find(
         (b) =>
            b.tier === "storage" &&
            b.destinationName === destination &&
            (b.sourceIDs ?? []).some((id) => chain.has(id)),
      );
      if (!source) throw across();
      return { basis, source };
   }

   /**
    * Answer a join branch in a storage destination (known limitation 26): the
    * query's source as the served copy declares it, over its table there,
    * with the join re-pointed at the joined source's rollup in the same
    * destination, so the whole query runs on that one connection. Every
    * check of the branch ran before; this adds the served copy's own checks for
    * the tables the variant reads, and that its SQL reads both tables.
    */
   private async answerJoinInDestination(
      branch: JoinRollupBranch,
      q: Parameters<RollupVariantServe["attemptJoin"]>[2],
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
      at: { basis: ServedCopyJoinBasis; source: Binding },
   ): Promise<RollupAnswer> {
      const { plan, join } = branch;
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
            branch,
            destination: { basis, keepsJoin },
         },
         table,
      );
      if (variant.refusal) throw storedReadSkip(variant.refusal);
      // A definition the copy left out reads a join across connections: a
      // query that reads one does not compile over the copy.
      const leftOutHere = basis.leftOut.length > 0;
      if (!variant.materializer) {
         this.warnCompileFailure(
            variant,
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
      if (query.gates && !this.host.rollupGateEnforcedAtRead(plan)) {
         throw contractSkip();
      }
      // A source with its own `timezone:` truncated and compared its times in
      // that zone when the rollup was built; the variant redeclares it over
      // the rollup without one, so a time grain dimension would be read, and
      // a grafted gate or a caller's filter on it compared, in another zone:
      // live's answer would differ, and a truncation gate would admit groups
      // live refuses.
      if (timezoneOf(query.struct) !== undefined) {
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
      const coarse = grainDecline(shape, grain, measures);
      if (coarse) throw coarse;
      const missing = coverageDecline(shape, measures);
      if (missing) throw missing;
      if (query.zoned) {
         const zone = zoneDecline(
            plan,
            query.struct,
            query.zoned.usage,
            query.gates,
         );
         if (zone) throw zone;
      }
      // A rollup's re-applied `where:` is written on its base's own variant,
      // in the base's file, where its givens are declared: a query on
      // another source over the base is answered live.
      if (
         (plan.readTimeTerms ?? []).length > 0 &&
         query.sourceID !== plan.baseSourceID
      ) {
         throw new LegDecline(
            "rollup_grain_too_coarse",
            "the query's source extends one whose `where:` reads a given, which only that source's own rollup re-applies",
         );
      }
      const table = this.tableOf(plan, args.pinned);
      if (!table) {
         throw this.noFreshTable(plan, args.pinned);
      }
      // A gate that reads a stored column no read gives live's values (the
      // bind's, or the read plan's) could not be grafted on the variant: the
      // query answers live rather than being denied.
      if (query.gates) {
         const unread = gateReadsLeftOut(
            query.gates,
            this.leftOutColumns(plan, table),
         );
         if (unread) {
            throw new LegDecline(
               "rollup_missing_field",
               `the stored rollup cannot answer with live's columns: a gate reads \`${unread}\`, which its table no longer serves`,
            );
         }
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
      if (variant.refusal) throw storedReadSkip(variant.refusal);
      if (!variant.materializer) {
         // A generated model that does not compile is a publisher bug.
         this.warnCompileFailure(
            variant,
            "A #(preaggregate) rollup variant did not compile",
            {
               modelPath: this.host.modelPath(),
               rollup: plan.rollupSourceName,
               error: variant.error,
            },
         );
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
            ? args.pinned.compilerManifest.entries[table.sourceEntityId]
                 ?.tableName
            : undefined;
      if (!table.virtualMap && !colocatedTable) {
         throw this.noFreshTable(plan, args.pinned, table.sourceEntityId);
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
               : args.pinned.compilerManifest,
         } as never);
      } catch {
         throw new LegDecline(
            "rollup_missing_field",
            notCompiledDetail(variant),
         );
      }
      const routed = await readPreparedQuery(runnable);
      checkCoverage(plan, queryLevelFacts(routed));
      // Route, then govern: past an entry-point gate, the gate must be
      // provable on this rollup before it is used, so `govern()`'s re-proof
      // (graft, bind guard, gate-set equality) denies only on a defect.
      if (query.gates) proveGates(plan, query, routed, variant.modelDef);
      proveGivenTerms(query, routed, variant.modelDef);
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
    * that is `keyColumn` (known limitation 29): `unknown` when not checked,
    * not built by a run that checked it, or a table bound from a host's
    * manifest.
    */
   private keyUniqueness(
      plan: RollupVariantPlan,
      keyColumn: string | undefined,
      pinned: Pinned,
   ): KeyVerdict {
      if (
         keyColumn === undefined ||
         plan.grainDimensions.length !== 1 ||
         plan.grainDimensions[0] !== keyColumn
      ) {
         return "unknown";
      }
      // The table's content address fixes its grain, so its check is of
      // this key. A stored rollup's rides on its binding.
      const table = this.tableOf(plan, pinned);
      const id = table?.sourceEntityId;
      return verdictOf(
         table?.binding
            ? table.binding.rollupKey?.unique
            : id === undefined
              ? undefined
              : pinned.snapshot.entries[id]?.keyUniqueness?.unique,
      );
   }

   /**
    * What the build of `plan`'s table found of the query source's primary key
    * (its parent key check): `unique` when unique and never NULL, `unknown`
    * when not checked, or a table a host's manifest binds.
    */
   private parentKeyUniqueness(
      plan: RollupVariantPlan,
      struct: unknown,
      pinned: Pinned,
   ): KeyVerdict {
      const sourceID = (struct as { sourceID?: unknown } | undefined)?.sourceID;
      const id = this.tableOf(plan, pinned)?.sourceEntityId;
      if (typeof sourceID !== "string" || id === undefined) return "unknown";
      return verdictOf(
         pinned.snapshot.entries[id]?.keyUniqueness?.parentKeys?.[sourceID],
      );
   }

   /**
    * What the build of a stored table the served copy reads for the query's
    * source found of `key`: `unique` when a source it stores declares `key`
    * its primary key and the check found it unique and never NULL;
    * `repeated` when one found it repeated or NULL; `unknown` when not
    * checked, or a binding a host's manifest made.
    */
   private storedKeyUniqueness(
      binding: Binding,
      key: string | undefined,
   ): KeyVerdict {
      if (key === undefined || !binding.primaryKeys) return "unknown";
      const map = SourceMap.of(this.host.modelDef());
      let found: boolean | undefined;
      for (const id of binding.sourceIDs ?? []) {
         const unique = binding.primaryKeys[id];
         if (unique === undefined) continue;
         if (primaryKeyOf(map.node(id)?.def) !== key) continue;
         if (!unique) return "repeated";
         found = true;
      }
      return verdictOf(found);
   }

   /**
    * The first join_one of the query's source, other than `branchJoin`, that the
    * query reads and whose key a rollup's build found repeated (see
    * {@link keyUniqueness}), or undefined.
    */
   private nonUniqueJoinOneRead(
      struct: unknown,
      branchJoin: string,
      usage: readonly (readonly string[])[],
      pinned: Pinned,
   ): string | undefined {
      const sourceID = (struct as { sourceID?: unknown } | undefined)?.sourceID;
      if (typeof sourceID !== "string") return undefined;
      const byBase = this.plansByBase();
      for (const j of SourceMap.of(this.host.modelDef()).joinsOf(sourceID)) {
         if (
            j.name === branchJoin ||
            j.relationship !== "one" ||
            j.sourceID === undefined ||
            j.keyColumn === undefined ||
            !usage.some((p) => p[0] === j.name)
         ) {
            continue;
         }
         for (const plan of byBase.get(j.sourceID) ?? []) {
            if (this.keyUniqueness(plan, j.keyColumn, pinned) === "repeated") {
               return j.name;
            }
         }
      }
      return undefined;
   }

   /**
    * The rollup has no table to read at the pin: `rollup_stale` when it has
    * one past its freshness window (a stale storage binding, or a colocated
    * entry bound but left out of the pinned manifest), else
    * `rollup_not_built`.
    */
   private noFreshTable(
      plan: RollupVariantPlan,
      pinned: Pinned,
      sourceEntityId?: string,
   ): LegDecline {
      const sourceID = rollupSourceID(plan);
      const lookup =
         sourceID === undefined
            ? undefined
            : pinned.tableFor(sourceID, "rollup");
      const staleColocated =
         sourceEntityId !== undefined &&
         Object.hasOwn(pinned.snapshot.entries, sourceEntityId) &&
         !Object.hasOwn(pinned.compilerManifest.entries, sourceEntityId);
      return lookup?.state === "stale" || staleColocated
         ? new LegDecline(
              "rollup_stale",
              "the rollup's table is past its freshness window",
           )
         : new LegDecline("rollup_not_built", "the rollup has no fresh table");
   }

   /**
    * The storage destination the rollup's table is in, or undefined when it
    * is colocated: its binding's, else the annotation's (storageLookupOf).
    */
   private storedIn(
      plan: RollupVariantPlan,
      pinned: Pinned,
   ): string | undefined {
      const lookup = storageLookupOf(
         pinned,
         rollupSourceID(plan),
         "rollup",
         plan.storage !== undefined,
      );
      if (!lookup) return undefined;
      return (
         this.tableOf(plan, pinned)?.binding?.destinationName ??
         (lookup.state !== "not_built"
            ? lookup.binding.destinationName
            : plan.storage)
      );
   }

   /** The rollup's table at the pin: a fresh storage binding, or a planned colocated one. */
   private tableOf(
      plan: RollupVariantPlan,
      pinned: Pinned,
   ):
      | {
           virtualMap?: VirtualMap;
           quotedPath?: string;
           binding?: Binding;
           sourceEntityId?: string;
           /** A colocated table's columns the bind left out (`leftOutColumns`). */
           leftOut?: readonly string[];
        }
      | undefined {
      const sourceID = rollupSourceID(plan);
      // Keyed by the rollup's own `sourceID`, never by its name; the
      // binding decides the tier (storageLookupOf).
      const lookup = storageLookupOf(
         pinned,
         sourceID,
         "rollup",
         plan.storage !== undefined,
      );
      if (lookup) {
         const binding =
            lookup?.state === "fresh" || lookup?.state === "stale_ok"
               ? lookup.binding
               : undefined;
         if (!binding || !this.host.serveDestinationConfig()) return undefined;
         const alias = servedCopyAlias(binding.destinationName);
         return {
            virtualMap: new Map([
               [
                  alias,
                  new Map([
                     [
                        binding.virtualHandle,
                        quoteManifestTablePath(
                           binding.tablePath,
                           STORAGE_DIALECT,
                        ),
                     ],
                  ]),
               ],
            ]),
            quotedPath: quoteManifestTablePath(
               binding.tablePath,
               STORAGE_DIALECT,
            ),
            binding,
         };
      }
      const planned = sourceID
         ? this.host.catalog()?.planned.get(sourceID)
         : undefined;
      if (!planned) return undefined;
      const entry = pinned.snapshot.entries[planned.sourceEntityId];
      const leftOut = entry?.leftOutColumns;
      // Altered outside its build: read as a `storage=` rollup is, through a
      // typed source on its own connection, when the pin reads it at all.
      const held =
         entry &&
         Object.hasOwn(pinned.compilerManifest.entries, planned.sourceEntityId)
            ? heldReadBinding("rollup", planned.sourceEntityId, entry)
            : undefined;
      if (held) {
         return {
            sourceEntityId: planned.sourceEntityId,
            virtualMap: new Map([
               [
                  held.destinationName,
                  new Map([[held.virtualHandle, held.tablePath]]),
               ],
            ]),
            quotedPath: held.tablePath,
            binding: held,
            ...(leftOut?.length ? { leftOut } : {}),
         };
      }
      return {
         sourceEntityId: planned.sourceEntityId,
         ...(leftOut?.length ? { leftOut } : {}),
      };
   }

   /** Warn once per compiled variant that failed, then log at debug. */
   private warnCompileFailure(
      variant: CompiledVariant,
      message: string,
      fields: Record<string, unknown>,
   ): void {
      if (this.warnedFailures.has(variant)) {
         logger.debug(message, fields);
         return;
      }
      this.warnedFailures.add(variant);
      logger.warn(message, fields);
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
            ? `join:${use.branch.join.name}${use.destination ? `\u0000${use.destination.basis.key}\u0000${use.destination.keepsJoin}` : ""}`
            : "base",
         table.binding
            ? `${table.binding.destinationName}/${table.binding.virtualHandle}`
            : `colocated${(table.leftOut ?? []).map((c) => `\u0001${c}`).join("")}`,
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
         const unreadDetail = this.unreadDetailOf(plan, table);
         compiled = this.compile(plan, use, table).then(
            (v) => ({
               ...v,
               cacheScope,
               ...(unreadDetail ? { unreadDetail } : {}),
            }),
            (error) => ({
               error: error instanceof Error ? error.message : String(error),
            }),
         );
         this.compiled.set(key, compiled);
      }
      return compiled;
   }

   /**
    * Why a stored rollup leaves columns out, for the decline of a query that
    * reads one: the bind's (a database table altered outside its build),
    * else the first its read plan leaves out ({@link storedRollupReads}).
    */
   /**
    * The stored columns a rollup does not declare: the bind's (a database
    * table altered outside its build), and those its read plan leaves out
    * ({@link storedRollupReads}).
    */
   private leftOutColumns(
      plan: RollupVariantPlan,
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
   ): string[] {
      return [
         ...(table.leftOut ?? []),
         ...(table.binding
            ? storedRollupReads(
                 plan,
                 table.binding,
                 this.host.modelDef(),
                 declaredRollupColumns(this.host.catalog(), table.binding),
              ).leftOut
            : []),
      ];
   }

   private unreadDetailOf(
      plan: RollupVariantPlan,
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
   ): string | undefined {
      if (table.leftOut?.length) {
         return alteredColumnsDetail(table.leftOut);
      }
      if (!table.binding) return undefined;
      return storedRollupReads(
         plan,
         table.binding,
         this.host.modelDef(),
         declaredRollupColumns(this.host.catalog(), table.binding),
      ).refusal;
   }

   private async compile(
      plan: RollupVariantPlan,
      use: VariantUse,
      table: NonNullable<ReturnType<RollupVariantServe["tableOf"]>>,
   ): Promise<CompiledVariant> {
      // A join branch reads a storage rollup only over the served copy
      // (answerJoinInDestination).
      if (use.kind === "join" && table.binding && !use.destination) {
         return {
            error: "a join branch reads a storage rollup over the served copy only",
         };
      }
      const packagePath = this.packagePath;
      const packageConfig = this.packageConfig;
      if (!packagePath || !packageConfig) return { error: "no context" };
      const modelPath = this.host.modelPath();
      const modelFile = path.join(packagePath, modelPath);
      const queryModelURL = pathToFileURL(`${modelFile}.rollup.malloy`);
      const reads = table.binding
         ? storedRollupReads(
              plan,
              table.binding,
              this.host.modelDef(),
              declaredRollupColumns(this.host.catalog(), table.binding),
           )
         : undefined;
      const destinations = this.host.serveDestinationConfig();
      const config =
         table.binding && !isColocated(table.binding) && destinations
            ? servedCopyConfig(
                 packageConfig,
                 destinations(),
                 new Set([table.binding.destinationName]),
              )
            : packageConfig;
      if (
         use.kind === "join" &&
         use.destination &&
         table.binding &&
         !isColocated(table.binding) &&
         reads
      ) {
         return this.compileJoinInDestination(
            plan,
            { ...use, destination: use.destination },
            table.binding,
            reads,
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
            ...(reads ? { reads } : {}),
         });
      }
      const { localName } = use;
      // The package's own files, read only when a file is read.
      const authorFiles = packageFiles(packagePath, modelPath, packageConfig);
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
      if (!companion || !text || !declaringFile) {
         return { error: "no companion" };
      }
      overlay.set(companion.href, text);
      // The shared, checked blanking (persist_blanking.ts), read through
      // the package's own reader, as the build companion reads them.
      const blanked = await authorFiles.blanked(this.host.modelDef());
      if (!blanked) return { error: "a `#@` line could not be left out" };
      for (const [url, file] of blanked) overlay.set(url, file);
      // The base's build copy, which the companion imports it from, made as
      // the plan made it, so the build source's SQL (and its table) is one.
      if (readsBuildCopy(plan)) {
         const baseURL = pathToFileURL(declaringFile);
         const made = rollupBuildCopyText(
            blanked.get(overlayKey(baseURL)) ??
               (await authorFiles.readIfAny(baseURL.href)) ??
               "",
            plan,
            (plan.privateGrain ?? []).length > 0,
         );
         if (!("copy" in made)) return { error: made.reason };
         overlay.set(
            pathToFileURL(
               path.join(
                  path.dirname(declaringFile),
                  rollupBuildCopyName(
                     path.basename(declaringFile),
                     plan.baseDeclaredName,
                  ),
               ),
            ).href,
            made.copy,
         );
      }
      // A column the bind left out is not declared, so a query reading it
      // does not compile against the rollup.
      const read = withoutLeftOut(plan, this.host.modelDef(), table.leftOut);
      const declaration = `${colocatedRollupSource(read.plan, this.host.modelDef(), servedName, relativeImport(queryModelURL, companion.href), read.dropped)}
${sourceOverRollup(read.plan, use, servedName, "")}`;
      const modelText = queryModelText(
         this.host.modelDef(),
         queryModelURL,
         pathToFileURL(modelFile).href,
         { alias: new Map([[localName, parentAlias(plan)]]) },
         declaration,
      );
      overlay.set(queryModelURL.href, modelText);
      const { materializer } = await packageFiles(
         packagePath,
         modelPath,
         config,
      ).compile(queryModelURL, overlay);
      return { materializer };
   }

   /**
    * Compile a join branch in a storage destination (known limitation 26): the
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
      binding: Binding,
      reads: StoredRollupReads,
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
      // A stored column no read gives live's values is not declared, so a
      // query reading it does not compile against the rollup.
      const read = withoutLeftOut(plan, modelDef, reads.leftOut);
      const declaration = `${storedRollupDeclaration(read.plan, binding, reads, servedName, modelDef, read.dropped)}
${sourceOverRollup(read.plan, use, servedName, "")}`;
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
         ctx.queryModelURL.href,
         `${flagLine(modelDef)}
${imports}

${declaration}
`,
      );
      const { materializer } = await packageFiles(
         ctx.packagePath,
         ctx.modelPath,
         basis.config,
      ).compile(ctx.queryModelURL, overlay);
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
         /** A `storage=` rollup's stored reads ({@link storedRollupReads}). */
         reads?: StoredRollupReads;
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
            gates,
            sourceName,
            copyName,
            declaringURL,
            addFile,
            authorText,
         }) => {
            // What the run target adds over the rollup's fields: an
            // extension's dimensions over the grain, and the views the
            // query reads, as the author wrote them.
            const added = await variantAdditions(use, (url) =>
               this.readAuthorText(ctx, url),
            );
            if ("error" in added) return added;
            declaredViews = added.views;
            // A stored column the bind left out, or no read gives live's
            // values, is not declared, so a query reading it does not
            // compile against the rollup.
            const read = withoutLeftOut(plan, modelDef, [
               ...(table.leftOut ?? []),
               ...(ctx.reads?.leftOut ?? []),
            ]);
            const over = sourceOverRollup(
               read.plan,
               { ...use, localName: sourceName },
               servedName,
               added.text,
            );
            if (table.binding && ctx.reads) {
               const stored = storedRollupDeclaration(
                  read.plan,
                  table.binding,
                  ctx.reads,
                  servedName,
                  modelDef,
                  read.dropped,
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
            const importPath = overlaid ? copyName : path.basename(baseFile);
            const text = generateRollupBuildFile([plan], importPath);
            if (!text) return { error: "no companion" };
            addFile(companion, text);
            // The base's build copy, made as the plan made it, so the build
            // source's SQL (and so its table) is the plan's.
            if (readsBuildCopy(plan)) {
               const made = rollupBuildCopyText(
                  (await authorText(pathToFileURL(baseFile))) ?? "",
                  plan,
                  (plan.privateGrain ?? []).length > 0,
               );
               if (!("copy" in made)) return { error: made.reason };
               addFile(
                  pathToFileURL(
                     path.join(
                        path.dirname(baseFile),
                        rollupBuildCopyName(importPath, plan.baseDeclaredName),
                     ),
                  ),
                  made.copy,
               );
            }
            return `${colocatedRollupSource(read.plan, modelDef, servedName, relativeImport(declaringURL, companion.href), read.dropped)}
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
         return await packageFiles(
            ctx.packagePath,
            ctx.modelPath,
            ctx.packageConfig,
         ).read(url);
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
   if (recordedTimezoneOf(struct) !== recordedTimezoneOf(baseDef)) {
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

/**
 * A colocated rollup's served source: its build source imported from the
 * rollup companion (`from`, relative to the importing file), so the build
 * manifest substitutes its table, extended with the rollup's merged measures.
 */
function colocatedRollupSource(
   plan: RollupVariantPlan,
   modelDef: unknown,
   servedName: string,
   from: string | undefined,
   /** Stored columns the served source drops ({@link withoutLeftOut}). */
   dropped: readonly string[] = [],
): string {
   const built = `${plan.rollupSourceName}__built`;
   const except =
      dropped.length > 0
         ? ` include { *; except: ${dropped.map((g) => identifier(g)).join(", ")} }`
         : "";
   return `import { ${identifier(built)} is ${identifier(plan.rollupSourceName)} } from ${doubleQuotedString(from ?? "")}
source: ${identifier(servedName)} is ${identifier(built)}${except} extend {${rollupStatements(plan, modelDef)}
}`;
}

/**
 * A colocated rollup read without the stored columns the bind left out
 * (`leftOutColumns`: altered outside a build, and no faithful cast reads
 * them as declared), as the served copy leaves such a column out: each
 * merged measure with a left-out partial is not declared, and its partials
 * and each left-out grain column are dropped (`dropped`), so only a query
 * that reads one fails to compile against the rollup and answers live. The build source is still
 * the plan's own, so its table's address does not move.
 */
function withoutLeftOut(
   plan: RollupVariantPlan,
   modelDef: unknown,
   leftOut: readonly string[] | undefined,
): { plan: RollupVariantPlan; dropped: string[] } {
   if (!leftOut?.length) return { plan, dropped: [] };
   const out = new Set(leftOut.map((c) => c.toLowerCase()));
   const gone = (c: string) => out.has(c.toLowerCase());
   const kept = plan.rollupMeasures.filter(
      (m) => !m.partials.some((p) => gone(p.column)),
   );
   // Every partial of a measure not declared is dropped too: no longer one
   // of the rollup's internal columns, it would otherwise be public.
   const dropped = [
      ...grainColumns(plan, modelDef)
         .map((g) => g.name)
         .filter(gone),
      ...plan.rollupMeasures
         .filter((m) => !kept.includes(m))
         .flatMap((m) => m.partials.map((p) => p.column)),
   ];
   return {
      plan: {
         ...plan,
         rollupMeasures: kept,
         ...(plan.internalGrain
            ? { internalGrain: plan.internalGrain.filter((g) => !gone(g)) }
            : {}),
         ...(plan.privateGrain
            ? { privateGrain: plan.privateGrain.filter((g) => !gone(g)) }
            : {}),
      },
      dropped,
   };
}

/**
 * Why a query that did not compile against a rollup declines: a stored
 * rollup declares none of the columns no read gives live's values, so a
 * query reading one lands here.
 */
function notCompiledDetail(
   variant: Pick<CompiledVariant, "unreadDetail">,
): string {
   return variant.unreadDetail
      ? `the stored rollup cannot answer with live's columns: ${variant.unreadDetail}`
      : "the query does not compile against the rollup";
}

/**
 * The first name an entry-point gate reads that is a stored column the
 * rollup leaves out, or undefined. A name is any identifier in a gate's text
 * (`big = $TENANT` reads `big`), matched without case; a given or keyword
 * that happens to share a column's name only declines more often.
 *
 * Written over the gate texts alone, so a shared reader of a gate's
 * identifiers (`gateIdentifiers`) can replace {@link gateNames}: the rollup
 * counterpart of the served copy's check that a gate's columns are kept
 * (`checkGateColumnsKept`).
 */
function gateReadsLeftOut(
   gates: EnforcedGates,
   leftOut: readonly string[],
): string | undefined {
   if (leftOut.length === 0) return undefined;
   const out = new Set(leftOut.map((c) => c.toLowerCase()));
   return gateNames([...gates.authorize, ...gates.accessFilter]).find((n) =>
      out.has(n.toLowerCase()),
   );
}

/** The identifiers a gate's text names, fields or not. */
function gateNames(exprs: readonly string[]): string[] {
   return [...new Set(exprs.flatMap((expr) => malloyIdentifiers(expr)))];
}

/** The rollup's own `sourceID`, as its build companion declares it. */
function rollupSourceID(plan: RollupVariantPlan): string | undefined {
   const declared = splitSourceID(plan.baseSourceID);
   const companion = declared ? rollupCompanionURLOf(declared.url) : undefined;
   return companion ? `${plan.rollupSourceName}@${companion.href}` : undefined;
}

/** A `storage=` rollup's stored columns, as {@link storedTypedSource} reads them. */
interface StoredRollupReads {
   columns: {
      name: string;
      timeframe?: string;
      notes: string[];
      kind: "measure" | "dimension";
      timestamptz?: true;
   }[];
   casts: Map<string, ColumnCast>;
   /**
    * Each stored column no read gives live's values (`refusal` says why of
    * the first): left out, with every merged measure over it, so only a
    * query that reads one answers live.
    */
   leftOut: string[];
   refusal?: string;
}

/**
 * How a stored rollup's columns read as live's (a `storage=` rollup's, or a
 * database rollup's altered outside its build), each one that cannot left
 * out, so only a query reading it answers live:
 *
 * - a grain column whose dimension live types `timestamptz`, stored with its
 *   zone, is declared `timestamptz` (a `type:` would declare the stored
 *   type's `timestamp`);
 * - a column stored as another type than its build source declares (a grain
 *   dimension or any partial, whatever expression it aggregates) is read
 *   through a faithful cast ({@link storedColumnReadPlan}), or is the
 *   refusal;
 * - a big integer (a grain dimension, or the partial of a measure live types
 *   as a `bigint` number) is read through its native type
 *   ({@link storedColumnReadPlan}): a `type:` declares it only as a `number`,
 *   which Malloy reads through a JavaScript number and rounds above 2^53.
 *   One stored where no cast reads it exactly (a float, its digits already
 *   lost) is the refusal.
 */
function storedRollupReads(
   plan: RollupVariantPlan,
   binding: Pick<Binding, "schema" | "tier" | "nativeSchema">,
   modelDef: unknown,
   /**
    * The columns the rollup's build source declares, by name: the type each
    * stored column must serve as (`boundTableTypeFindings` reads the same).
    */
   declared: ReadonlyMap<string, string>,
): StoredRollupReads {
   const base = SourceMap.of(modelDef).node(plan.baseSourceID)?.def as
      | {
           dialect?: unknown;
           fields?: { name?: unknown; as?: unknown; type?: unknown }[];
        }
      | undefined;
   const fields = new Map(
      (base?.fields ?? []).map((f) => [String(f.as ?? f.name), f]),
   );
   // Matched as the served copy matches, by a unique case fold too: a
   // warehouse that folded the build's quoted aliases (P3-06..09).
   const grain = grainColumns(plan, modelDef);
   const stored = new Map(
      canonicalStoredSchema(binding.schema, [
         ...grain.map((g) => g.name),
         ...plan.rollupMeasures.flatMap((m) => m.partials.map((p) => p.column)),
      ]).map((c) => [c.name, c]),
   );
   const columns: StoredRollupReads["columns"] = [];
   const casts = new Map<string, ColumnCast>();
   const leftOut: string[] = [];
   let refusal: string | undefined;
   // A database table is read where it was built, each big integer through
   // the type its catalog names; a `storage=` one is in DuckLake.
   const site = isColocated(binding)
      ? {
           colocated: true,
           nativeTypeOf: nativeTypeLookup(binding.nativeSchema),
        }
      : { ducklake: true };
   const read = (
      column: StoredRollupReads["columns"][number],
      fieldName: string,
      /** The type live gives the column, when it is checked; "" when not. */
      liveType: string,
   ): string | undefined => {
      const storedColumn = stored.get(column.name);
      const field = fields.get(fieldName);
      if (storedColumn) {
         const plan = storedColumnReadPlan(
            [storedColumn],
            [{ name: column.name, type: liveType, field }],
            {
               dialect: base?.dialect,
               queryTimezone: queryTimezoneOf(base),
               ...site,
            },
         );
         // A column whose stored type drifted from live's, with no faithful
         // cast to read it back: never read as its stored type.
         const [drifted] = plan.leftOut;
         if (drifted) {
            return `the stored \`${fieldName}\` (${drifted.storedType}) cannot be read as live's ${drifted.declaredType}`;
         }
         if (plan.refusals.length > 0) {
            return `the stored \`${fieldName}\` (${storedColumn.type}) cannot hold live's integer exactly`;
         }
         const [read] = plan.casts;
         if (read) {
            casts.set(
               column.name,
               read.native
                  ? { nativeCast: read.castTo }
                  : { castTo: read.castTo === "BOOLEAN" ? "BOOLEAN" : "DATE" },
            );
         }
         if (
            column.kind === "dimension" &&
            field?.type === "timestamptz" &&
            /^(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ)$/i.test(
               storedColumn.type.trim(),
            )
         ) {
            column = { ...column, timestamptz: true };
         }
      }
      columns.push(column);
      return undefined;
   };
   const leave = (column: string, why: string) => {
      leftOut.push(column);
      refusal ??= why;
   };
   for (const g of grain) {
      const why = read(
         { ...g, kind: "dimension" },
         g.name,
         declared.get(g.name) ?? "",
      );
      if (why) leave(g.name, why);
   }
   for (const m of plan.rollupMeasures) {
      for (const p of m.partials) {
         const why =
            partialTypeRefusal(p, stored.get(p.column), m.name) ??
            read(
               { name: p.column, notes: [], kind: "measure" },
               m.name,
               declared.get(p.column) ?? "",
            );
         if (why) leave(p.column, why);
      }
   }
   return { columns, casts, leftOut, ...(refusal ? { refusal } : {}) };
}

/**
 * A `storage=` rollup's declared columns: its build source's, from the
 * package's plan, matched by the table's content address (and its sourceID
 * where several share it). Empty when the plan is not known: no column is
 * then checked but a big integer's, as before.
 */
function declaredRollupColumns(
   catalog: MaterializedTableCatalog | undefined,
   binding: Pick<Binding, "virtualHandle" | "sourceIDs">,
): ReadonlyMap<string, string> {
   const atAddress = [...(catalog?.planned.values() ?? [])].filter(
      (t) => t.sourceEntityId === binding.virtualHandle,
   );
   const table =
      atAddress.find((t) => binding.sourceIDs?.includes(t.sourceID)) ??
      atAddress[0];
   return new Map((table?.columns ?? []).map((c) => [c.name, c.type]));
}

/**
 * Why a `sum` or `count` partial stored as a non-numeric type cannot merge,
 * or undefined. A `min` or `max` partial takes its argument's type, which
 * {@link storedRollupReads} checks against the build source's declared one.
 */
function partialTypeRefusal(
   partial: RollupVariantPlan["rollupMeasures"][number]["partials"][number],
   storedColumn: { type: string } | undefined,
   measureName: string,
): string | undefined {
   const aggregate = partial.leaf.aggregate;
   if (
      !storedColumn ||
      (aggregate !== "sum" && aggregate !== "count") ||
      storedTypeServesAs(storedColumn.type, "number") !== false
   ) {
      return undefined;
   }
   return `the stored \`${measureName}\` (${storedColumn.type}) cannot be read as live's number`;
}

/**
 * A stored rollup declared over its table (a `storage=` rollup's in the
 * destination, or a database rollup's altered outside its build), as
 * `servedName`, without the columns no read gives live's values: a `type:` of its stored columns (each grain column with its
 * dimension's notes, as the served copy's stored columns carry theirs), the
 * `virtual()` source on the destination's served-copy alias, its
 * timeframes and big-integer reads restated ({@link storedRollupReads}) and
 * its merged measures.
 */
function storedRollupDeclaration(
   plan: RollupVariantPlan,
   binding: Binding,
   reads: StoredRollupReads,
   servedName: string,
   modelDef: unknown,
   /** Stored columns not declared ({@link withoutLeftOut}). */
   dropped: readonly string[] = [],
): string {
   const out = new Set(
      [...dropped, ...reads.leftOut].map((c) => c.toLowerCase()),
   );
   const { typeText, timeframes } = storedTypedSource({
      columns: reads.columns,
      casts: reads.casts,
      binding: {
         ...binding,
         schema: binding.schema.filter((c) => !out.has(c.name.toLowerCase())),
      },
      typeName: `${plan.rollupSourceName}__type`,
      stored: servedName,
   });
   // malloy-text: storedTypedSource's text; rollupStatements re-emits the grammar's parse through malloy_text
   return `${typeText} extend {${timeframes}${rollupStatements(plan, modelDef)}
}`;
}

/**
 * A `storage=` rollup whose stored columns cannot read as live's
 * ({@link storedRollupReads}), as the persisted view's leg declines one.
 */
function storedReadSkip(refusal: string): LegDecline {
   return new LegDecline(
      "rollup_missing_field",
      `the stored rollup cannot answer with live's columns: ${refusal}`,
   );
}

/**
 * The served copy's decline of a join branch's query in a storage destination,
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

/** The author's source, imported for a join branch under this name. */
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
      return `source: ${identifier(use.localName)} is ${identifier(servedName)}${internalPartials(plan)}${reappliedTerms(plan)}${extension}`;
   }
   const { join } = use.branch;
   const keyword = use.branch.relationship === "one" ? "join_one" : "join_many";
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
   return measures ? `\n  measure:\n${measures}` : "";
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
   const priv = (plan.privateGrain ?? []).map((n) => identifier(n)).join(", ");
   // None left (every measure's partials left out of a stored rollup).
   if (!hidden && !priv) return "";
   return ` include {${hidden ? ` internal: ${hidden}` : ""}${priv ? ` private: ${priv}` : ""} public: * }`;
}

/**
 * The base's given-reading `where:` terms, as the author wrote them, written
 * again on the variant: the build left them out, so each caller's givens bind
 * them over the rollup's groups, as live applies them over the base's rows.
 */
function reappliedTerms(plan: RollupVariantPlan): string {
   const terms = plan.readTimeTerms ?? [];
   // malloy-text: each term is the author's own `where:` text, verbatim
   return terms.length > 0
      ? ` extend {\n${terms.map((t) => `  where: ${t.code}`).join("\n")}\n}`
      : "";
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
   // alias, for a join branch), givens included, so a query that supplies or
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
         for (const key of experimentalFlagKeys(notes)) {
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
 * The pre-proof of the re-applied `where:` (R3 for given-reading terms): throw
 * `CONTRACT` unless the variant's run target writes exactly the given-reading
 * terms the live run target applies, by text ({@link givenWhereCodes}), the
 * rule `govern()` re-proves on the routed compile. A rollup's build left them
 * out, so a variant without them would answer with every caller's rows.
 */
function proveGivenTerms(
   query: Pick<RollupQuery, "struct">,
   routed: Awaited<ReturnType<typeof readPreparedQuery>>,
   variantModelDef: ModelDef | undefined,
): void {
   const model = routed.modelDef ?? variantModelDef;
   const struct = model
      ? resolveStructRefSource(routed.structRef, model)
      : undefined;
   const live = givenWhereCodes(query.struct);
   const written = struct ? givenWhereCodes(struct) : undefined;
   if (!live || !written || live.join("\u0000") !== written.join("\u0000")) {
      throw contractSkip();
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
function checkCoverage(
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
 * with `PUBLISHER_MATERIALIZE` (`readsPreaggregateDeclarations`), and whether a gated base's rollup
 * is read.
 */
function planSwitches(): string {
   const s = gateSwitches();
   return [s.materialize, s.gatedSources].join("/");
}

/**
 * The name a model gives the source `structRef` holds inline, when it is that
 * named source unchanged: Malloy inlines a source a model imports without
 * exporting it (preaggregation_seams.spec.ts), so the query's struct is an
 * object, not a name. It is the named source only when the model's entry of
 * the struct's `as` has the same `sourceID`, fields and filters; an inline
 * extension (`orders extend { … } -> …`) changes one of them and stays
 * unnamed.
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

/** The joined source's compiled declaration, as the join branch reads it. */
interface JoinedSourceDef {
   queryTimezone?: unknown;
   fields?: {
      name?: unknown;
      as?: unknown;
      e?: unknown;
      join?: unknown;
   }[];
}

/**
 * The join-branch choice: a branch is tried only for a query that reads
 * through its join (an unreadable one might), and only when the join offers
 * a rollup it can use. A {@link JoinNotRead} when the query reads nothing
 * through the join, the branch's refusal as a {@link LegDecline}, or
 * undefined to try it.
 */
export function joinBranchDecline(
   name: string,
   usage: readonly (readonly string[])[] | undefined,
   refusal: ReturnType<typeof joinRollupBranches>[number]["refusal"],
): Error | undefined {
   if (usage && !usage.some((p) => p[0] === name)) {
      return new JoinNotRead(name);
   }
   if (refusal) {
      return new LegDecline(refusal.code, refusal.callerDetail, {
         shapeCheck: true,
      });
   }
   return undefined;
}

/** The fields of `paths` read through the join `name`, by their names in it. */
function readThroughJoin(name: string, paths: readonly string[]): string[] {
   return paths
      .filter((p) => p.startsWith(`${name}.`))
      .map((p) => p.slice(name.length + 1));
}

/**
 * The grain check of a join branch: a field the query groups or filters by
 * through the join `name` must be one of the rollup's grain dimensions or
 * stored measures, else the rollup is too coarse.
 */
export function joinGrainDecline(
   name: string,
   shape: Pick<ReturnType<typeof queryShapeOf>, "groupedBy" | "filteredBy">,
   grain: ReadonlySet<string>,
   measures: ReadonlySet<string>,
): LegDecline | undefined {
   const coarse = readThroughJoin(name, [
      ...shape.groupedBy,
      ...shape.filteredBy,
   ]).find((f) => !grain.has(f) && !measures.has(f));
   if (coarse !== undefined) {
      return new LegDecline(
         "rollup_grain_too_coarse",
         `the query reads \`${name}.${coarse}\`, which the rollup's grain does not keep`,
         { shapeCheck: true },
      );
   }
   return undefined;
}

/**
 * The coverage check of a join branch: a measure the query aggregates through
 * the join `name` must be one the rollup stores (or a grain dimension).
 */
export function joinCoverageDecline(
   name: string,
   shape: Pick<ReturnType<typeof queryShapeOf>, "measures">,
   grain: ReadonlySet<string>,
   measures: ReadonlySet<string>,
): LegDecline | undefined {
   const missing = readThroughJoin(name, shape.measures).find(
      (m) => !measures.has(m) && !grain.has(m),
   );
   if (missing !== undefined) {
      return new LegDecline(
         "rollup_missing_field",
         `the query aggregates \`${name}.${missing}\`, which the rollup does not store`,
         { shapeCheck: true },
      );
   }
   return undefined;
}

/** A zoned query's decline: the rollup was built in the model's zone. */
function zonedQueryDecline(): LegDecline {
   return new LegDecline(
      "rollup_time_zone",
      "the query sets its own time zone and reads a time the rollup stored in the model's",
   );
}

/** The temporal types, the only ones a query's `timezone:` reads differently. */
const ZONED_TYPES = new Set(["date", "timestamp", "timestamptz"]);

/**
 * Why a query that sets its own `timezone:` is not read from `plan`, or
 * undefined (A5). The build truncated and compared times in the model's
 * zone, so the rollup answers a zoned query only when nothing it reads
 * depends on a zone: no field path the query reads (expanded through its
 * dimensions and measures), no stored partial, no `where:` of the base and no
 * field the entry point's gates read reads a temporal field, directly or
 * through a computed dimension, nor anything through a join. The gates are
 * grafted on the variant after this check, in the query's zone, so a gate
 * over a stored day truncated in the model's zone would admit another day's
 * groups. Fails closed on usage that could not be read.
 */
function zoneDecline(
   plan: RollupVariantPlan,
   struct: object,
   usage: readonly (readonly string[])[] | undefined,
   gates: EnforcedGates | undefined,
): LegDecline | undefined {
   if (usage === undefined) return zonedQueryDecline();
   const def = struct as { fields?: unknown[]; filterList?: unknown[] };
   const fields = new Map(
      (def.fields ?? []).map((f) => {
         const field = f as { name?: unknown; as?: unknown };
         return [String(field.as ?? field.name), f as object];
      }),
   );
   const seen = new Set<string>();
   const readsTime = (path: readonly string[]): boolean => {
      const [name] = path;
      // An empty path reads no field (`count()`).
      if (name === undefined) return false;
      if (path.length !== 1) return true;
      if (seen.has(name)) return false;
      seen.add(name);
      const f = fields.get(name) as
         | { type?: unknown; e?: unknown; join?: unknown }
         | undefined;
      if (!f) return false;
      if (f.join !== undefined || ZONED_TYPES.has(String(f.type))) return true;
      return fieldPathsIn(f.e).some(readsTime);
   };
   const stored = plan.rollupMeasures.flatMap((m) =>
      m.partials.flatMap((p) => [p.leaf.joinPath, ...p.leaf.reads]),
   );
   const where = fieldPathsIn(def.filterList);
   const gated = gateIdentifiers(
      gates ? [...gates.authorize, ...gates.accessFilter] : [],
   ).map((id) => [id]);
   return [...usage, ...where, ...gated].some(readsTime) ||
      stored.some((p) => p.length > 0 && readsTime(p))
      ? zonedQueryDecline()
      : undefined;
}

/**
 * The grain check of a base branch, cheap, from the caller's own compile: what
 * the query groups and filters by must be in the grain (or a stored
 * measure), else the rollup is too coarse.
 */
export function grainDecline(
   shape: Pick<ReturnType<typeof queryShapeOf>, "groupedBy" | "filteredBy">,
   grain: ReadonlySet<string>,
   measures: ReadonlySet<string>,
): LegDecline | undefined {
   const coarse = [...shape.groupedBy, ...shape.filteredBy].find(
      (f) => !grain.has(f) && !measures.has(f),
   );
   if (coarse !== undefined) {
      return new LegDecline(
         "rollup_grain_too_coarse",
         `the query reads \`${coarse}\`, which the rollup's grain does not keep`,
         { shapeCheck: true },
      );
   }
   return undefined;
}

/**
 * The coverage check of a base branch: every measure the query aggregates must
 * be one the rollup stores.
 */
export function coverageDecline(
   shape: Pick<ReturnType<typeof queryShapeOf>, "measures">,
   measures: ReadonlySet<string>,
): LegDecline | undefined {
   const missing = shape.measures.find((m) => !measures.has(m));
   if (missing !== undefined) {
      return new LegDecline(
         "rollup_missing_field",
         `the query aggregates \`${missing}\`, which the rollup does not store`,
         { shapeCheck: true },
      );
   }
   return undefined;
}
