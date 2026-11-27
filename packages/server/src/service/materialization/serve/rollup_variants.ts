// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rollup variants: the router's rollup leg for `#(rollup)`.
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
 * a database one by importing its build source from the rollup companion and
 * letting Malloy's build manifest substitute its table, or, once its table
 * was altered outside its build, through `virtual()` on its own connection
 * as a `storage=` rollup is read. A stored column no read gives live's
 * values (`storedRollupReads`, and the bind's own left-out columns) is not
 * declared, with every merged measure over it, so only a query reading one
 * declines (`MISSING_FIELD`).
 *
 * A rollup answers only a query it reproduces exactly (skip never bend):
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
 * - a fan-out grain dimension is fixed at every aggregating level, by a
 *   `group_by:` there or above, or a `where:` to one value
 *   (`GRAIN_TOO_COARSE`);
 * - a `timezone:` it sets, at any stage or in any nest, changes nothing it
 *   reads from the rollup (the build truncated and compared in the
 *   model's default time zone): it reads no temporal field, directly or
 *   through a dimension, measure or the base's `where:`, and nothing through
 *   a join; else `TIME_ZONE`;
 * - its table is built and fresh, and the compiled SQL reads it.
 *
 * The queried source's name in that model may also carry what the query
 * reads beyond the rollup's fields, re-declared in an `extend`:
 *
 * - a named view the query runs, refines, starts from or nests, as
 *   the author wrote it (read at its compiled location), so the view's own
 *   query is what compiles and is checked, with each view its definition
 *   builds on (`view: a is b + { … }`, at any depth); one that no longer
 *   compiles to the view the author's model holds (its file edited since) or
 *   that reads raw SQL declines (`MISSING_FIELD`); a persisted view of the
 *   name is the router's earlier leg;
 * - for a query on an extension or rename of the rolled-up source (found
 *   through the source map's base edges), the extension's dimensions
 *   over the grain alone. One that adds a `where:` (`GRAIN_TOO_COARSE`), a
 *   `timezone:` (`TIME_ZONE`), or binds a base field's name to another field
 *   (`MISSING_FIELD`) declines.
 *
 * Route, then govern (for rollups): a base variant
 * redeclares the run target over the rollup IN AN OVERLAY OF ITS OWN
 * DECLARING FILE (`declaring_file_variant.ts`), so it keeps the author's
 * `sourceID` and carries the run target's entry-point gates as note lines.
 * With `materialize.gatedSources` on, or under own-gate keeping with it off for
 * a database rollup or for an entry point whose gates are locks reading no
 * column on either tier (the gate policy, `domain/gate_policy.ts`), the router
 * lets the leg past a gated entry point; a rollup then answers only when it was
 * built for its gate (`rollupGateEnforcedAtRead`) and the pre-proof holds
 * (`proveGates`): the variant's run target is the author's `sourceID`, its
 * gates equal the live entry point's (`sameGateSet`, the rule `govern()`
 * re-proves with) and each field they read is a grain dimension. Else it is
 * skipped `ROW_LEVEL_GATE`, with a detail that never names the gate. `govern()`
 * then grafts the gates on the variant's own scope; a denial there is a defect,
 * not a routing outcome. A join branch answers past a gate only over a rollup
 * of an ungated joined source, its gate reading the query source's own plain
 * columns (`joinGateDecline`), redeclared in its declaring file with its gates
 * (`compileJoinGated`; in a storage destination, in the served copy's copy of
 * it) and proven by `joinGraft`.
 *
 * Rollups are offered in the plan's order, cheapest read first (coarsest,
 * then database before `storage=`); the first that answers is used. A
 * `#(rollup)` rollup is read only with `materialize.enabled` on; a v1
 * `#@ preaggregate` one is read either way.
 */

import { VariantCompileCache } from "./variant_compile_cache";
import { requireUnique, verdictOf, type KeyVerdict } from "../domain/key_check";
import {
   givenWhereCodes,
   primaryKeyOf,
   modelDefOf,
   queryTimezoneOf,
   recordedTimezoneOf,
   timezoneOf,
} from "../../governance/ir/compiled_model";
import type {
   MalloyConfig,
   ModelDef,
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
import { fieldPathsIn } from "./gate_paths";
import { entryGates, gateIdentifiers } from "./gate_reads";
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
import {
   splitSourceID,
   type RollupPathColumn,
   type RollupVariantPlan,
} from "../domain/table_plans";
import { grainColumnOf, grainPathOf } from "../domain/rollup_grain";
import { TIMEFRAMES } from "../text/inlined_join_text";
import { annotatedJoinedOutput } from "../../governance/ir/joined_field_output";
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
import type { GraftRuntime, GraftScope } from "../../gate_classification";
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
   checkedKeyIs,
   heldReadBinding,
   isDatabase,
   type KeyedTable,
   manifestUnderCompilerKey,
   type Pinned,
   storageLookupOf,
   underCompilerKeys,
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
import { reportQueryWalk } from "./query_walks";

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
    *.
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
         readsRollupDeclarations?: boolean;
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
   /** The `sourceID` of the source that declares it. */
   rollupBaseSourceID: string;
   /** For a `storage=` rollup: how its `virtual()` handle resolves. */
   virtualMap?: VirtualMap;
   /** For a `storage=` rollup: its binding. */
   binding?: Binding;
   /**
    * For a database rollup: the table the build manifest substitutes, as
    * the compiled SQL names it, so a check of the stored table's own
    * definition (its collation) reads that table.
    */
   databaseTablePath?: string;
   /** For a database rollup: its table's content address (its contract's key). */
   databaseTableId?: string;
   /**
    * For a database rollup whose address is not Malloy's key for it: the run
    * that answers reads its entry under that key too.
    */
   keyedTable?: KeyedTable;
   /**
    * Every stored table the answer reads, when that is more than the
    * rollup's: a join branch answered in a storage destination reads the query
    * source's table there too, whose binding decides whether a failed run
    * may be retried live.
    */
   bindings?: Binding[];
   /**
    * The tables a join branch answered in a storage destination is judged
    * to read (`answerKeepsGates`, the row-scope and collation checks): the
    * query source's tables there and the rollup's. Absent: the rollup's own.
    */
   gateTablesRead?: Binding[];
   /**
    * Where `govern()` grafts the run target's gates on the variant: set
    * for every base variant, and for a join branch answered past an
    * entry-point gate; the one such an answer is governed on.
    */
   graftScope?: GraftScope;
}

/**
 * How a variant model declares the query's source: as the rollup itself
 * (`base`, the source's own rollup), or as the author's source with one join
 * re-pointed at a joined source's rollup (`join`).
 */
type VariantUse =
   | ({ kind: "base" } & VariantTarget)
   | {
        kind: "join";
        localName: string;
        branch: JoinRollupBranch;
        /**
         * Set when the query's source is read from the rollup's storage
         * destination through the served copy: the
         * variant is compiled over the copy's files, and `keepsJoin` says
         * whether the copy's source still declares the join (it leaves a
         * join to a source not stored there out across connections).
         */
        destination?: { basis: ServedCopyJoinBasis; keepsJoin: boolean };
        /**
         * Set past an entry-point gate: the query source's `sourceID`, whose
         * declaring file the variant redeclares it in, so it keeps the
         * author's `sourceID` and gates (`compileJoinGated`).
         */
        gated?: { sourceID: string };
     };

/** A join branch that does not apply to this query: it reads nothing through the join. */
class JoinNotRead extends Error {}

interface CompiledVariant {
   materializer?: ModelMaterializer;
   /** A base variant's compiled model, where `govern()` grafts. */
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
    * the router let the rollup leg past them, so a rollup answers only
    * when the same gates are written on its variant and each field they read
    * is a grain column (`proveGates`).
    */
   gates?: EnforcedGates;
   /** The run target's compiled definition. */
   struct: object;
   shape: ReturnType<typeof queryShapeOf>;
   /** The views of the run target the query reads by name. */
   views: string[];
   /**
    * Whether the query, its views expanded, holds raw SQL (`sql_*`): its text
    * names columns the compiler does not record, which a rollup may lack.
    */
   rawSql: boolean;
   /**
    * Whether the run target sets its own `timezone:`, which the rollup's
    * stored times are not read in ({@link RollupStatics}).
    */
   timeZoned?: boolean;
   /**
    * Set when the run target extends or renames the source the rollups
    * aggregate: that base's definition, and what the run target
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
   /** The compiled variants, by key: a graft cache scope per generation. */
   private readonly compiled = new VariantCompileCache<CompiledVariant>();
   /**
    * Compiled variants whose failure was warned about: each is cached for its
    * binding, so every later attempt would repeat the same warning.
    */
   private warnedFailures = new WeakSet<CompiledVariant>();
   private planMemo?: {
      modelDef: unknown;
      /**
       * The switches the plans were read under ({@link planSwitches}): a
       * plan read with one is not the plan with the other.
       */
      switches: string;
      byBase: Map<string, RollupVariantPlan[]>;
      /** Each source's join branches, by its `sourceID`. */
      joinBranches?: Map<string, ReturnType<typeof joinRollupBranches>>;
      /**
       * The pre-filtered rollup list: each source a rollup can answer
       * (its own, one through its extends and renames, or one through a
       * join), with its target and join branches, by `sourceID`. Built
       * with the plans; a source absent from it has none.
       */
      candidates?: ReadonlyMap<string, RollupCandidates>;
      /**
       * What a query on each candidate source declines by before reading
       * its own shape ({@link RollupStatics}), by `sourceID`: the same for
       * every query on the source, so read with the plans.
       */
      statics?: ReadonlyMap<string, RollupStatics>;
   };

   constructor(private readonly host: RollupVariantHost) {}

   setContext(packagePath: string, packageConfig: ModelConnectionInput): void {
      this.packagePath = packagePath;
      this.packageConfig = packageConfig;
      this.planMemo = undefined;
      this.invalidate();
   }

   invalidate(): void {
      this.compiled.invalidate();
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
               // `#(rollup)` ones only with the materialize switch on.
               readsRollupDeclarations: getMaterializeEnabled(),
            });
            if (plans.length > 0) byBase.set(node.sourceID, plans);
         }
      }
      const built: NonNullable<typeof this.planMemo> = {
         modelDef,
         switches,
         byBase,
      };
      this.planMemo = built;
      const candidates = new Map<string, RollupCandidates>();
      const statics = new Map<string, RollupStatics>();
      if (modelDef && byBase.size > 0) {
         for (const node of SourceMap.of(modelDef).nodes()) {
            const c = this.walkCandidates(node.sourceID);
            if (c.target || c.branches.length > 0) {
               candidates.set(node.sourceID, c);
               statics.set(
                  node.sourceID,
                  rollupStaticsOf(node.def as object, modelDef, c),
               );
            }
         }
      }
      built.candidates = candidates;
      built.statics = statics;
      return byBase;
   }

   /** What a query on `sourceID` could read a rollup through: the walks. */
   private walkCandidates(sourceID: string): RollupCandidates {
      const target = this.rollupTarget(sourceID);
      return {
         ...(target ? { target } : {}),
         branches: this.joinBranchesOf(sourceID),
      };
   }

   /**
    * {@link walkCandidates} for a query on `sourceID`: looked up in the
    * list built with the plans, walked (and counted) only when no list was
    * built.
    */
   private candidatesFor(sourceID: string): RollupCandidates {
      this.plansByBase();
      const list = this.planMemo?.candidates;
      if (list !== undefined) return list.get(sourceID) ?? NONE;
      reportQueryWalk("rollup_candidates");
      return this.walkCandidates(sourceID);
   }

   /**
    * {@link rollupStaticsOf} for a query on `sourceID`: looked up in what
    * the plans recorded, else walked on the query's own compile of the
    * source (and counted).
    */
   private staticsFor(
      sourceID: string,
      struct: object,
      modelDef: unknown,
      candidates: RollupCandidates,
   ): RollupStatics {
      const recorded = this.planMemo?.statics?.get(sourceID);
      if (recorded) return recorded;
      reportQueryWalk("rollup_candidates");
      return rollupStaticsOf(struct, modelDef, candidates);
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
      this.compiled.over(args.pinned);
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
      // The rollups a query on its source could read, from the list the
      // plans were read with: its own, through its bases, or through a join
      // whose joined source has rollups.
      const candidates =
         localName && sourceID ? this.candidatesFor(sourceID) : NONE;
      const target = candidates.target;
      const branches = candidates.branches;
      if (
         !localName ||
         !sourceID ||
         !struct ||
         (!target && branches.length === 0)
      ) {
         throw new NoRollupForQuery("the query's source declares no rollup");
      }
      const plans = target?.plans ?? [];
      // The source's own facts, recorded with the plans: its live entry
      // point's gates (as the governor walks them: a query reaches here past
      // them only when the router let the leg through), its zone, its
      // extension's verdict and each join branch's gate decline.
      const statics = this.staticsFor(
         sourceID,
         struct,
         prepared.modelDef ?? modelDef,
         candidates,
      );
      const gates = statics.gates;
      const shape = queryShapeOf(prepared.query, struct, {
         sourceName: localName,
      });
      // Anywhere in the query, a nest's own `timezone:` included: the build
      // evaluated the grain's dimensions and the measures in the model's
      // zone, so a zoned query is read from a rollup only where nothing the
      // build evaluated depends on the zone (`zoneDecline`).
      const zoned = setsTimezone(prepared.query)
         ? { usage: queryFieldUsagePaths(prepared) }
         : undefined;
      // The reason reported is the first rollup's that got past the grain
      // and measure check (the one closest to answering), else the first's.
      let first: LegDecline | undefined;
      let closest: LegDecline | undefined;
      // Each rollup's own decline, by its synthesized source name: the path
      // report gives each rollup its own reason.
      const members = new Map<string, LegDecline>();
      // An extension that adds more than fields declines its base's rollups
      //; its own joins' rollups are still tried.
      let ownPlans = plans;
      let extension: RollupQuery["extension"];
      if (target?.extension) {
         const verdict = statics.extension;
         if (verdict && "decline" in verdict) {
            first = verdict.decline;
            ownPlans = [];
            for (const plan of plans) {
               members.set(plan.rollupSourceName, verdict.decline);
            }
         } else {
            extension = {
               baseDef: target.extension.baseDef,
               ...(verdict ? { delta: verdict.delta } : {}),
            };
         }
      }
      // The views the query reads by name: the one its text runs,
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
         timeZoned: statics.timeZoned,
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
      // Then a joined source's rollup, read through the join: the
      // join branches, inside the rollup step.
      const usage = branches.length > 0 ? queryFieldUsagePaths(prepared) : [];
      for (const [i, { branch, refusal }] of branches.entries()) {
         // Through a join, a zoned query is read from the joined source's
         // rollup by the same rule as its own: what the query reads
         // through the join, and the grain the join meets, must not depend
         // on the zone the build evaluated it in.
         if (zoned) {
            const zone = zoneDecline(
               branch.plan,
               SourceMap.of(this.host.modelDef()).node(branch.plan.baseSourceID)
                  ?.def as object | undefined,
               usage === undefined
                  ? undefined
                  : usage
                       .filter((p) => p[0] === branch.join.name)
                       .map((p) => p.slice(1)),
               // A join branch past an entry point's gate declines below.
               undefined,
               branch.plan.grainDimensions.map((d) => d.split(".")),
            );
            if (zone) {
               first ??= zone;
               members.set(branch.plan.rollupSourceName, zone);
               continue;
            }
         }
         // Past an entry-point gate (the ungated-rows rule for rollups), a
         // join branch answers only when the gate reads the query source's
         // own plain columns, none through a join, and its joined base is
         // ungated: the gate is grafted on the variant's query source, whose
         // rows are the author's, so it filters them as live does before
         // the re-pointed join is read (`joinGateDecline`).
         if (gates) {
            // On either tier: in a storage destination the variant is
            // declared in an overlay of the served copy's own copy of the
            // query source's declaring file (`compileJoinGated`),
            // so the gate lands there as on a database rollup's variant.
            const gated = statics.branchGates[i];
            if (gated) {
               first ??= gated;
               members.set(branch.plan.rollupSourceName, gated);
               continue;
            }
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
               ...(gates ? { gates } : {}),
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
    * The rollups that can answer a query on `sourceID`: its own, or
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
    * The join branches the joins of source `sourceID` offer, each with why it
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
    * through one of its joins, for the path report.
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
    * re-pointed join (see join_rollup.ts), or throw a {@link LegDecline}
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
         /** The live entry point's gates, when the leg is past them. */
         gates?: EnforcedGates;
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
      // A path grain is read only on the base's own variant: through a
      // join, the joined source's rollup declares no record of it.
      const grain = new Set(
         plan.grainDimensions.filter((g) => grainPathOf(g) === undefined),
      );
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
      // Fan-out grains and join-level measures through the join: a fan-out
      // grain dimension, and the other grain dimensions of a join-level sum or
      // count, grouped or fixed at every level that aggregates through the
      // join.
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
      return this.answerJoinDatabase(branch, q, table);
   }

   /**
    * The join branch's key checks: that live's rows and
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
      // grouped: then live's rows and the variant's
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
            ? this.bindingKeyUniqueness(
                 this.tableOf(plan, q.args.pinned)?.binding,
                 plan.baseSourceID,
                 primaryKeyOf(childDef),
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
    * Answer a join branch over a database rollup: the query compiled against
    * the variant, read through the build manifest, whose SQL must read the
    * rollup's table; else a {@link LegDecline}.
    */
   private async answerJoinDatabase(
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
         {
            kind: "join",
            localName: q.localName,
            branch,
            ...(q.gates ? { gated: { sourceID: q.sourceID } } : {}),
         },
         table,
      );
      if (!variant.materializer) {
         this.warnCompileFailure(
            variant,
            "A #(rollup) join-to-rollup variant did not compile",
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
      const databaseTable =
         table.sourceEntityId !== undefined
            ? q.args.pinned.compilerManifest.entries[table.sourceEntityId]
                 ?.tableName
            : undefined;
      if (!databaseTable) {
         throw this.noFreshTable(plan, q.args.pinned, table.sourceEntityId);
      }
      let result;
      try {
         result = await runnable.getPreparedResult({
            ...(q.args.givens ? { givens: q.args.givens as never } : {}),
            buildManifest: manifestUnderCompilerKey(
               underCompilerKeys(
                  q.args.pinned.compilerManifest,
                  q.args.pinned.snapshot.compilerKeys,
               ),
               table.keyedTable,
            ),
         } as never);
      } catch {
         throw new LegDecline(
            "rollup_missing_field",
            notCompiledDetail(variant),
         );
      }
      if (!result.sql.includes(databaseTable)) {
         throw new LegDecline(
            "rollup_not_built",
            "the rollup has no fresh table",
         );
      }
      return {
         runnable,
         rollupSourceName: plan.rollupSourceName,
         rollupBaseSourceID: plan.baseSourceID,
         databaseTablePath: databaseTable,
         ...(table.sourceEntityId !== undefined
            ? { databaseTableId: table.sourceEntityId }
            : {}),
         ...(table.keyedTable ? { keyedTable: table.keyedTable } : {}),
         ...(await this.joinGraft(q, runnable, variant)),
      };
   }

   /**
    * Past an entry-point gate, a join branch's answer is governed on its
    * variant (route, then govern): the gates written on the variant's query
    * source must be exactly the live entry point's (`sameGateSet`, the rule
    * `govern()` re-proves with), so the author's gate notes survived its
    * `include { *; except: … }`; else `ROW_LEVEL_GATE`. Returns where `govern()`
    * grafts them. Nothing past no gate.
    */
   private async joinGraft(
      q: Parameters<RollupVariantServe["attemptJoin"]>[2],
      runnable: QueryMaterializer,
      variant: CompiledVariant,
   ): Promise<Pick<RollupAnswer, "graftScope">> {
      if (!q.gates) return {};
      if (!variant.modelDef || !variant.materializer || !variant.cacheScope) {
         throw contractSkip();
      }
      const routed = await readPreparedQuery(runnable);
      const model = routed.modelDef ?? variant.modelDef;
      const struct = resolveStructRefSource<{ sourceID?: unknown }>(
         routed.structRef,
         model,
      );
      // The author's source, redeclared in its declaring file: where the
      // governor finds the gate's graft target.
      if (!struct || struct.sourceID !== q.sourceID) throw contractSkip();
      if (!sameGateSet(q.gates, writtenGatesOf(struct, model))) {
         throw contractSkip();
      }
      return {
         graftScope: {
            modelDef: variant.modelDef,
            materializer: variant.materializer,
            cacheScope: variant.cacheScope,
            ...(variant.runtime ? { runtime: variant.runtime } : {}),
         },
      };
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
    * Answer a join branch in a storage destination: the
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
            ...(q.gates ? { gated: { sourceID: q.sourceID } } : {}),
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
            "A #(rollup) join-to-rollup variant in a storage destination did not compile",
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
      const graft = await this.joinGraft(q, runnable, variant);
      return {
         runnable,
         rollupSourceName: plan.rollupSourceName,
         rollupBaseSourceID: plan.baseSourceID,
         virtualMap,
         binding: table.binding,
         bindings: [...tablesRead, table.binding],
         ...graft,
         // Every table the answer reads is judged: past a gate, the query
         // source's own tables in the destination must each be a gate-keeping
         // table (admitted past its own gate, or holding only ungated rows),
         // and the rollup one that holds only ungated rows, each holding the
         // gate's columns exactly (the exactness rule, in the router); past a
         // gate or under a row scope, each is compared as its source compares
         // it (the stored-side collation check reads every one).
         gateTablesRead: [...tablesRead, table.binding],
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
      if (query.timeZoned) {
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
      // variant, and read as grain dimensions here.
      const dimensions = query.extension
         ? extensionDimensions(query.extension, plan, measures)
         : [];
      const grain = queryGrainOf(plan, dimensions);
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
            "A #(rollup) rollup variant did not compile",
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
      // A database rollup is read through the build manifest: built and
      // fresh only when the manifest holds its entry.
      const databaseTable =
         table.sourceEntityId !== undefined
            ? args.pinned.compilerManifest.entries[table.sourceEntityId]
                 ?.tableName
            : undefined;
      if (!table.virtualMap && !databaseTable) {
         throw this.noFreshTable(plan, args.pinned, table.sourceEntityId);
      }
      let result;
      try {
         result = await runnable.getPreparedResult({
            ...(table.virtualMap ? { virtualMap: table.virtualMap } : {}),
            ...(args.givens ? { givens: args.givens as never } : {}),
            // Explicit even beside a virtualMap: an undefined manifest reads
            // the Runtime's, unfiltered by freshness. A database table filed
            // under a key not its address is substituted under that key.
            buildManifest: table.virtualMap
               ? EMPTY_BUILD_MANIFEST
               : manifestUnderCompilerKey(
                    underCompilerKeys(
                       args.pinned.compilerManifest,
                       args.pinned.snapshot.compilerKeys,
                    ),
                    table.keyedTable,
                 ),
         } as never);
      } catch {
         throw new LegDecline(
            "rollup_missing_field",
            notCompiledDetail(variant),
         );
      }
      const routed = await readPreparedQuery(runnable);
      checkCoverage(plan, queryLevelFacts(routed));
      annotatedPathDecline(plan, routed.pipeline, query.struct);
      // Route, then govern: past an entry-point gate, the gate must be
      // provable on this rollup before it is used, so `govern()`'s re-proof
      // (graft, bind guard, gate-set equality) denies only on a defect.
      if (query.gates) proveGates(plan, query, routed, variant.modelDef);
      proveGivenTerms(query, routed, variant.modelDef);
      // The compiled SQL must read the table, never recompute the rollup.
      const readsTable = result.sql.includes(
         (table.virtualMap ? table.quotedPath : databaseTable) ?? "\u0000",
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
         rollupBaseSourceID: plan.baseSourceID,
         ...(table.virtualMap ? { virtualMap: table.virtualMap } : {}),
         ...(table.binding ? { binding: table.binding } : {}),
         ...(!table.virtualMap && databaseTable
            ? {
                 databaseTablePath: databaseTable,
                 databaseTableId: table.sourceEntityId,
              }
            : {}),
         ...(!table.virtualMap && table.keyedTable
            ? { keyedTable: table.keyedTable }
            : {}),
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
    * that is `keyColumn`: `unknown` when not checked,
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
    * (its parent key check), as the table's contract records it: `unique`
    * when unique and never NULL, `unknown` when not checked.
    */
   private parentKeyUniqueness(
      plan: RollupVariantPlan,
      struct: unknown,
      pinned: Pinned,
   ): KeyVerdict {
      const sourceID = (struct as { sourceID?: unknown } | undefined)?.sourceID;
      const id = this.tableOf(plan, pinned)?.sourceEntityId;
      if (typeof sourceID !== "string" || id === undefined) return "unknown";
      const checks = pinned.snapshot.entries[id]?.keyUniqueness;
      // A check of the key the source declared then, not now, says nothing.
      const key = primaryKeyOf(
         SourceMap.of(this.host.modelDef()).node(sourceID)?.def,
      );
      if (!checkedKeyIs(checks?.parentKeyColumns, sourceID, key)) {
         return "unknown";
      }
      return verdictOf(checks?.parentKeys?.[sourceID]);
   }

   /**
    * What `binding`'s build found of `sourceID`'s primary key `key`: unknown
    * when it checked another column, or recorded none.
    */
   private bindingKeyUniqueness(
      binding: Binding | undefined,
      sourceID: string,
      key: string | undefined,
   ): KeyVerdict {
      if (!checkedKeyIs(binding?.primaryKeyColumns, sourceID, key)) {
         return "unknown";
      }
      return verdictOf(binding?.primaryKeys?.[sourceID]);
   }

   /**
    * What the build of a stored table the served copy reads for the query's
    * source found of `key`: `unique` when a source it stores declares `key`
    * its primary key and the check found it unique and never NULL;
    * `repeated` when one found it repeated or NULL; `unknown` when its
    * contract records no check of `key`.
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
         // Checked as another column: the key moved since the build.
         if (!checkedKeyIs(binding.primaryKeyColumns, id, key)) continue;
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
    * one past its freshness window (a stale storage binding, or a database
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
      const staleDatabase =
         sourceEntityId !== undefined &&
         Object.hasOwn(pinned.snapshot.entries, sourceEntityId) &&
         !Object.hasOwn(pinned.compilerManifest.entries, sourceEntityId);
      return lookup?.state === "stale" || staleDatabase
         ? new LegDecline(
              "rollup_stale",
              "the rollup's table is past its freshness window",
           )
         : new LegDecline("rollup_not_built", "the rollup has no fresh table");
   }

   /**
    * The storage destination the rollup's table is in, or undefined when it
    * is in the source's own database: its binding's, else the annotation's (storageLookupOf).
    */
   private storedIn(
      plan: RollupVariantPlan,
      pinned: Pinned,
   ): string | undefined {
      const lookup = this.storageLookup(plan, pinned);
      if (!lookup) return undefined;
      return (
         this.tableOf(plan, pinned)?.binding?.destinationName ??
         (lookup.state !== "not_built"
            ? lookup.binding.destinationName
            : plan.storage)
      );
   }

   /**
    * The rollup's storage table at the pin, or undefined when it is read in
    * the source's own database. Keyed by the rollup's own `sourceID`, never
    * by its name; the binding decides the tier (storageLookupOf). While no
    * storage destination is read (`PERSIST_STORAGE_MODE` write-only or off),
    * a database table bound beside a storage one answers, as `on`'s rules
    * would with the storage tier removed.
    */
   private storageLookup(
      plan: RollupVariantPlan,
      pinned: Pinned,
   ): ReturnType<typeof storageLookupOf> {
      const sourceID = rollupSourceID(plan);
      const lookup = storageLookupOf(
         pinned,
         sourceID,
         "rollup",
         plan.storage !== undefined,
      );
      if (!lookup || this.host.serveDestinationConfig()) return lookup;
      const planned = sourceID
         ? this.host.catalog()?.planned.get(sourceID)
         : undefined;
      return planned &&
         Object.hasOwn(pinned.snapshot.entries, planned.sourceEntityId)
         ? undefined
         : lookup;
   }

   /** The rollup's table at the pin: a fresh storage binding, or a planned database one. */
   private tableOf(
      plan: RollupVariantPlan,
      pinned: Pinned,
   ):
      | {
           virtualMap?: VirtualMap;
           quotedPath?: string;
           binding?: Binding;
           sourceEntityId?: string;
           /** A database table filed under a key not its address. */
           keyedTable?: KeyedTable;
           /** A database table's columns the bind left out (`leftOutColumns`). */
           leftOut?: readonly string[];
        }
      | undefined {
      const sourceID = rollupSourceID(plan);
      const lookup = this.storageLookup(plan, pinned);
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
         ...(planned.compilerKey
            ? {
                 keyedTable: {
                    tableId: planned.sourceEntityId,
                    compilerKey: planned.compilerKey,
                 },
              }
            : {}),
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
            ? `join:${use.branch.join.name}${use.destination ? `\u0000${use.destination.basis.key}\u0000${use.destination.keepsJoin}` : ""}${use.gated ? "\u0000gated" : ""}`
            : "base",
         table.binding
            ? `${table.binding.destinationName}/${table.binding.virtualHandle}`
            : `database${(table.leftOut ?? []).map((c) => `\u0001${c}`).join("")}`,
         // The views vary by query; an extension's dimensions follow from
         // its name and the plan.
         ...(use.kind === "base" ? use.views : []),
      ].join("\u0000");
      return this.compiled.get(
         key,
         (scope) => {
            // One graft cache scope per compile: two variants naming the
            // same source (other views, another rollup, a reload, a compile
            // again after the cache dropped it) never share a grafted
            // materializer.
            const cacheScope = `rollup\u0000${scope}\u0000${key}`;
            const unreadDetail = this.unreadDetailOf(plan, table);
            return this.compile(plan, use, table).then((v) => ({
               ...v,
               cacheScope,
               ...(unreadDetail ? { unreadDetail } : {}),
            }));
         },
         (error) => ({ error }),
      );
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
         table.binding && !isDatabase(table.binding) && destinations
            ? servedCopyConfig(
                 packageConfig,
                 destinations(),
                 new Set([table.binding.destinationName]),
              )
            : packageConfig;
      const storedInDestination =
         use.kind === "join" &&
         use.destination &&
         table.binding &&
         !isDatabase(table.binding) &&
         reads
            ? { binding: table.binding, reads, basis: use.destination.basis }
            : undefined;
      if (
         use.kind === "join" &&
         use.gated &&
         (storedInDestination || !use.destination)
      ) {
         return this.compileJoinGated(plan, use, use.gated, {
            packagePath,
            modelPath,
            queryModelURL,
            ...(storedInDestination
               ? {
                    packageConfig: storedInDestination.basis.config,
                    config: storedInDestination.basis.config,
                    stored: storedInDestination,
                 }
               : { packageConfig, config, leftOut: table.leftOut }),
         });
      }
      if (use.kind === "join" && use.destination && storedInDestination) {
         return this.compileJoinInDestination(
            plan,
            { ...use, destination: use.destination },
            storedInDestination.binding,
            storedInDestination.reads,
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
      // the join re-pointed at it.
      const servedName = servedNameOf(plan);
      // Database: import the build source from the declaring model's
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
      const declaration = `${databaseRollupSource(read.plan, this.host.modelDef(), servedName, relativeImport(queryModelURL, companion.href), read.dropped)}
${sourceOverRollup(read.plan, use, servedName, "")}`;
      const modelText = queryModelText(
         this.host.modelDef(),
         queryModelURL,
         pathToFileURL(modelFile).href,
         { alias: new Map([[localName, parentAlias(plan)]]) },
         declaration,
      );
      overlay.set(queryModelURL.href, modelText);
      const { materializer, model, runtime } = await packageFiles(
         packagePath,
         modelPath,
         config,
      ).compile(queryModelURL, overlay);
      // Its model and runtime: where `govern()` grafts the query source's
      // gates when the branch answers past them.
      return {
         materializer,
         modelDef: modelDefOf(model as unknown as { _modelDef: ModelDef }),
         runtime: runtime as unknown as GraftRuntime,
      };
   }

   /**
    * Compile a join branch in a storage destination: the
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
      const servedName = servedNameOf(plan);
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
      const { materializer, model, runtime } = await packageFiles(
         ctx.packagePath,
         ctx.modelPath,
         basis.config,
      ).compile(ctx.queryModelURL, overlay);
      return {
         materializer,
         modelDef: modelDefOf(model as unknown as { _modelDef: ModelDef }),
         runtime: runtime as unknown as GraftRuntime,
      };
   }

   /**
    * Compile a join branch answered past an entry-point gate: the query
    * source redeclared IN AN OVERLAY OF ITS OWN DECLARING FILE
    * (`declaring_file_variant.ts`), as a base variant is, so it keeps the
    * author's `sourceID` and carries the source's gates as note lines, where
    * `govern()` grafts and proves them:
    *
    *     import { <parent> is flights } from "<the declaring file's copy>"
    *     <the rollup>
    *     #(access_filter) …                    // the source's gates
    *     source: flights is <parent> include { *; except: segments } extend {
    *       join_many: segments is <the rollup> on segments.flight_id = id
    *     }
    *
    * Every other field, join and annotation is the author's, read from the
    * copy, so its rows are the author's rows. Over a database rollup the
    * overlay is of the author's files and the rollup is declared over its
    * build source; over a `storage=` rollup in a destination (`stored`) it is
    * of the served copy's file set (`ServedCopyJoinBasis.files`, keyed by the
    * author's own file URLs) and the rollup is declared over its table there.
    */
   private async compileJoinGated(
      plan: RollupVariantPlan,
      use: Extract<VariantUse, { kind: "join" }>,
      gated: { sourceID: string },
      ctx: {
         packagePath: string;
         packageConfig: ModelConnectionInput;
         config: ModelConnectionInput;
         modelPath: string;
         queryModelURL: URL;
         /** Over a database rollup: the columns the bind left out. */
         leftOut?: readonly string[];
         /** Over a `storage=` rollup in a destination. */
         stored?: {
            binding: Binding;
            reads: StoredRollupReads;
            basis: { files: ReadonlyMap<string, string>; modelDef: unknown };
         };
      },
   ): Promise<CompiledVariant> {
      const modelDef = this.host.modelDef() as ModelDef | undefined;
      if (!modelDef) return { error: "no model" };
      const servedName = servedNameOf(plan);
      const { stored } = ctx;
      // A column the bind left out, or no read gives live's values, is not
      // declared, so a query reading it does not compile against the rollup.
      const read = withoutLeftOut(
         plan,
         modelDef,
         stored ? stored.reads.leftOut : ctx.leftOut,
      );
      return compileDeclaringFileVariant({
         packagePath: ctx.packagePath,
         modelPath: ctx.modelPath,
         packageConfig: ctx.packageConfig,
         config: ctx.config,
         modelDef,
         sourceID: gated.sourceID,
         copySuffix: stored ? ".rollup-copy.malloy" : ".rollup-author.malloy",
         queryModelURL: ctx.queryModelURL,
         flags: flagLine(modelDef),
         ...(stored
            ? {
                 basis: {
                    files: stored.basis.files,
                    modelDef: stored.basis.modelDef as ModelDef,
                 },
              }
            : {}),
         // The rollup and the author's source the variant includes, seen
         // from the query model too: the build manifest substitutes the
         // rollup's table there, and the governor resolves the source that
         // declared an inherited gate.
         alsoExported: [servedName, parentAlias(plan)],
         declare: async (variant) => {
            const rollup = stored
               ? storedRollupDeclaration(
                    read.plan,
                    stored.binding,
                    stored.reads,
                    servedName,
                    modelDef,
                    read.dropped,
                 )
               : await databaseRollupInOverlay(plan, read, modelDef, {
                    servedName,
                    declaringURL: variant.declaringURL,
                    copyName: variant.copyName,
                    addFile: variant.addFile,
                    authorText: variant.authorText,
                 });
            if (typeof rollup !== "string") return rollup;
            const parent = parentAlias(plan);
            // malloy-text: storedRollupDeclaration's or databaseRollupSource's declaration, every name through identifier
            const rollupText = rollup;
            // malloy-text: the author's own gate notes, as gateLines writes them for every declaring-file variant
            const gateText = variant.gates;
            return `import { ${identifier(parent)} is ${identifier(variant.sourceName)} } from ${doubleQuotedString(variant.copyName)}
${rollupText}
${gateText}${sourceOverRollup(read.plan, { ...use, localName: variant.sourceName }, servedName, "")}`;
         },
      });
   }

   /**
    * Compile a base variant: the query's run target redeclared over the
    * rollup in an overlay of ITS OWN DECLARING FILE
    * (`declaring_file_variant.ts`), so it keeps the author's `sourceID` and
    * carries the run target's gates as note lines, which `govern()` grafts
    * and proves on the variant when the router lets it past an
    * entry-point gate. A database rollup's build source is imported from
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
      const servedName = servedNameOf(plan);
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
            // Database: the build source from the base's companion, so the
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
            return `${databaseRollupSource(read.plan, modelDef, servedName, relativeImport(declaringURL, companion.href), read.dropped)}
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

/**
 * A database rollup's declaration in a declaring-file overlay: its build
 * source imported from the companion beside its base's declaring file (added
 * to the overlay, generated over the copy of that file when it is the
 * overlaid one), so its build SQL, and so its address and table, are the
 * build plan's, and the build manifest substitutes its table.
 */
async function databaseRollupInOverlay(
   plan: RollupVariantPlan,
   /** The plan as read: its left-out columns dropped (`withoutLeftOut`). */
   read: { plan: RollupVariantPlan; dropped: readonly string[] },
   modelDef: ModelDef,
   ctx: {
      servedName: string;
      declaringURL: URL;
      copyName: string;
      addFile(url: URL, text: string): void;
      authorText(url: URL): Promise<string | undefined>;
   },
): Promise<string | { error: string }> {
   const { declaringURL, copyName, addFile, authorText } = ctx;
   const base = splitSourceID(plan.baseSourceID);
   const companion = base ? rollupCompanionURLOf(base.url) : undefined;
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
   // The base's build copy, made as the plan made it, so the build source's
   // SQL (and so its table) is the plan's.
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
   return databaseRollupSource(
      read.plan,
      modelDef,
      ctx.servedName,
      relativeImport(declaringURL, companion.href),
      read.dropped,
   );
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
 * What an extension adds over the base its rollups aggregate, or a
 * {@link LegDecline} when that is more than fields: a `where:` of its own
 * (its rows are not the base's, `GRAIN_TOO_COARSE`), or a `timezone:` of its
 * own (`TIME_ZONE`). A gate of its own never reaches here: the router skips
 * the leg for a gated entry point (`ROW_LEVEL_GATE`).
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
 * grain dimensions and dimensions already taken, in an order where
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
 * A database rollup's served source: its build source imported from the
 * rollup companion (`from`, relative to the importing file), so the build
 * manifest substitutes its table, extended with the rollup's merged measures.
 */
function databaseRollupSource(
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
 * A database rollup read without the stored columns the bind left out
 * (`leftOutColumns`: altered outside a build, and no faithful cast reads
 * them as declared), as the served copy leaves such a column out: each
 * merged measure with a left-out partial is not declared, and its partials
 * and each left-out grain column are dropped (`dropped`), so only a query
 * that reads one fails to compile against the rollup and answers live. The build source is still
 * the plan's own, so its table's address does not move.
 */
/** The name a variant declares its rollup under. */
function servedNameOf(plan: RollupVariantPlan): string {
   return `${plan.rollupSourceName}__served`;
}

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
         ...(plan.pathColumns
            ? { pathColumns: plan.pathColumns.filter((p) => !gone(p.column)) }
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
 * The rollup counterpart of the served copy's check that a gate's columns
 * are kept (`checkGateColumnsKept`).
 */
function gateReadsLeftOut(
   gates: EnforcedGates,
   leftOut: readonly string[],
): string | undefined {
   if (leftOut.length === 0) return undefined;
   const out = new Set(leftOut.map((c) => c.toLowerCase()));
   return gateIdentifiers([...gates.authorize, ...gates.accessFilter]).find(
      (n) => out.has(n.toLowerCase()),
   );
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
   // warehouse that folded the build's quoted aliases.
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
   const site = isDatabase(binding)
      ? {
           database: true,
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
 * rollup itself under the source's name, or the author's source with
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
      return `source: ${identifier(use.localName)} is ${identifier(servedName)}${internalPartials(plan)}${pathRecords(plan)}${reappliedTerms(plan)}${extension}`;
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
 * `internal` grain dimension a gate reads, redeclared with the author's
 * modifier, readable by the grafted gate and by no query, as on live.
 */
function internalPartials(plan: RollupVariantPlan): string {
   const hidden = [
      ...plan.rollupMeasures.flatMap((m) => m.partials.map((p) => p.column)),
      ...(plan.internalGrain ?? []),
      // A path grain's stored column (`account__region`): read through the
      // join's record, as live reads the path, and by no query by its name.
      ...storedPathColumns(plan),
   ]
      .map((n) => identifier(n))
      .join(", ");
   const priv = (plan.privateGrain ?? []).map((n) => identifier(n)).join(", ");
   // None left (every measure's partials left out of a stored rollup).
   if (!hidden && !priv) return "";
   return ` include {${hidden ? ` internal: ${hidden}` : ""}${priv ? ` private: ${priv}` : ""} public: * }`;
}

/** The stored columns of the plan's path grains (`account__region`). */
function storedPathColumns(plan: RollupVariantPlan): string[] {
   return (plan.pathColumns ?? [])
      .filter((p) => p.column !== p.grain)
      .map((p) => p.column);
}

/**
 * ` extend { dimension: account is { region is account__region } }`: each
 * join a stored column holds a one-hop path through, declared as a record
 * of those columns (a timeframe restored, as the served copy's `inline=`
 * record restores one), so `account.region` in a query, a view or a
 * filter compiles against the rollup and reads the stored column. Any
 * other field of the join, its measures and an aggregate through it
 * (`account.count()`) are not in the record and do not compile.
 */
function pathRecords(plan: RollupVariantPlan): string {
   const byJoin = new Map<string, Map<string, RollupPathColumn>>();
   for (const p of plan.pathColumns ?? []) {
      const fields = byJoin.get(p.join) ?? new Map();
      if (!fields.has(p.field)) fields.set(p.field, p);
      byJoin.set(p.join, fields);
   }
   if (byJoin.size === 0) return "";
   // malloy-text: names through identifier, a timeframe from TIMEFRAMES
   const records = [...byJoin]
      .map(([join, fields]) => {
         const lines = [...fields.values()]
            .map((p) => {
               const read =
                  p.timeframe && TIMEFRAMES.has(p.timeframe)
                     ? `${identifier(p.column)}.${p.timeframe}`
                     : identifier(p.column);
               return `      ${identifier(p.field)} is ${read}`;
            })
            .join(",\n");
         return `    ${identifier(join)} is {\n${lines}\n    }`;
      })
      .join("\n");
   return ` extend {\n  dimension:\n${records}\n}`;
}

/**
 * What a query on the base may group and filter by: the grain, each path a
 * stored column holds (`account.region`), and an extension's dimensions over
 * the grain.
 */
function queryGrainOf(
   plan: RollupVariantPlan,
   dimensions: readonly { name: string }[],
): Set<string> {
   return new Set([
      ...plan.grainDimensions,
      ...pathGrainEntries(plan).keys(),
      ...dimensions.map((d) => d.name),
   ]);
}

/** Each path a stored column holds, dotted (`account.region`), to its grain entry. */
function pathGrainEntries(plan: RollupVariantPlan): Map<string, string> {
   const out = new Map<string, string>();
   for (const p of plan.pathColumns ?? []) {
      const path = `${p.join}.${p.field}`;
      if (!out.has(path)) out.set(path, p.grain);
   }
   return out;
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
   // A path grain is stored under its column's name, with the joined
   // dimension's timeframe; a record's field carries no notes.
   for (const p of plan.pathColumns ?? []) {
      if (p.column !== p.grain && p.timeframe !== undefined) {
         timeframes.set(p.column, p.timeframe);
      }
   }
   return plan.grainDimensions.map((grain) => {
      const name = grainColumnOf(grain);
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
 * A rollup skipped for the query's gate (`ROW_LEVEL_GATE`). Its detail never names
 * the gate's field or text: a caller may not learn a gate's shape from why
 * its query was answered live.
 */
/**
 * The gates written on a variant's run target ({@link entryGates}), for the
 * proof that they are exactly the live entry point's; a walk that cannot be
 * read is a gate the rollup cannot prove (`ROW_LEVEL_GATE`).
 */
export function writtenGatesOf(struct: unknown, model: unknown): EnforcedGates {
   const entries = entryGates(struct, model);
   if (entries === "unreadable") throw contractSkip();
   return enforcedGatesOf(entries);
}

function contractSkip(): LegDecline {
   return new LegDecline(
      "rollup_row_level_gate",
      "the query's source has an access rule this rollup cannot apply",
      { shapeCheck: true },
   );
}

/**
 * Why a join branch declines past an entry-point gate (the ungated-rows
 * rule for rollups), or undefined when it may answer: its joined base is
 * ungated (`baseGated`, its own, inherited and joined gates alike) and
 * re-applies no given-reading term, and every field the gate reads is one of
 * the query source's own plain columns. The gate is then grafted on the
 * variant's query source, which keeps the author's rows and columns, so it
 * filters the parents exactly as live does, before the re-pointed join reads
 * the rollup; the join-key checks keep each parent's rollup rows its own. A
 * gate that reads through any join (the re-pointed one above all: a filter on
 * a fanned-out rollup row admits a parent through one group while it
 * aggregates the others), a computed dimension, or a measure declines
 * (`ROW_LEVEL_GATE`).
 */
function joinGateDecline(
   gates: EnforcedGates,
   struct: unknown,
   plan: RollupVariantPlan,
): LegDecline | undefined {
   if (plan.baseGated !== false || (plan.readTimeTerms ?? []).length > 0) {
      return contractSkip();
   }
   const fields = new Map(
      (
         ((struct as { fields?: unknown[] } | undefined)?.fields ?? []) as {
            name?: unknown;
            as?: unknown;
            e?: unknown;
            join?: unknown;
            expressionType?: unknown;
         }[]
      ).map((f) => [String(f.as ?? f.name), f] as const),
   );
   for (const expr of [...gates.authorize, ...gates.accessFilter]) {
      for (const id of malloyIdentifiers(expr)) {
         const f = fields.get(id);
         if (!f) continue;
         const plain =
            f.e === undefined &&
            f.join === undefined &&
            (f.expressionType === undefined || f.expressionType === "scalar");
         if (!plain) return contractSkip();
      }
   }
   return undefined;
}

/**
 * The gates the live entry point carries ({@link entryGates}), or undefined
 * when it has none. A walk that cannot be read counts as a gate no rollup
 * proves.
 */
export function liveEntryPointGates(
   struct: unknown,
   modelDef: unknown,
): EnforcedGates | undefined {
   const entries = entryGates(struct, modelDef);
   if (entries === "unreadable")
      return { authorize: ["false"], accessFilter: [] };
   const gates = enforcedGatesOf(entries);
   return gates.authorize.length + gates.accessFilter.length > 0
      ? gates
      : undefined;
}

/**
 * The pre-proof for a query let past an entry-point gate (for rollups):
 * throw `ROW_LEVEL_GATE` unless the variant's run target is the author's source (its
 * `sourceID`), the gates written on it are exactly the live entry point's
 * (by route and expression, `sameGateSet`: one rule with `govern()`'s
 * re-proof), and every field those gates read is one of the rollup's grain
 * dimensions, so filtering its groups equals filtering the base's rows
 * (table 1).
 */
export function proveGates(
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
   const written = writtenGatesOf(struct, model);
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
 * The pre-proof of the re-applied `where:` (for given-reading terms): throw
 * `ROW_LEVEL_GATE` unless the variant's run target writes exactly the given-reading
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
 * Throw `MISSING_FIELD` when the compiled query returns, as a column, a
 * joined dimension a stored path column holds and the author annotated: the
 * variant's record field cannot carry its notes, which live's column
 * carries (the served copy's rule for an `inline=` record,
 * governance/ir/joined_field_output.ts). A field that reads it under another
 * name, a filter and a grouping by a dimension over it carry none live
 * either, and are served.
 */
function annotatedPathDecline(
   plan: RollupVariantPlan,
   pipeline: unknown,
   struct: object | undefined,
): void {
   const annotated = new Map<string, Set<string>>();
   for (const p of plan.pathColumns ?? []) {
      if (!p.annotated) continue;
      const fields = annotated.get(p.join) ?? new Set<string>();
      fields.add(p.field);
      annotated.set(p.join, fields);
   }
   if (annotated.size === 0) return;
   const out = annotatedJoinedOutput(
      pipeline,
      (struct as { fields?: unknown } | undefined)?.fields,
      annotated,
   );
   if (out.output || out.unreadable) {
      throw new LegDecline(
         "rollup_missing_field",
         out.output
            ? `the query returns \`${out.output.path.join(".")}\`, whose annotations the rollup's record of stored columns cannot carry`
            : "the query's output columns cannot be read",
      );
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
 * aggregates: the plan's own fan-out dimensions (read through a
 * join_many), and those a measure read at that level fans out across (a
 * join-level sum or count re-summed across groups that share its key).
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
 * The switches {@link planSourceRollups} reads, as a memo key: `#(rollup)`
 * with `PUBLISHER_MATERIALIZE` (`readsRollupDeclarations`), and whether a gated base's rollup
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

/** Literals and calls whose value an expression takes in the zone it is evaluated in. */
const ZONED_NODES = new Set(["now", "timestampLiteral", "timestamptzLiteral"]);

/** Whether an expression holds a literal or call read in the evaluating zone (`now`, a timestamp literal). */
function hasZonedNode(e: unknown, depth = 0): boolean {
   if (depth > 64 || e === null || typeof e !== "object") return false;
   if (Array.isArray(e)) return e.some((x) => hasZonedNode(x, depth + 1));
   if (ZONED_NODES.has(String((e as { node?: unknown }).node))) return true;
   return Object.values(e).some((v) => hasZonedNode(v, depth + 1));
}

/**
 * Why a query that sets its own `timezone:` is not read from `plan`, or
 * undefined. It is the served copy's rule: what the build evaluated in
 * the model's zone must not depend on the zone, and what the query evaluates
 * itself it evaluates in its own zone over the stored values, as live does.
 *
 * - A column stored as the base has it (a `date`, a `timestamp`, anything
 *   without an expression) holds the same value in every zone, so the query
 *   may read it, truncate it or filter on it; but not a column a query base
 *   truncated (it carries a timeframe), stored in the base's zone.
 * - A computed dimension or a measure was evaluated by the build: it depends
 *   on the zone when its expression reads a time (a truncation, an
 *   extraction, a comparison), directly or through another computed field,
 *   or holds `now` or a timestamp literal.
 * - The stored partials and the base's `where:` were evaluated by the
 *   build: any time they read depends on the zone.
 * - A field reached through one of the base's joins is not proved: declined.
 *
 * - A field the entry point's gates read is checked as the build's own
 *   reads are: the gates are grafted on the variant after this check, in the
 *   query's zone, so a gate over a stored day truncated in the model's zone
 *   would admit another day's groups.
 *
 * `extra` are build-evaluated reads the caller adds (a join branch's grain).
 * Fails closed on usage that could not be read.
 */
function zoneDecline(
   plan: RollupVariantPlan,
   struct: object | undefined,
   usage: readonly (readonly string[])[] | undefined,
   gates: EnforcedGates | undefined,
   extra: readonly (readonly string[])[] = [],
): LegDecline | undefined {
   if (usage === undefined || struct === undefined) return zonedQueryDecline();
   const def = struct as { fields?: unknown[]; filterList?: unknown[] };
   const fields = new Map(
      (def.fields ?? []).map((f) => {
         const field = f as { name?: unknown; as?: unknown };
         return [String(field.as ?? field.name), f as object];
      }),
   );
   type Field = {
      type?: unknown;
      e?: unknown;
      join?: unknown;
      timeframe?: unknown;
   };
   const memo = new Map<string, boolean>();
   const storedPaths = pathGrainEntries(plan);
   // A one-hop path a stored column holds, as the build read it through
   // the join: a column of the joined source stored as it has it holds the
   // same value in every zone; a truncation, a computed time, or a join
   // whose condition reads a time does not. Anything unread is zoned.
   const joinOf = (name: string) =>
      fields.get(name) as
         | (Field & { fields?: unknown[]; onExpression?: unknown })
         | undefined;
   const joinedOf = (join: { fields?: unknown[] }) =>
      new Map(
         (join.fields ?? []).map((f) => {
            const field = f as { name?: unknown; as?: unknown };
            return [String(field.as ?? field.name), f as Field];
         }),
      );
   const temporal = (f: Field | undefined) =>
      !f || f.join !== undefined || ZONED_TYPES.has(String(f.type));
   // Whether the condition of the join `name` reads a time, or anything it
   // cannot read: the build evaluated it in the model's zone.
   const conditionDependsOnZone = (name: string): boolean => {
      const join = joinOf(name);
      if (!join || join.join === undefined) return true;
      const joined = joinedOf(join);
      return (
         hasZonedNode(join.onExpression) ||
         fieldPathsIn(join.onExpression).some((p) =>
            p.length === 1
               ? temporal(fields.get(p[0] as string) as Field | undefined)
               : p.length !== 2 ||
                 p[0] !== name ||
                 temporal(joined.get(p[1] as string)),
         )
      );
   };
   // A one-hop path a stored column holds, as the build read it through
   // the join: a column of the joined source stored as it has it holds the
   // same value in every zone; a truncation, a computed time, or a join
   // whose condition reads a time does not. Anything unread is zoned.
   const joinedDependsOnZone = (path: readonly string[]): boolean => {
      const join = joinOf(path[0] as string);
      if (!join || conditionDependsOnZone(path[0] as string)) return true;
      const joined = joinedOf(join);
      const f = joined.get(path[1] as string);
      if (!f || f.join !== undefined) return true;
      if (f.e === undefined) {
         return f.timeframe !== undefined && ZONED_TYPES.has(String(f.type));
      }
      return (
         ZONED_TYPES.has(String(f.type)) ||
         hasZonedNode(f.e) ||
         fieldPathsIn(f.e).some(
            (p) =>
               p.length !== 1 ||
               temporal(joined.get(p[0] as string)) ||
               joined.get(p[0] as string)?.e !== undefined,
         )
      );
   };
   const pathJoins = new Set(
      [...storedPaths.keys()].map((p) => p.slice(0, p.indexOf("."))),
   );
   // A read of a stored path's join condition (`account.account_id`, which
   // a query reading the path reads through the join): judged with it.
   const conditionRead = (path: readonly string[]) =>
      path.length === 2 &&
      pathJoins.has(path[0] as string) &&
      fieldPathsIn(joinOf(path[0] as string)?.onExpression).some(
         (p) => p.join(".") === path.join("."),
      );
   const isStoredPath = (path: readonly string[]) =>
      path.length === 2 && storedPaths.has(path.join("."));
   // A field's value as the table holds it: does it depend on the zone?
   const storedDependsOnZone = (path: readonly string[]): boolean => {
      const [name] = path;
      // An empty path reads no field (`count()`).
      if (name === undefined) return false;
      if (isStoredPath(path)) return joinedDependsOnZone(path);
      if (conditionRead(path)) {
         return conditionDependsOnZone(path[0] as string);
      }
      if (path.length !== 1) return true;
      const f = fields.get(name) as Field | undefined;
      if (!f) return false;
      if (f.join !== undefined) return true;
      // A column a query base truncated (`group_by: d is t.day`) carries its
      // timeframe: a truncation stored in the base's zone.
      if (f.e === undefined) {
         return f.timeframe !== undefined && ZONED_TYPES.has(String(f.type));
      }
      const known = memo.get(name);
      if (known !== undefined) return known;
      // A cycle cannot compile; read as independent while it is walked.
      memo.set(name, false);
      const depends =
         hasZonedNode(f.e) || fieldPathsIn(f.e).some(evaluatedReadsTime);
      memo.set(name, depends);
      return depends;
   };
   // A read inside an expression the build evaluated: any time is zoned.
   const evaluatedReadsTime = (path: readonly string[]): boolean => {
      const [name] = path;
      if (name === undefined) return false;
      if (isStoredPath(path)) return joinedDependsOnZone(path);
      if (path.length !== 1) return true;
      const f = fields.get(name) as Field | undefined;
      if (!f) return false;
      if (f.e === undefined && f.join === undefined) {
         return ZONED_TYPES.has(String(f.type));
      }
      return storedDependsOnZone(path);
   };
   const stored = plan.rollupMeasures.flatMap((m) =>
      m.partials.flatMap((p) => [p.leaf.joinPath, ...p.leaf.reads]),
   );
   const where = fieldPathsIn(def.filterList);
   const gated = gateIdentifiers(
      gates ? [...gates.authorize, ...gates.accessFilter] : [],
   ).map((id) => [id]);
   return usage.some(storedDependsOnZone) ||
      extra.some(storedDependsOnZone) ||
      where.some(evaluatedReadsTime) ||
      gated.some(evaluatedReadsTime) ||
      hasZonedNode(def.filterList) ||
      stored.some((p) => p.length > 0 && evaluatedReadsTime(p))
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

/** What a query on one source could read a rollup through. */
interface RollupCandidates {
   readonly target?: {
      plans: RollupVariantPlan[];
      extension?: RollupQuery["extension"];
   };
   readonly branches: ReturnType<typeof joinRollupBranches>;
}

const NONE: RollupCandidates = { branches: [] };

/**
 * What a query on one candidate source declines by, or carries, before its
 * own shape is read: the same for every query on the source, so recorded
 * with the plans and looked up per query.
 */
interface RollupStatics {
   /** The live entry point's gates ({@link liveEntryPointGates}). */
   readonly gates?: EnforcedGates;
   /** Whether the source sets its own `timezone:`. */
   readonly timeZoned: boolean;
   /**
    * For a source that extends or renames the rolled-up one, what it adds
    * ({@link checkExtension}), or why its base's rollups decline.
    */
   readonly extension?: { delta: SourceDelta } | { decline: LegDecline };
   /**
    * Each join branch's decline past the entry point's gates
    * ({@link joinGateDecline}), by the branch's index; empty when ungated.
    */
   readonly branchGates: readonly (LegDecline | undefined)[];
}

/** {@link RollupStatics} of the source `def`, as `modelDef` declares it. */
function rollupStaticsOf(
   def: object,
   modelDef: unknown,
   candidates: RollupCandidates,
): RollupStatics {
   const gates = liveEntryPointGates(def, modelDef);
   let extension: RollupStatics["extension"];
   const baseDef = candidates.target?.extension?.baseDef;
   if (baseDef !== undefined) {
      try {
         extension = { delta: checkExtension(def, baseDef) };
      } catch (error) {
         if (!isDeclineOf(error, "rollup")) throw error;
         extension = { decline: error };
      }
   }
   return {
      ...(gates ? { gates } : {}),
      timeZoned: timezoneOf(def) !== undefined,
      ...(extension ? { extension } : {}),
      branchGates: gates
         ? candidates.branches.map(({ branch }) =>
              joinGateDecline(gates, def, branch.plan),
           )
         : [],
   };
}
