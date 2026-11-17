// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Persisted named queries: the router's query leg for `#@ persist` on a
 * top-level `query:`.
 *
 * A persisted query's table holds the query's result, so it answers exactly
 * `run: q` (in every spelling a caller sends it: REST or MCP `queryName`, a
 * report naming it, a bare tile) and `run: q -> { … }`, whose later stages
 * read the stored result. A refinement (`run: q + { … }`) changes the
 * query's own last stage and is declined (`MISSING_FIELD`); any use of `q`
 * inside another definition never reads as a call, so the leg does not apply.
 *
 * The caller's text compiles unchanged, in a variant of the declaring file in
 * which the run target is redeclared over the table and the query over that:
 *
 *     source: flights is <the query's table>
 *     # bar_chart                               // the query's own notes
 *     query: q is flights -> { group_by: carrier; aggregate: n is n.max(); order_by: n desc }
 *
 * so the result keeps the query's name, its columns in their order and kinds
 * (a measure re-aggregates its one stored value per row, so it is a measure
 * again, as live reports it), its ordering and its render tags. The columns,
 * the regroup rule and its refusals are the view leg's (`view_variants.ts`):
 * a result that does not regroup exactly answers live. A run target written
 * inline has no name to redeclare: the query alone is redeclared over the
 * table, under a generated source name.
 *
 * Gates: a persisted query is never built over a gated source (the planner
 * refuses one, `persisted_view_compile.ts`), and its leg is never tried past
 * an entry-point gate (`leg.ts`), so the gate policy has no row for it.
 *
 * Behind `materialize.enabled`.
 */

import type {
   MalloyConfig,
   ModelDef,
   ModelMaterializer,
   QueryMaterializer,
} from "@malloydata/malloy";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { queryTimezoneOf } from "../../governance/ir/compiled_model";
import { identifier } from "../../malloy_text";
import type { ModelConnectionInput } from "../../model_runtime";
import { extractRunQueryCall, type RunQueryCall } from "../../query_text";
import type { Binding, Pinned } from "../domain/binding";
import { LegDecline } from "../domain/leg_decline";
import type {
   PersistedQueryPlan,
   PersistedViewPlan,
} from "../domain/table_plans";
import type { BuildManifest } from "../domain/types";
import { viewNoteTexts } from "../text/read_text";
import {
   callerTextDecline,
   compilePersistedResultVariant,
   type CompiledVariant,
   type PersistedViewServe,
   readStoredVariant,
   type ResolvedTable,
   resultKinds,
   type StageIR,
   stageShape,
   type ViewAnswer,
   viewDeclaration,
   type ViewShape,
   type ViewTable,
} from "./view_variants";

/** What the leg reads from the model that owns it. */
interface QueryVariantHost {
   modelPath(): string;
   modelDef(): ModelDef | undefined;
   modelMaterializer(): ModelMaterializer | undefined;
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
   /**
    * Every persisted view and named query a compiled model can see, as the
    * planner reads them: passed in by the model, since serving never imports
    * the planner.
    */
   planModelPersistedViews(modelDef: unknown): { plans: PersistedViewPlan[] };
   planModelPersistedQueries(modelDef: unknown): {
      plans: PersistedQueryPlan[];
   };
}

/** One model's persisted named queries. */
export class PersistedQueryServe {
   private packagePath?: string;
   private packageConfig?: ModelConnectionInput;
   /** Bumped when the context or the bindings change: a graft cache scope. */
   private generation = 0;
   private compiled = new Map<string, Promise<CompiledVariant>>();
   /** The snapshot bindings {@link compiled} was compiled over. */
   private compiledFor: readonly Binding[] | undefined;
   private planMemo?: { modelDef: unknown; plans: PersistedQueryPlan[] };

   /**
    * `views` is the same model's view leg, whose table lookup a query's
    * table shares (one catalog, one pin).
    */
   constructor(
      private readonly host: QueryVariantHost,
      private readonly views: PersistedViewServe,
   ) {}

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

   /** Every persisted named query this model can see. */
   plans(): PersistedQueryPlan[] {
      const modelDef = this.host.modelDef();
      const memo = this.planMemo;
      if (memo && memo.modelDef === modelDef) return memo.plans;
      const plans = modelDef
         ? this.host.planModelPersistedQueries(modelDef).plans
         : [];
      this.planMemo = { modelDef, plans };
      return plans;
   }

   hasQueries(): boolean {
      return this.plans().length > 0;
   }

   /**
    * The `storage=` tables of a pinned snapshot that store a query this
    * model can see: every one bound, or only those read at the pin.
    */
   queryBindings(pinned: Pinned, which: "bound" | "fresh"): Binding[] {
      const ids = new Set(this.plans().map((p) => p.buildSourceID));
      return (
         which === "bound" ? pinned.snapshot.bindings : pinned.fresh
      ).filter(
         (b) =>
            b.kind === "query" &&
            b.tier === "storage" &&
            (b.sourceIDs ?? []).some((id) => ids.has(id)),
      );
   }

   /**
    * The persisted named query the text calls (`run: q`, `run: q -> { … }`,
    * or a refinement, which the attempt declines): a token scan, with no
    * compile, by the name the model gives it, declared in the file the plan
    * names.
    */
   calledQuery(
      queryString: string,
   ): { call: RunQueryCall; plan: PersistedQueryPlan } | undefined {
      if (!this.hasQueries()) return undefined;
      const call = extractRunQueryCall(queryString);
      if (!call) return undefined;
      const entry = (
         this.host.modelDef()?.contents as Record<string, unknown> | undefined
      )?.[call.query] as
         | { type?: unknown; name?: unknown; location?: { url?: unknown } }
         | undefined;
      if (entry?.type !== "query") return undefined;
      const plan = this.plans().find(
         (p) =>
            p.queryName === call.query &&
            p.queryName === (entry.name ?? call.query) &&
            p.declaringURL === entry.location?.url,
      );
      return plan ? { call, plan } : undefined;
   }

   /** The table a query calling a persisted query would read, for the path report. */
   reportedTable(
      queryString: string,
      pinned: Pinned,
   ): (ViewTable & { kind: "query" }) | undefined {
      const called = this.calledQuery(queryString);
      if (!called) return undefined;
      const { virtualMap, quotedPath, binding, databaseTable, ...table } =
         this.views.tableOf(called.plan, pinned, pinned.compilerManifest);
      void virtualMap;
      void quotedPath;
      void binding;
      void databaseTable;
      return { ...table, kind: "query" };
   }

   /**
    * Answer `run: q` (or `run: q -> { … }`) from the persisted query's
    * table, or throw a {@link LegDecline}. The compiled query must be the
    * declaration: its name, in its declaring file, over its run target; a
    * caller's redefinition of the name is another query.
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
      const called = this.calledQuery(args.queryString);
      if (!called) throw new LegDecline("query_shape_mismatch");
      const { call, plan } = called;
      if (call.refined) {
         throw new LegDecline(
            "query_shape_mismatch",
            `the query refines \`${call.query}\` (\`+ { … }\`), and the stored result holds only the query as written`,
         );
      }
      const unfollowed = callerTextDecline(
         call.otherNames,
         [
            call.query,
            plan.queryName,
            plan.sourceName,
            plan.importedSource,
            inlineTargetOf(plan),
         ],
         call.query,
         {
            contents: this.host.modelDef()?.contents as
               | Record<string, unknown>
               | undefined,
            declaringURL: plan.declaringURL,
         },
         "query",
      );
      if (unfollowed) throw unfollowed;
      const prepared = await readPreparedQuery(args.authorRunnable);
      const compiledModel = (prepared.modelDef ?? this.host.modelDef()) as
         | { contents?: Record<string, unknown> }
         | undefined;
      const compiled = compiledModel?.contents?.[plan.queryName] as
         | { type?: unknown; location?: { url?: unknown } }
         | undefined;
      const notTheQuery = new LegDecline(
         "query_shape_mismatch",
         `the query does not call \`${call.query}\` as its file declares it`,
      );
      if (
         compiled?.type !== "query" ||
         compiled.location?.url !== plan.declaringURL
      ) {
         throw notTheQuery;
      }
      let runTarget: string | undefined;
      if (plan.sourceID !== undefined) {
         const struct = resolveStructRefSource<{ sourceID?: unknown }>(
            prepared.structRef,
            prepared.modelDef ?? this.host.modelDef(),
         );
         if (
            typeof prepared.structRef !== "string" ||
            struct?.sourceID !== plan.sourceID
         ) {
            throw notTheQuery;
         }
         runTarget = prepared.structRef;
      }
      // An exact call shows in the public result schema; a query that goes
      // on past it shows only in its text.
      if (call.laterStages === 0) {
         const result = await args.authorRunnable.getPreparedResult({
            ...(args.givens ? { givens: args.givens as never } : {}),
         } as never);
         if (result.resultExplore.name !== plan.queryName) throw notTheQuery;
      }
      const table = this.views.tableOf(plan, args.pinned, args.buildManifest);
      if (!table.built) throw new LegDecline("query_not_built");
      if (table.stale || (!table.virtualMap && !table.databaseTable)) {
         throw new LegDecline("query_stale");
      }
      const variant = await this.compiledVariant(plan, table);
      if (variant.refusal) {
         throw new LegDecline(
            "query_missing_field",
            `the stored result cannot answer \`${call.query}\` with live's columns: ${variant.refusal}`,
         );
      }
      return readStoredVariant({
         ...args,
         variant,
         table,
         modelPath: this.host.modelPath(),
         subject: `query ${plan.queryName}`,
         cacheScope: `persisted_query\u0000${this.generation}\u0000${plan.buildSourceID}\u0000${runTarget ?? ""}`,
         compilerKey: this.views.compilerKeyOf(plan.buildSourceID),
      });
   }

   private compiledVariant(
      plan: PersistedQueryPlan,
      table: ResolvedTable,
   ): Promise<CompiledVariant> {
      const key = [
         plan.buildSourceID,
         table.binding
            ? `${table.binding.destinationName}/${table.binding.virtualHandle}`
            : "database",
      ].join("\u0000");
      let compiled = this.compiled.get(key);
      if (!compiled) {
         compiled = this.compile(plan, table).catch((error) => ({
            error: error instanceof Error ? error.message : String(error),
         }));
         this.compiled.set(key, compiled);
      }
      return compiled;
   }

   /**
    * Compile the variant: the run target redeclared over the query's table
    * in an overlay of their one declaring file (so it keeps the author's
    * `sourceID`, as the view leg's does), and the query redeclared over
    * that. A run target written inline has no name: under the name the
    * model gives an imported one, else a generated one.
    */
   private async compile(
      plan: PersistedQueryPlan,
      table: ResolvedTable,
   ): Promise<CompiledVariant> {
      const packagePath = this.packagePath;
      const packageConfig = this.packageConfig;
      const modelDef = this.host.modelDef();
      const author = this.host.modelMaterializer();
      if (!packagePath || !packageConfig || !modelDef || !author) {
         return { error: "no context" };
      }
      const shape = await queryShape(author, modelDef, plan);
      const inlineTarget = plan.importedSource ?? inlineTargetOf(plan);
      // The run target's name in its declaring file (the plan read it there),
      // or the inline one's.
      const target = plan.sourceName ?? inlineTarget;
      const query = viewDeclaration(
         plan.queryName,
         shape,
         `query: ${identifier(plan.queryName)} is ${identifier(target)} ->`,
      );
      if ("refusal" in query) return { refusal: query.refusal };
      // malloy-text: viewDeclaration's text, every value through identifier or noteSource
      const queryText = query.text;
      return compilePersistedResultVariant({
         packagePath,
         packageConfig,
         modelDef,
         modelPath: this.host.modelPath(),
         destinations: this.host.serveDestinationConfig(),
         companionPlans: () => ({
            views: this.host.planModelPersistedViews(modelDef).plans,
            queries: this.plans(),
         }),
         plan,
         table,
         shape,
         redeclare:
            plan.sourceID !== undefined
               ? { sourceID: plan.sourceID, alsoRedeclared: [plan.queryName] }
               : {
                    declaringURL: plan.declaringURL,
                    alsoRedeclared: [plan.queryName],
                    // An inline run target's generated source, which the
                    // query names by a bare reference the query model must
                    // resolve too.
                    alsoExported: [inlineTarget],
                 },
         databaseAlias: `${plan.buildSourceName}__stored`,
         emit: (read) => {
            // malloy-text: the run target's own gate expressions, as the author wrote them, one per line (see gateLines)
            const gates = read.gates;
            // malloy-text: the run target's own `timezone:` statement, its zone through stringLiteral
            const zone = read.sourceTimezone;
            // The run target's own time zone: on the stored source beside the
            // timeframes a `storage=` read truncates again, on the
            // redeclaration for a database table, read as built.
            let storedSource: string;
            let redeclaredZone = "";
            if ("typeText" in read.stored) {
               // malloy-text: storedTypedSource's text, names through identifier
               const typeText = read.stored.typeText;
               // malloy-text: the served copy's timeframe and cast emitter, the author's notes, names through identifier
               const timeframes = read.stored.timeframes;
               storedSource = `${typeText}${zone || timeframes ? ` extend {${zone}${timeframes}\n}` : ""}`;
            } else {
               // malloy-text: the companion import, names through identifier and its file through doubleQuotedString
               storedSource = read.stored.importLine;
               if (zone) redeclaredZone = ` extend {${zone}\n}`;
            }
            return `${storedSource}
${gates}source: ${identifier(target)} is ${identifier(read.stored.name)}${redeclaredZone}
${queryText}`;
         },
      });
   }
}

/** The generated name a run target written inline is redeclared under. */
function inlineTargetOf(plan: PersistedQueryPlan): string {
   return `${plan.buildSourceName}__run`;
}

/**
 * The query's output columns (with live's kinds), ordering, notes and time
 * zone, from a compile of the bare call on the author's model, as the view
 * leg reads a view's.
 */
async function queryShape(
   author: ModelMaterializer,
   modelDef: ModelDef,
   plan: PersistedQueryPlan,
): Promise<ViewShape> {
   const call = author.loadQuery(`run: ${identifier(plan.queryName)}`);
   const prepared = await readPreparedQuery(call);
   const stages = (
      Array.isArray(prepared.pipeline) ? prepared.pipeline : []
   ) as StageIR[];
   const last = stages[stages.length - 1];
   const result = await call.getPreparedResult();
   const { columns, orderBy } = stageShape(
      last,
      resultKinds(result.resultExplore),
   );
   const def = (modelDef.contents as Record<string, unknown>)[
      plan.queryName
   ] as { annotations?: Parameters<typeof viewNoteTexts>[0] } | undefined;
   const source = resolveStructRefSource<{
      queryTimezone?: unknown;
      dialect?: unknown;
   }>(prepared.structRef, prepared.modelDef ?? modelDef);
   const sourceTimezone = queryTimezoneOf({
      queryTimezone: source?.queryTimezone,
   });
   const viewTimezone = queryTimezoneOf({ query: { pipeline: stages } });
   const queryTimezone = viewTimezone ?? sourceTimezone;
   const dialect = source?.dialect;
   return {
      columns,
      orderBy,
      notes: viewNoteTexts(def?.annotations),
      ...(typeof last?.type === "string" ? { stageType: last.type } : {}),
      ...(queryTimezone ? { queryTimezone } : {}),
      ...(sourceTimezone ? { sourceTimezone } : {}),
      ...(viewTimezone ? { viewTimezone } : {}),
      ...(dialect !== undefined ? { dialect } : {}),
   };
}
