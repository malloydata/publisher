// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served model on `virtual`: one model's served copy (served_copy.ts),
 * compiled once per binding set and answering queries the router sends it
 * (materialization v2, phase 3).
 *
 * Behind `materialize.enabled` (`PUBLISHER_MATERIALIZE`, default off), the one switch for `#(materialize)` build and serve. With it off nothing here
 * runs and serving goes through today's storage serve shape, unchanged.
 *
 * The served copy compiles in a runtime whose connections are the package's
 * own plus each storage destination under a per-process alias
 * (served_copy_config.ts), so the author's text never names a destination.
 * A compile failure leaves the model's sources serving through the next
 * candidate (today's serve shape, then live) and is reported as a coded
 * package warning.
 */

import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import type {
   MalloyConfig,
   ModelMaterializer,
   QueryMaterializer,
} from "@malloydata/malloy";
import { quoteManifestTablePath } from "../../quoting";
import { overlayKey } from "../../overlay_key";
import {
   createModelRuntime,
   type ModelConnectionInput,
} from "../../model_runtime";
import { SourceMap } from "../plan/source_map";
import type { MaterializePlan } from "../plan/build_file_generator";
import { evaluateManifestFreshness } from "../run/freshness";
import {
   declaredColumnsOf,
   storedColumnTypeMismatches,
} from "../stored_column_types";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import {
   authorModelLiftContext,
   buildVirtualMap,
   narrowSchemaToPublic,
   optsOutOfPersist,
   type DerivedSourceDef,
   type ServeBinding,
} from "./materialization_serve_transform";
import {
   buildServedCopy,
   declaredBaseName,
   type Reroot,
   type ServedColumn,
   type ServedCopyRange,
} from "./served_copy";
import { servedCopyConfig, servedCopyAlias } from "./served_copy_config";

type VirtualMap = Map<string, Map<string, string>>;

/** The code on a package warning for a served copy that did not compile or re-root. */
export const SERVED_COPY_WARNING_CODE = "served-copy-not-served";

/** Why a model's served copy, or one source in it, is not served. */
export interface ServedCopyProblem {
   /** The source, when the problem is one source's. */
   sourceName?: string;
   reason:
      | "compile_failed"
      | "cross_connection_unbuilt"
      | "column_types"
      | "no_declaration"
      | "range_not_found"
      | "definition_not_parsed"
      | "fields_not_carried";
   /** The compiler's message, for logs and the package warning only. */
   detail?: string;
}

/** What the served copy reads from the model that owns it. */
export interface ServedCopyHost {
   modelPath(): string;
   modelDef(): unknown;
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
}

interface Compiled {
   key: string;
   /** The invalidation generation it was compiled under. */
   generation: number;
   materializer?: ModelMaterializer;
   /** The served copy's compiled model, read (never edited) for source chains. */
   modelDef?: unknown;
   /** The `sourceID`s of sources that opt out with `#@ -persist`. */
   optOutIDs?: ReadonlySet<string>;
   /** Bindings actually re-rooted, with their destination under its alias. */
   rerooted: ServeBinding[];
   /**
    * Tables that store a computed dimension (`#(materialize)`), by table
    * path. The build evaluated it in the default timezone, so a query that
    * sets its own `timezone:` would read values live computes differently.
    */
   computedTables?: ReadonlySet<string>;
   aliased: ServeBinding[];
   problems: ServedCopyProblem[];
   /**
    * Queries this compiled copy declined for their shape (they read no
    * re-rooted table, or read an opted-out source), by query text, with the
    * reason. Neither depends on the givens or the clock, only on the query and
    * this compile, so a repeat is declined without compiling it again
    * (decision 6). Dropped with the compile: a rebind or reload starts empty.
    */
   declined?: Map<string, string>;
}

/** How many declined queries one compiled copy remembers before starting over. */
const DECLINED_LIMIT = 1000;

/** One model's served copy. */
export class ServedCopyServe {
   private packagePath?: string;
   private packageConfig?: ModelConnectionInput;
   private materializePlans: Readonly<Record<string, MaterializePlan>> = {};
   /**
    * Every binding the package pushed, unfiltered: a `#(materialize)` build
    * source (`<src>__mat`) is no source of this model's by name, so the serve
    * shape's own filter drops it. The served copy matches each binding to a
    * declaration of this model by `sourceID` instead.
    */
   private bindings: ServeBinding[] = [];
   private compiled?: Compiled;
   private inFlight?: {
      key: string;
      generation: number;
      promise: Promise<Compiled>;
   };
   /** Bumped on every invalidation, so a compile started before it is never kept. */
   private generation = 0;

   constructor(private readonly host: ServedCopyHost) {}

   /**
    * The package directory and connections the served copy compiles with, and
    * the build plan's generated `#(materialize)` sources by sourceID.
    */
   setContext(
      packagePath: string,
      packageConfig: ModelConnectionInput,
      materializePlans: Readonly<Record<string, MaterializePlan>> = {},
   ): void {
      this.packagePath = packagePath;
      this.packageConfig = packageConfig;
      this.materializePlans = materializePlans;
      this.invalidate();
   }

   /** The package's bindings, as pushed to the model. */
   setBindings(bindings: ServeBinding[]): void {
      this.bindings = bindings;
      this.invalidate();
   }

   /** Whether any binding could re-root a source (before compiling). */
   hasBindings(): boolean {
      return this.bindings.some((b) => b.origin !== "preaggregate");
   }

   /** Drop the compiled copy (bindings, destinations or the model changed). */
   invalidate(): void {
      this.generation++;
      this.compiled = undefined;
      this.inFlight = undefined;
   }

   /** Why the last compile left sources unserved; empty before the first. */
   problems(): readonly ServedCopyProblem[] {
      return this.compiled?.problems ?? [];
   }

   /** The served copy's compiled model, compiling it first; for reading only. */
   async compiledModelDef(): Promise<unknown> {
      return (await this.ensureCompiled()).modelDef;
   }

   /** Compile now (at bind), so warnings exist before the first query. */
   async prepare(): Promise<void> {
      try {
         await this.ensureCompiled();
      } catch {
         // Recorded on the compiled result; serving falls through.
      }
   }

   /**
    * Compile `queryString` against the served copy. Throws when the served copy
    * cannot answer it (no binding, a compile failure, a query reading a source
    * the copy left out); the router then tries the next candidate.
    */
   async loadQuery(
      queryString: string,
      givens?: Record<string, unknown>,
   ): Promise<{
      runnable: QueryMaterializer;
      virtualMap: VirtualMap;
      bindings: ServeBinding[];
      tablesRead: ServeBinding[];
   }> {
      const compiled = await this.ensureCompiled();
      if (!compiled.materializer || compiled.rerooted.length === 0) {
         throw new Error(
            compiled.problems[0]?.detail ??
               "no stored table is re-rooted in this model's served copy",
         );
      }
      const declined = compiled.declined?.get(queryString);
      if (declined !== undefined) throw new Error(declined);
      const decline = (reason: string): never => {
         const memo = (compiled.declined ??= new Map());
         if (memo.size >= DECLINED_LIMIT) memo.clear();
         memo.set(queryString, reason);
         throw new Error(reason);
      };
      const virtualMap = buildVirtualMap(compiled.aliased);
      const runnable = compiled.materializer.loadRestrictedQuery(queryString);
      // `#@ -persist` means recompute rather than read the pre-built table. In
      // the served copy every name reads its re-rooted base, so a query whose
      // source is an opted-out reader, or derives from one, is declined here
      // and answered by the next candidate (which never carries one).
      if (compiled.optOutIDs && compiled.optOutIDs.size > 0) {
         const prepared = await readPreparedQuery(runnable);
         const struct = resolveStructRefSource<unknown>(
            prepared.structRef,
            compiled.modelDef as never,
         );
         const chain = SourceMap.of(compiled.modelDef).chainOf(struct);
         if (chain.some((id) => compiled.optOutIDs?.has(id))) {
            decline("the query reads a source that opts out of persist");
         }
      }
      // Compile eagerly so a query the copy cannot answer fails inside the
      // router's attempt, not at run time past the fallback.
      const sql = await runnable.getSQL({
         virtualMap,
         givens: givens as never,
      });
      const tablesRead = compiled.rerooted.filter((b) =>
         sql.includes(quoteManifestTablePath(b.tablePath, "duckdb")),
      );
      if (tablesRead.length === 0) {
         // The query reads no stored table: the served copy would only add
         // work over live. Let the next candidate answer.
         decline("the query reads no re-rooted table");
      }
      // A stored computed dimension (`order_ts.day`, `day_of_week(order_ts)`)
      // was evaluated at build in the default timezone; live evaluates it in
      // the query's. A query that sets a timezone reads such a table only
      // live: the serve shape never carries a `#(materialize)` table.
      if (
         tablesRead.some((b) => compiled.computedTables?.has(b.tablePath)) &&
         setsTimezone((await readPreparedQuery(runnable)).query)
      ) {
         decline("the query sets a timezone over a stored computed dimension");
      }
      return {
         runnable,
         virtualMap,
         bindings: compiled.rerooted,
         tablesRead,
      };
   }

   private freshBindings(now: number): ServeBinding[] {
      const at = new Date(now);
      return this.bindings.filter(
         (b) =>
            b.origin !== "preaggregate" &&
            evaluateManifestFreshness(
               {
                  tableName: b.tablePath,
                  dataAsOf: b.freshAsOf,
                  freshnessWindowSeconds: b.freshnessWindowSeconds,
                  freshnessFallback: b.freshnessFallback,
               },
               at,
            ) === "serve_table",
      );
   }

   private async ensureCompiled(): Promise<Compiled> {
      const bindings = this.freshBindings(Date.now());
      const key = [
         ...bindings
            .map(
               (b) =>
                  `${b.sourceName}@${b.destinationName}/${b.virtualHandle}/${b.freshnessFallback ?? ""}`,
            )
            .sort(),
      ].join("|");
      const generation = this.generation;
      if (
         this.compiled &&
         this.compiled.key === key &&
         this.compiled.generation === generation
      ) {
         return this.compiled;
      }
      if (
         !this.inFlight ||
         this.inFlight.key !== key ||
         this.inFlight.generation !== generation
      ) {
         const started = {
            key,
            generation,
            promise: this.compile(key, generation, bindings),
         };
         this.inFlight = started;
         void started.promise.finally(() => {
            if (this.inFlight === started) this.inFlight = undefined;
         });
      }
      const compiled = await this.inFlight.promise;
      // An invalidation while it compiled makes it stale: keep it only when
      // it is still the current generation's.
      if (compiled.generation === this.generation) {
         this.compiled = compiled;
      }
      return compiled;
   }

   /**
    * The re-root for one binding, when it stores a source this model
    * declares (directly or through an import), else undefined.
    */
   private rerootFor(
      binding: ServeBinding,
      map: SourceMap,
      problems: ServedCopyProblem[],
      reservedNames: readonly string[],
   ): Reroot | undefined {
      // `#(materialize)`: the binding is the generated build source's; its
      // plan names the author's source, by sourceID.
      const plan = binding.sourceIDs
         ?.map((id) => this.materializePlans[id])
         .find((p) => p !== undefined);
      const authorIDs = plan
         ? [plan.sourceID]
         : (binding.sourceIDs ??
           [map.sourceIDOf(binding.sourceName)].filter(
              (id): id is string => id !== undefined,
           ));
      const node = authorIDs
         .map((id) => map.node(id))
         .find((n) => n !== undefined);
      // Another model's source: not this served copy's to re-root.
      if (!node) return undefined;
      const sourceName = plan?.sourceName ?? binding.sourceName;
      const def = node.def as {
         fields?: unknown[];
         location?: { url?: string; range?: ServedCopyRange };
      };
      const url = def.location?.url;
      const range = def.location?.range;
      if (!url || !range) {
         problems.push({ sourceName, reason: "no_declaration" });
         return undefined;
      }
      // A table an older build typed reads through a cast in today's serve
      // shape; the served copy does not carry casts yet, so it leaves the
      // source to that shape (or live).
      if (binding.readCasts?.length) {
         problems.push({ sourceName, reason: "column_types" });
         return undefined;
      }
      // The author's field of each name: its annotations, and the timeframe a
      // truncation carries, which the served copy restores on the field.
      const fieldsByName = new Map(
         (
            (def.fields ?? []) as {
               name?: unknown;
               as?: unknown;
               timeframe?: unknown;
            }[]
         ).map((f) => [String(f.as ?? f.name), f] as const),
      );
      const timeframeOf = (name: string) => {
         const tf = fieldsByName.get(name)?.timeframe;
         return typeof tf === "string" ? { timeframe: tf } : {};
      };
      let columns: ServedColumn[];
      if (plan) {
         // Exactly what the build stored: the plan's columns (the source's
         // public atomic dimensions, computed ones too, each under its own
         // name), typed as stored, with each field's annotations.
         const stored = new Map(binding.schema.map((c) => [c.name, c.type]));
         columns = [];
         for (const c of plan.columns) {
            const type = stored.get(c.column);
            // A build that stored a dimension under another name cannot be
            // read back as that field.
            if (type === undefined || c.column !== c.dimension) {
               problems.push({ sourceName, reason: "column_types" });
               return undefined;
            }
            const notes = fieldNotes(fieldsByName.get(c.dimension));
            columns.push({
               name: c.column,
               type,
               ...(notes.length > 0 ? { notes } : {}),
               ...timeframeOf(c.dimension),
            });
         }
      } else {
         const narrowed = narrowSchemaToPublic(binding.schema, def.fields);
         if (
            storedColumnTypeMismatches(narrowed, declaredColumnsOf(def.fields))
               .length > 0
         ) {
            problems.push({ sourceName, reason: "column_types" });
            return undefined;
         }
         columns = narrowed.map((c) => ({ ...c, ...timeframeOf(c.name) }));
      }
      return {
         url,
         range,
         sourceName,
         connectionAlias: servedCopyAlias(binding.destinationName),
         virtualHandle: binding.virtualHandle,
         typeName: `${sourceName}__served`,
         columns,
         reservedNames,
         ...(plan
            ? {
                 storedDimensions: plan.columns
                    .filter((c) => c.computed)
                    .map((c) => c.dimension),
              }
            : {}),
         sourceID: node.sourceID,
      } as Reroot & { sourceID: string };
   }

   private async compile(
      key: string,
      generation: number,
      bindings: ServeBinding[],
   ): Promise<Compiled> {
      const problems: ServedCopyProblem[] = [];
      const destinations = this.host.serveDestinationConfig();
      if (!destinations || !this.packagePath || !this.packageConfig) {
         return { key, generation, rerooted: [], aliased: [], problems };
      }
      const modelDef = this.host.modelDef();
      const map = SourceMap.of(modelDef);
      let candidates: {
         binding: ServeBinding;
         reroot: Reroot & { sourceID: string };
      }[] = [];
      const reservedNames = modelFieldNames(map);
      for (const binding of bindings) {
         const reroot = this.rerootFor(binding, map, problems, reservedNames);
         if (reroot) {
            candidates.push({
               binding,
               reroot: reroot as Reroot & { sourceID: string },
            });
         }
      }
      // An extension of a re-rooted source reads it by name already, and
      // re-rooting the extension too would replace the base it inherits its
      // parent's fields from (a `#(materialize)` extension inherits the
      // annotation, so it has a table of its own). Leave it as written.
      const lift = authorModelLiftContext(modelDef, (u) => {
         try {
            return readFileSync(fileURLToPath(u), "utf8");
         } catch {
            return undefined;
         }
      });
      // The chain comes from the compiled `extends`, which Malloy records only
      // in a model with the persistence flag (a `#(materialize)` model needs
      // none), so the declaration's own text, `<name> is <base> extend …`,
      // answers too.
      const rerootedIDs = new Set(candidates.map((c) => c.reroot.sourceID));
      const rerootedNames = new Set(candidates.map((c) => c.reroot.sourceName));
      candidates = candidates.filter((c) => {
         const node = map.node(c.reroot.sourceID);
         const chain = node ? map.chainOf(node.def) : [];
         if (chain.slice(1).some((id) => rerootedIDs.has(id))) return false;
         const text = lift.fileText(c.reroot.url);
         const base =
            text === undefined
               ? undefined
               : declaredBaseName(text, c.reroot.range);
         return base === undefined || !rerootedNames.has(base);
      });
      // At most one pass per candidate: each pass either compiles with every
      // remaining source whole, or drops the sources it lost fields on.
      for (let pass = 0; pass <= candidates.length; pass++) {
         const copy = buildServedCopy(
            candidates.map((c) => c.reroot),
            (u) => lift.fileText(u),
         );
         const refused = new Set(
            copy.refused.map((r) => `${r.url}\u0000${r.sourceName}`),
         );
         if (pass === 0) {
            for (const r of copy.refused) {
               // A source declared by refinement stays as written by design,
               // and still reads a re-rooted base by name: nothing to warn of.
               if (r.reason === "not_query_shaped") continue;
               problems.push({ sourceName: r.sourceName, reason: r.reason });
            }
         }
         candidates = candidates.filter(
            (c) => !refused.has(`${c.reroot.url}\u0000${c.reroot.sourceName}`),
         );
         if (candidates.length === 0) {
            return { key, generation, rerooted: [], aliased: [], problems };
         }
         const rerooted = candidates.map((c) => c.binding);
         const aliased = rerooted.map((b) => ({
            ...b,
            destinationName: servedCopyAlias(b.destinationName),
         }));
         const overlay = new Map<string, string>();
         for (const [url, text] of copy.files) {
            overlay.set(overlayKey(url), text);
         }
         let materializer: ModelMaterializer;
         let servedDef: unknown;
         try {
            const { runtime, modelURL, importBaseURL } =
               await createModelRuntime(
                  this.packagePath,
                  this.host.modelPath(),
                  servedCopyConfig(
                     this.packageConfig,
                     destinations(),
                     new Set(rerooted.map((b) => b.destinationName)),
                  ),
                  { overlay },
               );
            materializer = runtime.loadModel(modelURL, { importBaseURL });
            const model = await materializer.getModel();
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            servedDef = (model as any)._modelDef;
         } catch (err) {
            const detail = err instanceof Error ? err.message : String(err);
            problems.push({
               reason: classifyServedCopyFailure(detail),
               detail,
            });
            return { key, generation, rerooted: [], aliased: [], problems };
         }
         // A re-rooted source must keep every field the author's has. One it
         // inherited from a base the re-root replaced (a measure, a view, a
         // join declared on the base) is not in its own text, so it is lost:
         // that source is left as written and the copy compiled again.
         const servedMap = SourceMap.of(servedDef);
         const lost = candidates.filter((c) => {
            const author = fieldNames(map.node(c.reroot.sourceID)?.def);
            const served = fieldNames(servedMap.node(c.reroot.sourceID)?.def);
            return [...author].some((name) => !served.has(name));
         });
         if (lost.length > 0) {
            for (const c of lost) {
               problems.push({
                  sourceName: c.reroot.sourceName,
                  reason: "fields_not_carried",
               });
            }
            candidates = candidates.filter((c) => !lost.includes(c));
            continue;
         }
         // Every declaration the model reaches, imported ones included.
         const optOutIDs = new Set<string>();
         for (const node of servedMap.nodes()) {
            if (optsOutOfPersist(node.def as DerivedSourceDef)) {
               optOutIDs.add(node.sourceID);
            }
         }
         return {
            key,
            generation,
            computedTables: new Set(
               candidates
                  .filter((c) => (c.reroot.storedDimensions ?? []).length > 0)
                  .map((c) => c.binding.tablePath),
            ),
            materializer,
            modelDef: servedDef,
            optOutIDs,
            rerooted,
            aliased,
            problems,
         };
      }
      return { key, generation, rerooted: [], aliased: [], problems };
   }
}

/**
 * The annotation lines on a compiled field, outermost first: what it inherits,
 * then its statement's (block) notes, then its own. Each is the author's text
 * (`#(doc) "…"`), so it can be written again on the field in a `type:`.
 */
function fieldNotes(field: unknown): string[] {
   interface Annotation {
      inherits?: Annotation;
      blockNotes?: { text?: unknown }[];
      notes?: { text?: unknown }[];
   }
   const out: string[] = [];
   const walk = (a: Annotation | undefined, depth: number) => {
      if (!a || depth > 16) return;
      walk(a.inherits, depth + 1);
      for (const n of [...(a.blockNotes ?? []), ...(a.notes ?? [])]) {
         if (typeof n.text === "string" && !out.includes(n.text)) {
            out.push(n.text);
         }
      }
   };
   const f = field as
      | { annotations?: Annotation; annotation?: Annotation }
      | undefined;
   walk(f?.annotations ?? f?.annotation, 0);
   return out;
}

/** Whether a compiled query sets a `timezone:` anywhere in it. */
function setsTimezone(query: unknown, depth = 0): boolean {
   if (!query || typeof query !== "object" || depth > 64) return false;
   if (Array.isArray(query)) {
      return query.some((q) => setsTimezone(q, depth + 1));
   }
   for (const [k, v] of Object.entries(query)) {
      if (k === "queryTimezone" && typeof v === "string") return true;
      if (typeof v === "object" && setsTimezone(v, depth + 1)) return true;
   }
   return false;
}

/** Every field name any source in the model declares. */
function modelFieldNames(map: SourceMap): string[] {
   const names = new Set<string>();
   for (const node of map.nodes()) {
      for (const n of fieldNames(node.def)) names.add(n);
   }
   return [...names];
}

/** The names of a compiled source's fields. */
function fieldNames(def: unknown): Set<string> {
   const fields = (def as { fields?: { name?: unknown; as?: unknown }[] })
      ?.fields;
   return new Set(
      (fields ?? [])
         .map((f) => (typeof f.as === "string" ? f.as : f.name))
         .filter((n): n is string => typeof n === "string"),
   );
}

/**
 * Why a served copy did not compile. A join across origin connections compiles
 * only once both sides are re-rooted onto one destination; until the other
 * side is built, Malloy refuses the join ("Cannot join … on connection …"),
 * and that is a coded wait, not a broken model.
 */
export function classifyServedCopyFailure(
   detail: string,
): "cross_connection_unbuilt" | "compile_failed" {
   return /Cannot join .* on connection/i.test(detail)
      ? "cross_connection_unbuilt"
      : "compile_failed";
}
