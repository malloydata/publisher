// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated `#(materialize)` build companion (see
 * build_file_generator.ts), as rollup_compile.ts compiles the rollup
 * companion: the text is served from an overlay at a URL beside the author's
 * model, so its `import` resolves while it never shows up in
 * `getModelPaths()`, the API or the package's own compile, and cannot go stale
 * against the author's edits.
 */

import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type Connection,
   type Model as MalloyModel,
   type ModelMaterializer,
} from "@malloydata/malloy";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../../logger";
import {
   omittedByID,
   omittedFromFile,
   type Omission,
} from "../domain/omitted_sources";
import { registerUniquenessCheckCompiler } from "../run/inline_uniqueness";
import {
   buildCopyDeclarationAt,
   buildCopyName,
   buildCopyText,
   chainLevels,
   extensionOverStoredModel,
   generatedNamesOf,
   generateBuildFiles,
   givenReadTerms,
   hiddenAccess,
   keptBasesInText,
   levelFileName,
   MATERIALIZE_COMPANION_SUFFIX,
   planModelMaterialize,
   settledChains,
   splitSourceID,
   storedChainText,
   withoutParent,
   type MaterializePlan,
   type MaterializeRefusal,
   type StoredColumnType,
} from "./build_file_generator";
import type { TermUse } from "./persist_dynamic_terms";
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";
import { leftOutWindowTerms } from "./rolling_window";
import { storedColumnsOfOutput } from "../stored_column_types";
import {
   probeSourceDependence,
   type FieldDependence,
   type ProbeCompiler,
} from "../../probe_compiler";

interface GeneratedMaterialize {
   /** The sources the generator planned. Never empty. */
   plans: MaterializePlan[];
   /** The compiled companion: its build targets are the generated sources. */
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   /** The generated text, for logging and tests. */
   text: string;
   /**
    * The build copies of the author's model, by the sourceID of the source each is for
    * (P2): the author's text with that source's read-time terms taken out.
    */
   buildCopies?: ReadonlyMap<string, string>;
   /**
    * The URL of every file generated beside the author's (the companion, its
    * level files and the build copies). A source declared in one that no plan
    * names (a parent a build copy re-roots, P10) shares a generated source's
    * table and is no build of its own.
    */
   generatedURLs: ReadonlySet<string>;
}

/** Where the generated companion for `modelPath` lives, as an absolute URL. */
function materializeCompanionURL(packagePath: string, modelPath: string): URL {
   // pathToFileURL, not `file://${path}`: on Windows the path is `D:\…`, and
   // the overlay key must match the URL Malloy normalizes to.
   return pathToFileURL(
      `${path.join(packagePath, modelPath)}${MATERIALIZE_COMPANION_SUFFIX}`,
   );
}

/**
 * Plan, generate and compile the build companion for one author model.
 * `generated` is undefined when the model declares no plannable
 * `#(materialize)`, or when the companion does not compile: a failure is a
 * publisher bug (the text is generated), so it is logged at `error` level and
 * costs that model its generated builds and nothing else. `refused` holds the
 * declarations the generator would not plan, for the caller to report; the
 * eligibility gates judge the generated sources like any persist source once
 * they are in the plan.
 */
export async function tryCompileMaterializeCompanion(args: {
   packagePath: string;
   modelPath: string;
   contents: Record<string, unknown>;
   /**
    * The author model's `ModelDef.givens`, which its read-time terms read.
    * Required: without it every given-reading `where:` is refused.
    */
   givens: Record<string, unknown>;
   getRuntime: SynthesizedRuntimeFactory;
   /**
    * Compiles probes against the author's model (P9): which of each planned
    * source's computed dimensions read a given, the current time or the query
    * time zone. Absent: the IR walks alone decide.
    */
   probe?: ProbeCompiler;
}): Promise<{
   generated?: GeneratedMaterialize;
   refused: MaterializeRefusal[];
   /** The sources a failure left out of the plan (see omitted_sources). */
   omitted?: Omission[];
}> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const refused: MaterializeRefusal[] = [];
   // The author's sourceIDs each generated source is presented as, once
   // planned; until then a failure cannot name them.
   let plannedIDs: string[] | undefined;
   try {
      const declaredHere = (declaringURL: string) => {
         try {
            return path.resolve(fileURLToPath(declaringURL)) === modelFile;
         } catch {
            // Not a local file (a host, another scheme): planned, as before.
            return true;
         }
      };
      // P9: plan once from the IR, probe the computed dimensions that plan
      // would store, and plan again with the answers. A probe only ever
      // leaves a dimension out or refuses a source, so every dimension the
      // second plan stores was probed.
      const probed = args.probe
         ? await probeMaterializeDimensions(
              args.probe,
              planModelMaterialize(args.contents, declaredHere, args.givens)
                 .plans,
              args.contents,
           )
         : undefined;
      const planned = planModelMaterialize(
         args.contents,
         declaredHere,
         args.givens,
         probed?.fields,
         probed?.zoneFreeRows,
      );
      refused.push(...planned.refused);
      plannedIDs = planned.plans.map((p) => p.sourceID);
      // The text the author's model was compiled from, through the reader
      // the companion compiles with: a package's build view (joins across
      // connections left out) or a worker's file map, not necessarily the
      // file on disk, and the compiled locations the terms are found by are
      // positions in that text. Read once.
      let modelText: Promise<string> | undefined;
      const readModelText = () =>
         (modelText ??= (async () => {
            const { runtime } = await args.getRuntime(new Map());
            const read = await runtime.urlReader.readURL(
               pathToFileURL(modelFile),
            );
            return typeof read === "string" ? read : read.contents;
         })());
      // A build copy compiled on its own, beside the author's model, so its
      // imports resolve as the author's do; never run. P3 reads which hidden
      // fields the copy lets the build select off it.
      const compileCopy = async (fileName: string, text: string) => {
         const url = pathToFileURL(
            path.join(path.dirname(modelFile), fileName),
         );
         const { runtime, importBaseURL } = await args.getRuntime(
            new Map([[url.href, text]]),
         );
         const model = await runtime
            .loadModel(url, { importBaseURL })
            .getModel();
         return {
            contents: model._modelDef.contents as Record<string, unknown>,
            url: url.href,
         };
      };
      const refusedIDs = new Set<string>();
      const prepare = async (from: MaterializePlan[]) => {
         const out = await withBuildCopies(
            from,
            modelFile,
            readModelText,
            compileCopy,
            args.contents,
         );
         for (const r of out.refused) {
            if (r.sourceID !== undefined && refusedIDs.has(r.sourceID))
               continue;
            if (r.sourceID !== undefined) refusedIDs.add(r.sourceID);
            refused.push(r);
         }
         return out;
      };
      // One source's generated text that does not compile costs that source
      // alone (see compileContained).
      const compile = (from: MaterializePlan[]) =>
         compileContained(args, from, prepare, (refusal) => {
            if (refusedIDs.has(refusal.sourceID as string)) return;
            refusedIDs.add(refusal.sourceID as string);
            refused.push(refusal);
         });
      let { plans, copies, generated } = await compile(planned.plans);
      // A `storage=` extension table builds in its destination from its
      // chain alone, over its parent's stored table (P10). One whose chain
      // does not compile there, with nothing else to read, builds over the
      // raw chain, from the warehouse, as it did before P10.
      const unstored = generated
         ? await storedChainsNotCompiling(generated)
         : new Map<string, string>();
      if (unstored.size > 0) {
         for (const [sourceID, error] of unstored) {
            logger.warn(
               "A storage= extension table does not compile over its parent's stored table alone; it builds over the raw chain",
               {
                  modelPath: args.modelPath,
                  sourceName: planOf(plans, sourceID)?.sourceName,
                  error,
               },
            );
         }
         ({ plans, copies, generated } = await compile(
            plans.map((p) => (unstored.has(p.sourceID) ? withoutParent(p) : p)),
         ));
      }
      // A build copy re-declares its whole file, so a `#@ persist` source the
      // copied source reads (its base, a join) would enter the plan a second
      // time under the copy's URL: a build node that no planned source names.
      // A chained source builds over the raw chain instead; a source whose
      // given term needs the copy is refused, and the companion compiled again
      // without it.
      let leaked = generated
         ? await copyDeclaredBuilds(generated, modelFile, copies)
         : new Map<string, string[]>();
      if ([...leaked.keys()].some((id) => planOf(plans, id)?.buildsOver)) {
         ({ plans, copies, generated } = await compile(
            plans.map((p) => (leaked.has(p.sourceID) ? withoutParent(p) : p)),
         ));
         leaked = generated
            ? await copyDeclaredBuilds(generated, modelFile, copies)
            : new Map<string, string[]>();
      }
      if (leaked.size > 0) {
         for (const plan of plans) {
            const names = leaked.get(plan.sourceID);
            if (!names) continue;
            if (
               givenReadTerms(plan).length === 0 &&
               leftOutWindowTerms(plan.readTimeTerms).length === 0
            ) {
               // A copy made only to store hidden columns (P3): build from the
               // author's file instead, storing the public ones.
               copies.delete(plan.sourceID);
               const columns = plan.columns.filter((c) => !c.access);
               const kept = new Set(columns.map((c) => c.column));
               if (
                  columns.length > 0 &&
                  (plan.readTimeTerms ?? []).every((t) =>
                     t.columns.every((c) => kept.has(c)),
                  )
               ) {
                  leaked.delete(plan.sourceID);
                  plans = plans.map((p) =>
                     p === plan ? { ...plan, columns } : p,
                  );
                  continue;
               }
            }
            refused.push({
               sourceName: plan.sourceName,
               sourceID: plan.sourceID,
               reason:
                  givenReadTerms(plan).length > 0 ||
                  leftOutWindowTerms(plan.readTimeTerms).length > 0
                     ? "a `where:` reading a given or keeping rows older than a " +
                       "moving cut-off cannot yet be left out of the " +
                       "build of a source that reads a #@ persist source " +
                       `(${names.join(", ")}): its build would declare that source ` +
                       "a second time"
                     : "the source stores only hidden fields, which its build " +
                       "cannot read while it reads a #@ persist source " +
                       `(${names.join(", ")})`,
            });
         }
         ({ plans, copies, generated } = await compile(
            plans.filter((p) => !leaked.has(p.sourceID)),
         ));
      }
      return generated
         ? { generated: withStoredColumns(generated), refused }
         : { refused };
   } catch (error) {
      logger.error(
         "Failed to plan or compile the generated #(materialize) companion",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return {
         refused,
         omitted: plannedIDs
            ? omittedByID(
                 plannedIDs.filter(
                    (id) => !refused.some((r) => r.sourceID === id),
                 ),
                 "materialize_companion_failed",
              )
            : omittedFromFile(
                 pathToFileURL(modelFile).href,
                 undefined,
                 "materialize_companion_failed",
              ),
      };
   }
}

/**
 * Each plan with its build source's compiled output columns
 * ({@link MaterializePlan.storedColumns}): what a colocated build writes,
 * which its serve binding declares. A nested column is kept only on DuckDB,
 * whose tables hold nests natively; elsewhere a nest is stored as JSON, which
 * the served copy cannot declare, so the column is left out and the source
 * serves live.
 */
function withStoredColumns(
   generated: GeneratedMaterialize,
): GeneratedMaterialize {
   const contents = (
      generated.model as unknown as {
         _modelDef?: { contents?: Record<string, unknown> };
      }
   )._modelDef?.contents;
   return {
      ...generated,
      plans: generated.plans.map((plan) => {
         const def = contents?.[plan.buildSourceName] as
            | { fields?: unknown[]; dialect?: unknown }
            | undefined;
         if (!def?.fields) return plan;
         return {
            ...plan,
            storedColumns: storedColumnsOfOutput(def.fields, {
               nested: def.dialect === "duckdb",
            }),
         };
      }),
   };
}

/**
 * The probe compiler's answers for each plan's own computed dimensions, by
 * `sourceID` then dimension, and the sources whose rows it found the same in
 * every query time zone (P9). Compiled against the author's model by the
 * source's name there; never run.
 */
async function probeMaterializeDimensions(
   compiler: ProbeCompiler,
   plans: readonly MaterializePlan[],
   contents: Record<string, unknown>,
): Promise<{
   fields: Map<string, Map<string, FieldDependence>>;
   zoneFreeRows: Set<string>;
}> {
   const fields = new Map<string, Map<string, FieldDependence>>();
   const zoneFreeRows = new Set<string>();
   for (const plan of plans) {
      // Every plan is probed, one with no computed dimension too: its rows
      // can depend on the zone through its own `where:` or base relation.
      const computed = plan.columns
         .filter((c) => c.computed && c.join === undefined)
         .map((c) => c.dimension);
      // By sourceID (the keying rule); the probe names the source as this
      // model does, which for a source planned here is its own name.
      const def = Object.values(contents).find(
         (o) =>
            !!o &&
            typeof o === "object" &&
            (o as { sourceID?: unknown }).sourceID === plan.sourceID,
      ) as { fields?: { name?: unknown; as?: unknown }[] } | undefined;
      const reserved = new Set(
         (def?.fields ?? []).map((f) => String(f.as ?? f.name)),
      );
      const answer = await probeSourceDependence(
         compiler,
         plan.sourceName,
         computed,
         reserved,
      );
      if (computed.length > 0) fields.set(plan.sourceID, answer.fields);
      if (answer.rowsZoneFree) zoneFreeRows.add(plan.sourceID);
   }
   return { fields, zoneFreeRows };
}

/** A planned source's build copy: its file name beside the model, and text. */
interface BuildCopy {
   fileName: string;
   text: string;
   /**
    * For a chained copy (P10): the declaration off the chain a place in the
    * copy lies in, if any, by sourceID and its range in the author's file
    * ({@link buildCopyDeclarationAt}).
    */
   declarationAt?: (at: {
      line: number;
      character: number;
   }) => { sourceID: string; declaration: TermUse } | undefined;
}

/** Generate and compile one companion, or undefined when nothing is planned. */
async function compileCompanion(
   args: {
      packagePath: string;
      modelPath: string;
      getRuntime: SynthesizedRuntimeFactory;
   },
   plans: MaterializePlan[],
   copies: ReadonlyMap<string, BuildCopy>,
): Promise<GeneratedMaterialize | undefined> {
   const used = new Map(
      plans.flatMap((p) => {
         const copy = copies.get(p.sourceID);
         return copy ? [[p.sourceID, copy] as const] : [];
      }),
   );
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const files = generateBuildFiles(
      plans,
      path.basename(args.modelPath),
      (declaringURL) => {
         let declaringFile: string;
         try {
            declaringFile = fileURLToPath(declaringURL);
         } catch {
            return undefined;
         }
         if (path.resolve(declaringFile) === modelFile) return undefined;
         return path
            .relative(path.dirname(modelFile), declaringFile)
            .split(path.sep)
            .join("/");
      },
      new Map([...used].map(([name, c]) => [name, c.fileName])),
   );
   if (!files) return undefined;
   const text = files.entry;
   const url = materializeCompanionURL(args.packagePath, args.modelPath);
   const overlay = new Map([[url.href, text]]);
   const dir = path.dirname(modelFile);
   for (const copy of used.values()) {
      overlay.set(pathToFileURL(path.join(dir, copy.fileName)).href, copy.text);
   }
   for (const [name, level] of files.levels) {
      overlay.set(pathToFileURL(path.join(dir, name)).href, level);
   }
   const { runtime, importBaseURL } = await args.getRuntime(overlay);
   const materializer = runtime.loadModel(url, { importBaseURL });
   const copyOwners = new Map(
      [...used].map(
         ([sourceID, c]) =>
            [pathToFileURL(path.join(dir, c.fileName)).href, sourceID] as const,
      ),
   );
   const model = await materializer.getModel().catch((error: unknown) => {
      throw new CompanionCompileError(error, overlay, copyOwners);
   });
   // The P8 checks' SQL, compiled now (never run here) so a build runs it
   // against the joined source's connection before it writes anything.
   // Each check can also be compiled again over a run's manifest, so a run
   // whose parent build reads the joined source's persisted table checks that
   // table (inline_uniqueness.ts).
   for (const plan of plans) {
      for (const check of plan.uniquenessChecks ?? []) {
         check.sql = await materializer
            .loadQueryByName(check.queryName)
            .getSQL()
            .catch((error: unknown) => {
               throw new CompanionCompileError(
                  error,
                  overlay,
                  copyOwners,
                  plan.sourceID,
               );
            });
         registerUniquenessCheckCompiler(
            check,
            (buildManifest, connectionDigests) =>
               materializer
                  .loadQueryByName(check.queryName)
                  .getSQL({ buildManifest, connectionDigests }),
         );
      }
   }
   return {
      plans,
      model,
      materializer,
      runtime,
      text,
      generatedURLs: new Set(overlay.keys()),
      ...(used.size > 0
         ? {
              buildCopies: new Map(
                 [...used].map(([name, c]) => [name, c.text] as const),
              ),
           }
         : {}),
   };
}

/**
 * The `storage=` extension tables among `generated`'s plans (P10) whose build
 * does not compile in a destination from {@link extensionOverStoredModel}
 * alone, by sourceID, with why. Checked here, at plan time, against a
 * stand-in for the destination that has DuckDB's dialect and no table, and
 * the parent's stored columns as its build source declares them, so a chain
 * that reaches anything but the parent's table (a join to a warehouse source,
 * a table named in place) never chains, and its build is never refused for a
 * shape the plan could have seen.
 */
async function storedChainsNotCompiling(
   generated: GeneratedMaterialize,
): Promise<Map<string, string>> {
   const out = new Map<string, string>();
   for (const plan of generated.plans) {
      const over = plan.buildsOver;
      const chain = over?.overStored;
      const destination = plan.persist.storage;
      if (!over || !chain || destination === undefined) continue;
      const parent = planOf(generated.plans, over.sourceID);
      try {
         if (!parent) throw new Error("its parent is no longer planned");
         const columns = parentStoredColumns(
            generated.model,
            parent.buildSourceName,
         );
         const handle = "stand_in";
         const url = "file:///materialize-stored-chain/m.malloy";
         const runtime = new Runtime({
            urlReader: new InMemoryURLReader(
               new Map([
                  [
                     url,
                     extensionOverStoredModel({
                        plan,
                        chain,
                        connectionName: destination,
                        virtualHandle: handle,
                        columns,
                     }),
                  ],
               ]),
            ),
            connections: new FixedConnectionMap(
               new Map([[destination, destinationStandIn(destination)]]),
               destination,
            ),
         });
         const model = await runtime.loadModel(new URL(url)).getModel();
         const targets = await runtime.getBuildTargets(model, {
            virtualMap: new Map([[destination, new Map([[handle, "t"]])]]),
         });
         const built = targets.connections.some((c) =>
            c.targets.some((t) =>
               t.sources.some((s) => s.name === plan.buildSourceName),
            ),
         );
         if (!built) throw new Error("its build source is not planned there");
      } catch (error) {
         out.set(
            plan.sourceID,
            error instanceof Error ? error.message : String(error),
         );
      }
   }
   return out;
}

/**
 * The parent's stored columns as its generated build source declares them,
 * each with the Malloy type a stored table of it reads back as. Throws for a
 * column of a type a stored table does not carry faithfully.
 */
function parentStoredColumns(
   model: MalloyModel,
   buildSourceName: string,
): StoredColumnType[] {
   return model
      .getExploreByName(buildSourceName)
      .allFields.filter((f) => f.isAtomicField())
      .map((f) => {
         const type = STORED_TYPES[String(f.type)];
         if (!type) {
            throw new Error(`column '${f.name}' is of type ${String(f.type)}`);
         }
         return { name: f.name, type };
      });
}

/** The Malloy type a stored column of each atomic type reads back as. */
const STORED_TYPES: Readonly<Record<string, string>> = {
   string: "string",
   number: "number",
   boolean: "boolean",
   date: "date",
   timestamp: "timestamp",
   timestamptz: "timestamp",
   json: "json",
};

/**
 * A destination for a plan-time compile: DuckDB's dialect (every destination
 * is DuckDB or DuckLake), and no table or SQL to read, so anything a chain
 * names but its parent's stored table fails to compile.
 */
function destinationStandIn(name: string): Connection {
   const none = "not available to an extension table's build";
   const standIn = {
      name,
      dialectName: "duckdb",
      getDigest: () => `materialize-stored-chain:${name}`,
      fetchSchemaForTables: async (tables: Record<string, string>) => ({
         schemas: {},
         errors: Object.fromEntries(Object.keys(tables).map((k) => [k, none])),
      }),
      fetchSchemaForSQLStruct: async () => ({ error: none }),
   };
   return standIn as unknown as Connection;
}

/** The plan of `sourceID` among `plans`. */
function planOf(
   plans: readonly MaterializePlan[],
   sourceID: string,
): MaterializePlan | undefined {
   return plans.find((p) => p.sourceID === sourceID);
}

/**
 * For each planned source whose build plans a source only its build copy
 * declares, the names of those sources. A `sourceID` is `name@<declaring
 * URL>`, so a build copy's own are told apart by the copy's URL. A copy's
 * source on the table of a generated source (a parent an extension's copy
 * re-roots on its build source, P10, and the extension over it) shares that
 * table: no build of its own, so not one the copy declared.
 */
async function copyDeclaredBuilds(
   generated: GeneratedMaterialize,
   modelFile: string,
   copies: ReadonlyMap<string, BuildCopy>,
): Promise<Map<string, string[]>> {
   const out = new Map<string, string[]>();
   if (!generated.buildCopies?.size) return out;
   const dir = path.dirname(modelFile);
   const ownerByURL = new Map(
      [...generated.buildCopies.keys()].map((name) => [
         pathToFileURL(path.join(dir, copies.get(name)?.fileName ?? "")).href,
         name,
      ]),
   );
   const urlOf = (id: string) => {
      const at = id.indexOf("@");
      return at < 0 ? undefined : id.slice(at + 1);
   };
   const targets = await generated.runtime.getBuildTargets(generated.model);
   const groups = [
      ...targets.connections.flatMap((c) =>
         c.targets.map((t) => t.sources.map((s) => s.sourceID)),
      ),
      ...targets.unbuildable.map((u) => [u.source.sourceID]),
   ];
   for (const ids of groups) {
      const sharesGenerated = ids.some((id) => {
         const url = urlOf(id);
         return (
            url !== undefined &&
            generated.generatedURLs.has(url) &&
            !ownerByURL.has(url)
         );
      });
      if (sharesGenerated) continue;
      for (const id of ids) {
         const url = urlOf(id);
         const owner = url === undefined ? undefined : ownerByURL.get(url);
         if (owner === undefined) continue;
         const name = id.slice(0, id.indexOf("@"));
         const names = out.get(owner) ?? [];
         if (!names.includes(name)) names.push(name);
         out.set(owner, names.sort());
      }
   }
   return out;
}

/**
 * The field of the build copy's source a hidden column is read through: an
 * inlined join's column through its join (the join is what is hidden, and
 * what the lift must make readable), any other through its own dimension.
 */
function liftedName(c: { dimension: string; join?: string }): string {
   return c.join ?? c.dimension;
}

/**
 * Give each plan that needs one a build copy of the author's model, whose
 * text is read back here: a plan with given-reading terms (P2) or
 * current-time terms holding an upper bound (Rolling windows), which the copy
 * takes out; an extension table that builds over its parent's build
 * source (P10), whose copy re-roots the parent on it; and a plan with hidden
 * columns (P3), whose own access modifiers the copy lifts. One copy does all
 * three a plan needs.
 *
 * Given terms: a P2 source must be declared in that model and read givens
 * declared in local files; otherwise, or when the file cannot be read or a
 * term not placed in it, the plan is refused (fail closed: built from the
 * author's file, the term would be baked into the table with the given's
 * default). Each given declared in another file is imported from that file.
 * A current-time term the build leaves out must be written in the model file
 * itself, its own declaration or a base declared there; otherwise the plan is
 * refused the same way (built from the author's file, the table would hold
 * only the rows older than the build's cut-off).
 *
 * Extension tables: one whose copy cannot be written (its parent no longer
 * planned, a declaration that does not re-root) builds over the raw chain
 * instead, which a P2 or P3 source still gets a copy for.
 *
 * Hidden columns: which ones the build can select is settled by compiling the
 * copy over the raw chain (`compileCopy`) and reading its source: a field
 * public there is stored, any other is dropped from the plan and stays
 * computed at read (live), such as a field private on a source this one
 * extends. The same set is stored whether or not the plan chains, so a
 * chained build (whose parent's re-root reads hidden parent columns as plain
 * stored ones) never stores a field the raw chain could not select. A copy
 * that cannot be made or compiled drops them all. A plan left with no column,
 * or with a read-time term over a dropped column, is refused.
 *
 * Plans are visited parents first, so a copy imports its parent's build
 * source from the parent's level file as it finally stands, and re-roots the
 * parent over the columns its table finally stores; the returned plans keep
 * `plans`' order.
 */
async function withBuildCopies(
   input: MaterializePlan[],
   modelFile: string,
   readModelText: () => Promise<string>,
   compileCopy: (
      fileName: string,
      text: string,
   ) => Promise<{ contents: Record<string, unknown>; url: string }>,
   /** The author model's contents, to trace a chained copy's problems. */
   contents: Record<string, unknown> = {},
): Promise<{
   plans: MaterializePlan[];
   copies: Map<string, BuildCopy>;
   refused: MaterializeRefusal[];
}> {
   const plans = settledChains(input);
   const copies = new Map<string, BuildCopy>();
   const refused: MaterializeRefusal[] = [];
   const hiddenOf = (plan: MaterializePlan) =>
      plan.columns.filter((c) => c.access !== undefined);
   if (
      !plans.some(
         (p) =>
            givenReadTerms(p).length > 0 ||
            leftOutWindowTerms(p.readTimeTerms).length > 0 ||
            p.buildsOver ||
            hiddenOf(p).length > 0 ||
            p.keptBases !== undefined,
      )
   ) {
      return { plans, copies, refused };
   }
   const refuse = (plan: MaterializePlan, reason: string) =>
      refused.push({
         sourceName: plan.sourceName,
         sourceID: plan.sourceID,
         reason,
      });
   const localPath = (url: string): string | undefined => {
      try {
         return path.resolve(fileURLToPath(url));
      } catch {
         return undefined;
      }
   };
   let text: string | undefined;
   let unreadable: string | undefined;
   try {
      text = await readModelText();
   } catch (error) {
      unreadable = error instanceof Error ? error.message : String(error);
   }
   const modelFileName = path.basename(modelFile);
   // Parents first: a stable sort by depth in the planned chains.
   const depth = chainLevels(plans);
   const visitOrder = plans
      .map((plan, index) => ({ plan, index }))
      .sort(
         (a, b) =>
            (depth.get(a.plan.sourceID) ?? 0) -
               (depth.get(b.plan.sourceID) ?? 0) || a.index - b.index,
      );
   const kept = new Map<string, MaterializePlan>();
   const levels = new Map<string, number>();
   for (const { plan: original } of visitOrder) {
      const hidden = hiddenOf(original);
      const declaredIn = original.declaration
         ? localPath(original.declaration.url)
         : undefined;
      // P3: the hidden columns the build copy lets the build select, read
      // off the copy over the raw chain (see above).
      let lifted: Set<string> | undefined;
      if (hidden.length > 0 && declaredIn === modelFile && text !== undefined) {
         const probe = buildCopyText(text, withoutParent(original), undefined, {
            liftAccess: true,
         });
         if ("copy" in probe) {
            const compiled = await compileCopy(
               buildCopyName(modelFileName, copies.size),
               probe.copy,
            ).catch(() => undefined);
            // The copy's own declaration of the source: its sourceID names
            // the copy's file in place of the author's (the keying rule).
            const declared = splitSourceID(original.sourceID);
            const copyID =
               compiled && declared
                  ? `${declared.name}@${compiled.url}`
                  : undefined;
            const fields = (
               Object.values(compiled?.contents ?? {}).find(
                  (d) =>
                     (d as { sourceID?: unknown } | null)?.sourceID === copyID,
               ) as
                  | {
                       fields?: {
                          name?: unknown;
                          as?: unknown;
                          accessModifier?: unknown;
                       }[];
                    }
                  | undefined
            )?.fields;
            const selectable = new Set(
               (fields ?? [])
                  .filter((f) => hiddenAccess(f) === undefined)
                  .map((f) => String(f.as ?? f.name)),
            );
            if (hidden.some((c) => selectable.has(liftedName(c)))) {
               lifted = selectable;
            }
         }
      }
      const columns = original.columns.filter(
         (c) => c.access === undefined || lifted?.has(liftedName(c)),
      );
      let plan: MaterializePlan =
         columns.length === original.columns.length
            ? original
            : { ...original, columns };
      if (columns.length === 0) {
         refuse(plan, "the source has no dimension the build can store");
         continue;
      }
      const stored = new Set(columns.map((c) => c.column));
      const unstored = (plan.readTimeTerms ?? []).find(
         (t) => !t.columns.every((c) => stored.has(c)),
      );
      if (unstored) {
         refuse(
            plan,
            `\`where: ${unstored.code}\` reads a field the build cannot ` +
               "select (one private on a source this one extends, or hidden " +
               "where the build copy cannot lift it), so it could not be " +
               "re-applied over the table at read",
         );
         continue;
      }
      // A term or dimension a base writes is left out of the build only when
      // the served copy keeps that base's block: confirmed against the text
      // before anything is built.
      if (plan.keptBases) {
         const bases =
            text === undefined
               ? {
                    reason:
                       "the model file could not be read to confirm the " +
                       `blocks its served copy keeps: ${unreadable}`,
                 }
               : declaredIn !== modelFile
                 ? {
                      reason:
                         "a `where:` reading a given or a caller-dependent " +
                         "dimension written on a base can only be left out " +
                         "of the build for a source declared in a local model file",
                   }
                 : keptBasesInText(text, plan);
         if ("reason" in bases) {
            refuse(plan, bases.reason);
            continue;
         }
      }
      const liftAccess = lifted !== undefined;
      const given = givenReadTerms(plan).length > 0;
      const givens = (plan.givenDeclarations ?? []).map((g) => ({
         ...g,
         file: localPath(g.url),
      }));
      if (given && (declaredIn !== modelFile || givens.some((g) => !g.file))) {
         refuse(
            plan,
            "a `where:` reading a given can only be left out of the build " +
               "for a source declared in a local model file, reading givens " +
               "declared in local files",
         );
         continue;
      }
      if (given && text === undefined) {
         refuse(
            plan,
            "the model file could not be read to leave its given-reading " +
               `\`where:\` out of the build: ${unreadable}`,
         );
         continue;
      }
      // An upper-bound current-time term (option A): taken out of the copy
      // wherever this file writes it, the source's own or a base's.
      const windowsOut = leftOutWindowTerms(plan.readTimeTerms);
      const elsewhere = windowsOut.find((t) =>
         t.at.some((use) => localPath(use.url) !== modelFile),
      );
      if (elsewhere) {
         refuse(
            plan,
            `\`where: ${elsewhere.code}\` keeps rows older than a moving ` +
               "cut-off and is written in another file than this source's " +
               "model, so the build cannot leave it out. Write it in this " +
               "file, or materialize the source in the file that writes it",
         );
         continue;
      }
      if (windowsOut.length > 0 && text === undefined) {
         refuse(
            plan,
            "the model file could not be read to leave its `where:` keeping " +
               `rows older than a moving cut-off out of the build: ${unreadable}`,
         );
         continue;
      }
      const leftOut = given || windowsOut.length > 0;
      const parent = plan.buildsOver
         ? kept.get(plan.buildsOver.sourceID)
         : undefined;
      if (
         plan.buildsOver &&
         (!parent ||
            text === undefined ||
            localPath(plan.buildsOver.declaration.url) !== modelFile)
      ) {
         plan = withoutParent(plan);
      }
      // A `storage=` pair also carries what its build compiles in the
      // destination over the parent's stored table, lifted as its build copy
      // is (P3); without it, the raw chain.
      if (plan.buildsOver && parent && plan.persist.storage !== undefined) {
         const overStored = storedChainText(text as string, plan, parent, {
            liftAccess,
         });
         plan = overStored
            ? { ...plan, buildsOver: { ...plan.buildsOver, overStored } }
            : withoutParent(plan);
      }
      if (!leftOut && !plan.buildsOver && !liftAccess) {
         kept.set(plan.sourceID, plan);
         levels.set(plan.sourceID, 0);
         continue;
      }
      const parentLevel = parent ? (levels.get(parent.sourceID) ?? 0) : 0;
      // One copy per source: its terms taken out, its parent re-rooted, and
      // its access modifiers lifted, as it needs.
      let copy = buildCopyText(
         text as string,
         plan,
         plan.buildsOver && parent
            ? {
                 parent,
                 importFrom: levelFileName(modelFileName, parentLevel),
              }
            : undefined,
         { liftAccess },
      );
      if ("reason" in copy && plan.buildsOver) {
         plan = withoutParent(plan);
         if (!leftOut && !liftAccess) {
            kept.set(plan.sourceID, plan);
            levels.set(plan.sourceID, 0);
            continue;
         }
         copy = buildCopyText(text as string, plan, undefined, { liftAccess });
      }
      if ("reason" in copy) {
         refuse(plan, copy.reason);
         continue;
      }
      const chain =
         plan.buildsOver && parent
            ? {
                 parent,
                 importFrom: levelFileName(modelFileName, parentLevel),
              }
            : undefined;
      const located = plan;
      copies.set(plan.sourceID, {
         fileName: buildCopyName(modelFileName, copies.size),
         text: copy.copy,
         ...(chain
            ? {
                 declarationAt: (at: { line: number; character: number }) =>
                    buildCopyDeclarationAt(
                       text as string,
                       located,
                       chain,
                       { liftAccess },
                       contents,
                       at,
                    ),
              }
            : {}),
      });
      levels.set(plan.sourceID, plan.buildsOver ? parentLevel + 1 : 0);
      kept.set(
         plan.sourceID,
         given
            ? {
                 ...plan,
                 givenDeclarations: givens.map(({ file, ...g }) =>
                    file === modelFile
                       ? g
                       : {
                            ...g,
                            from: path
                               .relative(
                                  path.dirname(modelFile),
                                  file as string,
                               )
                               .split(path.sep)
                               .join("/"),
                         },
                 ),
              }
            : plan,
      );
   }
   return {
      plans: plans.flatMap((p) => {
         const k = kept.get(p.sourceID);
         return k ? [k] : [];
      }),
      copies,
      refused,
   };
}

/**
 * A generated companion that did not compile, with what is needed to tell
 * whose generated text the failure is in: every generated file's text by URL,
 * the source each build copy is for by its URL, and, when known outright, the
 * failing source.
 */
class CompanionCompileError extends Error {
   constructor(
      readonly error: unknown,
      readonly overlay: ReadonlyMap<string, string>,
      readonly copyOwners: ReadonlyMap<string, string>,
      readonly sourceID?: string,
   ) {
      super(error instanceof Error ? error.message : String(error));
   }
}

/** A compile problem's message and where it is, as Malloy reports it. */
interface CompileProblem {
   message?: unknown;
   at?: { url?: unknown; range?: { start?: { line?: unknown } } };
}

/**
 * The planned source whose generated text a companion's compile failure is
 * in, by sourceID, or undefined when it cannot be told. A problem in a build
 * copy is the copy's source's (a copy is one source's text, and a problem in
 * one leaves the files importing it with problems of their own, so a copy's
 * comes first); one in a generated file is the plan named on its line, or on
 * the nearest line above it naming exactly one plan (a `select:` list sits
 * under its build source's line).
 */
function failingSourceOf(
   failure: CompanionCompileError,
   plans: readonly MaterializePlan[],
): string | undefined {
   if (failure.sourceID !== undefined) return failure.sourceID;
   const problems = ((failure.error as { problems?: unknown } | null)
      ?.problems ?? []) as CompileProblem[];
   const located = problems.filter(
      (p): p is CompileProblem & { at: { url: string } } =>
         typeof p?.at?.url === "string",
   );
   const inCopy = located.find((p) => failure.copyOwners.has(p.at.url));
   if (inCopy) return failure.copyOwners.get(inCopy.at.url);
   const names = plans.map((plan) => ({
      sourceID: plan.sourceID,
      patterns: generatedNamesOf(plan).map(
         (name) =>
            new RegExp(
               `(^|[^A-Za-z0-9_])${name.replace(/[^A-Za-z0-9_]/g, "\\$&")}($|[^A-Za-z0-9_])`,
            ),
      ),
   }));
   for (const problem of located) {
      const text = failure.overlay.get(problem.at.url);
      const line = problem.at.range?.start?.line;
      if (text === undefined || typeof line !== "number") continue;
      const lines = text.split("\n");
      for (let at = Math.min(line, lines.length - 1); at >= 0; at--) {
         const named = names.filter((n) =>
            n.patterns.some((re) => re.test(lines[at])),
         );
         if (named.length === 1) return named[0].sourceID;
         if (named.length > 1) return undefined;
      }
      return undefined;
   }
   return undefined;
}

/**
 * The declarations off the chain that the problems in `plan`'s chained build
 * copy lie in, each with its range in the author's file and each named once,
 * leaving out any the copy already omits. Empty when every problem is on the
 * chain (the plan's own declaration, its parent's or one between) or in no
 * declaration. All of them at once, so a file with many siblings that do not
 * compile over the re-root settles in one round rather than one per sibling.
 */
function copyOmissions(
   failure: CompanionCompileError,
   copy: BuildCopy | undefined,
   plan: MaterializePlan,
): { sourceID: string; declaration: TermUse }[] {
   if (!copy?.declarationAt) return [];
   const url = [...failure.copyOwners].find(
      ([, id]) => id === plan.sourceID,
   )?.[0];
   if (url === undefined) return [];
   const problems = ((failure.error as { problems?: unknown } | null)
      ?.problems ?? []) as CompileProblem[];
   const key = (d: TermUse) =>
      `${d.url}:${d.range.start.line}:${d.range.start.character}`;
   const seen = new Set((plan.buildsOver?.omit ?? []).map(key));
   const out: { sourceID: string; declaration: TermUse }[] = [];
   for (const problem of problems) {
      if (problem?.at?.url !== url) continue;
      const start = problem.at.range?.start as
         | { line?: unknown; character?: unknown }
         | undefined;
      if (typeof start?.line !== "number") continue;
      const found = copy.declarationAt({
         line: start.line,
         character: typeof start.character === "number" ? start.character : 0,
      });
      if (found === undefined || seen.has(key(found.declaration))) continue;
      seen.add(key(found.declaration));
      out.push(found);
   }
   return out;
}

/** The first problem's message, or the error's own, for a refusal reason. */
function firstProblem(failure: CompanionCompileError): string {
   const [problem] = ((failure.error as { problems?: unknown } | null)
      ?.problems ?? []) as CompileProblem[];
   const message =
      typeof problem?.message === "string" ? problem.message : failure.message;
   return message.split("\n")[0];
}

/**
 * How many rounds one chained source may leave declarations out of its build
 * copy before it builds over the raw chain instead. Each round leaves out
 * every declaration its copy's problems lie in, so more rounds are needed
 * only where leaving one out fails another that reads it.
 */
const MAX_OMISSION_ROUNDS = 4;

/**
 * Prepare `from` (build copies, P2/P3/P10) and compile its companion, so that
 * a failure in one source's generated text costs that source alone. The
 * source a failure is in ({@link failingSourceOf}) is an extension table
 * built over its parent's build source (P10): it alone builds over the raw
 * chain instead, with the same rows, and every other extension keeps reading
 * its parent's table. Any other source is refused (`refuse`, reported as
 * `materialize-not-built`) and answers live, and the rest are compiled again.
 * A failure no source can be told for sends every extension to the raw chain
 * once, as before; one that still cannot be told throws, costing the model
 * its builds (logged by the caller as a Publisher bug, as before).
 */
async function compileContained(
   args: Parameters<typeof compileCompanion>[0],
   from: MaterializePlan[],
   prepare: (
      plans: MaterializePlan[],
   ) => Promise<{ plans: MaterializePlan[]; copies: Map<string, BuildCopy> }>,
   refuse: (refusal: MaterializeRefusal) => void,
): Promise<{
   plans: MaterializePlan[];
   copies: Map<string, BuildCopy>;
   generated?: GeneratedMaterialize;
}> {
   let input = from;
   // How many rounds each chained source has left declarations out of its
   // copy. Past MAX_OMISSION_ROUNDS it builds over the raw chain instead.
   const omissionRounds = new Map<string, number>();
   // Each round leaves declarations out of one copy (at most
   // MAX_OMISSION_ROUNDS times per source), unchains or drops one source, or
   // unchains every one once, so this many rounds always settle.
   const rounds = (MAX_OMISSION_ROUNDS + 2) * from.length + 2;
   for (let round = 0; round < rounds; round++) {
      const { plans, copies } = await prepare(input);
      try {
         return {
            plans,
            copies,
            generated: await compileCompanion(args, plans, copies),
         };
      } catch (error) {
         if (!(error instanceof CompanionCompileError)) throw error;
         const failing = failingSourceOf(error, plans);
         const plan =
            failing === undefined ? undefined : planOf(plans, failing);
         // In a chained copy, a problem in another source's declaration
         // (one that reads the parent and does not compile over its re-root)
         // is that source's: the copy leaves it out, empty, and keeps the
         // chain.
         const omitted =
            plan?.buildsOver &&
            (omissionRounds.get(plan.sourceID) ?? 0) < MAX_OMISSION_ROUNDS
               ? copyOmissions(error, copies.get(plan.sourceID), plan)
               : [];
         if (plan?.buildsOver && omitted.length > 0) {
            omissionRounds.set(
               plan.sourceID,
               (omissionRounds.get(plan.sourceID) ?? 0) + 1,
            );
            logger.warn(
               "A chained #(materialize) build copy leaves out declarations that do not compile over the parent's build source",
               {
                  modelPath: args.modelPath,
                  sourceName: plan.sourceName,
                  omitted: omitted.map((o) => splitSourceID(o.sourceID)?.name),
               },
            );
            input = plans.map((p) =>
               p === plan && p.buildsOver
                  ? {
                       ...p,
                       buildsOver: {
                          ...p.buildsOver,
                          omit: [
                             ...(p.buildsOver.omit ?? []),
                             ...omitted.map((o) => o.declaration),
                          ],
                       },
                    }
                  : p,
            );
         } else if (plan?.buildsOver) {
            logger.warn(
               "An extension table of a #(materialize) model did not compile over its parent's build source; it builds over the raw chain",
               {
                  modelPath: args.modelPath,
                  sourceName: plan.sourceName,
                  error: error.message,
               },
            );
            input = plans.map((p) => (p === plan ? withoutParent(p) : p));
         } else if (plan) {
            logger.error(
               "A #(materialize) source's generated build did not compile; it is not built, and the model's other sources are",
               {
                  modelPath: args.modelPath,
                  sourceName: plan.sourceName,
                  error: error.message,
               },
            );
            refuse({
               sourceName: plan.sourceName,
               sourceID: plan.sourceID,
               reason:
                  "its generated build did not compile, so it is not built " +
                  `(a Publisher bug; please report it): ${firstProblem(error)}`,
            });
            input = plans.filter((p) => p !== plan);
         } else if (plans.some((p) => p.buildsOver)) {
            // No source to blame: every extension builds over the raw chain,
            // as it did before P10, with the same rows.
            logger.warn(
               "Extension tables of a #(materialize) model did not compile over their parents' build sources; they build over the raw chain",
               { modelPath: args.modelPath, error: error.message },
            );
            input = plans.map(withoutParent);
         } else {
            throw error.error;
         }
      }
   }
   throw new Error(
      "the generated #(materialize) companion did not settle on sources that compile",
   );
}
