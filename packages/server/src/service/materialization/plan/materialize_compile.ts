// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated `#(materialize)` build companion (see
 * build_file_generator.ts), as preaggregation_compile.ts compiles the rollup
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
import { registerUniquenessCheckCompiler } from "../run/inline_uniqueness";
import {
   buildCopyName,
   buildCopyText,
   chainLevels,
   extensionOverStoredModel,
   generateBuildFiles,
   givenReadTerms,
   hiddenAccess,
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
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";
import { leftOutWindowTerms } from "./rolling_window";

export interface GeneratedMaterialize {
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
export function materializeCompanionURL(
   packagePath: string,
   modelPath: string,
): URL {
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
}): Promise<{
   generated?: GeneratedMaterialize;
   refused: MaterializeRefusal[];
}> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const refused: MaterializeRefusal[] = [];
   try {
      const planned = planModelMaterialize(
         args.contents,
         (declaringURL) => {
            try {
               return path.resolve(fileURLToPath(declaringURL)) === modelFile;
            } catch {
               // Not a local file (a host, another scheme): planned, as before.
               return true;
            }
         },
         args.givens,
      );
      refused.push(...planned.refused);
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
         );
         for (const r of out.refused) {
            if (r.sourceID !== undefined && refusedIDs.has(r.sourceID))
               continue;
            if (r.sourceID !== undefined) refusedIDs.add(r.sourceID);
            refused.push(r);
         }
         return out;
      };
      let { plans, copies } = await prepare(planned.plans);
      let generated: GeneratedMaterialize | undefined;
      try {
         generated = await compileCompanion(args, plans, copies);
      } catch (error) {
         // A chained build that does not compile (an extension reading a
         // field its parent's table does not carry) is no reason to lose the
         // model's builds: every extension builds over the raw chain instead,
         // as it did before P10, with the same rows.
         if (!plans.some((p) => p.buildsOver)) throw error;
         logger.warn(
            "Extension tables of a #(materialize) model did not compile over their parents' build sources; they build over the raw chain",
            {
               modelPath: args.modelPath,
               error: error instanceof Error ? error.message : String(error),
            },
         );
         ({ plans, copies } = await prepare(plans.map(withoutParent)));
         generated = await compileCompanion(args, plans, copies);
      }
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
         ({ plans, copies } = await prepare(
            plans.map((p) => (unstored.has(p.sourceID) ? withoutParent(p) : p)),
         ));
         generated = await compileCompanion(args, plans, copies);
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
         ({ plans, copies } = await prepare(
            plans.map((p) => (leaked.has(p.sourceID) ? withoutParent(p) : p)),
         ));
         generated = await compileCompanion(args, plans, copies);
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
         ({ plans, copies } = await prepare(
            plans.filter((p) => !leaked.has(p.sourceID)),
         ));
         generated = await compileCompanion(args, plans, copies);
      }
      return generated ? { generated, refused } : { refused };
   } catch (error) {
      logger.error(
         "Failed to plan or compile the generated #(materialize) companion",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return { refused };
   }
}

/** A planned source's build copy: its file name beside the model, and text. */
interface BuildCopy {
   fileName: string;
   text: string;
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
   const model = await materializer.getModel();
   // The P8 checks' SQL, compiled now (never run here) so a build runs it
   // against the joined source's connection before it writes anything.
   // Each check can also be compiled again over a run's manifest, so a run
   // whose parent build reads the joined source's persisted table checks that
   // table (inline_uniqueness.ts).
   for (const plan of plans) {
      for (const check of plan.uniquenessChecks ?? []) {
         check.sql = await materializer
            .loadQueryByName(check.queryName)
            .getSQL();
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
            hiddenOf(p).length > 0,
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
            if (hidden.some((c) => selectable.has(c.dimension))) {
               lifted = selectable;
            }
         }
      }
      const columns = original.columns.filter(
         (c) => c.access === undefined || lifted?.has(c.dimension),
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
      copies.set(plan.sourceID, {
         fileName: buildCopyName(modelFileName, copies.size),
         text: copy.copy,
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
