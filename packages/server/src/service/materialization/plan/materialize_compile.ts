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

import type {
   Model as MalloyModel,
   ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../../logger";
import { sourceBuildsOf } from "./addressing";
import {
   buildCopyName,
   buildCopyText,
   generateBuildFile,
   givenReadTerms,
   hiddenAccess,
   MATERIALIZE_COMPANION_SUFFIX,
   planModelMaterialize,
   splitSourceID,
   type MaterializePlan,
   type MaterializeRefusal,
} from "./build_file_generator";
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";

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
      const withCopies = await withBuildCopies(
         planned.plans,
         modelFile,
         async () => {
            // The text the author's model was compiled from, through the
            // reader the companion compiles with: a package's build view
            // (joins across connections left out) or a worker's file map, not
            // necessarily the file on disk, and the compiled locations the
            // terms are found by are positions in that text.
            const { runtime } = await args.getRuntime(new Map());
            const read = await runtime.urlReader.readURL(
               pathToFileURL(modelFile),
            );
            return typeof read === "string" ? read : read.contents;
         },
         async (fileName, text) => {
            // The copy compiled on its own, beside the author's model, so its
            // imports resolve as the author's do; never run.
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
         },
      );
      refused.push(...withCopies.refused);
      let { plans } = withCopies;
      const { copies } = withCopies;
      let generated = await compileCompanion(args, plans, copies);
      // A build copy re-declares its whole file, so a `#@ persist` source the
      // copied source reads (its base, a join) would enter the plan a second
      // time under the copy's URL: a build node that no planned source names.
      // Such a source's term is not taken out yet; it is refused, and the
      // companion compiled again without it.
      const leaked = generated
         ? await copyDeclaredBuilds(generated, modelFile, copies)
         : new Map<string, string[]>();
      if (leaked.size > 0) {
         for (const plan of plans) {
            const names = leaked.get(plan.sourceID);
            if (!names) continue;
            if (givenReadTerms(plan).length === 0) {
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
                  givenReadTerms(plan).length > 0
                     ? "a `where:` reading a given cannot yet be left out of the " +
                       "build of a source that reads a #@ persist source " +
                       `(${names.join(", ")}): its build would declare that source ` +
                       "a second time"
                     : "the source stores only hidden fields, which its build " +
                       "cannot read while it reads a #@ persist source " +
                       `(${names.join(", ")})`,
            });
         }
         plans = plans.filter((p) => !leaked.has(p.sourceID));
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
   const text = generateBuildFile(
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
   if (!text) return undefined;
   const url = materializeCompanionURL(args.packagePath, args.modelPath);
   const overlay = new Map([[url.href, text]]);
   const dir = path.dirname(modelFile);
   for (const copy of used.values()) {
      overlay.set(pathToFileURL(path.join(dir, copy.fileName)).href, copy.text);
   }
   const { runtime, importBaseURL } = await args.getRuntime(overlay);
   const materializer = runtime.loadModel(url, { importBaseURL });
   const model = await materializer.getModel();
   // The P8 checks' SQL, compiled now (never run here) so a build runs it
   // against the joined source's connection before it writes anything.
   for (const plan of plans) {
      for (const check of plan.uniquenessChecks ?? []) {
         check.sql = await materializer
            .loadQueryByName(check.queryName)
            .getSQL();
      }
   }
   return {
      plans,
      model,
      materializer,
      runtime,
      text,
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
 * For each planned source whose build plans a source only its build copy
 * declares, the names of those sources. A `sourceID` is `name@<declaring
 * URL>`, so a build copy's own are told apart by the copy's URL.
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
   const targets = await generated.runtime.getBuildTargets(generated.model);
   for (const build of sourceBuildsOf(targets)) {
      for (const id of build.order) {
         const at = id.indexOf("@");
         const owner = at < 0 ? undefined : ownerByURL.get(id.slice(at + 1));
         if (owner === undefined) continue;
         const names = out.get(owner) ?? [];
         if (!names.includes(id.slice(0, at))) names.push(id.slice(0, at));
         out.set(owner, names.sort());
      }
   }
   return out;
}

/**
 * Give each plan that needs one a build copy of the author's model, whose
 * text is read back here: a plan with given-reading terms (P2), which the
 * copy takes out, and a plan with hidden columns (P3), whose own access
 * modifiers the copy lifts. Such a source must be declared in that model, and
 * its givens in local files.
 *
 * Given terms: when the source is not declared there, the file cannot be
 * read, or a term not placed in it, the plan is refused (fail closed: built
 * from the author's file, the term would be baked into the table with the
 * given's default). Each given declared in another file is imported from it.
 *
 * Hidden columns: which ones the build can select is settled by compiling the
 * copy (`compileCopy`) and reading its source: a field public there is
 * stored, any other is dropped from the plan and stays computed at read
 * (live), such as a field private on a source this one extends. A copy that
 * cannot be made or compiled drops them all. A plan left with no column, or
 * with a read-time term over a dropped column, is refused.
 */
async function withBuildCopies(
   plans: MaterializePlan[],
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
   const copies = new Map<string, BuildCopy>();
   const refused: MaterializeRefusal[] = [];
   const hiddenOf = (plan: MaterializePlan) =>
      plan.columns.filter((c) => c.access !== undefined);
   if (
      !plans.some(
         (plan) => givenReadTerms(plan).length > 0 || hiddenOf(plan).length > 0,
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
   const kept: MaterializePlan[] = [];
   for (const original of plans) {
      const hidden = hiddenOf(original);
      const declaredIn = original.declaration
         ? localPath(original.declaration.url)
         : undefined;
      // P3: the hidden columns the build copy lets the build select.
      let lifted: { copy: string; selectable: Set<string> } | undefined;
      if (hidden.length > 0 && declaredIn === modelFile && text !== undefined) {
         const copy = buildCopyText(text, original, { liftAccess: true });
         if ("copy" in copy) {
            const compiled = await compileCopy(
               buildCopyName(path.basename(modelFile), copies.size),
               copy.copy,
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
               lifted = { copy: copy.copy, selectable };
            }
         }
      }
      const columns = original.columns.filter(
         (c) => c.access === undefined || lifted?.selectable.has(c.dimension),
      );
      const plan: MaterializePlan =
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
      const needsTerms = givenReadTerms(plan).length > 0;
      if (!needsTerms) {
         if (lifted) {
            copies.set(plan.sourceID, {
               fileName: buildCopyName(path.basename(modelFile), copies.size),
               text: lifted.copy,
            });
         }
         kept.push(plan);
         continue;
      }
      const givens = (plan.givenDeclarations ?? []).map((g) => ({
         ...g,
         file: localPath(g.url),
      }));
      if (declaredIn !== modelFile || givens.some((g) => !g.file)) {
         refuse(
            plan,
            "a `where:` reading a given can only be left out of the build " +
               "for a source declared in a local model file, reading givens " +
               "declared in local files",
         );
         continue;
      }
      if (text === undefined) {
         refuse(
            plan,
            "the model file could not be read to leave its given-reading " +
               `\`where:\` out of the build: ${unreadable}`,
         );
         continue;
      }
      // One copy per source: the terms taken out, and its access modifiers
      // lifted when it stores a hidden column. `lifted.copy` already is both.
      const copy = lifted ? { copy: lifted.copy } : buildCopyText(text, plan);
      if ("reason" in copy) {
         refuse(plan, copy.reason);
         continue;
      }
      copies.set(plan.sourceID, {
         fileName: buildCopyName(path.basename(modelFile), copies.size),
         text: copy.copy,
      });
      kept.push({
         ...plan,
         givenDeclarations: givens.map(({ file, ...g }) =>
            file === modelFile
               ? g
               : {
                    ...g,
                    from: path
                       .relative(path.dirname(modelFile), file as string)
                       .split(path.sep)
                       .join("/"),
                 },
         ),
      });
   }
   return { plans: kept, copies, refused };
}
