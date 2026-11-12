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
   MATERIALIZE_COMPANION_SUFFIX,
   planModelMaterialize,
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
            refused.push({
               sourceName: plan.sourceName,
               sourceID: plan.sourceID,
               reason:
                  "a `where:` reading a given cannot yet be left out of the " +
                  "build of a source that reads a #@ persist source " +
                  `(${names.join(", ")}): its build would declare that source ` +
                  "a second time",
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
 * Give each plan with given-reading terms (P2) a build copy of the author's model,
 * whose text is read back here. Such a source must be declared in that model
 * and read givens declared in local files; otherwise, or when the file cannot
 * be read or a term not placed in it, the plan is refused (fail closed: built
 * from the author's file, the term would be baked into the table with the
 * given's default). Each given declared in another file is imported from that
 * file.
 */
async function withBuildCopies(
   plans: MaterializePlan[],
   modelFile: string,
   readModelText: () => Promise<string>,
): Promise<{
   plans: MaterializePlan[];
   copies: Map<string, BuildCopy>;
   refused: MaterializeRefusal[];
}> {
   const copies = new Map<string, BuildCopy>();
   const refused: MaterializeRefusal[] = [];
   if (!plans.some((plan) => givenReadTerms(plan).length > 0)) {
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
   for (const plan of plans) {
      if (givenReadTerms(plan).length === 0) {
         kept.push(plan);
         continue;
      }
      const declaredIn = plan.declaration
         ? localPath(plan.declaration.url)
         : undefined;
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
      const copy = buildCopyText(text, plan);
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
