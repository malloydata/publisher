// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The declaring-file overlay both router legs that compile a caller's text
 * over a stored table share: the persisted-view leg (`view_variants.ts`) and
 * the `#(preaggregate)` rollup leg (`rollup_variants.ts`).
 *
 * The queried source is redeclared IN AN OVERLAY OF ITS OWN DECLARING FILE,
 * over the stored table, so it keeps the author's `sourceID`, with the
 * source's entry-point gates written on it as note lines ({@link gateLines}):
 * every gate the governor keys on that source (by name, then by `sourceID`)
 * finds its graft target in the variant, exactly as the served copy keeps it
 * by re-rooting the author's own file (R3, route then govern). The file's
 * other names come from a copy of it (`<file><copySuffix>`), and the query
 * model imports every name of the author's model from its declaring file,
 * the redeclared source from the overlay.
 *
 *     // <declaring file>, overlaid
 *     import { customers, REGION } from "orders.malloy<copySuffix>"
 *     <the leg's stored-table declaration>
 *     #(access_filter) region = $REGION        // the source's gates
 *     source: orders is <stored> extend { … }
 *
 * The author's files are read with every source-level `#@` line blanked
 * (`persist_blanking.ts`), so nothing in the variant chains onto another
 * persisted table.
 */

import type { ModelDef, ModelMaterializer } from "@malloydata/malloy";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import type { GraftRuntime } from "../../gate_classification";
import { collectEntryPointGates } from "../../gate_classification";
import { doubleQuotedString, identifier } from "../../malloy_text";
import {
   createModelRuntime,
   type ModelConnectionInput,
} from "../../model_runtime";
import { overlayKey } from "../../overlay_key";
import { splitSourceID } from "../plan/persisted_view";
import { SourceMap } from "../plan/source_map";
import { persistBlankedFiles } from "../persist_blanking";
import { authorNameImports } from "./author_imports";

/** A variant compiled over the author's declaring file. */
export interface CompiledDeclaringFileVariant {
   materializer?: ModelMaterializer;
   modelDef?: ModelDef;
   /** The runtime the variant compiled in, which a graft on it reloads through. */
   runtime?: GraftRuntime;
   error?: string;
}

/** What the leg's declaration callback is handed. */
export interface DeclaringFileContext {
   /** The declaring file's URL (its overlay key is where the variant lives). */
   declaringURL: URL;
   /** The copy of the declaring file, holding its other names. */
   copyURL: URL;
   /** The copy's basename, for an `import` beside the declaring file. */
   copyName: string;
   /** The redeclared source's name in its declaring file. */
   sourceName: string;
   /**
    * The source's gates, as note lines ending in a newline, to write right
    * before its `source:` statement (empty for an ungated source).
    */
   gates: string;
   /**
    * Add a generated file to the overlay (a build companion), by URL. A file
    * that imports the declaring file's names should import them from the copy.
    */
   addFile(url: URL, text: string): void;
}

/**
 * Compile a variant of the author's model in which the source `sourceID`
 * names is redeclared by `declare` in an overlay of its declaring file, and
 * load the query model (`queryModelURL`) that imports the author's model's
 * names. `declare` returns the overlay's declarations (after the copy's names
 * are imported), the redeclared source among them, or an error.
 */
export async function compileDeclaringFileVariant(args: {
   packagePath: string;
   modelPath: string;
   /** The package's own connections: what the author's files are read with. */
   packageConfig: ModelConnectionInput;
   /** The connections the variant compiles with (a destination's included). */
   config: ModelConnectionInput;
   modelDef: ModelDef;
   /** The redeclared source's `sourceID` (the author's). */
   sourceID: string;
   /** Appended to the declaring file's path to name its copy. */
   copySuffix: string;
   /** The query model's URL, beside the author's model. */
   queryModelURL: URL;
   /** The `##!` line both generated files start with. */
   flags: string;
   declare(
      ctx: DeclaringFileContext,
   ): string | { error: string } | Promise<string | { error: string }>;
}): Promise<CompiledDeclaringFileVariant> {
   const { packagePath, modelPath, packageConfig, modelDef } = args;
   const declared = splitSourceID(args.sourceID);
   if (!declared) return { error: "no declaring file" };
   let declaringFile: string;
   try {
      declaringFile = fileURLToPath(declared.url);
   } catch {
      return { error: "not a local file" };
   }
   const modelFile = path.join(packagePath, modelPath);
   const declaringURL = pathToFileURL(declaringFile);
   const copyURL = pathToFileURL(`${declaringFile}${args.copySuffix}`);
   const copyName = path.basename(fileURLToPath(copyURL));
   const { runtime: reader } = await createModelRuntime(
      packagePath,
      modelPath,
      packageConfig,
   );
   const readText = async (url: string) => {
      try {
         const read = await reader.urlReader.readURL(new URL(url));
         return typeof read === "string" ? read : read.contents;
      } catch {
         return undefined;
      }
   };
   // The author's files with source-level `#@` lines blanked, the declaring
   // file's under the copy.
   const blanked = await persistBlankedFiles(modelDef, readText);
   if (!blanked) return { error: "a source-level #@ line not set aside" };
   const overlay = new Map<string, string>();
   for (const [url, text] of blanked) {
      if (url !== overlayKey(declaringURL)) overlay.set(url, text);
   }
   const declaringText =
      blanked.get(overlayKey(declaringURL)) ??
      (await readText(declaringURL.href));
   if (declaringText === undefined) return { error: "unreadable source" };
   overlay.set(overlayKey(copyURL), declaringText);
   // The copy's own names, to import into the declaring file's overlay.
   const copyNames = await namesDeclaredIn(
      packagePath,
      modelPath,
      packageConfig,
      overlay,
      copyURL,
   );
   let gates: string;
   try {
      gates = gateLines(modelDef, args.sourceID);
   } catch (error) {
      return { error: error instanceof Error ? error.message : String(error) };
   }
   const source = declared.name;
   const declaration = await args.declare({
      declaringURL,
      copyURL,
      copyName,
      sourceName: source,
      gates,
      addFile: (url, text) => overlay.set(overlayKey(url), text),
   });
   if (typeof declaration !== "string") return declaration;
   const others = copyNames.names.filter((n) => n !== source);
   const importOthers =
      others.length > 0
         ? `import { ${others.map((n) => identifier(n)).join(", ")} } from ${doubleQuotedString(copyName)}\n`
         : "";
   // Exported again, so a file that imports the declaring file whole (an
   // `index.malloy` re-exporting its sources) still sees every source and
   // query the author's file declares, not only the redeclared one.
   const reexported = others.filter((n) => !copyNames.givens.has(n));
   const exportAll =
      reexported.length > 0
         ? `export { ${[source, ...reexported].map((n) => identifier(n)).join(", ")} }\n`
         : "";
   overlay.set(
      overlayKey(declaringURL),
      `${args.flags}
${importOthers}
${declaration}
${exportAll}`,
   );
   // The query model: the author's model's names, each from the file that
   // declares it (the redeclared source from its declaring file's overlay).
   const imports = authorNameImports(
      modelDef,
      args.queryModelURL,
      pathToFileURL(modelFile).href,
      {
         redirect: {
            from: overlayKey(declaringURL),
            to: copyURL.href,
            keep: source,
         },
      },
   );
   overlay.set(
      overlayKey(args.queryModelURL.href),
      `${args.flags}
${imports}
`,
   );
   const { runtime, importBaseURL } = await createModelRuntime(
      packagePath,
      modelPath,
      args.config,
      { overlay },
   );
   const materializer = runtime.loadModel(args.queryModelURL, {
      importBaseURL,
   });
   const model = await materializer.getModel();
   return {
      materializer,
      modelDef: (model as unknown as { _modelDef: ModelDef })._modelDef,
      runtime: runtime as unknown as GraftRuntime,
   };
}

/**
 * The names a file declares (its sources, queries and givens), compiled on
 * its own, with which of them are givens.
 */
async function namesDeclaredIn(
   packagePath: string,
   modelPath: string,
   config: ModelConnectionInput,
   overlay: ReadonlyMap<string, string>,
   url: URL,
): Promise<{ names: string[]; givens: ReadonlySet<string> }> {
   const { runtime, importBaseURL } = await createModelRuntime(
      packagePath,
      modelPath,
      config,
      { overlay },
   );
   const model = await runtime.loadModel(url, { importBaseURL }).getModel();
   const def = (model as unknown as { _modelDef: ModelDef })._modelDef;
   const names = new Set<string>();
   for (const [name, entry] of Object.entries(def.contents ?? {})) {
      const located = entry as { location?: { url?: unknown } };
      // Declared in the file itself, not imported into it.
      if (located.location?.url === url.href) names.add(name);
   }
   const givens = new Set<string>();
   for (const given of Object.values(def.givens ?? {})) {
      const g = given as { name?: unknown; location?: { url?: unknown } };
      if (typeof g.name === "string" && g.location?.url === url.href) {
         names.add(g.name);
         givens.add(g.name);
      }
   }
   return { names: [...names].sort(), givens };
}

/** The routes a row-level gate is written on. */
const GATE_ROUTES = new Set(["authorize", "access_filter"]);

/**
 * The source's entry-point gates (the governor's walk: own, inherited, a
 * derivation's base), as note lines for the variant's source, so `govern()`
 * finds them there (R3). Throws when one cannot be written as a single line:
 * the variant is then not compiled and the stored table not read.
 */
export function gateLines(modelDef: ModelDef, sourceID: string): string {
   const def = SourceMap.of(modelDef).node(sourceID)?.def;
   if (!def) return "";
   const lines: string[] = [];
   for (const entry of collectEntryPointGates(def as never, modelDef)) {
      if (!GATE_ROUTES.has(entry.route)) {
         throw new Error(`unknown gate route ${entry.route}`);
      }
      for (const expr of entry.exprs) {
         if (expr.includes("\n")) throw new Error("a multi-line gate");
         lines.push(
            entry.route === "authorize"
               ? `#(authorize) ${expr}`
               : `#(access_filter) ${expr}`,
         );
      }
   }
   return lines.map((l) => `${l}\n`).join("");
}
