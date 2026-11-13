// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package-level classification reports: every published source a
 * package's models export, the coverage and downgrade warnings, the
 * `#(carries)` leaks that refuse a load, and the ceiling check over a
 * package's queries and report cells. Built on ./classification.ts, which
 * reads one compiled model and knows nothing of Model; this half walks a
 * package's Models, so ./classification.ts (which query governance and the
 * access record use) stays free of service/model.ts.
 */

import type { ModelDef } from "@malloydata/malloy";
import {
   CLASSIFICATION_DOWNGRADE,
   CLASSIFICATION_MALFORMED,
   ceilingRefusal,
   findIrSource,
   isDeniedToAll,
   type ApiPackageWarning,
   type IrField,
} from "./classification";
import type { Model } from "./model";
import {
   coverageWarnings,
   declaredUnknownReads,
   MAX_NAMED_FIELDS,
   publishedSourceOf,
   sourceLeaks,
   type PublishedSource,
} from "./classification_findings";

export {
   classificationFindingsBySource,
   type PublishedSource,
} from "./classification_findings";

/**
 * True when this source's declaration was already visited through another
 * model, and records it otherwise. A source with no recorded location is
 * keyed to its own model, so it is never skipped.
 */
function alreadySeen(
   seen: Set<string>,
   model: Model,
   modelPath: string,
   name: string,
): boolean {
   const key = model.definitionIdentity(name) ?? `${modelPath}\0${name}`;
   if (seen.has(key)) return true;
   seen.add(key);
   return false;
}

/** One source a listed model exports, as {@link packageSources} yields it. */
export interface PackageSource {
   modelPath: string;
   model: Model;
   sourceInfo: NonNullable<ReturnType<Model["getSourceInfos"]>>[number];
   ir: IrField;
   apiSource: NonNullable<ReturnType<Model["getSources"]>>[number] | undefined;
   modelDef: ModelDef | undefined;
}

/**
 * Every source the package's listed models export, with its compiled
 * definition. `isListed` says which models are on the surface, the same
 * answer `listModels()` gives; `getSourceInfos()` says which sources a listed
 * model exports. A model that cannot be read, or a source with no compiled
 * definition, contributes nothing. `includeHidden` also takes the sources a
 * model does not export, each once however many models import it.
 */
export function* packageSources(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
   includeHidden = false,
): Generator<PackageSource> {
   const seen = new Set<string>();
   for (const [modelPath, model] of models) {
      if (!isListed(modelPath)) continue;
      let sourceInfos: ReturnType<Model["getSourceInfos"]>;
      let apiSources: ReturnType<Model["getSources"]>;
      let modelDef: ModelDef | undefined;
      try {
         sourceInfos = model.getSourceInfos(includeHidden);
         apiSources = model.getSources(includeHidden);
         modelDef = model.getModelDef();
      } catch {
         continue;
      }
      for (const sourceInfo of sourceInfos ?? []) {
         if (
            includeHidden &&
            alreadySeen(seen, model, modelPath, sourceInfo.name)
         ) {
            continue;
         }
         const ir = findIrSource(modelDef, sourceInfo.name);
         if (!ir) continue;
         const apiSource = apiSources?.find((s) => s.name === sourceInfo.name);
         yield { modelPath, model, sourceInfo, ir, apiSource, modelDef };
      }
   }
}

/**
 * Every source the package publishes ({@link packageSources}), with its
 * reachable fields classified.
 */
export function publishedSources(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
   includeHidden = false,
): PublishedSource[] {
   const out: PublishedSource[] = [];
   for (const {
      modelPath,
      sourceInfo,
      ir,
      apiSource,
      modelDef,
   } of packageSources(models, isListed, includeHidden)) {
      out.push(
         publishedSourceOf(
            modelPath,
            sourceInfo.name,
            ir,
            modelDef,
            apiSource ? isDeniedToAll(apiSource) : false,
         ),
      );
   }
   return out;
}

/**
 * The classification warnings over a package's published models. Fields of
 * hidden models are only reported where a listed source reaches them through
 * a join, under that dotted path. A source locked with an unconditional deny
 * publishes nothing a caller can read, so its own fields are not demanded a
 * class (its fields reached through a join from an open source are).
 *
 * Coverage is self-declaring: `classification-coverage` fires only once some
 * field of some listed model carries a class, declared or inferred. Malformed
 * annotations are reported whether or not coverage is in force, since each
 * was written on purpose.
 */
export function classificationWarnings(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): ApiPackageWarning[] {
   const sources = publishedSources(models, isListed).filter(
      (s) => !s.deniedToAll,
   );
   const warnings: ApiPackageWarning[] = [];
   // One finding per source, however many of its fields are malformed: the
   // warnings ride every package response and `/status` entry, so their
   // count must not grow with a package's field count.
   for (const s of sources) {
      const malformed = s.fields
         .filter(([, c]) => c.malformed)
         .map(
            ([path, c]) =>
               `Field ${path} of source ${s.source} ${c.malformed}.`,
         );
      if (malformed.length === 0) continue;
      const more = malformed.length - MAX_NAMED_FIELDS;
      warnings.push({
         model: s.modelPath,
         subject: s.source,
         code: CLASSIFICATION_MALFORMED,
         severity: "warn",
         message:
            malformed.length === 1
               ? malformed[0]
               : `Source ${s.source} has ${malformed.length} malformed ` +
                 `#(classification) notes. ` +
                 malformed.slice(0, MAX_NAMED_FIELDS).join(" ") +
                 (more > 0 ? ` (+${more} more)` : ""),
      });
   }
   for (const s of sources) {
      const carries = s.carries;
      for (const [path, c] of [...s.fields, ...s.hiddenRead]) {
         const unknown = declaredUnknownReads(c);
         if (c.dropped.length === 0 && unknown.length === 0) continue;
         // A plain copy under #(carries) is refused at load, not warned.
         if (carries.declared && c.passthrough) continue;
         const reads = [
            ...(c.dropped.length > 0
               ? [`reads fields classified ${c.dropped.join(" ")}`]
               : []),
            ...(unknown.length > 0
               ? [
                    `reads fields whose class cannot be resolved: ${unknown.join(", ")}`,
                 ]
               : []),
         ].join(" and ");
         warnings.push({
            model: s.modelPath,
            subject: s.source,
            code: CLASSIFICATION_DOWNGRADE,
            severity: "warn",
            message:
               `Field ${path} of source ${s.source} declares ` +
               `${c.declared.join(" ")} but ${reads}. Publisher cannot tell ` +
               `whether this derivation de-identifies them, so a review has ` +
               `to: check that what it returns is no longer ` +
               `${c.dropped.length > 0 ? c.dropped.join(" or ") : "what those reads carry"}.`,
         });
      }
   }
   warnings.push(...coverageWarnings(sources));
   return warnings;
}

/** Each model with its notebook cells' prepared queries, the async input of {@link carriesCeilingLeaks}. */
export type PreparedCeilingQueries = ReadonlyArray<{
   modelPath: string;
   model: Model;
   cells: Awaited<ReturnType<Model["preparedCellQueries"]>>;
}>;

/** Prepare every notebook cell's query, which does not depend on the listing. */
export async function prepareCeilingQueries(
   models: ReadonlyMap<string, Model>,
): Promise<PreparedCeilingQueries> {
   const prepared = [];
   for (const [modelPath, model] of models) {
      prepared.push({
         modelPath,
         model,
         cells: await model.preparedCellQueries(),
      });
   }
   return prepared;
}

/**
 * Rejections for every author-declared query (a named query, a notebook cell)
 * that reads past the `#(carries)` of the source it enters, so it refuses at
 * load rather than on its first run. A caller's own queries are checked per
 * request ({@link ceilingRefusal}).
 */
export function carriesCeilingLeaks(
   prepared: PreparedCeilingQueries,
   isListed: (modelPath: string) => boolean,
): string[] {
   const messages: string[] = [];
   for (const { modelPath, model, cells } of prepared) {
      const modelDef = model.getModelDef();
      // A notebook is served whether or not the package lists it, so its cells are always read.
      const queries: Array<{
         label: string;
         query: unknown;
         modelDef: ModelDef | undefined;
      }> = Object.entries(isListed(modelPath) ? (modelDef?.contents ?? {}) : {})
         .filter(([, def]) => (def as { type?: string }).type === "query")
         .map(([name, query]) => ({
            label: `query ${name}`,
            query,
            modelDef,
         }));
      cells.forEach((cell, i) =>
         queries.push({ label: `notebook cell ${i + 1}`, ...cell }),
      );
      for (const { label, query, modelDef: def } of queries) {
         const refusal = ceilingRefusal(def, query);
         if (refusal) messages.push(`${modelPath}: ${label}: ${refusal}.`);
      }
   }
   return messages;
}

/**
 * Every leak in the package: a field reachable from a source with a
 * `#(carries)` ceiling with a class not on the list, or with no class at
 * all, plus a `#(carries)` that names nothing. One message per finding, in
 * the words the author fixes it by. The load refuses the package when there
 * are any ({@link Package.formatClassificationLeaks}), so an entry point never
 * serves what it says it does not carry.
 */
export function classificationLeaks(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
   includeHidden = false,
): string[] {
   return publishedSources(models, isListed, includeHidden).flatMap(
      sourceLeaks,
   );
}
