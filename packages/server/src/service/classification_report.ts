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
   byCodepoint,
   CLASSIFICATION_COVERAGE,
   CLASSIFICATION_DOWNGRADE,
   CLASSIFICATION_MALFORMED,
   ceilingRefusal,
   findIrSource,
   isDeniedToAll,
   readSourceClassifications,
   readViewExposures,
   reachesCarries,
   strictestCarries,
   structCeilings,
   type ApiPackageWarning,
   type FieldClassification,
   type IrField,
   type ViewExposure,
} from "./classification";
import {
   MAX_JOIN_PATHS,
   MAX_JOIN_RECURSION_DEPTH,
} from "./filter_binding_guard";
import type { Model } from "./model";

/** How many unclassified field paths one warning names before counting the rest. */
const MAX_NAMED_FIELDS = 12;

/** One published source and what the lineage found on it. */
export interface PublishedSource {
   modelPath: string;
   source: string;
   ir: IrField;
   /** The strictest `#(carries)` the source is held to: its own and its query's base's. */
   carries: { declared: boolean; classes: Set<string>; malformed?: string };
   deniedToAll: boolean;
   /** Reachable dimensions and measures a caller can read, by path. */
   fields: Array<[string, FieldClassification]>;
   /** Hidden dimensions and measures a view or a reachable field reads, directly or through other fields, by path. */
   hiddenRead: Array<[string, FieldClassification]>;
   /** What each of its views reads from it. */
   views: ViewExposure[];
   /** True when its join walk hit a bound, so fields past it were not read. */
   truncated: boolean;
}

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
      const lineage = readSourceClassifications(ir.fields, ir.query);
      const views = readViewExposures(ir.fields, lineage);
      const fields = Array.from(lineage).filter(
         ([, c]) => c.isField && !c.hidden,
      );
      const hiddenRead: Array<[string, FieldClassification]> = [];
      const visited = new Set<string>();
      const visit = (path: string): void => {
         if (visited.has(path)) return;
         visited.add(path);
         const c = lineage.get(path);
         if (!c) return;
         if (c.hidden && c.isField) hiddenRead.push([path, c]);
         for (const input of c.inputs) visit(input.field);
      };
      for (const v of views) for (const r of v.reads) visit(r.field);
      for (const [, c] of fields) for (const i of c.inputs) visit(i.field);
      const chain = structCeilings(modelDef, ir, sourceInfo.name);
      out.push({
         modelPath,
         source: sourceInfo.name,
         ir,
         carries: strictestCarries(
            chain.ceilings,
            chain.unresolved,
            ir,
            modelDef,
         ),
         deniedToAll: apiSource ? isDeniedToAll(apiSource) : false,
         fields,
         hiddenRead,
         views,
         truncated: lineage.truncated,
      });
   }
   return out;
}

/** The unresolved reads of a field that declares its own class, which the declaration cannot vouch for. */
function declaredUnknownReads(c: FieldClassification): string[] {
   return c.declared.length > 0 ? c.unresolved : [];
}

/** The refusal for a plain copy declared below, or over, what it copies, or for an output whose reads are unknown; undefined for any other field. */
function copyLeak(
   where: string,
   allowed: string,
   path: string,
   c: FieldClassification,
): string | undefined {
   // Its declaration cannot be the fix when what it reads is unknown.
   if (c.unread) {
      return (
         `${where} carries [${allowed}] but exposes ${path}, an output ` +
         `Publisher cannot follow to what it reads, so its class is ` +
         `unknown: a nest with stages of its own is one. Flatten the nest ` +
         `into one stage, or move it out of the source.`
      );
   }
   if (!c.passthrough) return undefined;
   const unknown = declaredUnknownReads(c);
   if (c.dropped.length === 0 && unknown.length === 0) return undefined;
   const copied = [
      ...(c.dropped.length > 0
         ? [
              c.inputs
                 .filter((i) =>
                    i.classifications.some((k) => c.dropped.includes(k)),
                 )
                 .map((i) => `${i.field} (${i.classifications.join(" ")})`)
                 .join(", "),
           ]
         : []),
      ...(unknown.length > 0
         ? [`${unknown.join(", ")}, which cannot be resolved`]
         : []),
   ].join(" and ");
   return (
      `${where} carries [${allowed}] but exposes ${path}, declared ` +
      `${c.declared.join(" ")} over a plain copy of ${copied}. A ` +
      `copy does not de-identify anything: remove the declaration ` +
      `so it keeps its input's class, or derive a new field with a ` +
      `function that does.`
   );
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
   const inForce = sources.some((s) =>
      s.fields.some(([, c]) => c.classifications.length > 0),
   );
   if (!inForce) return warnings;
   for (const s of sources) {
      const unclassified = s.fields
         .filter(([, c]) => c.classifications.length === 0)
         .map(([path]) => path);
      if (unclassified.length === 0) continue;
      const named = unclassified.slice(0, MAX_NAMED_FIELDS).join(", ");
      const more = unclassified.length - MAX_NAMED_FIELDS;
      warnings.push({
         model: s.modelPath,
         subject: s.source,
         code: CLASSIFICATION_COVERAGE,
         severity: "warn",
         message:
            `Source ${s.source} publishes ${unclassified.length} ` +
            `${unclassified.length === 1 ? "field" : "fields"} with no ` +
            `#(classification): ${named}${more > 0 ? ` (+${more} more)` : ""}. ` +
            `This package classifies its fields, so every dimension and ` +
            `measure a caller can read needs a class, or the policy has ` +
            `nothing to say about it. Write #(classification) "<class>" above ` +
            `each; a field reached through a join, or derived from a ` +
            `classified field, is classified where that field is declared.`,
      });
   }
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
   const messages: string[] = [];
   for (const s of publishedSources(models, isListed, includeHidden)) {
      const carries = s.carries;
      const where = `source ${s.source} (${s.modelPath})`;
      // A read through a carried join is held to that join's list even when the source carries none.
      for (const [path, c] of s.fields) {
         if (
            c.ceilingUnresolved &&
            reachesCarries([s.ir], models.get(s.modelPath)?.getModelDef())
         ) {
            messages.push(
               `${where} exposes ${path}, which reads through a source whose #(carries) Publisher cannot read, so what holds it is unknown.`,
            );
         }
         // The entry's own #(carries), when it declares one, refuses the copy below.
         const copy =
            c.ceilings.length > 0 && !carries.declared
               ? copyLeak(
                    `${where} joins ${c.ceilings[0].source}, which`,
                    c.ceilings[0].classes.join(", "),
                    path,
                    c,
                 )
               : undefined;
         if (copy) messages.push(copy);
         for (const ceiling of c.ceilings) {
            const off = c.classifications.filter(
               (k) => !ceiling.classes.includes(k),
            );
            if (c.classifications.length > 0 && off.length === 0) continue;
            messages.push(
               `${where} exposes ${path}, ${
                  off.length > 0
                     ? `classified ${off.join(" ")}`
                     : "which has no class"
               }, through ${ceiling.source}, which carries [${ceiling.classes.join(", ")}].`,
            );
         }
      }
      if (!carries.declared) continue;
      if (carries.malformed) {
         messages.push(`${where} ${carries.malformed}.`);
         continue;
      }
      const allowed = Array.from(carries.classes).sort(byCodepoint).join(", ");
      if (s.truncated) {
         messages.push(
            `${where} carries [${allowed}] but joins more than ` +
               `${MAX_JOIN_RECURSION_DEPTH} deep or past ${MAX_JOIN_PATHS} ` +
               `join paths, beyond which Publisher cannot read what its ` +
               `fields carry. Drop the joins past that, or split the source.`,
         );
      }
      for (const view of s.views) {
         const off = view.reads.filter((r) =>
            r.classifications.some((k) => !carries.classes.has(k)),
         );
         if (off.length > 0) {
            const reads = off
               .map(
                  (r) =>
                     `${r.field}, classified ${r.classifications.join(" ")}`,
               )
               .join("; ");
            messages.push(
               `${where} carries [${allowed}] but its view ${view.name} reads ` +
                  `${reads}. A view returns what it groups by, aggregates and ` +
                  `filters on. Remove the view from the source, or move the ` +
                  `filter into a declared measure of a class the source carries.`,
            );
         }
         if (view.unread) {
            messages.push(
               `${where} carries [${allowed}] but its view ${view.name} has no ` +
                  `reference summary, so what it reads cannot be known. ` +
                  `Remove the view from the source.`,
            );
         }
         if (view.unclassified.length > 0) {
            messages.push(
               `${where} carries [${allowed}] but its view ${view.name} reads ` +
                  `${view.unclassified.join(", ")}, which has no class or ` +
                  `cannot be resolved against the source (a field the view ` +
                  `declares with extend: is one). Declare the field on the ` +
                  `source with a #(classification) and read it from there.`,
            );
         }
      }
      // An undeclared field already carries its filter's classes in its own.
      const filterLeak = (c: FieldClassification): void => {
         const filtered = c.filtered.filter((i) =>
            i.classifications.some((k) => !carries.classes.has(k)),
         );
         if (c.declared.length > 0 && filtered.length > 0) {
            const filters =
               `${where} carries [${allowed}] but its query filters on ` +
               `${filtered.map((i) => `${i.field} (${i.classifications.join(" ")})`).join(", ")}, ` +
               `which every row it returns reveals. Remove the filter, or ` +
               `add the class to #(carries) if the policy allows it.`;
            if (!messages.includes(filters)) messages.push(filters);
         }
      };
      for (const [path, c] of s.hiddenRead) {
         const copy = copyLeak(where, allowed, path, c);
         if (copy) messages.push(copy);
         else filterLeak(c);
      }
      for (const [path, c] of s.fields) {
         const copy = copyLeak(where, allowed, path, c);
         if (copy) {
            messages.push(copy);
            continue;
         }
         filterLeak(c);
         const off = c.classifications.filter((k) => !carries.classes.has(k));
         if (c.classifications.length > 0 && off.length === 0) {
            if (c.unresolved.length > 0 && c.declared.length === 0) {
               messages.push(
                  `${where} carries [${allowed}] but exposes ${path}, which ` +
                     `reads ${c.unresolved.join(", ")}, which cannot be ` +
                     `resolved against the source, so its class is unknown. ` +
                     `Declare a #(classification) on ${path}.`,
               );
            }
            continue;
         }
         if (c.classifications.length > 0) {
            const via =
               c.inferred.length > 0 && c.inputs.length > 0
                  ? `, inferred from ${c.inputs
                       .map(
                          (i) => `${i.field} (${i.classifications.join(" ")})`,
                       )
                       .join(", ")}`
                  : "";
            messages.push(
               `${where} carries [${allowed}] but exposes ${path}, classified ` +
                  `${c.classifications.join(" ")}${via}. Remove the field ` +
                  `from the source, derive one of a class the source carries ` +
                  `and declare it, or add the class to #(carries) if the ` +
                  `policy allows it.`,
            );
         } else {
            const why = c.malformed
               ? ` (it ${c.malformed})`
               : c.unresolved.length > 0
                 ? ` (it reads ${c.unresolved.join(", ")}, which cannot be resolved against the source)`
                 : " (it declares none and reads no classified field)";
            messages.push(
               `${where} carries [${allowed}] but exposes ${path}, which has ` +
                  `no class${why}. Every field reachable from a source with ` +
                  `#(carries) needs a #(classification) on the list.`,
            );
         }
      }
   }
   return messages;
}
