// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The classification findings of one compiled source: its `#(carries)` leaks
 * and its coverage, read from a compiled model alone. A leaf under
 * ./classification_report.ts, which walks a package's Models, so a caller
 * holding only a compiled model (the served copy) reads the same findings
 * without depending on service/model.ts.
 */

import type { ModelDef } from "@malloydata/malloy";
import {
   byCodepoint,
   CLASSIFICATION_COVERAGE,
   findIrSource,
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

/** How many unclassified field paths one warning names before counting the rest. */
export const MAX_NAMED_FIELDS = 12;

/** One published source and what the lineage found on it. */
export interface PublishedSource {
   modelPath: string;
   source: string;
   ir: IrField;
   /** The compiled model the source was read from. */
   modelDef: ModelDef | undefined;
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

/** One source of a compiled model, with its reachable fields classified. */
export function publishedSourceOf(
   modelPath: string,
   source: string,
   ir: IrField,
   modelDef: ModelDef | undefined,
   deniedToAll: boolean,
): PublishedSource {
   const lineage = readSourceClassifications(ir.fields, ir.query);
   const views = readViewExposures(ir.fields, lineage);
   const fields = Array.from(lineage).filter(([, c]) => c.isField && !c.hidden);
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
   const chain = structCeilings(modelDef, ir, source);
   return {
      modelPath,
      source,
      ir,
      modelDef,
      carries: strictestCarries(chain.ceilings, chain.unresolved, ir, modelDef),
      deniedToAll,
      fields,
      hiddenRead,
      views,
      truncated: lineage.truncated,
   };
}

/**
 * The `#(carries)` leaks and classification-coverage findings of every
 * top-level source one compiled model declares, by source name, read the way
 * a package's load reads its published sources ({@link classificationLeaks},
 * {@link classificationWarnings}). Every source counts, listed or not, and
 * none is taken as denied to all: the caller compares two compiles of one
 * model (the served copy against the compile of record), and only what one
 * adds matters.
 */
export function classificationFindingsBySource(
   modelPath: string,
   modelDef: ModelDef | undefined,
): Map<string, { leaks: string[]; coverage: string[] }> {
   const sources: PublishedSource[] = [];
   for (const [name, def] of Object.entries(modelDef?.contents ?? {})) {
      if ((def as { type?: string }).type === "query") continue;
      const ir = findIrSource(modelDef, name);
      if (!ir) continue;
      sources.push(publishedSourceOf(modelPath, name, ir, modelDef, false));
   }
   const out = new Map<string, { leaks: string[]; coverage: string[] }>();
   for (const s of sources) {
      out.set(s.source, { leaks: sourceLeaks(s), coverage: [] });
   }
   for (const w of coverageWarnings(sources)) {
      if (w.subject) out.get(w.subject)?.coverage.push(w.message ?? "");
   }
   return out;
}

/** The unresolved reads of a field that declares its own class, which the declaration cannot vouch for. */
export function declaredUnknownReads(c: FieldClassification): string[] {
   return c.declared.length > 0 ? c.unresolved : [];
}

/** The refusal for a plain copy declared below, or over, what it copies, or for an output whose reads are unknown; undefined for any other field. */
export function copyLeak(
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
 * The coverage half of {@link classificationWarnings}: once some field of
 * `sources` carries a class, each source's unclassified fields.
 */
export function coverageWarnings(
   sources: readonly PublishedSource[],
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
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

/** {@link classificationLeaks} for one published source. */
export function sourceLeaks(s: PublishedSource): string[] {
   const messages: string[] = [];
   const carries = s.carries;
   const where = `source ${s.source} (${s.modelPath})`;
   // A read through a carried join is held to that join's list even when the source carries none.
   for (const [path, c] of s.fields) {
      if (c.ceilingUnresolved && reachesCarries([s.ir], s.modelDef)) {
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
   if (!carries.declared) return messages;
   if (carries.malformed) {
      messages.push(`${where} ${carries.malformed}.`);
      return messages;
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
            .map((r) => `${r.field}, classified ${r.classifications.join(" ")}`)
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
                    .map((i) => `${i.field} (${i.classifications.join(" ")})`)
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
   return messages;
}
