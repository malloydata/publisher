// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseTag } from "@malloydata/malloy-tag";
import type { CompiledModel } from "../../client";
import { exportedSources } from "../DocumentCreate/exportedSources";
import { CHART_TAGS } from "./chartLine";
import { chartOf, docOf } from "./viewAnnotations";

export { chartOf, docOf };

export { CHART_TAGS };

/**
 * What a package offers a dashboard: the sources, the views on them, and the
 * fields a drill can be declared on.
 *
 * This is what makes a tile expression correct BY CONSTRUCTION. The builder only
 * ever offers `source -> view` pairs that came from here, so it cannot emit a
 * tile that does not resolve — which is the reason validation needs nothing more
 * than the existing compile endpoint.
 *
 * Shaped from the models endpoint as it is, rather than from a new one. The cost
 * is that annotations arrive RAW — `'# bar_chart\n'`, `'#(doc) Revenue by
 * category\n'` — so the small amount of reading below happens here instead of on
 * the server.
 */

export interface CatalogField {
   name: string;
   kind: "dimension" | "measure";
   type?: string;
}

export interface CatalogView {
   name: string;
   /** From a `#(doc)` annotation, which is what a picker should show. */
   description?: string;
   /** `bar_chart`, `line_chart`, `shape_map`, … if the view declares one. */
   chart?: string;
   /** Every output column is an aggregate, which is the only shape `big_value` renders. Absent when the model did not say. */
   aggregateOnly?: boolean;
   /** At least one group-by and exactly two totals: the shape a bare `# combo_chart` draws, bars for the first and a line for the second. Absent when the model did not say. */
   comboShape?: boolean;
}

export interface CatalogSource {
   name: string;
   /** The model that exports it, which its import names. */
   modelPath: string;
   /** Every model that exports it: a whole-file import of any of them carries the source. Absent from a hand-built catalog, where `modelPath` is the only one. */
   exporters?: string[];
   /** Every model that lists it, imported or exported: what a document extending that model can name without an import. */
   visibleIn?: string[];
   description?: string;
   views: CatalogView[];
   /**
    * The givens this source is scoped by, which is also the control row a tile
    * reading it will show — measured equal to the manifest's per-tile
    * `givenNames` for the `source -> view` form.
    */
   givens: string[];
   /** For declaring a `# drill` dimension. */
   fields: CatalogField[];
}

export interface PackageCatalog {
   sources: CatalogSource[];
}

/**
 * How far into a source's joins the field list reaches. One level:
 * `products.category` is a field people filter on; `products.supplier.region`
 * is where a list stops being a list.
 */
const JOIN_DEPTH = 1;

/**
 * The fields of one schema, with a JOIN's fields under it as `join.field`
 * paths — the spelling a `where:` uses for them, and the fields a filter most
 * often wants. A view is not a field and is left out.
 */
function schemaFields(
   fields: Array<Record<string, unknown>> | undefined,
   prefix: string,
   depth: number,
): CatalogField[] {
   const out: CatalogField[] = [];
   for (const field of fields ?? []) {
      const name = field["name"];
      const kind = field["kind"];
      if (typeof name !== "string") continue;
      if (kind === "dimension" || kind === "measure") {
         const type = (field["type"] as { kind?: string } | undefined)?.kind;
         out.push({ name: prefix + name, kind, ...(type ? { type } : {}) });
      } else if (kind === "join" && depth > 0) {
         const nested = (field["schema"] as { fields?: unknown } | undefined)
            ?.fields as Array<Record<string, unknown>> | undefined;
         out.push(...schemaFields(nested, `${prefix}${name}.`, depth - 1));
      }
   }
   return out;
}

/**
 * Per-source fields, from `sourceInfos`, which the endpoint returns as an array
 * of JSON STRINGS rather than objects. A malformed entry is skipped rather than
 * failing the catalog: a picker missing one source's fields is a smaller problem
 * than a builder that will not open.
 */
function fieldsOf(model: CompiledModel): Map<string, CatalogField[]> {
   const byName = new Map<string, CatalogField[]>();
   for (const info of parsedSourceInfos(model))
      byName.set(info.name, schemaFields(info.schema?.fields, "", JOIN_DEPTH));
   return byName;
}

interface ParsedSourceInfo {
   name: string;
   schema?: { fields?: Array<Record<string, unknown>> };
}

/** The well-formed `sourceInfos` entries; a malformed one is skipped. */
function parsedSourceInfos(model: CompiledModel): ParsedSourceInfo[] {
   const infos: ParsedSourceInfo[] = [];
   for (const entry of model.sourceInfos ?? []) {
      try {
         const parsed = (
            typeof entry === "string" ? JSON.parse(entry) : entry
         ) as ParsedSourceInfo | null;
         if (parsed?.name) infos.push(parsed);
      } catch {
         continue;
      }
   }
   return infos;
}

/** An aggregate output column has `calculation` as a top-level property of its `#(malloy)` note; a group-by or nested one does not. */
const isAggregateColumn = (field: Record<string, unknown>) =>
   ((field["annotations"] as Array<{ value?: string }> | undefined) ?? []).some(
      (note) => {
         const text = note.value ?? "";
         return (
            text.startsWith("#(malloy)") &&
            parseTag(text.slice("#(malloy)".length)).tag?.has("calculation") ===
               true
         );
      },
   );

/** Whether a column is a group-by a chart can put on its x axis: not an aggregate, and not a nest. */
const isGroupByColumn = (field: Record<string, unknown>) =>
   !isAggregateColumn(field) &&
   (field["type"] as { kind?: string } | undefined)?.kind !== "array_type";

/** Per source, the views whose every output column is an aggregate, and the views a bare `# combo_chart` can draw. */
function viewShapesOf(model: CompiledModel): {
   aggregateOnly: Map<string, Set<string>>;
   combo: Map<string, Set<string>>;
} {
   const aggregateOnly = new Map<string, Set<string>>();
   const combo = new Map<string, Set<string>>();
   for (const info of parsedSourceInfos(model)) {
      const totalsOnly = new Set<string>();
      const comboViews = new Set<string>();
      for (const field of info.schema?.fields ?? []) {
         if (field["kind"] !== "view" || typeof field["name"] !== "string")
            continue;
         const columns = (field["schema"] as { fields?: unknown } | undefined)
            ?.fields as Array<Record<string, unknown>> | undefined;
         if (!columns || columns.length === 0) continue;
         if (columns.every(isAggregateColumn)) totalsOnly.add(field["name"]);
         if (
            columns.filter(isAggregateColumn).length === 2 &&
            columns.some(isGroupByColumn)
         )
            comboViews.add(field["name"]);
      }
      aggregateOnly.set(info.name, totalsOnly);
      combo.set(info.name, comboViews);
   }
   return { aggregateOnly, combo };
}

/**
 * The fields a filter on `source` may name: its dimensions, joins included as
 * paths. Measures are not filterable with `where:` and are left out. Undefined
 * when the catalog has no such source, which a picker reads as "no list".
 */
export function filterableFields(
   catalog: PackageCatalog | undefined,
   source: string | undefined,
): CatalogField[] | undefined {
   if (!catalog || source === undefined) return undefined;
   const found = catalog.sources.find((s) => s.name === source);
   return found?.fields.filter((field) => field.kind === "dimension");
}

/**
 * A report file is itself a model, so the models endpoint may list it beside
 * the real ones. Offering a report's own tile views as things to put on a
 * report would be circular, so they are left out, by the paths the package's
 * report listing names: where a file lives never says whether it is a report.
 */
export const isReportModel = (
   path: string | undefined,
   reportPaths: ReadonlySet<string>,
) => path !== undefined && reportPaths.has(path);

/**
 * The model's own path: `path`, the field the API declares.
 *
 * A server before this release names a single model's path only as
 * `modelPath`, which the spec never declared, so it is still read when `path`
 * is missing. An empty path silently disables the report exclusion below and
 * leaves a preview with nothing to run against, so `useEditorCatalog` sets
 * `path` itself, from the path it asked for.
 */
const pathOf = (model: CompiledModel): string =>
   model.path || (model as { modelPath?: string }).modelPath || "";

export function buildCatalog(
   models: CompiledModel[],
   /** The package's report paths (`GET …/reports`), left out of the catalog. */
   reportPaths: ReadonlySet<string> = new Set(),
): PackageCatalog {
   const sources: CatalogSource[] = [];
   const seen = new Set<string>();
   const exporters = new Map<string, string[]>();
   const exported = new Map<CompiledModel, Set<string>>();
   const listed = new Map<string, string[]>();
   for (const model of models) {
      const modelPath = pathOf(model);
      if (isReportModel(modelPath, reportPaths)) continue;
      const names = exportedSources(model.modelInfo);
      exported.set(model, names);
      for (const { name } of model.sources ?? [])
         if (name) listed.set(name, [...(listed.get(name) ?? []), modelPath]);
      for (const name of names)
         exporters.set(name, [...(exporters.get(name) ?? []), modelPath]);
   }

   for (const model of models) {
      const modelPath = pathOf(model);
      if (isReportModel(modelPath, reportPaths)) continue;
      const fields = fieldsOf(model);
      const shapes = viewShapesOf(model);

      for (const source of model.sources ?? []) {
         const name = source.name;
         if (!name) continue;
         // `sources` also lists imported names, which an import cannot reach unless the model re-exports them.
         if (!exported.get(model)?.has(name) || seen.has(name)) continue;
         seen.add(name);

         const givens = (
            (source as { givens?: Array<{ name?: string }> }).givens ?? []
         )
            .map((g) => g.name)
            .filter((n): n is string => typeof n === "string");

         sources.push({
            name,
            modelPath,
            exporters: exporters.get(name) ?? [modelPath],
            visibleIn: listed.get(name) ?? [modelPath],
            ...(docOf(source.annotations)
               ? { description: docOf(source.annotations) as string }
               : {}),
            views: (source.views ?? [])
               .filter((view) => typeof view.name === "string")
               .map((view) => ({
                  name: view.name as string,
                  ...(docOf(view.annotations)
                     ? { description: docOf(view.annotations) as string }
                     : {}),
                  ...(chartOf(view.annotations)
                     ? { chart: chartOf(view.annotations) as string }
                     : {}),
                  ...(shapes.aggregateOnly.get(name)?.has(view.name as string)
                     ? { aggregateOnly: true }
                     : {}),
                  ...(shapes.combo.get(name)?.has(view.name as string)
                     ? { comboShape: true }
                     : {}),
               })),
            givens,
            fields: fields.get(name) ?? [],
         });
      }
   }

   return { sources };
}
