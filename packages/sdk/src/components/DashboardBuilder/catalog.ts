// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { CompiledModel } from "../../client";

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
}

export interface CatalogSource {
   name: string;
   /** The model that declares it, which is what a preview runs against. */
   modelPath: string;
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

/** The renderer's chart tags, as they are spelled in a model. */
const CHART_TAGS = [
   "bar_chart",
   "line_chart",
   "scatter_chart",
   "shape_map",
   "segment_map",
   "big_value",
   "sparkline",
];

/** `#(doc) Revenue by product category\n` -> `Revenue by product category`. */
export function docOf(annotations: string[] | undefined): string | undefined {
   for (const raw of annotations ?? []) {
      const m = /^#\(doc\)\s*(.*)$/s.exec(raw.trim());
      if (m) return m[1].trim();
   }
   return undefined;
}

/** `# bar_chart\n` -> `bar_chart`. Only the renderer's own chart tags count. */
export function chartOf(annotations: string[] | undefined): string | undefined {
   for (const raw of annotations ?? []) {
      const name = /^#\s*([a-z_]+)/.exec(raw.trim())?.[1];
      if (name && CHART_TAGS.includes(name)) return name;
   }
   return undefined;
}

/**
 * Per-source fields, from `sourceInfos`, which the endpoint returns as an array
 * of JSON STRINGS rather than objects. A malformed entry is skipped rather than
 * failing the catalog: a picker missing one source's fields is a smaller problem
 * than a builder that will not open.
 */
function fieldsOf(model: CompiledModel): Map<string, CatalogField[]> {
   const byName = new Map<string, CatalogField[]>();
   for (const entry of model.sourceInfos ?? []) {
      let parsed: unknown;
      try {
         parsed = typeof entry === "string" ? JSON.parse(entry) : entry;
      } catch {
         continue;
      }
      const info = parsed as {
         name?: string;
         schema?: { fields?: Array<Record<string, unknown>> };
      };
      if (!info?.name) continue;
      const fields: CatalogField[] = [];
      for (const field of info.schema?.fields ?? []) {
         const name = field["name"];
         const kind = field["kind"];
         if (typeof name !== "string") continue;
         if (kind !== "dimension" && kind !== "measure") continue;
         const type = (field["type"] as { kind?: string } | undefined)?.kind;
         fields.push({ name, kind, ...(type ? { type } : {}) });
      }
      byName.set(info.name, fields);
   }
   return byName;
}

/**
 * A dashboard file is itself a model, so the endpoint lists it alongside the
 * real ones. Offering a dashboard's own tile views as things to put on a
 * dashboard would be circular, so they are left out.
 */
export const isDashboardModel = (path: string | undefined) =>
   (path ?? "").startsWith("dashboards/");

/**
 * The model's own path.
 *
 * Two spellings, and the generated client only knows one: the models LIST
 * returns `path`, while a single model returns `modelPath`. Reading `path`
 * alone yields an empty string against the real server, which silently disables
 * the dashboard exclusion below and leaves a preview with nothing to run
 * against. Measured, not deduced from the type.
 */
const pathOf = (model: CompiledModel): string =>
   (model as { modelPath?: string }).modelPath ?? model.path ?? "";

export function buildCatalog(models: CompiledModel[]): PackageCatalog {
   const sources: CatalogSource[] = [];
   const seen = new Set<string>();

   for (const model of models) {
      const modelPath = pathOf(model);
      if (isDashboardModel(modelPath)) continue;
      const fields = fieldsOf(model);

      for (const source of model.sources ?? []) {
         const name = source.name;
         if (!name) continue;
         // A source reached through an import appears in every model that
         // imports it. The first model to declare it wins, so a preview runs
         // against the file that actually defines it.
         if (seen.has(name)) continue;
         seen.add(name);

         const givens = (
            (source as { givens?: Array<{ name?: string }> }).givens ?? []
         )
            .map((g) => g.name)
            .filter((n): n is string => typeof n === "string");

         sources.push({
            name,
            modelPath,
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
               })),
            givens,
            fields: fields.get(name) ?? [],
         });
      }
   }

   return { sources };
}

/**
 * What a tile depends on from the catalog, which is narrower than "the view
 * exists" and is the whole point of asking.
 *
 * A tile declared in the dashboard file does not need its own name in the
 * catalog — `overview -> kpis` is declared right there. What it needs is
 * whatever it was built FROM: a `reference` tile needs its base view to still
 * exist on the source its extension is built on; an `inline` tile is
 * self-contained and needs nothing; an `inherited` tile needs the view itself,
 * because that is the one case where the view really does live on the source.
 *
 * Reading this wrong reports every healthy tile as missing, which is exactly
 * what a first run against the real package did.
 */
export interface TileDependency {
   /** The source the tile names. May be an extension, or a catalog source. */
   source: string;
   name: string;
   declaration: { kind: "reference"; from: string } | { kind: string };
}

/**
 * Whether every tile in a document still resolves against the catalog.
 *
 * The staleness check. A draft can sit in storage while the model reloads and a
 * view disappears; nothing server-side will say so, because the draft is not a
 * package file. Run this on open and report what went missing rather than
 * showing a tile that cannot run.
 */
export function missingTiles(
   catalog: PackageCatalog,
   tiles: TileDependency[],
   declaredSources: Array<{ name: string; base: string }>,
): Array<{ source: string; name: string; needs: string }> {
   const byName = new Map(catalog.sources.map((s) => [s.name, s]));
   const baseOf = new Map(declaredSources.map((s) => [s.name, s.base]));
   const missing: Array<{ source: string; name: string; needs: string }> = [];

   for (const tile of tiles) {
      if (tile.declaration.kind === "inline") continue;

      // An extension resolves to whatever it is built on; a tile naming a
      // catalog source directly resolves to itself.
      const lookup = baseOf.get(tile.source) ?? tile.source;
      const source = byName.get(lookup);
      if (source === undefined) {
         missing.push({ source: tile.source, name: tile.name, needs: lookup });
         continue;
      }

      const needs =
         tile.declaration.kind === "reference"
            ? (tile.declaration as { from: string }).from
            : tile.name;
      if (!source.views.some((v) => v.name === needs))
         missing.push({ source: tile.source, name: tile.name, needs });
   }
   return missing;
}
