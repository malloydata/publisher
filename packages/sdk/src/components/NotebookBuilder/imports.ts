// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { CompiledModel } from "../../client";
import { buildCatalog, type CatalogSource } from "../DashboardBuilder/catalog";
import type { NotebookSource } from "./readNotebookSource";

/** One `import` of the notebook, with the package path it names. */
export type NotebookImport =
   | { kind: "all"; path: string }
   | { kind: "names"; names: string[]; path: string };

const NAMED = /\bimport\s*\{([^}]*)\}\s*from\s*(["'])(.+?)\2/g;
const WHOLE = /\bimport\s*(["'])(.+?)\1/g;

/** The package path `from` names, relative to the notebook; undefined for anything not a path inside the package. */
function resolve(from: string, modelPath: string): string | undefined {
   const dir = modelPath.slice(0, modelPath.lastIndexOf("/") + 1);
   try {
      const url = new URL(from, `https://malloy.invalid/${dir}`);
      return url.host === "malloy.invalid" ? url.pathname.slice(1) : undefined;
   } catch {
      return undefined;
   }
}

/** The name an import item brings in: `a is b` brings `a`. */
const broughtName = (item: string) =>
   item
      .split(/\s+is\s+/)[0]
      .trim()
      .replace(/^`|`$/g, "");

/**
 * The imports a notebook's text declares, read from its definition cells.
 * A curated package's compiled model leaves imported sources out of its own
 * `sources`, so this is the only place the notebook says what it brings in.
 */
export function notebookImports(
   notebook: NotebookSource,
   modelPath: string,
): NotebookImport[] {
   const found: NotebookImport[] = [];
   for (const cell of notebook.cells) {
      if (cell.kind !== "definition") continue;
      const text = notebook.text
         .slice(cell.span.start, cell.span.end)
         .split(/\r?\n/)
         .filter((line) => !/^\s*(\/\/|--|#)/.test(line))
         .join("\n");
      for (const m of text.matchAll(NAMED)) {
         const path = resolve(m[3], modelPath);
         const names = m[1].split(",").map(broughtName).filter(Boolean);
         if (path) found.push({ kind: "names", names, path });
      }
      for (const m of text.matchAll(WHOLE)) {
         const path = resolve(m[2], modelPath);
         if (path) found.push({ kind: "all", path });
      }
   }
   return found;
}

/** The sources the imports bring in, with their views, from the models those imports name. A model that could not be read brings nothing. */
export function importedCatalog(
   imports: readonly NotebookImport[],
   models: ReadonlyMap<string, CompiledModel>,
): CatalogSource[] {
   const out = new Map<string, CatalogSource>();
   for (const imported of imports) {
      const model = models.get(imported.path);
      if (!model) continue;
      const { sources } = buildCatalog([
         { ...model, modelPath: imported.path } as CompiledModel,
      ]);
      for (const source of sources)
         if (
            !out.has(source.name) &&
            (imported.kind === "all" || imported.names.includes(source.name))
         )
            out.set(source.name, source);
   }
   return [...out.values()];
}

/** The notebook's own sources, then any import brought in that it does not already list. */
export function mergeSources(
   declared: readonly CatalogSource[],
   imported: readonly CatalogSource[] | undefined,
): CatalogSource[] {
   const names = new Set(declared.map((source) => source.name));
   return [
      ...declared,
      ...(imported ?? []).filter((source) => !names.has(source.name)),
   ];
}
