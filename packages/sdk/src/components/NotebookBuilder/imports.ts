// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { CompiledModel } from "../../client";
import { buildCatalog, type CatalogSource } from "../DashboardBuilder/catalog";
import { exportedSources } from "../DocumentCreate/exportedSources";
import type { NotebookSource } from "./readNotebookSource";

/** One name an import brings in: the source as its file calls it, and as this notebook does. */
export interface ImportedName {
   from: string;
   as: string;
}

/** One `import` of the notebook, with the package path it names. */
export type NotebookImport =
   | { kind: "all"; path: string }
   | { kind: "names"; names: ImportedName[]; path: string };

const NAMED = /\bimport\s*\{([^}]*)\}\s*from\s*(["'])(.+?)\2/g;
const WHOLE = /\bimport\s*(["'])(.+?)\1/g;

/** The package path `from` names, relative to the notebook; undefined for anything not a path inside the package. */
function resolve(from: string, modelPath: string): string | undefined {
   const dir = modelPath.slice(0, modelPath.lastIndexOf("/") + 1);
   try {
      const url = new URL(from, `https://malloy.invalid/${dir}`);
      if (url.host !== "malloy.invalid") return undefined;
      const segments = url.pathname.slice(1).split("/").map(decodeURIComponent);
      // An encoded `..` or separator would otherwise reach the fetch as a traversal.
      return segments.some((s) => s === "." || s === ".." || /[\\/]/.test(s))
         ? undefined
         : segments.join("/");
   } catch {
      return undefined;
   }
}

/** `x is orders` brings `orders` in as `x`; a bare name brings itself. */
function importedName(item: string): ImportedName | undefined {
   const unquote = (name: string) => name.trim().replace(/^`|`$/g, "");
   const [as, from] = item.split(/\s+is\s+/);
   const brought = { from: unquote(from ?? as), as: unquote(as) };
   return brought.as && brought.from ? brought : undefined;
}

/** The text with comments blanked, newlines kept, and string and back-quoted contents left alone. */
function withoutComments(text: string): string {
   let out = "";
   let i = 0;
   while (i < text.length) {
      const c = text[i];
      const two = text.slice(i, i + 2);
      if (c === '"' || c === "'" || c === "`") {
         const end = text.indexOf(c, i + 1);
         const stop = end < 0 ? text.length : end + 1;
         out += text.slice(i, stop);
         i = stop;
      } else if (two === "/*") {
         const end = text.indexOf("*/", i + 2);
         const stop = end < 0 ? text.length : end + 2;
         out += text.slice(i, stop).replace(/[^\n]/g, " ");
         i = stop;
      } else if (two === "//" || two === "--" || c === "#") {
         while (i < text.length && text[i] !== "\n") i++;
      } else {
         out += c;
         i++;
      }
   }
   return out;
}

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
      const text = withoutComments(
         notebook.text.slice(cell.span.start, cell.span.end),
      );
      for (const m of text.matchAll(NAMED)) {
         const path = resolve(m[3], modelPath);
         const names = m[1]
            .split(",")
            .map(importedName)
            .filter((n): n is ImportedName => n !== undefined);
         if (path) found.push({ kind: "names", names, path });
      }
      for (const m of text.matchAll(WHOLE)) {
         const path = resolve(m[2], modelPath);
         if (path) found.push({ kind: "all", path });
      }
   }
   return found;
}

/** The sources the imports bring in, with their views, from the models those imports name. A whole-file import brings only what the model exports; a model that could not be read brings nothing. */
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
      const exported = exportedSources(model.modelInfo);
      const brought: CatalogSource[] =
         imported.kind === "all"
            ? sources.filter((s) => exported.has(s.name))
            : imported.names.flatMap(({ from, as }) => {
                 const found = sources.find((s) => s.name === from);
                 return found ? [{ ...found, name: as }] : [];
              });
      for (const source of brought)
         if (!out.has(source.name)) out.set(source.name, source);
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
