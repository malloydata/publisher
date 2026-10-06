// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardDocument, DashboardImport } from "./document";

/** Where a document sits when the caller does not say: one folder below the package root. */
const DEFAULT_DOCUMENT_PATH = "dashboards/document.malloy";

/** The folder a package path sits in, with its trailing slash, or "" at the root. */
const folderOf = (path: string) => path.slice(0, path.lastIndexOf("/") + 1);

/**
 * The package path an import names, read from where the document sits, so
 * `"../m.malloy"` and `"./../m.malloy"` are the same model: comparing the text
 * alone would miss the second, and importing a source the file already
 * reaches declares it twice.
 */
const resolvedOf = (from: string, documentPath: string) =>
   new URL(
      from,
      `https://malloy.invalid/${folderOf(documentPath)}`,
   ).pathname.slice(1);

/** How the document at `documentPath` names the model at `modelPath`. */
export function importPathOf(modelPath: string, documentPath: string): string {
   const from = folderOf(documentPath).split("/").filter(Boolean);
   const to = modelPath.split("/");
   let shared = 0;
   while (
      shared < from.length &&
      shared < to.length - 1 &&
      from[shared] === to[shared]
   )
      shared++;
   const up = from.length - shared;
   return `${up === 0 ? "./" : "../".repeat(up)}${to.slice(shared).join("/")}`;
}

/**
 * Whether the file can already see `name` from `modelPath`: it extends it,
 * imports it by name, or imports that model whole. A whole-file import brings
 * every source the model exports, and naming one of them again would declare
 * it twice. A source the file sees only through another model that re-exports
 * it is not followed: that needs the catalog to say which model declares each
 * source, and the compile check on save catches the duplicate.
 */
export function reaches(
   document: Pick<DashboardDocument, "imports" | "sources">,
   name: string,
   modelPath: string,
   documentPath: string = DEFAULT_DOCUMENT_PATH,
): boolean {
   if (document.sources.some((s) => s.base === name)) return true;
   return document.imports.some((i) =>
      i.kind === "names"
         ? i.names.includes(name)
         : resolvedOf(i.from, documentPath) === modelPath,
   );
}

/**
 * `imports` with `name` from `modelPath` brought in, by name — into that
 * model's existing `{ … }` import when there is one, else as a new statement
 * written relative to where the document sits. Unchanged when the file can
 * already see it.
 */
export function withSource(
   document: Pick<DashboardDocument, "imports" | "sources">,
   name: string,
   modelPath: string,
   documentPath: string = DEFAULT_DOCUMENT_PATH,
): DashboardImport[] {
   const { imports } = document;
   if (reaches(document, name, modelPath, documentPath)) return imports;
   const at = imports.findIndex(
      (i) =>
         i.kind === "names" && resolvedOf(i.from, documentPath) === modelPath,
   );
   if (at < 0)
      return [
         ...imports,
         {
            kind: "names",
            names: [name],
            from: importPathOf(modelPath, documentPath),
         },
      ];
   return imports.map((i, index) =>
      index === at && i.kind === "names"
         ? { ...i, names: [...i.names, name] }
         : i,
   );
}
