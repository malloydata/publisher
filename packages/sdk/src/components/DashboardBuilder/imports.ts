// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardDocument, DashboardImport } from "./document";

/** Where a model is imported from: documents sit one folder below the package root. */
const importPathOf = (modelPath: string) => `../${modelPath}`;

/**
 * The package path an import names, so `"../m.malloy"` and `"./../m.malloy"`
 * are the same model: comparing the text alone would miss the second, and
 * importing a source the file already reaches declares it twice.
 */
const resolvedOf = (from: string) =>
   new URL(from, "https://malloy.invalid/documents/").pathname.slice(1);

/**
 * Whether the file can already see `name` from `modelPath`: it extends it,
 * imports it by name, or imports that model whole. A whole-file import brings
 * every source the model exports, and naming one of them again would declare
 * it twice.
 */
export function reaches(
   document: Pick<DashboardDocument, "imports" | "sources">,
   name: string,
   modelPath: string,
): boolean {
   if (document.sources.some((s) => s.base === name)) return true;
   return document.imports.some((i) =>
      i.kind === "names"
         ? i.names.includes(name)
         : resolvedOf(i.from) === modelPath,
   );
}

/**
 * `imports` with `name` from `modelPath` brought in, by name — into that
 * model's existing `{ … }` import when there is one, else as a new statement.
 * Unchanged when the file can already see it.
 */
export function withSource(
   document: Pick<DashboardDocument, "imports" | "sources">,
   name: string,
   modelPath: string,
): DashboardImport[] {
   const { imports } = document;
   if (reaches(document, name, modelPath)) return imports;
   const from = importPathOf(modelPath);
   const at = imports.findIndex(
      (i) => i.kind === "names" && resolvedOf(i.from) === modelPath,
   );
   if (at < 0) return [...imports, { kind: "names", names: [name], from }];
   return imports.map((i, index) =>
      index === at && i.kind === "names"
         ? { ...i, names: [...i.names, name] }
         : i,
   );
}
