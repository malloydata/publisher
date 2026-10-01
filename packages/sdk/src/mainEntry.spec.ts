// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import ts from "typescript";

/**
 * The bundle boundary, walked rather than reviewed: `@malloydata/malloy` is
 * ~440 KB gzipped and belongs to `@malloy-publisher/sdk/builder` alone, so no
 * module statically reachable from the main entry may import it, or the
 * builder entry, or anything under the builders. Type-only imports are erased
 * and dynamic `import()` is the point, so neither counts.
 */
const SRC = import.meta.dir;

const EXTENSIONS = [".ts", ".tsx", "/index.ts", "/index.tsx"];

function resolve(from: string, specifier: string): string | undefined {
   const base = path.resolve(path.dirname(from), specifier);
   if (fs.existsSync(base) && fs.statSync(base).isFile()) return base;
   return EXTENSIONS.map((ext) => base + ext).find((file) =>
      fs.existsSync(file),
   );
}

/** The specifiers a module imports or re-exports at runtime. */
function runtimeImports(file: string): string[] {
   const source = ts.createSourceFile(
      file,
      fs.readFileSync(file, "utf8"),
      ts.ScriptTarget.Latest,
      false,
      file.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
   );
   const found: string[] = [];
   for (const statement of source.statements) {
      if (ts.isImportDeclaration(statement)) {
         const clause = statement.importClause;
         const bindings = clause?.namedBindings;
         const onlyTypes =
            clause !== undefined &&
            (clause.isTypeOnly ||
               (clause.name === undefined &&
                  bindings !== undefined &&
                  ts.isNamedImports(bindings) &&
                  bindings.elements.length > 0 &&
                  bindings.elements.every((element) => element.isTypeOnly)));
         if (!onlyTypes)
            found.push((statement.moduleSpecifier as ts.StringLiteral).text);
      } else if (
         ts.isExportDeclaration(statement) &&
         statement.moduleSpecifier !== undefined &&
         !statement.isTypeOnly &&
         !(
            statement.exportClause !== undefined &&
            ts.isNamedExports(statement.exportClause) &&
            statement.exportClause.elements.length > 0 &&
            statement.exportClause.elements.every(
               (element) => element.isTypeOnly,
            )
         )
      )
         found.push((statement.moduleSpecifier as ts.StringLiteral).text);
   }
   return found;
}

function staticGraph(entry: string) {
   const modules = new Set<string>();
   const packages = new Set<string>();
   // A relative import the walk cannot follow is a hole in the guard, so it is reported rather than skipped.
   const unresolved: string[] = [];
   const queue = [entry];
   while (queue.length > 0) {
      const file = queue.pop()!;
      if (modules.has(file)) continue;
      modules.add(file);
      for (const specifier of runtimeImports(file)) {
         if (!specifier.startsWith(".")) {
            packages.add(specifier);
            continue;
         }
         const target = resolve(file, specifier);
         if (target === undefined)
            unresolved.push(`${path.relative(SRC, file)}: ${specifier}`);
         else if (/\.tsx?$/.test(target)) queue.push(target);
      }
   }
   return { modules, packages, unresolved };
}

describe("the main entry", () => {
   const { modules, packages, unresolved } = staticGraph(
      path.join(SRC, "index.ts"),
   );

   it("follows every relative import it meets", () => {
      expect(unresolved).toEqual([]);
   });

   it("walks a real graph", () => {
      expect(modules.size).toBeGreaterThan(50);
      expect(modules.has(path.join(SRC, "components/ServerProvider.tsx"))).toBe(
         true,
      );
   });

   it("never statically imports the Malloy compiler", () => {
      const compiler = [...packages].filter(
         (name) =>
            name === "@malloydata/malloy" ||
            name.startsWith("@malloydata/malloy/"),
      );
      expect(compiler).toEqual([]);
   });

   it("never reaches the builders", () => {
      const builders = [...modules]
         .map((file) => path.relative(SRC, file))
         .filter(
            (file) =>
               file === "builder-entry.ts" ||
               file.startsWith("components/NotebookBuilder/") ||
               file === "components/DashboardBuilder/malloyTree.ts",
         );
      expect(builders).toEqual([]);
   });
});
