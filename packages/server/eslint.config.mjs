// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fixupConfigRules, fixupPluginRules } from "@eslint/compat";
import { FlatCompat } from "@eslint/eslintrc";
import js from "@eslint/js";
import typescriptEslint from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";
import globals from "globals";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
   baseDirectory: __dirname,
   recommendedConfig: js.configs.recommended,
   allConfig: js.configs.all,
});

// `@malloydata/malloy-interfaces` writes a string value into Malloy text
// unescaped: its `escapeString` is a TODO, so a value holding a double quote
// closes the literal and the rest is read as Malloy. `ASTQuery.toMalloy()` and
// `queryToMalloy` both go through it, and the Explorer was built on them, so a
// drilled `27" Monitor` was a compile error and a crafted value widened the
// filter it was drilled into. `filterToMalloy` goes through the same function,
// and `@malloydata/malloy-query-builder`'s `ASTQuery` writes through them, so
// nothing here may load that package. The two writers are refused where they
// are imported, aliased or re-exported, and where they are called; the SDK
// reads `@malloydata/malloy-interfaces` for its types alone, through `import
// type`. Lift these once a `@malloydata/*` bump carries the upstream fix.
const UNESCAPED_MALLOY_MESSAGE =
   "writes string values into Malloy unescaped (escapeString in @malloydata/malloy-interfaces is a TODO), so a value holding a double quote rewrites the query. Run a named query, or write the text with malloyLiteral / escapeMalloyString.";
const UNESCAPED_MALLOY_IMPORTS = [
   {
      group: ["@malloydata/malloy-explorer", "@malloydata/malloy-explorer/**"],
      message: `The Explorer ${UNESCAPED_MALLOY_MESSAGE}`,
   },
   {
      group: [
         "@malloydata/malloy-query-builder",
         "@malloydata/malloy-query-builder/**",
      ],
      message: `The query builder's ASTQuery ${UNESCAPED_MALLOY_MESSAGE}`,
   },
];
// typescript-eslint's rule, so a type-only namespace import (`import type * as
// Malloy`) stays allowed: the core rule refuses every namespace import of a
// module with a restricted name in it.
const UNESCAPED_MALLOY_IMPORT_NAMES = [
   "error",
   {
      paths: [
         {
            name: "@malloydata/malloy-interfaces",
            importNames: ["queryToMalloy", "filterToMalloy"],
            allowTypeImports: true,
            message: `queryToMalloy or filterToMalloy ${UNESCAPED_MALLOY_MESSAGE}`,
         },
      ],
   },
];
const UNESCAPED_MALLOY_CALLS = [
   "error",
   {
      selector:
         "CallExpression[callee.property.name=/^(toMalloy|queryToMalloy|filterToMalloy)$/]",
      message: `ASTQuery.toMalloy(), queryToMalloy or filterToMalloy ${UNESCAPED_MALLOY_MESSAGE}`,
   },
   {
      selector:
         "CallExpression[callee.name=/^(queryToMalloy|filterToMalloy)$/]",
      message: `queryToMalloy or filterToMalloy ${UNESCAPED_MALLOY_MESSAGE}`,
   },
   {
      // The Explorer was loaded lazily, which no-restricted-imports does not see.
      // esquery ends a regex at its first slash, escaped or not.
      selector:
         "ImportExpression[source.value=/malloy-explorer|malloy-query-builder/]",
      message: `The Explorer or the query builder ${UNESCAPED_MALLOY_MESSAGE}`,
   },
];

export default [
   {
      ignores: [
         "**/node_modules/**",
         "**/dist/**",
         "**/build/**",
         "**/.DS_Store",
         "**/__pycache__/**",
         "**/*.py[cod]",
         "**/*.egg-info/**",
         "**/.env",
         "**/*.log",
         "packages/sdk/src/client/**",
      ],
   },
   ...fixupConfigRules(
      compat.extends(
         "eslint:recommended",
         "plugin:react/recommended",
         "plugin:@typescript-eslint/recommended",
         "plugin:@typescript-eslint/eslint-recommended",
         "prettier",
         "plugin:prettier/recommended",
         "plugin:react-hooks/recommended",
         "plugin:storybook/recommended",
      ),
   ),
   {
      plugins: {
         "@typescript-eslint": fixupPluginRules(typescriptEslint),
      },

      languageOptions: {
         globals: {
            ...globals.jest,
            ...globals.browser,
         },

         parser: tsParser,
      },

      settings: {
         react: {
            version: "detect",
         },
      },

      rules: {
         "react/react-in-jsx-scope": "off",
         "@typescript-eslint/no-unused-vars": [
            "error",
            {
               argsIgnorePattern: "^_",
               varsIgnorePattern: "^_",
               caughtErrorsIgnorePattern: "^_",
            },
         ],
      },
   },
   {
      files: [
         "packages/sdk/src/**/*.{ts,tsx}",
         "packages/app/src/**/*.{ts,tsx}",
      ],
      rules: {
         "no-restricted-imports": [
            "error",
            { patterns: UNESCAPED_MALLOY_IMPORTS },
         ],
         "@typescript-eslint/no-restricted-imports":
            UNESCAPED_MALLOY_IMPORT_NAMES,
         "no-restricted-syntax": UNESCAPED_MALLOY_CALLS,
      },
   },
   {
      // `.malloynb` is deprecated and fenced off: its renderer lives in
      // `legacy/malloynb/` and nothing outside `legacy/` may import it, so the
      // format is deleted by deleting that folder and the legacy exports.
      // Restates the unescaped-Malloy imports: a later block's rule replaces an
      // earlier one's for the files both match, rather than adding to it.
      files: ["packages/sdk/src/**/*.{ts,tsx}"],
      ignores: ["packages/sdk/src/components/legacy/**"],
      rules: {
         "no-restricted-imports": [
            "error",
            {
               patterns: [
                  {
                     group: ["**/legacy/malloynb", "**/legacy/malloynb/**"],
                     message:
                        "legacy/malloynb is the deprecated .malloynb renderer: only legacy/ may import it. A .malloy report is tiles, drawn by DashboardView.",
                  },
                  ...UNESCAPED_MALLOY_IMPORTS,
               ],
            },
         ],
      },
   },
];
