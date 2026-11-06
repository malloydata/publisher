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
// `queryToMalloy` both go through it, and the Explorer is built on them, so a
// drilled `27" Monitor` is a compile error and a crafted value widens the
// filter it is drilled into. `filterToMalloy` goes through the same function,
// and `@malloydata/malloy-query-builder`'s `ASTQuery` writes through them, so
// nothing new may load that package. The two writers are refused where they
// are imported, aliased or re-exported, and where they are called; the SDK
// reads `@malloydata/malloy-interfaces` for its types alone, through `import
// type`. The Explorer's own files are exempt by name (EXPLORER_FILES, below)
// while `PUBLISHER_MODEL_PAGE=explorer` still shows it. Lift these once a
// `@malloydata/*` bump carries the upstream fix.
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
      // Any reach for the member, called or not: `q.toMalloy()`,
      // `q.toMalloy.call(q)`, `const f = q.toMalloy`, `q["toMalloy"]()`, and
      // `const { toMalloy } = q`.
      selector:
         "MemberExpression[property.name=/^(toMalloy|queryToMalloy|filterToMalloy)$/], MemberExpression[computed=true][property.value=/^(toMalloy|queryToMalloy|filterToMalloy)$/], ObjectPattern > Property[key.name=/^(toMalloy|queryToMalloy|filterToMalloy)$/]",
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
// The Explorer itself. A server shows it unless `PUBLISHER_MODEL_PAGE=viewer`,
// and the Console keeps both pages, so it is exempt from every rule above.
// Named one by one, so a new file that reaches for the Explorer is still
// refused. The query builder's loader, `loadQueryBuilder` in the shared
// `loadMalloy.ts`, disables the one rule its dynamic import needs, inline.
const EXPLORER_FILES = [
   "packages/sdk/src/components/Model/SourcesExplorer.tsx",
];
// The SDK's own Explorer modules, and the query builder's loader. Only a page
// that has checked `useServer().modelPage` may show the Explorer, so only
// these files may import them: `Model` (through `ExplorerPage`), the legacy
// notebook's "Data sources" (`NotebookCell`), the Explorer's own chain, and
// the barrels that export it. A type import is fine.
const EXPLORER_MODULE_MESSAGE =
   "is the Explorer, which a server with PUBLISHER_MODEL_PAGE=viewer does not show: only a page that checks useServer().modelPage may import it. Add the file to EXPLORER_CALLERS in eslint.config.mjs once it does.";
const EXPLORER_MODULE_IMPORTS = [
   {
      group: [
         "**/SourcesExplorer",
         "**/ModelExplorer",
         "**/ModelExplorerDialog",
         "**/ExplorerPage",
      ],
      allowTypeImports: true,
      message: `This module ${EXPLORER_MODULE_MESSAGE}`,
   },
   {
      group: ["**/Model", "**/components"],
      importNames: ["ModelExplorer", "ModelExplorerDialog", "SourcesExplorer"],
      allowTypeImports: true,
      message: `ModelExplorer, ModelExplorerDialog and SourcesExplorer: each ${EXPLORER_MODULE_MESSAGE}`,
   },
   {
      group: ["**/loadMalloy"],
      importNames: ["loadQueryBuilder"],
      message: `loadQueryBuilder loads the Explorer's query builder, and ${EXPLORER_MODULE_MESSAGE}`,
   },
];
// The Console reaches the same components through the SDK's package name.
const EXPLORER_IMPORT_NAMES = [
   "error",
   {
      paths: [
         ...UNESCAPED_MALLOY_IMPORT_NAMES[1].paths,
         {
            name: "@malloy-publisher/sdk",
            importNames: [
               "ModelExplorer",
               "ModelExplorerDialog",
               "SourcesExplorer",
            ],
            allowTypeImports: true,
            message: `ModelExplorer, ModelExplorerDialog and SourcesExplorer: each ${EXPLORER_MODULE_MESSAGE}`,
         },
      ],
   },
];
const EXPLORER_CALLERS = [
   "packages/sdk/src/index.ts",
   "packages/sdk/src/components/index.ts",
   "packages/sdk/src/components/Model/index.ts",
   "packages/sdk/src/components/Model/Model.tsx",
   "packages/sdk/src/components/Model/ExplorerPage.tsx",
   "packages/sdk/src/components/Model/ModelExplorer.tsx",
   "packages/sdk/src/components/Model/ModelExplorerDialog.tsx",
   "packages/sdk/src/components/legacy/malloynb/NotebookCell.tsx",
   "packages/sdk/src/**/*.spec.{ts,tsx}",
];
// Files that load only the Explorer's stylesheet: that one import is allowed,
// and every other ban, the unescaped writers' among them, still holds.
const EXPLORER_STYLESHEET_FILES = [
   "packages/app/src/App.tsx",
   "packages/sdk/src/components/Model/ExplorerPage.tsx",
   "packages/sdk/src/components/legacy/malloynb/CellRunnerView.tsx",
];
// The bare package goes in `paths`: a negated pattern cannot re-include
// `styles.css` once a pattern matching its parent has excluded it.
const UNESCAPED_MALLOY_IMPORTS_BUT_STYLESHEET = {
   paths: [
      {
         name: "@malloydata/malloy-explorer",
         message: UNESCAPED_MALLOY_IMPORTS[0].message,
      },
   ],
   patterns: [
      {
         ...UNESCAPED_MALLOY_IMPORTS[0],
         group: [
            "@malloydata/malloy-explorer/**",
            "!@malloydata/malloy-explorer/styles.css",
         ],
      },
      UNESCAPED_MALLOY_IMPORTS[1],
   ],
};
const LEGACY_MALLOYNB_IMPORT = {
   group: ["**/legacy/malloynb", "**/legacy/malloynb/**"],
   message:
      "legacy/malloynb is the deprecated .malloynb renderer: only legacy/ may import it. A .malloy report is tiles, drawn by DashboardView.",
};

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
         "@typescript-eslint/no-restricted-imports": EXPLORER_IMPORT_NAMES,
         "no-restricted-syntax": UNESCAPED_MALLOY_CALLS,
      },
   },
   {
      // The SDK's own Explorer modules by path (EXPLORER_MODULE_IMPORTS), in
      // the SDK alone: the Console's `components` folder is its own.
      files: ["packages/sdk/src/**/*.{ts,tsx}"],
      rules: {
         "@typescript-eslint/no-restricted-imports": [
            "error",
            {
               ...EXPLORER_IMPORT_NAMES[1],
               patterns: EXPLORER_MODULE_IMPORTS,
            },
         ],
      },
   },
   {
      // The Explorer's callers (see EXPLORER_CALLERS): the unescaped writers'
      // names stay refused.
      files: EXPLORER_CALLERS,
      rules: {
         "@typescript-eslint/no-restricted-imports":
            UNESCAPED_MALLOY_IMPORT_NAMES,
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
               patterns: [LEGACY_MALLOYNB_IMPORT, ...UNESCAPED_MALLOY_IMPORTS],
            },
         ],
      },
   },
   {
      // The Explorer's own files (see EXPLORER_FILES). Last, so it replaces the
      // blocks above for them; the legacy fence still holds.
      files: EXPLORER_FILES,
      rules: {
         "no-restricted-imports": [
            "error",
            { patterns: [LEGACY_MALLOYNB_IMPORT] },
         ],
         "@typescript-eslint/no-restricted-imports": "off",
         "no-restricted-syntax": "off",
      },
   },
   {
      // The stylesheet's importers (see EXPLORER_STYLESHEET_FILES): only
      // `no-restricted-imports` is replaced, so the name and call bans above
      // still apply to them.
      files: EXPLORER_STYLESHEET_FILES,
      rules: {
         "no-restricted-imports": [
            "error",
            {
               paths: UNESCAPED_MALLOY_IMPORTS_BUT_STYLESHEET.paths,
               patterns: [
                  LEGACY_MALLOYNB_IMPORT,
                  ...UNESCAPED_MALLOY_IMPORTS_BUT_STYLESHEET.patterns,
               ],
            },
         ],
      },
   },
   {
      // Every setting is read through the registry (src/settings), never from
      // process.env, so no read can bypass its names, types and precedence.
      // Specs and test helpers set up processes and are exempt.
      files: ["src/**/*.ts"],
      ignores: [
         "src/settings/**",
         "src/**/*.spec.ts",
         "src/test_helpers/**",
         "src/package_load/test_fixtures/**",
      ],
      rules: {
         "no-restricted-properties": [
            "error",
            {
               object: "process",
               property: "env",
               message:
                  'Read a setting with settings.get("<key>"), a variable Publisher does not own with externalEnv(), or a ${VAR} with referencedVariable(); see src/settings.',
            },
         ],
      },
   },
];
