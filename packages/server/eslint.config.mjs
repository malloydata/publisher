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
import {
   MATERIALIZATION_HEADER_ALLOWLIST,
   MATERIALIZATION_HISTORY_ALLOWLIST,
   MATERIALIZATION_IMPORT_ALLOWLIST,
} from "./materialization_allowlists.mjs";

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
   "packages/sdk/src/components/Report/malloynb/NotebookCell.tsx",
   "packages/sdk/src/**/*.spec.{ts,tsx}",
];
// Files that load only the Explorer's stylesheet: that one import is allowed,
// and every other ban, the unescaped writers' among them, still holds.
const EXPLORER_STYLESHEET_FILES = [
   "packages/app/src/App.tsx",
   "packages/sdk/src/components/Model/ExplorerPage.tsx",
   "packages/sdk/src/components/Report/malloynb/CellRunnerView.tsx",
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

// ---------------------------------------------------------------------------
// Import direction (docs: the architecture refactor's dependency rule).
//
//   R1  service/** (domain) and retrieval/** never import mcp/**,
//       controller/** or routes/** -- not even for a type.
//   R2  mcp/** never imports a VALUE from controller/**: a tool receives the
//       use-case instance it calls, it does not construct one. `import type`
//       (or an import whose every specifier is `type`) is allowed.
//   R3  service/governance/** never imports service/model.ts.
//   R4  service/** never imports retrieval/** -- not even for a type. The
//       domain loads packages; retrieval indexes them. What package loading
//       reads from a package's `retrieval` manifest block lives in
//       service/package_retrieval_settings.ts, which retrieval imports.
//
// A local rule rather than `no-restricted-imports`: the core rule matches the
// specifier text, so `../mcp/x` and `../../mcp/x` need a pattern per directory
// depth, it cannot except one file pair without exempting the whole file, and
// it does not see `import()` or `import("x").T`. eslint-plugin-import (and its
// `no-restricted-paths`) is not a dependency. This rule resolves each relative
// specifier against the importing file, so it judges the module, not the text.
// Specs and test helpers are exempt (see the block that enables it, below).
//
// IMPORT_DIRECTION_ALLOWLIST is today's violations, one entry per file pair
// ("<importer, src-relative, with extension> -> <imported module, src-relative,
// no extension>"), each tagged with the slice of the architecture plan that
// removes it. It must shrink to empty. src/import_direction.spec.ts fails when
// an entry is stale (the import is gone: delete the entry) and when the tree
// has a violation the list does not name (fix the import; do not add it).
// ---------------------------------------------------------------------------
const SERVER_SRC = path.join(__dirname, "src");
const MZ = /^service\/materialization(\/|$)/;
const MZ_DOMAIN = /^service\/materialization\/domain(\/|$)/;
const MZ_TEXT = /^service\/materialization\/text(\/|$)/;
const MZ_PLAN = /^service\/materialization\/plan(\/|$)/;
const MZ_RUN = /^service\/materialization\/run(\/|$)/;
const MZ_SERVE = /^service\/materialization\/serve(\/|$)/;
const MZ_ENTRY = "service/materialization";
const MZ_FACADE = new Set([
   "service/materialization/index.ts",
   "service/materialization/package_materialization.ts",
   "service/materialization/package_warnings.ts",
   "service/materialization/plan_lifecycle.ts",
   "service/materialization/serve_verdicts.ts",
]);
const MZ_SHARED_LEAVES = new Set([
   "service/materialization/host.ts",
   "service/materialization/stored_column_types.ts",
]);
const mzLayerOf = (p) =>
   /^service\/materialization\/(plan|run|serve)(\/|$)/.exec(p)?.[1];
const TEXT_MAY_IMPORT = (m) =>
   MZ_DOMAIN.test(m) ||
   /^service\/governance\/ir(\/|$)/.test(m) ||
   m === "service/malloy_text" ||
   m === "service/overlay_key" ||
   m === "service/annotation_reader" ||
   // The annotation bundle's texts, which a variant's view notes read.
   m === "service/annotations" ||
   // The Compiler port's factory: text/generated_files.ts compiles through it.
   m === "service/model_runtime" ||
   // The one Malloy lexer core, shared with the caller-text reader.
   m === "service/malloy_lexer_core";
const IMPORT_DIRECTION_RULES = [
   {
      id: "R1",
      from: (f) => /^(service|retrieval)\//.test(f),
      to: (m) => /^(mcp|controller|routes)(\/|$)/.test(m),
      allowTypeImports: false,
      message:
         "the domain (service/**, retrieval/**) must not depend on an adapter (mcp/**, controller/**, routes/**)",
   },
   {
      id: "R2",
      from: (f) => /^mcp\//.test(f),
      to: (m) => /^controller(\/|$)/.test(m),
      allowTypeImports: true,
      message:
         "an MCP tool must not construct a controller: take the use-case instance from the composition root (initializeMcpServer) and import only its type",
   },
   {
      id: "R3",
      from: (f) => /^service\/governance\//.test(f),
      to: (m) => m === "service/model",
      allowTypeImports: false,
      message: "governance/** must not depend on service/model.ts",
   },
   {
      id: "R4",
      from: (f) => /^service\//.test(f),
      to: (m) => /^retrieval(\/|$)/.test(m),
      allowTypeImports: false,
      message:
         "the domain (service/**) must not depend on the retrieval module (retrieval/**); retrieval depends on the domain",
   },
   // Materialization's layers (service/materialization/README.md). `domain/`
   // and `text/` are pure leaves; `plan/`, `run/` and `serve/` are siblings
   // over them that never import one another; only the facade composes them,
   // and the rest of the server enters through `index.ts` or `domain/`.
   {
      id: "R5",
      from: (f) => MZ_DOMAIN.test(f),
      to: (m) => !MZ_DOMAIN.test(m) && m !== "api",
      allowTypeImports: false,
      message:
         "materialization/domain/ is the shared vocabulary: it imports only itself and api.ts types",
   },
   {
      id: "R6",
      from: (f) => MZ_TEXT.test(f),
      to: (m) => !MZ_TEXT.test(m) && !TEXT_MAY_IMPORT(m),
      allowTypeImports: false,
      message:
         "materialization/text/ imports only domain/, governance/ir, malloy_text.ts, overlay_key.ts, annotation_reader.ts, annotations.ts, model_runtime.ts (the Compiler port's factory) and malloy_lexer_core.ts",
   },
   {
      id: "R7",
      from: (f) => MZ_PLAN.test(f),
      to: (m) => MZ_RUN.test(m) || MZ_SERVE.test(m) || m === "service/model",
      allowTypeImports: false,
      message:
         "materialization/plan/ compiles and never runs: it must not import run/, serve/ or service/model.ts",
   },
   {
      id: "R8",
      from: (f) =>
         MZ_RUN.test(f) || MZ_SERVE.test(f) || MZ_SHARED_LEAVES.has(f),
      to: (m, f) =>
         (MZ_PLAN.test(m) || MZ_RUN.test(m) || MZ_SERVE.test(m)) &&
         mzLayerOf(m) !== mzLayerOf(f),
      allowTypeImports: false,
      message:
         "plan/, run/ and serve/ are siblings and never import one another, nor do the modules they share (host.ts, stored_column_types.ts); what two of them need belongs in domain/ or text/",
   },
   {
      id: "R9",
      from: (f) => /^service\/governance\//.test(f),
      to: (m) => MZ.test(m) && !MZ_DOMAIN.test(m),
      allowTypeImports: false,
      message:
         "governance/ imports nothing of materialization but materialization/domain/",
   },
   {
      id: "R10",
      from: (f) => !MZ.test(f),
      to: (m) => MZ.test(m) && !MZ_DOMAIN.test(m) && m !== MZ_ENTRY,
      allowTypeImports: false,
      message:
         "outside materialization/, import only materialization/index.ts and materialization/domain/",
   },
   {
      id: "H5",
      from: (f) => MZ.test(f) && !MZ_RUN.test(f) && !MZ_FACADE.has(f),
      to: (m) => m === "logger" || m === "materialization_metrics",
      allowTypeImports: true,
      message:
         "telemetry is emitted from run/, the facade and the query pipeline only: return a value and let the shell log or count it",
   },
   {
      id: "H6",
      from: (f) =>
         MZ_PLAN.test(f) ||
         MZ_SERVE.test(f) ||
         MZ_DOMAIN.test(f) ||
         MZ_TEXT.test(f),
      to: (m) => m === "config",
      allowTypeImports: true,
      message:
         "plan/, serve/, domain/ and text/ read no setting: the shell reads the switches once (domain/switches.ts) and passes them down",
   },
];
export const IMPORT_DIRECTION_ALLOWLIST = {
   ...MATERIALIZATION_IMPORT_ALLOWLIST,
};

const toPosix = (p) => p.split(path.sep).join("/");
const importDirectionRule = {
   meta: {
      type: "problem",
      docs: { description: "Enforce the server's layer import direction" },
      schema: [
         {
            type: "object",
            properties: { allow: { type: "object" } },
            additionalProperties: false,
         },
      ],
   },
   create(context) {
      const allow = context.options[0]?.allow ?? {};
      const filename = context.filename ?? context.getFilename();
      const from = toPosix(path.relative(SERVER_SRC, filename));
      if (from.startsWith("..") || path.isAbsolute(from)) return {};
      const rules = IMPORT_DIRECTION_RULES.filter((r) => r.from(from));
      if (rules.length === 0) return {};
      const check = (node, specifier, typeOnly) => {
         if (typeof specifier !== "string" || !specifier.startsWith("."))
            return;
         const target = path.posix
            .normalize(path.posix.join(path.posix.dirname(from), specifier))
            .replace(/\.(ts|js|mjs|cjs)$/, "")
            .replace(/\/index$/, "");
         for (const r of rules) {
            if (!r.to(target, from)) continue;
            if (typeOnly && r.allowTypeImports) continue;
            const key = `${from} -> ${target}`;
            if (Object.hasOwn(allow, key)) continue;
            context.report({
               node,
               message: `Import direction ${r.id}: ${r.message}. (${key})`,
            });
         }
      };
      const allTypeSpecifiers = (node, kindKey) =>
         node[kindKey] === "type" ||
         (node.specifiers.length > 0 &&
            node.specifiers.every((s) => s[kindKey] === "type"));
      return {
         ImportDeclaration(node) {
            check(
               node,
               node.source.value,
               allTypeSpecifiers(node, "importKind"),
            );
         },
         ExportNamedDeclaration(node) {
            if (node.source)
               check(
                  node,
                  node.source.value,
                  allTypeSpecifiers(node, "exportKind"),
               );
         },
         ExportAllDeclaration(node) {
            check(node, node.source.value, node.exportKind === "type");
         },
         ImportExpression(node) {
            if (node.source.type === "Literal")
               check(node, node.source.value, false);
         },
         TSImportType(node) {
            const arg = node.argument;
            const lit = arg?.type === "TSLiteralType" ? arg.literal : arg;
            if (lit?.type === "Literal") check(node, lit.value, true);
         },
      };
   },
};
// ---------------------------------------------------------------------------
// Materialization's documentation checks (service/materialization/README.md).
//
//   H1  every non-spec module under materialization/ opens, after the
//       license, with a block comment saying what it is responsible for.
//   H2  no history in a comment there: no ticket number, `phase N`, `step N`
//       or `pull request N` label, or date, and no plan or review label
//       standing for a concept (`P10`, `G3`, `known limitation 29`,
//       `question 14`, `g-mixed`): spell the concept. History belongs in the
//       commit message.
//
// Both allow no exceptions: their lists in materialization_allowlists.mjs are
// empty, and src/service/materialization/layer_rules.spec.ts fails on any
// violation.
// ---------------------------------------------------------------------------
const mzFileOf = (context) => {
   const filename = context.filename ?? context.getFilename();
   const rel = toPosix(path.relative(SERVER_SRC, filename));
   return rel.startsWith("service/materialization/") ? rel : undefined;
};
const moduleHeaderRule = {
   meta: {
      type: "suggestion",
      docs: { description: "A materialization module opens with a header" },
      schema: [
         {
            type: "object",
            properties: { allow: { type: "object" } },
            additionalProperties: false,
         },
      ],
   },
   create(context) {
      const file = mzFileOf(context);
      if (!file) return {};
      const allow = context.options[0]?.allow ?? {};
      if (Object.hasOwn(allow, file)) return {};
      return {
         Program(node) {
            const source = context.sourceCode ?? context.getSourceCode();
            const first = node.body[0];
            const before = first
               ? source.getCommentsBefore(first)
               : source.getAllComments();
            if (before.some((c) => c.type === "Block")) return;
            context.report({
               node: first ?? node,
               message: `Module header H1: open ${file} with a block comment saying what the module is responsible for, its surface, invariants and effects.`,
            });
         },
      };
   },
};
/**
 * A ticket number, a `phase N`, `step N` or `pull request N` label, or a
 * date, in a comment.
 */
export const HISTORY_IN_COMMENT =
   /#\d{3,}\b|\bphase \d|\bstep \d+[a-z]?\b|\bpull request \d|\b20\d\d-\d\d-\d\d\b|\bknown limitation \d|\bquestion \d|\bg-mixed\b|\bPR \d|\b\w+\/\w+#\d/gi;
/**
 * A plan or review label standing for a concept, from the plan's own
 * series: the plan audit's P1–P14, the gate findings G1–G8, the architecture
 * audit's A1–A7, D1–D3, H1–H6 and R3–R10, a finding id (`P2-41`, `F-08`).
 * Labels that collide with ordinary words (R1/R2, the leaks L1–L3, a
 * question Qn) count only in parentheses, as the plan cites them (`(R2)`);
 * a percentile (`P95`), a quarter (`Q1`) or a cache level (`L2`) do not.
 * Case-sensitive, so a duration (`2d`) or a directory (`r4`) is not one.
 */
export const LABEL_IN_COMMENT =
   /\b(?:P(?:1[0-4]|[1-9])|G[1-8]|A[1-7]|D[1-3]|H[1-6]|R(?:10|[3-9]))(?:-\d+)?\b|\((?:R[12]|L[1-3]|Q\d{1,2})\)|\bF-\d{2}\b/g;
const noHistoryCommentsRule = {
   meta: {
      type: "suggestion",
      docs: { description: "No history in a materialization comment" },
      schema: [
         {
            type: "object",
            properties: { allow: { type: "object" } },
            additionalProperties: false,
         },
      ],
   },
   create(context) {
      const file = mzFileOf(context);
      if (!file) return {};
      const allowed = context.options[0]?.allow?.[file]?.count ?? 0;
      return {
         "Program:exit"(node) {
            const source = context.sourceCode ?? context.getSourceCode();
            let count = 0;
            for (const c of source.getAllComments()) {
               count += (c.value.match(HISTORY_IN_COMMENT) ?? []).length;
               count += (c.value.match(LABEL_IN_COMMENT) ?? []).length;
            }
            if (count > allowed) {
               context.report({
                  node,
                  message: `History in comments H2: ${file} has ${count} ticket numbers, phase, step or plan labels or dates in comments, ${allowed} allowed. Move them to the commit message.`,
               });
            }
         },
      };
   },
};
const publisherPlugin = {
   rules: {
      "import-direction": importDirectionRule,
      "module-header": moduleHeaderRule,
      "no-history-comments": noHistoryCommentsRule,
   },
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
      // The Explorer's own files (see EXPLORER_FILES). Last, so it replaces the
      // blocks above for them.
      files: EXPLORER_FILES,
      rules: {
         "no-restricted-imports": "off",
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
               patterns: UNESCAPED_MALLOY_IMPORTS_BUT_STYLESHEET.patterns,
            },
         ],
      },
   },
   {
      // The layer import direction (see IMPORT_DIRECTION_RULES). Specs and
      // test helpers wire layers together on purpose and are exempt.
      files: ["src/**/*.ts"],
      ignores: [
         "src/**/*.spec.ts",
         "src/test_helpers/**",
         "src/package_load/test_fixtures/**",
      ],
      plugins: { publisher: publisherPlugin },
      rules: {
         "publisher/import-direction": [
            "error",
            { allow: IMPORT_DIRECTION_ALLOWLIST },
         ],
      },
   },
   {
      // Materialization's documentation checks (H1, H2). Specs are exempt.
      files: ["src/service/materialization/**/*.ts"],
      ignores: ["src/**/*.spec.ts"],
      plugins: { publisher: publisherPlugin },
      rules: {
         "publisher/module-header": [
            "error",
            { allow: MATERIALIZATION_HEADER_ALLOWLIST },
         ],
         "publisher/no-history-comments": [
            "error",
            { allow: MATERIALIZATION_HISTORY_ALLOWLIST },
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
   {
      // Bun's `use…` fixtures register before/after hooks, not React state, so
      // a spec may call them at a file's top level.
      files: ["src/**/*.spec.ts", "tests/**/*.ts"],
      rules: { "react-hooks/rules-of-hooks": "off" },
   },
];
