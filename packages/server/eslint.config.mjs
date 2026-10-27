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
      // `.malloynb` is deprecated and fenced off: its renderer lives in
      // `legacy/malloynb/` and nothing outside `legacy/` may import it, so the
      // format is deleted by deleting that folder and the legacy exports.
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
