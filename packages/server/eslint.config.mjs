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
];
