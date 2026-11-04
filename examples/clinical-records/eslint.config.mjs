// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The page ships as plain ES modules the browser loads directly, so nothing
// else reads these files before a browser does. This is what caught a string
// that stopped parsing when the annotation syntax it quotes changed. Same rules
// as examples/storefront/eslint.config.mjs; the globals are the exact set
// these modules use.
const browserGlobals = [
   "console",
   "document",
   "fetch",
   "getComputedStyle",
   "history",
   "Intl",
   "location",
   "Node",
   "requestAnimationFrame",
   "structuredClone",
   "URLSearchParams",
   "window",
];

export default [
   { ignores: ["**/examples/clinical-records/tests/**"] },
   {
      files: ["**/*.js"],
      languageOptions: {
         ecmaVersion: 2022,
         sourceType: "module",
         globals: {
            ...Object.fromEntries(browserGlobals.map((n) => [n, "readonly"])),
            // The Publisher browser runtime, loaded before these modules.
            Publisher: "readonly",
         },
      },
      rules: {
         "no-undef": "error",
         "no-unused-vars": "error",
         "no-use-before-define": [
            "error",
            { variables: true, functions: false, classes: false },
         ],
      },
   },
];
