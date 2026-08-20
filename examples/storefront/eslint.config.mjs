// The page ships as plain ES modules the browser loads directly: no bundler, no
// typechecker, nothing else that reads these files. Without this, nothing checks
// them at all. (Measured: the root `scripts/` directory is unchecked too, since
// each package's lint script covers only its own tree. That is pre-existing and
// not what this file is for.)
//
// `no-use-before-define` is the rule that earns its place. A `let` read above
// its own declaration throws at runtime and no test around `format.js` can see
// it, because it only fires when a control is built against a real DOM. That
// broke this page once while the unit suite stayed green.
//
// Self-contained on purpose: no `@eslint/js`, no `globals`. Neither is declared
// at the root, so importing them here would depend on workspace hoisting, which
// is a strange thing for an example to rely on. The globals below are the exact
// set these modules use, so an unlisted one is a typo or a new dependency worth
// a deliberate line here.
const browserGlobals = [
   "console",
   "document",
   "fetch",
   "getComputedStyle",
   "history",
   "Intl",
   "location",
   "requestAnimationFrame",
   "URLSearchParams",
   "window",
];

export default [
   {
      // Vendored build output and the node test suite, neither of them this
      // page's own browser code. Written `**/`-prefixed on purpose: flat-config
      // ignores resolve against the working directory, not this file, so
      // `public/vendor/**` silently matches nothing when eslint runs from the
      // repo root, which is exactly how the root `lint:examples` script runs it.
      ignores: ["**/public/vendor/**", "**/examples/storefront/tests/**"],
   },
   {
      files: ["**/*.js"],
      languageOptions: {
         ecmaVersion: 2022,
         sourceType: "module",
         globals: {
            ...Object.fromEntries(browserGlobals.map((n) => [n, "readonly"])),
            // Provided by the page itself: the Publisher browser runtime and
            // the vendored chart library, both loaded before these modules.
            Publisher: "readonly",
            Chart: "readonly",
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
