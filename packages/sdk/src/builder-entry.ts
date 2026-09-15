// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The dashboard builder's entry point, separate from the SDK's main entry on
// purpose: the builder reads Malloy files with the Malloy parser, and that
// parser is 440 KB gzipped. Most Console users never open the builder, so this
// module is what a host loads LAZILY (`React.lazy(() => import(
// "@malloy-publisher/sdk/builder"))`), and nothing reachable from the main
// entry imports it. One static import of the parser anywhere on the main path
// would hoist all of it into every page load.
//
// The parser's dependencies (antlr4ts -> assert -> util) read `process.env` at
// module scope, which a browser does not define. A bundler's `define` does not
// reach pre-bundled dependencies, so the shim has to exist at runtime, before
// the parser's chunk is evaluated — and the reader's `await import` runs after
// this module has, which is what makes this the right place for it.
declare const globalThis: { process?: { env?: Record<string, string> } };
if (typeof globalThis.process === "undefined") {
   globalThis.process = { env: {} };
} else if (globalThis.process.env === undefined) {
   globalThis.process.env = {};
}

export { DashboardBuilder } from "./components/DashboardBuilder/DashboardBuilder";
export type {
   BuilderGiven,
   DashboardBuilderProps,
} from "./components/DashboardBuilder/DashboardBuilder";
export {
   DashboardEditor,
   type DashboardEditorProps,
} from "./components/DashboardBuilder/DashboardEditor";
export type {
   DashboardDocument,
   DashboardTile,
   LocalGiven,
} from "./components/DashboardBuilder/document";
export {
   readDashboardDocument,
   readFailed,
   type ReadResult,
} from "./components/DashboardBuilder/readDocument";
export {
   spliceDashboardDocument,
   spliceFailed,
   type SpliceResult,
} from "./components/DashboardBuilder/spliceDocument";
export {
   buildCatalog,
   type PackageCatalog,
} from "./components/DashboardBuilder/catalog";
