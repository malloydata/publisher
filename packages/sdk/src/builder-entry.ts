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

export { ReportBuilder } from "./components/ReportBuilder/ReportBuilder";
export {
   BuilderBar,
   type BuilderBarProps,
} from "./components/ReportBuilder/BuilderBar";
export type {
   BuilderControls,
   BuilderControlsHandler,
} from "./components/ReportBuilder/builderControls";
export type {
   BuilderGiven,
   ReportBuilderProps,
} from "./components/ReportBuilder/ReportBuilder";
export {
   ReportEditor,
   type ReportEditorProps,
   type TextSourceOptions,
} from "./components/ReportBuilder/ReportEditor";
// Deprecated aliases of the two above.
export {
   DashboardBuilder,
   DashboardEditor,
   type DashboardBuilderProps,
   type DashboardEditorProps,
} from "./components/legacy/builderNames";
export type {
   SaveContext,
   SaveHandler,
} from "./components/ReportBuilder/useDocumentEditor";
export type {
   DashboardDocument,
   DashboardTile,
   LocalGiven,
} from "./components/ReportBuilder/document";
export {
   readDashboardDocument,
   readFailed,
   type ReadResult,
} from "./components/ReportBuilder/readDocument";
// What a report's text is, by the server's rule, for a host that files a document by its layout.
export {
   readReportTagOfText,
   type ReportTag,
} from "./components/ReportBuilder/reportTag";
export {
   spliceDashboardDocument,
   spliceFailed,
   type SpliceResult,
} from "./components/ReportBuilder/spliceDocument";
export {
   buildCatalog,
   type PackageCatalog,
} from "./components/ReportBuilder/catalog";
export type {
   DrillOutcome,
   ReportEvent,
   ReportEventContext,
   ReportEventHandler,
   ReportFailure,
   SavesTo,
} from "./components/Report/telemetry";
export {
   NotebookEditor,
   type NotebookEditorProps,
} from "./components/legacy/NotebookEditor";
export {
   createDocument,
   createRoute,
   locatorFor,
   MAX_SLUG_ATTEMPTS,
   newDashboardSource,
   newDocumentProblem,
   newNotebookSource,
   reportPathFor,
   reportPathForTitle,
   slugFor,
   slugOrFallback,
   useDocumentChoices,
   type CreatedDocument,
   type CreateDocumentOptions,
   type CreateTarget,
   type DocumentChoice,
   type NewDocument,
} from "./components/DocumentCreate";
