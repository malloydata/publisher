// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Deprecated surface kept for hosts that still use it: the by-name lookups
 * behind old links, and the kind-typed vocabulary. Report code imports nothing
 * from here; removing a feature is deleting its module and its line below.
 */
export { type DocumentKind } from "./kindAdapter";
export {
   documentRoute,
   legacyAliasOf,
   locateDocument,
   locateReportByName,
   type NameLookup,
} from "./nameAlias";
