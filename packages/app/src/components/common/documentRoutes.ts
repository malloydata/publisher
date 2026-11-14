// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   REPORT_EDIT_PARAM,
   reportEditRoute,
   reportRoute,
} from "@malloy-publisher/sdk";

/**
 * Where the Console puts the builder: the report's own route with the query
 * parameter `?edit`. Every place that builds an edit route or reads one back
 * goes through here, so the breadcrumb, the header's Edit/View, the package
 * page and the router agree on it.
 *
 * A trailing `/edit` segment is a legacy alias: the router redirects
 * `<path>/edit` to `<path>?edit` only when no report is at
 * `<path>/edit.malloy` (see `RoutedDocument`), because a segment is also a
 * path.
 */
const LEGACY_EDIT_SEGMENT = "/edit";

/**
 * A report's Console route, to read it or to edit it: its package path
 * without `.malloy`, so `sales/q3.malloy` is `/<env>/<pkg>/sales/q3`, and
 * `/<env>/<pkg>/sales/q3?edit` to edit.
 */
export const documentPath = (
   environmentName: string,
   packageName: string,
   path: string,
   mode: "view" | "edit",
) =>
   mode === "edit"
      ? reportEditRoute(environmentName, packageName, path)
      : reportRoute(environmentName, packageName, path);

/** Whether a location's search string asks for the builder. */
export const isEditSearch = (search: string): boolean =>
   new URLSearchParams(search).has(REPORT_EDIT_PARAM);

/** `search` without the edit parameter: `"?X=1"`, or `""` when nothing is left. */
export const withoutEdit = (search: string): string => {
   const params = new URLSearchParams(search);
   params.delete(REPORT_EDIT_PARAM);
   const rest = params.toString();
   return rest === "" ? "" : `?${rest}`;
};

/** `search` with the edit parameter first, spelled `?edit` rather than `?edit=`. */
export const withEdit = (search: string): string => {
   const rest = withoutEdit(search).slice(1);
   return `?${REPORT_EDIT_PARAM}${rest === "" ? "" : `&${rest}`}`;
};

/** The route path a legacy `<path>/edit` link edits, or undefined when it does not end in one. */
export const legacyEditBase = (routePath: string): string | undefined => {
   const trimmed = routePath.replace(/\/$/, "");
   if (!trimmed.endsWith(LEGACY_EDIT_SEGMENT)) return undefined;
   const base = trimmed.slice(0, -LEGACY_EDIT_SEGMENT.length);
   return base === "" ? undefined : base;
};
