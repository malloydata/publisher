// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { documentRoute } from "@malloy-publisher/sdk";

/**
 * Where the Console puts the builder: one segment under the document it edits.
 * Every place that builds an edit route or reads one back goes through here,
 * so the breadcrumb, the header's Edit/View, the package page and the router
 * agree on it.
 */
const EDIT_SEGMENT = "/edit";

/** A dashboard's or notebook's Console route, to read it or to edit it. */
export const documentPath = (
   environmentName: string,
   packageName: string,
   kind: "dashboard" | "notebook",
   slug: string,
   mode: "view" | "edit",
) =>
   `${documentRoute(environmentName, packageName, kind, slug)}${mode === "edit" ? EDIT_SEGMENT : ""}`;

/** `path` with any trailing edit segment taken off, and whether it had one. */
export const splitEdit = (path: string): { path: string; edit: boolean } => {
   const trimmed = path.replace(/\/$/, "");
   return trimmed.endsWith(EDIT_SEGMENT)
      ? { path: trimmed.slice(0, -EDIT_SEGMENT.length), edit: true }
      : { path: trimmed, edit: false };
};

/** The edit route of a document's read route. */
export const editPathOf = (readPath: string) => `${readPath}${EDIT_SEGMENT}`;
