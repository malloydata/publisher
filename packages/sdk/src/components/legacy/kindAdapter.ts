// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ReportLayout } from "../Package/documentLocation";

/**
 * @deprecated A report has a layout; pass `layout` (`grid` or `linear`). The
 * layout's old name, which the kind-typed props hosts still pass speak:
 * `dashboard` is a grid report, `notebook` a linear one.
 */
export type DocumentKind = "dashboard" | "notebook";

/** The layout a kind-typed prop names. Only the deprecated props call this. */
export const layoutOfKind = (kind: DocumentKind): ReportLayout =>
   kind === "notebook" ? "linear" : "grid";

/** The file the deprecated name-only editor props open: `<kind>s/<name>.malloy`. */
export const legacyNamedPath = (kind: DocumentKind, name: string): string =>
   `${kind}s/${name}.malloy`;
