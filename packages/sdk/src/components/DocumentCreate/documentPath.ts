// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { slugFor } from "../DashboardBuilder/newDashboard";
import type { DocumentLocator, DocumentType } from "../DocumentStorage";

export { newDashboardSource, slugFor } from "../DashboardBuilder/newDashboard";

/** The package-relative file for a slug: always `dashboards/<slug>.malloy` or `notebooks/<slug>.malloy`, the only shapes the server writes. */
export function documentPathFor(kind: DocumentType, slug: string): string {
   return `${kind}s/${slug}.malloy`;
}

/** A title's slug, with a stand-in for a title that has no letters or digits. */
export function slugOrFallback(title: string): string {
   return slugFor(title) || "untitled";
}

/** The file for a title; `suffix` 1 is the bare slug, 2 and up append `-N`. */
export function documentPathForTitle(
   kind: DocumentType,
   title: string,
   suffix = 1,
): string {
   const slug = slugOrFallback(title);
   return documentPathFor(kind, suffix > 1 ? `${slug}-${suffix}` : slug);
}

/** The storage key for a document's copy; the Console's editors and `createDocument` mint it the same way. */
export const locatorFor = (
   kind: DocumentType,
   workspace: string,
   environmentName: string,
   packageName: string,
   path: string,
): DocumentLocator => ({
   workspace,
   type: kind,
   path: `${environmentName}/${packageName}/${path}`,
});
