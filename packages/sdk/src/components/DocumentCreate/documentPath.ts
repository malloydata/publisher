// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { slugFor } from "../DashboardBuilder/newDashboard";
import type { DocumentLocator, DocumentType } from "../DocumentStorage";

export { newDashboardSource, slugFor } from "../DashboardBuilder/newDashboard";

/**
 * A directory as the create dialog takes it: no leading or trailing slash,
 * `""` for the package root.
 */
export const normalizeDirectory = (directory: string | undefined) =>
   (directory ?? "")
      .trim()
      .replace(/\\/g, "/")
      .replace(/^\/+|\/+$/g, "")
      .replace(/\/{2,}/g, "/");

/**
 * The package-relative file for a slug: `<slug>.malloy` at the package root,
 * or under `directory` when the author typed one. No directory is chosen for
 * a report by its layout.
 */
export function reportPathFor(slug: string, directory?: string): string {
   const dir = normalizeDirectory(directory);
   return `${dir === "" ? "" : `${dir}/`}${slug}.malloy`;
}

/**
 * The file a slug was written to before reports: `dashboards/<slug>.malloy` or
 * `notebooks/<slug>.malloy`, by kind.
 *
 * @deprecated Folders no longer mean anything to Publisher, and a new report
 * goes to the package root. Use {@link reportPathFor}. Kept with its original
 * signature and result for hosts that still file documents by kind.
 */
export function documentPathFor(kind: DocumentType, slug: string): string {
   return `${kind}s/${slug}.malloy`;
}

/** A title's slug, with a stand-in for a title that has no letters or digits. */
export function slugOrFallback(title: string): string {
   return slugFor(title) || "untitled";
}

/** The file for a title; `suffix` 1 is the bare slug, 2 and up append `-N`. */
export function reportPathForTitle(
   title: string,
   suffix = 1,
   directory?: string,
): string {
   const slug = slugOrFallback(title);
   return reportPathFor(suffix > 1 ? `${slug}-${suffix}` : slug, directory);
}

/**
 * The kind-folder file for a title; `suffix` 1 is the bare slug, 2 and up
 * append `-N`.
 *
 * @deprecated Use {@link reportPathForTitle}. Kept with its original signature
 * and result, like {@link documentPathFor}.
 */
export function documentPathForTitle(
   kind: DocumentType,
   title: string,
   suffix = 1,
): string {
   const slug = slugOrFallback(title);
   return documentPathFor(kind, suffix > 1 ? `${slug}-${suffix}` : slug);
}

/** The storage key for a document's copy, which is its package path; the Console's editors and `createDocument` mint it the same way. */
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
