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
