// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { slugFor } from "../ReportBuilder/newDashboard";
import type { DocumentLocator, DocumentType } from "../DocumentStorage";

export { newDashboardSource, slugFor } from "../ReportBuilder/newDashboard";

/**
 * A directory as the create dialog takes it: no leading or trailing slash,
 * `""` for the package root.
 */
export const normalizeDirectory = (directory: string | undefined) =>
   // Split rather than trim with `^\/+|\/+$`, which backtracks polynomially on a run of slashes.
   (directory ?? "")
      .trim()
      .replace(/\\/g, "/")
      .split("/")
      .filter((segment) => segment !== "")
      .join("/");

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

/**
 * A report's identity in a host's store: `<environment>/<package>/<path>`.
 * Every reader and writer of a store mints it here, and `draftKey(env, pkg,
 * "")` is the prefix every report of one package shares.
 */
export const draftKey = (
   environmentName: string,
   packageName: string,
   path: string,
) => `${environmentName}/${packageName}/${path}`;

/** A report's address in a store, its {@link draftKey}, with the layout a save records. */
export const locatorFor = (
   layout: DocumentType,
   workspace: string,
   environmentName: string,
   packageName: string,
   path: string,
): DocumentLocator => ({
   workspace,
   type: layout,
   path: draftKey(environmentName, packageName, path),
});
