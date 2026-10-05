// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** A layout document is a dashboard or a notebook; the artifact tag says which, not the folder. */
export type DocumentKind = "dashboard" | "notebook";

export interface LocatedDocument {
   kind: DocumentKind;
   /** The file within the package, wherever it lives. */
   path: string;
}

const DOCUMENT_PATH = /^(?:dashboards|notebooks)\/([^/]+)\.malloy$/;

/** The slug of a document in either folder: `notebooks/tour.malloy` and `dashboards/tour.malloy` are `tour`. */
export const documentSlug = (path: string | undefined) =>
   DOCUMENT_PATH.exec(path ?? "")?.[1];

/** The Console route of a document of this kind; the slug is a filename, so it is encoded. */
export const documentRoute = (
   environmentName: string,
   packageName: string,
   kind: DocumentKind,
   slug: string,
) => `/${environmentName}/${packageName}/${kind}s/${encodeURIComponent(slug)}`;

/**
 * Where the document a route names really is, and what kind its tag makes it.
 *
 * The listings sort by tag, so a hit in one says the kind. The route's own kind
 * is tried first, so a slug in both folders opens the one the link was built for.
 */
export function locateDocument({
   kind,
   slug,
   dashboards,
   notebooks,
}: {
   kind: DocumentKind;
   slug: string;
   dashboards: readonly { name?: string; path?: string }[];
   notebooks: readonly { path?: string }[];
}): LocatedDocument | undefined {
   const asDashboard = (): LocatedDocument | undefined => {
      const found = dashboards.find((d) => d.name === slug && d.path);
      return found?.path ? { kind: "dashboard", path: found.path } : undefined;
   };
   const asNotebook = (): LocatedDocument | undefined => {
      const found = notebooks.find((n) => documentSlug(n.path) === slug);
      return found?.path ? { kind: "notebook", path: found.path } : undefined;
   };
   return kind === "dashboard"
      ? (asDashboard() ?? asNotebook())
      : (asNotebook() ?? asDashboard());
}
