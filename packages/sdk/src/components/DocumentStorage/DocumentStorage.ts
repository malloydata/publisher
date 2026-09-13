// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where authored documents are kept, as an interface the host hands the SDK.
 *
 * The SDK renders dashboards and notebooks that a Publisher server serves out
 * of a package, and it will author them too, but it does not decide where an
 * authored document goes. That is the host's business: the Console keeps one
 * in the browser, a platform keeps one in its own document store, a repo-backed
 * host writes it to the package directory. So the SDK asks for a
 * {@link DocumentStorage} through {@link DocumentStorageProvider} and calls it,
 * and never touches a backend itself.
 *
 * A document is a string. The interface does not know whether it is a
 * `dashboards/*.malloy` file, a `.malloynb`, or something else; the
 * {@link DocumentLocator.type} says what kind it is so a backend can keep the
 * kinds apart (a database column, a directory, a key prefix) and a listing can
 * ask for one kind. Everything else a backend may know about a document, who
 * changed it and when, who may see it, stays with the backend.
 *
 * Every method rejects rather than returning a sentinel when the document is
 * not there, so a caller can tell "missing" from "empty".
 */

/**
 * The kinds of document the SDK authors. A `dashboard` is a
 * `dashboards/<slug>.malloy` file; a `notebook` is a `.malloynb`.
 */
export type DocumentType = "dashboard" | "notebook";

/** A place documents live, as the backend presents it to a reader. */
export interface Workspace {
   name: string;
   /** False for a workspace this reader may list and open but not save into. */
   writeable: boolean;
   description: string;
}

/** One document's address: which workspace, which kind, and its path there. */
export interface DocumentLocator {
   workspace: string;
   type: DocumentType;
   /**
    * The document's path within the workspace, spelled the way the backend
    * spells it. For a repo-backed store that is the file path relative to the
    * package (`dashboards/overview.malloy`); a database store may use a slug.
    */
   path: string;
}

export interface DocumentStorage {
   /** The workspaces this reader can see; only the writeable ones when asked. */
   listWorkspaces(writeableOnly: boolean): Promise<Workspace[]>;

   /** Every document in a workspace, or only those of one kind. */
   listDocuments(
      workspace: Workspace,
      type?: DocumentType,
   ): Promise<DocumentLocator[]>;

   /** The document's content. Rejects when there is no document there. */
   getDocument(locator: DocumentLocator): Promise<string>;

   /** Write the content, creating the document or replacing it. */
   saveDocument(locator: DocumentLocator, content: string): Promise<void>;

   /** Remove the document. Rejects when there is no document there. */
   deleteDocument(locator: DocumentLocator): Promise<void>;

   /** Rename or relocate a document, keeping its content. Rejects when `from` is missing. */
   moveDocument(from: DocumentLocator, to: DocumentLocator): Promise<void>;
}
