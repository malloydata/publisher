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
 * A host hands the SDK ONE storage and keeps handing it the same one. What a
 * storage says about its workspaces decides how the editor routes a save, and
 * the editor asks once, when it opens; a host that re-creates its storage mid
 * session leaves the editor routing by the old answer until the new list
 * resolves.
 *
 * Every method rejects rather than returning a sentinel when the document is
 * not there, so a caller can tell "missing" from "empty". Absence rejects with
 * a {@link DocumentNotFoundError} specifically, so a caller can tell either of
 * those from a backend that could not be reached: a failed read answered with
 * "there is no document" reads as "the package is the only copy", and saving
 * on that belief overwrites the copy that was actually there.
 */

/**
 * Absence, and only absence: the document is not at that locator. Every other
 * rejection means the backend could not answer, which is not the same thing and
 * must not be treated as one.
 */
export class DocumentNotFoundError extends Error {
   constructor(message: string) {
      super(message);
      // Not `instanceof`: the `es` and `cjs` builds carry their own copy of
      // this class, so an error thrown by one fails the check in the other.
      this.name = "DocumentNotFoundError";
   }
}

/** Whether a rejection means absence rather than a backend that could not answer. */
export const isDocumentNotFound = (error: unknown): boolean =>
   error instanceof DocumentNotFoundError ||
   (error instanceof Error && error.name === "DocumentNotFoundError");

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
   /**
    * What this place is, in the backend's own words, for the editor to show a
    * reader rather than guessing on their behalf.
    */
   description: string;
   /**
    * This workspace holds the package's system of record: what it keeps for a
    * document IS that document, and the package file is a copy of it or a
    * deploy behind it.
    *
    * Left out, the workspace is a place a reader's work is kept beside the
    * package — the Console's browser copy, a scratch space — and the package
    * file is the record.
    *
    * The contract, which the editor relies on and does not police:
    *
    * - At most one workspace of a storage declares itself authoritative. The
    *   editor takes the first it is given and does not arbitrate between two.
    * - It is orthogonal to {@link Workspace.writeable} and to the server's
    *   `mutable`. A workspace can be the record and still be read-only to this
    *   reader, and a server that refuses writes to its own package says
    *   nothing about where the record lives.
    * - A host whose authority differs per package mounts one storage per
    *   context rather than flipping this between renders; the editor reads it
    *   once, when it opens.
    */
   authoritative?: boolean;
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

   /**
    * The document's content. Rejects with a {@link DocumentNotFoundError} when
    * there is no document there, and with anything else when the backend could
    * not be asked.
    */
   getDocument(locator: DocumentLocator): Promise<string>;

   /**
    * Write the content, creating the document or replacing it.
    *
    * There is no expected-version slot, so this is last writer wins and a
    * caller cannot tell that it overwrote someone. A backend that has to stop
    * two authors clobbering each other enforces that itself, in the backend.
    */
   saveDocument(locator: DocumentLocator, content: string): Promise<void>;

   /**
    * Remove the document. Rejects with a {@link DocumentNotFoundError} when
    * there is no document there, and with anything else when the backend could
    * not be asked — a caller must not read the second as the first and report
    * the document gone.
    */
   deleteDocument(locator: DocumentLocator): Promise<void>;

   /**
    * Rename or relocate a document, keeping its content. Rejects with a
    * {@link DocumentNotFoundError} when `from` is missing.
    */
   moveDocument(from: DocumentLocator, to: DocumentLocator): Promise<void>;
}
