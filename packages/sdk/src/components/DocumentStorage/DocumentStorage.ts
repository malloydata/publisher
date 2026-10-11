// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ReportLayout } from "../Package/documentLocation";

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
 * A document is a string, addressed by its workspace and its path: one path is
 * one document. The {@link DocumentLocator.type} is metadata beside it, the
 * report's layout, which a backend keeps (a column, a field) so a listing can
 * say which layout each document is without reading it, and filter by one.
 * Everything else a backend may know about a document, who changed it and
 * when, who may see it, stays with the backend.
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
 * The layout of a report the SDK authors, as a store records it beside the
 * document: `grid` or `linear`. Metadata, not identity: a report's path is
 * its identity wherever in the package the file lives, and a layout switch
 * changes this and nothing else.
 */
export type DocumentType = ReportLayout;

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

/**
 * One document's address, `workspace` and `path`, and its layout.
 *
 * The SDK spells `path` as `<environment>/<package>/<package path>`
 * (`draftKey`), and a store keys a document by it: a read, a delete or a move
 * finds the document at `path` whatever `type` says, and a save records
 * `type` as the document's layout, replacing what it was.
 */
export interface DocumentLocator {
   workspace: string;
   /** The report's layout, which a save records and a listing reports. */
   type: DocumentType;
   /** The document's identity within the workspace. */
   path: string;
}

export interface DocumentStorage {
   /** The workspaces this reader can see; only the writeable ones when asked. */
   listWorkspaces(writeableOnly: boolean): Promise<Workspace[]>;

   /** Every document in a workspace, or only those of one layout, each with the layout its last save recorded. */
   listDocuments(
      workspace: Workspace,
      type?: DocumentType,
   ): Promise<DocumentLocator[]>;

   /**
    * The content of the document at `locator.path`, whatever its layout.
    * Rejects with a {@link DocumentNotFoundError} when there is no document
    * there, and with anything else when the backend could not be asked.
    */
   getDocument(locator: DocumentLocator): Promise<string>;

   /**
    * Write the content at `locator.path`, creating the document or replacing
    * it, and record `locator.type` as its layout.
    *
    * There is no expected-version slot, so this is last writer wins and a
    * caller cannot tell that it overwrote someone. A backend that has to stop
    * two authors clobbering each other enforces that itself, in the backend.
    */
   saveDocument(locator: DocumentLocator, content: string): Promise<void>;

   /**
    * Remove the document at `locator.path`. Rejects with a {@link DocumentNotFoundError} when
    * there is no document there, and with anything else when the backend could
    * not be asked — a caller must not read the second as the first and report
    * the document gone.
    */
   deleteDocument(locator: DocumentLocator): Promise<void>;

   /**
    * Rename or relocate a document, keeping its content, and record `to.type`
    * as its layout. Rejects with a {@link DocumentNotFoundError} when nothing
    * is at `from.path`.
    */
   moveDocument(from: DocumentLocator, to: DocumentLocator): Promise<void>;

   /**
    * Optional. The package-relative path at which {@link saveDocument} will
    * put the file for `locator`, for a store that files a document somewhere
    * other than the package path the SDK asked for (one that files a report
    * by its layout, say). `createDocument` asks before it renders a new
    * report, so the report's imports are written relative to
    * where the file really lands, and returns that path as the report's
    * `path`. Left out, the SDK assumes the requested package path: the
    * locator's `path` minus its `<environment>/<package>/` prefix.
    */
   resolveDocumentPath?(locator: DocumentLocator): string | Promise<string>;
}
