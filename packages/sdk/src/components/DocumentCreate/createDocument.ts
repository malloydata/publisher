// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { newDashboardSource } from "../DashboardBuilder/newDashboard";
import { saveTarget } from "../DashboardBuilder/documentSession";
import {
   isDocumentNotFound,
   type DocumentLocator,
   type DocumentStorage,
   type DocumentType,
   type Workspace,
} from "../DocumentStorage/DocumentStorage";
import { documentPathForTitle, locatorFor } from "./documentPath";
import type { DocumentCreatedEvent } from "./events";
import { newDocumentProblem, type NewDocument } from "./guards";
import { newNotebookSource } from "./newNotebook";

/** How many names are tried before a create gives up: `title`, `title-2`, ... */
export const MAX_SLUG_ATTEMPTS = 20;

/**
 * Where a create may land, or `undefined` for nowhere. A non-authoritative
 * browser workspace is never a target (`savesTo === "browser"`): a document
 * created there would exist for one reader on one machine and look saved.
 */
export function createRoute(host: {
   authoritative: boolean;
   mutable: boolean;
   /** The host keeps documents and the chosen workspace takes writes. */
   canStore: boolean;
}): "package" | "storage" | undefined {
   const { savesTo, writer } = saveTarget({ ...host, readFailed: false });
   return savesTo === "browser" ? undefined : writer;
}

export type CreateTarget =
   | {
        route: "package";
        /** Package-relative paths already in the package, as its listing gives them. */
        existing: readonly string[];
        /** A create-only write: no hash, so the server refuses a file that is there. */
        write: (path: string, source: string) => Promise<void>;
     }
   | {
        route: "storage";
        storage: DocumentStorage;
        workspace: Workspace;
        environmentName: string;
        packageName: string;
     };

export interface CreatedDocument {
   kind: DocumentType;
   /** Package-relative, e.g. `dashboards/sales.malloy`. */
   path: string;
   /** `sales` for `dashboards/sales.malloy`: what an editor route names. */
   slug: string;
   /** Set on the storage route: the new document's address there. */
   locator?: DocumentLocator;
}

export interface CreateDocumentOptions {
   kind: DocumentType;
   document: NewDocument;
   target: CreateTarget;
   onCreated?: (created: CreatedDocument) => void;
   onEvent?: (event: DocumentCreatedEvent) => void;
}

const slugOf = (path: string) =>
   path.slice(path.indexOf("/") + 1, -".malloy".length);

/**
 * Write a new dashboard or notebook, never over an existing file, and say where
 * it went. Reads nothing back: the host navigates from what this returns.
 *
 * On the storage route the listing alone cannot be trusted to be current, and
 * `saveDocument` replaces what is there, so each candidate name is read first
 * and used only when the read says the document is absent.
 */
export async function createDocument(
   options: CreateDocumentOptions,
): Promise<CreatedDocument> {
   const { kind, document, target, onCreated, onEvent } = options;
   const problem = newDocumentProblem(kind, document);
   if (problem) throw new Error(problem);
   const source =
      kind === "dashboard"
         ? newDashboardSource(document)
         : newNotebookSource(document);

   let created: CreatedDocument;
   if (target.route === "package") {
      const taken = new Set(target.existing);
      let path: string | undefined;
      for (let n = 1; n <= MAX_SLUG_ATTEMPTS && path === undefined; n++) {
         const candidate = documentPathForTitle(kind, document.title, n);
         if (!taken.has(candidate)) path = candidate;
      }
      if (path === undefined) throw noFreeName(document.title);
      await target.write(path, source);
      created = { kind, path, slug: slugOf(path) };
   } else {
      const { storage, workspace, environmentName, packageName } = target;
      const listed = new Set(
         (await storage.listDocuments(workspace, kind)).map((l) => l.path),
      );
      let found: CreatedDocument | undefined;
      for (let n = 1; n <= MAX_SLUG_ATTEMPTS && found === undefined; n++) {
         const path = documentPathForTitle(kind, document.title, n);
         const locator = locatorFor(
            kind,
            workspace.name,
            environmentName,
            packageName,
            path,
         );
         if (listed.has(locator.path)) continue;
         let present = true;
         try {
            await storage.getDocument(locator);
         } catch (error) {
            // Only absence frees the name; a backend that could not answer says nothing about it.
            if (!isDocumentNotFound(error)) throw error;
            present = false;
         }
         if (!present) found = { kind, path, slug: slugOf(path), locator };
      }
      if (found === undefined) throw noFreeName(document.title);
      await storage.saveDocument(found.locator!, source);
      created = found;
   }

   onEvent?.({
      type: `${kind}.created`,
      where: target.route === "package" ? "package" : "host",
   });
   onCreated?.(created);
   return created;
}

const noFreeName = (title: string) =>
   new Error(
      `No free file name for "${title}" after ${MAX_SLUG_ATTEMPTS} tries. Fix: choose a different title.`,
   );
