// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type DocumentType,
   type Workspace,
} from "./DocumentStorage";

const LOCAL_WORKSPACE: Workspace = {
   name: "Local",
   description: "Stored in this browser only",
   writeable: true,
};

/**
 * Every key this store writes starts with this, so it can list its own
 * documents without reading everything else the page keeps in localStorage
 * (the theme mode, other libraries' state). The predecessor enumerated every
 * key as a document.
 */
const KEY_PREFIX = "publisher:document:";

/**
 * A {@link DocumentStorage} in the browser's localStorage: one workspace,
 * `Local`, private to this browser and this origin. The Console's default, and
 * what a host uses before it has a store of its own.
 */
export class BrowserDocumentStorage implements DocumentStorage {
   async listWorkspaces(writeableOnly: boolean): Promise<Workspace[]> {
      void writeableOnly; // the one workspace is writeable
      return [LOCAL_WORKSPACE];
   }

   async listDocuments(
      workspace: Workspace,
      type?: DocumentType,
   ): Promise<DocumentLocator[]> {
      if (workspace.name !== LOCAL_WORKSPACE.name) return [];
      const found: DocumentLocator[] = [];
      for (let i = 0; i < localStorage.length; i++) {
         const key = localStorage.key(i);
         const locator = key === null ? undefined : locatorOf(key);
         if (locator && (type === undefined || locator.type === type)) {
            found.push(locator);
         }
      }
      return found;
   }

   async getDocument(locator: DocumentLocator): Promise<string> {
      const content = localStorage.getItem(keyOf(locator));
      if (content === null) throw missing(locator);
      return content;
   }

   async saveDocument(
      locator: DocumentLocator,
      content: string,
   ): Promise<void> {
      localStorage.setItem(keyOf(locator), content);
   }

   async deleteDocument(locator: DocumentLocator): Promise<void> {
      const key = keyOf(locator);
      if (localStorage.getItem(key) === null) throw missing(locator);
      localStorage.removeItem(key);
   }

   async moveDocument(
      from: DocumentLocator,
      to: DocumentLocator,
   ): Promise<void> {
      const content = await this.getDocument(from);
      localStorage.setItem(keyOf(to), content);
      localStorage.removeItem(keyOf(from));
   }
}

function keyOf(locator: DocumentLocator): string {
   // The type is one segment; the path may contain anything, so it goes last
   // and is read back as "the rest".
   return `${KEY_PREFIX}${locator.type}:${locator.path}`;
}

function locatorOf(key: string): DocumentLocator | undefined {
   if (!key.startsWith(KEY_PREFIX)) return undefined;
   const rest = key.slice(KEY_PREFIX.length);
   const separator = rest.indexOf(":");
   if (separator <= 0) return undefined;
   const type = rest.slice(0, separator);
   if (type !== "dashboard" && type !== "notebook") return undefined;
   return {
      workspace: LOCAL_WORKSPACE.name,
      type,
      path: rest.slice(separator + 1),
   };
}

function missing(locator: DocumentLocator): Error {
   return new DocumentNotFoundError(
      `No ${locator.type} at ${locator.workspace}/${locator.path}`,
   );
}
