// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { migrateKindTypedDrafts } from "../legacy/kindTypedDrafts";
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
 * Every key this store writes starts with this, followed by the document's
 * path, so it can list its own documents without reading everything else the
 * page keeps in localStorage (the theme mode, other libraries' state).
 */
const KEY_PREFIX = "publisher:report:";

/** What one key holds: the content, and the layout its last save recorded. */
interface Stored {
   type: DocumentType;
   content: string;
}

const keyOf = (path: string) => `${KEY_PREFIX}${path}`;

function read(key: string): Stored | undefined {
   const raw = localStorage.getItem(key);
   if (raw === null) return undefined;
   try {
      const stored = JSON.parse(raw) as Partial<Stored>;
      if (
         typeof stored.content === "string" &&
         (stored.type === "grid" || stored.type === "linear")
      )
         return { type: stored.type, content: stored.content };
   } catch {
      // Not ours to read: treated as absent rather than as a document.
   }
   return undefined;
}

const write = (path: string, stored: Stored) =>
   localStorage.setItem(keyOf(path), JSON.stringify(stored));

/**
 * A {@link DocumentStorage} in the browser's localStorage: one workspace,
 * `Local`, private to this browser and this origin. The Console's default, and
 * what a host uses before it has a store of its own.
 *
 * A document is keyed by its path alone; its layout is stored with it.
 */
export class BrowserDocumentStorage implements DocumentStorage {
   private migrated = false;

   /** Drafts saved under the old type-keyed scheme, moved once per store so they are still found. */
   private migrate(): void {
      if (this.migrated) return;
      this.migrated = true;
      migrateKindTypedDrafts(localStorage, (path, type, content) => {
         if (read(keyOf(path)) !== undefined) return false;
         write(path, { type, content });
         return true;
      });
   }

   async listWorkspaces(writeableOnly: boolean): Promise<Workspace[]> {
      void writeableOnly; // the one workspace is writeable
      return [LOCAL_WORKSPACE];
   }

   async listDocuments(
      workspace: Workspace,
      type?: DocumentType,
   ): Promise<DocumentLocator[]> {
      if (workspace.name !== LOCAL_WORKSPACE.name) return [];
      this.migrate();
      const found: DocumentLocator[] = [];
      for (let i = 0; i < localStorage.length; i++) {
         const key = localStorage.key(i);
         if (key === null || !key.startsWith(KEY_PREFIX)) continue;
         const stored = read(key);
         if (stored && (type === undefined || stored.type === type))
            found.push({
               workspace: LOCAL_WORKSPACE.name,
               type: stored.type,
               path: key.slice(KEY_PREFIX.length),
            });
      }
      return found;
   }

   async getDocument(locator: DocumentLocator): Promise<string> {
      this.migrate();
      const stored = read(keyOf(locator.path));
      if (stored === undefined) throw missing(locator);
      return stored.content;
   }

   async saveDocument(
      locator: DocumentLocator,
      content: string,
   ): Promise<void> {
      this.migrate();
      write(locator.path, { type: locator.type, content });
   }

   async deleteDocument(locator: DocumentLocator): Promise<void> {
      this.migrate();
      if (read(keyOf(locator.path)) === undefined) throw missing(locator);
      localStorage.removeItem(keyOf(locator.path));
   }

   async moveDocument(
      from: DocumentLocator,
      to: DocumentLocator,
   ): Promise<void> {
      const content = await this.getDocument(from);
      write(to.path, { type: to.type, content });
      if (to.path !== from.path) localStorage.removeItem(keyOf(from.path));
   }
}

function missing(locator: DocumentLocator): Error {
   return new DocumentNotFoundError(
      `No report at ${locator.workspace}/${locator.path}`,
   );
}
