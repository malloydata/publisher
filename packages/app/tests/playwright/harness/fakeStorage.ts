// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   DocumentNotFoundError,
   type DocumentLocator,
   type DocumentStorage,
   type DocumentType,
   type Workspace,
} from "@malloy-publisher/sdk";

/**
 * An in-memory host store the harness page hands the Console, standing in for
 * a platform that keeps its own documents. Specs read what it holds and what
 * was saved through `window.__host`.
 */
export class FakeStorage implements DocumentStorage {
   readonly documents = new Map<string, string>();
   readonly saves: Array<{ locator: DocumentLocator; content: string }> = [];
   private readonly types = new Map<string, DocumentType>();

   constructor(readonly workspace: Workspace) {}

   async listWorkspaces(writeableOnly: boolean): Promise<Workspace[]> {
      return writeableOnly && !this.workspace.writeable ? [] : [this.workspace];
   }

   async listDocuments(
      _workspace: Workspace,
      type?: DocumentType,
   ): Promise<DocumentLocator[]> {
      // A document's type is what it was saved as; a seeded one, which has
      // none, answers to either. Never read off the path: a report lives
      // anywhere in its package.
      return [...this.documents.keys()]
         .filter((path) => {
            const saved = this.types.get(path);
            return !type || !saved || saved === type;
         })
         .map((path) => ({
            workspace: this.workspace.name,
            type: this.types.get(path) ?? type ?? "notebook",
            path,
         }));
   }

   async getDocument(locator: DocumentLocator): Promise<string> {
      // Keyed by type as well as path, as a real backend may be: a document
      // saved as one type is not found under the other. A seeded one, which
      // has no type, answers to either.
      const content = this.documents.get(locator.path);
      const saved = this.types.get(locator.path);
      if (content === undefined || (saved && saved !== locator.type))
         throw new DocumentNotFoundError(`No document at ${locator.path}`);
      return content;
   }

   async saveDocument(
      locator: DocumentLocator,
      content: string,
   ): Promise<void> {
      this.documents.set(locator.path, content);
      this.types.set(locator.path, locator.type);
      this.saves.push({ locator, content });
   }

   async deleteDocument(locator: DocumentLocator): Promise<void> {
      if (!this.documents.delete(locator.path))
         throw new DocumentNotFoundError(`No document at ${locator.path}`);
      this.types.delete(locator.path);
   }

   async moveDocument(
      from: DocumentLocator,
      to: DocumentLocator,
   ): Promise<void> {
      const content = await this.getDocument(from);
      this.documents.delete(from.path);
      this.documents.set(to.path, content);
   }
}

/** A host whose store IS the record of its documents. */
export class FakeAuthoritativeStorage extends FakeStorage {
   constructor() {
      super({
         name: "Record",
         writeable: true,
         description: "Kept in the host's record",
         authoritative: true,
      });
   }
}

/** A host that keeps a copy beside the package, which the package file outranks. */
export class FakeScratchStorage extends FakeStorage {
   constructor() {
      super({
         name: "Scratch",
         writeable: true,
         description: "Kept in scratch",
      });
   }
}
