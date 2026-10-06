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

   constructor(readonly workspace: Workspace) {}

   async listWorkspaces(writeableOnly: boolean): Promise<Workspace[]> {
      return writeableOnly && !this.workspace.writeable ? [] : [this.workspace];
   }

   async listDocuments(
      _workspace: Workspace,
      type?: DocumentType,
   ): Promise<DocumentLocator[]> {
      const prefix = type ? `${type}s/` : "";
      return [...this.documents.keys()]
         .filter((path) =>
            path.split("/").slice(2).join("/").startsWith(prefix),
         )
         .map((path) => ({
            workspace: this.workspace.name,
            type:
               type ??
               (path.includes("/dashboards/") ? "dashboard" : "notebook"),
            path,
         }));
   }

   async getDocument(locator: DocumentLocator): Promise<string> {
      const content = this.documents.get(locator.path);
      if (content === undefined)
         throw new DocumentNotFoundError(`No document at ${locator.path}`);
      return content;
   }

   async saveDocument(
      locator: DocumentLocator,
      content: string,
   ): Promise<void> {
      this.documents.set(locator.path, content);
      this.saves.push({ locator, content });
   }

   async deleteDocument(locator: DocumentLocator): Promise<void> {
      if (!this.documents.delete(locator.path))
         throw new DocumentNotFoundError(`No document at ${locator.path}`);
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
