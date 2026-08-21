// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   WorkbookLocator,
   WorkbookStorage,
   Workspace,
} from "./WorkbookStorage";

const LOCAL_WORKSPACE_NAME = "Local";

export class BrowserWorkbookStorage implements WorkbookStorage {
   async listWorkspaces(writeableOnly: boolean): Promise<Workspace[]> {
      // Parameters not used in browser storage implementation
      void writeableOnly;
      return Promise.resolve([
         {
            name: LOCAL_WORKSPACE_NAME,
            description: "Stored locally- only accessible to this browser",
            writeable: true,
         },
      ]);
   }

   async listWorkbooks(_workspace: Workspace): Promise<WorkbookLocator[]> {
      const keys: WorkbookLocator[] = [];
      for (let i = 0; i < localStorage.length; i++) {
         const key = localStorage.key(i);
         keys.push({ path: key, workspace: LOCAL_WORKSPACE_NAME });
      }
      return keys;
   }

   async getWorkbook(path: WorkbookLocator): Promise<string> {
      const notebook = localStorage.getItem(path.path);
      if (notebook === null) {
         throw new Error(`Notebook not found at path: ${path}`);
      }
      return notebook;
   }

   async deleteWorkbook(path: WorkbookLocator): Promise<void> {
      if (localStorage.getItem(path.path) === null) {
         throw new Error(`Notebook not found at path: ${path}`);
      }
      localStorage.removeItem(path.path);
   }

   async saveWorkbook(path: WorkbookLocator, notebook: string): Promise<void> {
      localStorage.setItem(path.path, notebook);
   }

   async moveWorkbook(
      from: WorkbookLocator,
      to: WorkbookLocator,
   ): Promise<void> {
      const notebook = localStorage.getItem(from.path);
      if (notebook === null) {
         throw new Error(`Notebook not found at path: ${from.path}`);
      }
      localStorage.setItem(to.path, notebook);
      localStorage.removeItem(from.path);
   }
}
