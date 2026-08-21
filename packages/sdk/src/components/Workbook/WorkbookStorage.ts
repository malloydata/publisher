// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

export interface Workspace {
   name: string;
   writeable: boolean;
   description: string;
}

export interface WorkbookLocator {
   path: string;
   workspace: string;
}

export interface WorkbookStorage {
   // Lists all available workspaces for the context.
   listWorkspaces(writeableOnly: boolean): Promise<Workspace[]>;

   // Lists all available workbooks for the context.
   // Workbooks names are like S3 paths- / denote hierarchical
   // folders, but otherwise folders are not "real" objects
   listWorkbooks(workspace: Workspace): Promise<WorkbookLocator[]>;

   // Returns the workbook at the specific path, throws an exception if no such workbook exists (or cannot be accessed)
   getWorkbook(path: WorkbookLocator): Promise<string>;

   // Deletes the workbook at the specified path, or throws an
   // Exception on failure
   deleteWorkbook(path: WorkbookLocator): Promise<void>;

   saveWorkbook(path: WorkbookLocator, workbook: string): Promise<void>;

   // Moves workbook from the "from" path to the "to" path
   moveWorkbook(from: WorkbookLocator, to: WorkbookLocator): Promise<void>;
}
