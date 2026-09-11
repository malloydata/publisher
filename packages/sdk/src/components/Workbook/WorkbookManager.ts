// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { WorkbookLocator, WorkbookStorage } from "./WorkbookStorage";

/**
 * Interface representing the data structure of a Mutable Workbook
 * @interface WorkbookData
 * @property {string[]} models - Array of model paths used in the workbook
 * @property {WorkbookCellValue[]} cells - Array of cells in the workbook
 * @property {WorkbookLocator} workbookPath - Path to the workbook file (relative to project/package)
 */
export interface WorkbookData {
   models: string[];
   cells: WorkbookCellValue[];
   workbookPath: WorkbookLocator;
}

/**
 * Interface representing a cell in the workbook
 * @interface WorkbookCellValue
 * @property {boolean} isMarkdown - Whether the cell is a markdown cell
 * @property {string} [value] - The content of the cell
 * @property {string} [result] - The result of executing the cell
 * @property {string} [modelPath] - modelPath associated with the query in the cell
 * @property {string} [sourceName] - Name of the source associated with the cell
 * @property {string} [queryInfo] - Information about the query in the cell
 */
export interface WorkbookCellValue {
   isMarkdown: boolean;
   value?: string;
   result?: string;
   modelPath?: string;
   sourceName?: string;
   queryInfo?: string;
}

/**
 * Class for managing workbook operations
 * @class WorkbookManager
 */
export class WorkbookManager {
   private isSaved: boolean;
   private workbookStorage: WorkbookStorage;

   /**
    * Creates a new WorkbookManager instance
    * @param {WorkbookStorage} workbookStorage - Storage implementation
    * @param {WorkbookData} workbookData - Initial workbook data
    */
   constructor(
      workbookStorage: WorkbookStorage,
      private workbookData: WorkbookData,
   ) {
      this.workbookStorage = workbookStorage;
      if (this.workbookData) {
         this.isSaved = true;
      } else {
         this.workbookData = {
            models: [],
            cells: [],
            workbookPath: undefined,
         };
         this.isSaved = false;
      }
   }

   /**
    * Gets the current workbook data
    * @returns {WorkbookData} The current workbook data
    */
   getWorkbookData(): WorkbookData {
      return this.workbookData;
   }

   /**
    * Gets the current workbook path
    * @returns {string} The path to the workbook
    */
   getWorkbookPath(): WorkbookLocator {
      return this.workbookData.workbookPath;
   }

   /**
    * Renames the workbook and updates storage
    * @param {string} workbookPath - New path for the workbook
    * @returns {WorkbookManager} The updated WorkbookManager instance
    */
   async renameWorkbook(workbookPath: string): Promise<WorkbookManager> {
      if (this.workbookData.workbookPath.path !== workbookPath) {
         try {
            await this.workbookStorage.moveWorkbook(
               this.workbookData.workbookPath,
               {
                  path: workbookPath,
                  workspace: this.workbookData.workbookPath.workspace,
               },
            );
         } catch {
            // ignore if not found
         }
      }
      this.workbookData.workbookPath.path = workbookPath;
      this.isSaved = false;
      return await this.saveWorkbook();
   }

   getCells(): WorkbookCellValue[] {
      return this.workbookData.cells;
   }
   deleteCell(index: number): WorkbookManager {
      this.workbookData.cells = [
         ...this.workbookData.cells.slice(0, index),
         ...this.workbookData.cells.slice(index + 1),
      ];
      this.isSaved = false;
      return this;
   }
   insertCell(index: number, cell: WorkbookCellValue): WorkbookManager {
      this.workbookData.cells = [
         ...this.workbookData.cells.slice(0, index),
         cell,
         ...this.workbookData.cells.slice(index),
      ];
      this.isSaved = false;
      return this;
   }
   setCell(index: number, cell: WorkbookCellValue): WorkbookManager {
      this.workbookData.cells[index] = cell;
      this.isSaved = false;
      return this;
   }
   setModels(models: string[]): WorkbookManager {
      this.workbookData.models = models;
      this.isSaved = false;
      return this;
   }
   getModels(): string[] {
      return this.workbookData.models;
   }

   updateWorkbookData(workbookData: WorkbookData): WorkbookManager {
      this.workbookData = workbookData;
      this.isSaved = false;
      return this;
   }

   async saveWorkbook(): Promise<WorkbookManager> {
      if (!this.isSaved) {
         if (!this.workbookData.workbookPath) {
            throw new Error("Workbook path is not set");
         }
         await this.workbookStorage.saveWorkbook(
            this.workbookData.workbookPath,
            JSON.stringify(this.workbookData),
         );
         this.isSaved = true;
      }
      return new WorkbookManager(this.workbookStorage, this.workbookData);
   }

   /**
    * Converts the workbook data to a Malloy workbook string.
    * @returns {string} The Malloy workbook string
    */
   toMalloyWorkbook(): string {
      return this.workbookData.cells
         .map((cell) => {
            if (cell.isMarkdown) {
               return ">>>markdown\n" + cell.value;
            } else {
               return (
                  ">>>malloy\n" +
                  `import {${cell.sourceName}}" from '${cell.modelPath}'"\n` +
                  cell.value +
                  "\n"
               );
            }
         })
         .join("\n");
   }

   static newWorkbook(workbookStorage: WorkbookStorage): WorkbookManager {
      return new WorkbookManager(workbookStorage, undefined);
   }

   /**
    * Creates a new workbook manager by loading from local storage.
    * Returns an empty instance if the workbook is not found.
    * @param workbookStorage - The storage implementation
    * @param userContext - The user context for storage
    * @param workbookPath - The path to the workbook file (relative to project/package)
    */
   static async loadWorkbook(
      workbookStorage: WorkbookStorage,
      workbookPath: WorkbookLocator,
   ): Promise<WorkbookManager> {
      let workbookData: WorkbookData | undefined = undefined;
      try {
         const saved = await workbookStorage.getWorkbook(workbookPath);
         if (saved) {
            workbookData = JSON.parse(saved);
         }
      } catch {
         // Not found, create a new workbook
         workbookData = {
            models: [],
            cells: [],
            workbookPath: workbookPath,
         };
      }
      return new WorkbookManager(workbookStorage, workbookData);
   }
}
