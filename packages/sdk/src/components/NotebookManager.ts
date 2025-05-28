import type {
   NotebookStorage,
   UserContext,
} from "./MutableNotebook/NotebookStorage";

/**
 * Interface representing the data structure of a Mutable Notebook
 * @interface NotebookData
 * @property {string[]} models - Array of model paths used in the notebook
 * @property {NotebookCellValue[]} cells - Array of cells in the notebook
 * @property {string} notebookPath - Path to the notebook file (relative to project/package)
 */
export interface NotebookData {
   models: string[];
   cells: NotebookCellValue[];
   notebookPath: string;
}

/**
 * Interface representing a cell in the notebook
 * @interface NotebookCellValue
 * @property {boolean} isMarkdown - Whether the cell is a markdown cell
 * @property {string} [value] - The content of the cell
 * @property {string} [result] - The result of executing the cell
 * @property {string} [modelPath] - modelPath associated with the query in the cell
 * @property {string} [sourceName] - Name of the source associated with the cell
 * @property {string} [queryInfo] - Information about the query in the cell
 */
export interface NotebookCellValue {
   isMarkdown: boolean;
   value?: string;
   result?: string;
   modelPath?: string;
   sourceName?: string;
   queryInfo?: string;
}

/**
 * Class for managing notebook operations
 * @class NotebookManager
 */
export class NotebookManager {
   private isSaved: boolean;
   private notebookStorage: NotebookStorage;
   private userContext: UserContext;

   /**
    * Creates a new NotebookManager instance
    * @param {NotebookStorage} notebookStorage - Storage implementation
    * @param {UserContext} userContext - User context for storage
    * @param {NotebookData} notebookData - Initial notebook data
    */
   constructor(
      notebookStorage: NotebookStorage,
      userContext: UserContext,
      private notebookData: NotebookData,
   ) {
      this.notebookStorage = notebookStorage;
      this.userContext = userContext;
      if (this.notebookData) {
         this.isSaved = true;
      } else {
         this.notebookData = {
            models: [],
            cells: [],
            notebookPath: undefined,
         };
         this.isSaved = false;
      }
   }

   /**
    * Gets the current notebook data
    * @returns {NotebookData} The current notebook data
    */
   getNotebookData(): NotebookData {
      return this.notebookData;
   }

   /**
    * Gets the current notebook path
    * @returns {string} The path to the notebook
    */
   getNotebookPath(): string {
      return this.notebookData.notebookPath;
   }

   /**
    * Renames the notebook and updates storage
    * @param {string} notebookPath - New path for the notebook
    * @returns {NotebookManager} The updated NotebookManager instance
    */
   renameNotebook(notebookPath: string): NotebookManager {
      if (this.notebookData.notebookPath !== notebookPath) {
         try {
            this.notebookStorage.deleteNotebook(
               this.userContext,
               this.notebookData.notebookPath,
            );
         } catch {
            // ignore if not found
         }
      }
      this.notebookData.notebookPath = notebookPath;
      this.isSaved = false;
      this.saveNotebook();
      return this;
   }

   getCells(): NotebookCellValue[] {
      return this.notebookData.cells;
   }
   deleteCell(index: number): NotebookManager {
      this.notebookData.cells = [
         ...this.notebookData.cells.slice(0, index),
         ...this.notebookData.cells.slice(index + 1),
      ];
      this.isSaved = false;
      return this;
   }
   insertCell(index: number, cell: NotebookCellValue): NotebookManager {
      this.notebookData.cells = [
         ...this.notebookData.cells.slice(0, index),
         cell,
         ...this.notebookData.cells.slice(index),
      ];
      this.isSaved = false;
      return this;
   }
   setCell(index: number, cell: NotebookCellValue): NotebookManager {
      this.notebookData.cells[index] = cell;
      this.isSaved = false;
      return this;
   }
   setModels(models: string[]): NotebookManager {
      this.notebookData.models = models;
      this.isSaved = false;
      return this;
   }
   getModels(): string[] {
      return this.notebookData.models;
   }

   updateNotebookData(notebookData: NotebookData): NotebookManager {
      this.notebookData = notebookData;
      this.isSaved = false;
      return this;
   }

   saveNotebook(): NotebookManager {
      if (!this.isSaved) {
         if (!this.notebookData.notebookPath) {
            throw new Error("Notebook path is not set");
         }
         this.notebookStorage.saveNotebook(
            this.userContext,
            this.notebookData.notebookPath,
            JSON.stringify(this.notebookData),
         );
         this.isSaved = true;
      }
      return new NotebookManager(
         this.notebookStorage,
         this.userContext,
         this.notebookData,
      );
   }

   /**
    * Converts the notebook data to a Malloy notebook string.
    * @returns {string} The Malloy notebook string
    */
   toMalloyNotebook(): string {
      return this.notebookData.cells
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

   static newNotebook(
      notebookStorage: NotebookStorage,
      userContext: UserContext,
   ): NotebookManager {
      return new NotebookManager(notebookStorage, userContext, undefined);
   }

   /**
    * Creates a new notebook manager by loading from local storage.
    * Returns an empty instance if the notebook is not found.
    * @param notebookStorage - The storage implementation
    * @param userContext - The user context for storage
    * @param notebookPath - The path to the notebook file (relative to project/package)
    */
   static loadNotebook(
      notebookStorage: NotebookStorage,
      userContext: UserContext,
      notebookPath: string,
   ): NotebookManager {
      let notebookData: NotebookData | undefined = undefined;
      try {
         const saved = notebookStorage.getNotebook(userContext, notebookPath);
         if (saved) {
            notebookData = JSON.parse(saved);
         }
      } catch {
         // Not found, return empty
      }
      return new NotebookManager(notebookStorage, userContext, notebookData);
   }
}
