import { PackageContextProps } from "../Package";

export interface WorkbookStorage {
   // Lists all available workbooks for the context.
   // Workbooks names are like S3 paths- / denote hierarchical
   // folders, but otherwise folders are not "real" objects
   listWorkbooks(context: PackageContextProps): Promise<string[]>;

   // Returns the workbook at the specific path, throws an exception if no such workbook exists (or cannot be accessed)
   getWorkbook(context: PackageContextProps, path: string): Promise<string>;

   // Deletes the workbook at the specified path, or throws an
   // Exception on failure
   deleteWorkbook(context: PackageContextProps, path: string): Promise<void>;

   saveWorkbook(
      context: PackageContextProps,
      path: string,
      workbook: string,
   ): Promise<void>;

   // Moves workbook from the "from" path to the "to" path
   moveWorkbook(
      context: PackageContextProps,
      from: string,
      to: string,
   ): Promise<void>;
}
