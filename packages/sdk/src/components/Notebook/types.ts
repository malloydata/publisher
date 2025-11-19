import { NotebookCell as ClientNotebookCell } from "../../client";

/**
 * Enhanced notebook cell that extends the base API cell with execution results.
 * The base ClientNotebookCell only contains type and text (raw content).
 * This interface adds the runtime execution data.
 */
export interface EnhancedNotebookCell extends ClientNotebookCell {
   /**
    * Name of the query that was executed (for code cells that run queries)
    */
   queryName?: string;

   /**
    * JSON string containing the query execution results
    */
   result?: string;

   /**
    * Array of JSON strings containing SourceInfo objects for data sources
    * that become available in this cell (e.g., from imports or source definitions)
    */
   newSources?: string[];
}
