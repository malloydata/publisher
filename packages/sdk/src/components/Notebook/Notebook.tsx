import "@malloydata/malloy-explorer/styles.css";
import { Stack, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import { RawNotebook } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";

import { parseResourceUri } from "../../utils/formatting";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";
import { NotebookCell } from "./NotebookCell";
import { EnhancedNotebookCell } from "./types";

interface NotebookProps {
   resourceUri: string;
   maxResultSize?: number;
}

// Requires PackageProvider
export default function Notebook({
   resourceUri,
   maxResultSize = 0,
}: NotebookProps) {
   const { apiClients } = useServer();
   const {
      projectName,
      packageName,
      versionId,
      modelPath: notebookPath,
   } = parseResourceUri(resourceUri);

   // Fetch the raw notebook cells
   const {
      data: notebook,
      isSuccess,
      isError,
      error,
   } = useQueryWithApiError<RawNotebook>({
      queryKey: [resourceUri],
      queryFn: async () => {
         const response = await apiClients.notebooks.getNotebook(
            projectName,
            packageName,
            notebookPath,
            versionId,
         );
         return response.data;
      },
   });

   // State to store executed cells with results
   const [enhancedCells, setEnhancedCells] = useState<EnhancedNotebookCell[]>(
      [],
   );
   const [isExecuting, setIsExecuting] = useState(false);
   const [executionError, setExecutionError] = useState<Error | null>(null);

   // Execute cells sequentially when notebook is loaded
   useEffect(() => {
      if (!isSuccess || !notebook.notebookCells) return;

      const executeCells = async () => {
         setIsExecuting(true);
         setExecutionError(null);
         const cells: EnhancedNotebookCell[] = [];

         try {
            // Execute cells sequentially
            for (let i = 0; i < notebook.notebookCells.length; i++) {
               const rawCell = notebook.notebookCells[i];

               // Markdown cells don't need execution - use raw content directly
               if (rawCell.type === "markdown") {
                  cells.push({
                     type: rawCell.type,
                     text: rawCell.text,
                  });
                  continue;
               }

               // Execute code cells
               try {
                  // Call the executeNotebookCell API
                  const response = await fetch(
                     `/api/v0/projects/${projectName}/packages/${packageName}/notebooks/${notebookPath}/cells/${i}${versionId ? `?versionId=${versionId}` : ""}`,
                     {
                        method: "GET",
                        credentials: "include",
                     },
                  );

                  if (!response.ok) {
                     throw new Error(
                        `Failed to execute cell ${i}: ${response.statusText}`,
                     );
                  }

                  const executedCell = await response.json();

                  // Combine raw cell with execution results
                  cells.push({
                     type: rawCell.type,
                     text: rawCell.text,
                     queryName: executedCell.queryName,
                     result: executedCell.result,
                     newSources: executedCell.newSources,
                  });
               } catch (cellError) {
                  // If a cell fails, add it without execution results
                  console.error(`Error executing cell ${i}:`, cellError);
                  cells.push({
                     type: rawCell.type,
                     text: rawCell.text,
                  });
               }
            }

            setEnhancedCells(cells);
         } catch (error) {
            console.error("Error executing notebook cells:", error);
            setExecutionError(error as Error);
         } finally {
            setIsExecuting(false);
         }
      };

      executeCells();
   }, [isSuccess, notebook, projectName, packageName, notebookPath, versionId]);

   return (
      <CleanNotebookContainer>
         <CleanNotebookSection>
            <Stack spacing={3} component="section">
               {(!isSuccess || isExecuting) && !isError && (
                  <Loading
                     text={
                        isExecuting
                           ? "Executing Notebook..."
                           : "Fetching Notebook..."
                     }
                  />
               )}
               {isSuccess &&
                  !isExecuting &&
                  enhancedCells.map((cell, index) => (
                     <NotebookCell
                        cell={cell}
                        key={index}
                        index={index}
                        resourceUri={resourceUri}
                        maxResultSize={maxResultSize}
                     />
                  ))}
               {isError && error.status === 404 && (
                  <Typography variant="body2" sx={{ color: "#666666" }}>
                     <code>{`${projectName} > ${packageName} > ${notebookPath}`}</code>{" "}
                     not found.
                  </Typography>
               )}

               {isError && error.status !== 404 && (
                  <ApiErrorDisplay
                     error={error}
                     context={`${projectName} > ${packageName} > ${notebookPath}`}
                  />
               )}

               {executionError && (
                  <ApiErrorDisplay
                     error={{
                        message: executionError.message,
                        status: 500,
                        name: "ExecutionError",
                     }}
                     context="Notebook Execution"
                  />
               )}
            </Stack>
         </CleanNotebookSection>
      </CleanNotebookContainer>
   );
}
