import "@malloydata/malloy-explorer/styles.css";
import * as Malloy from "@malloydata/malloy-interfaces";
import { Box, Paper, Stack, Typography } from "@mui/material";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { RawNotebook } from "../../client";
import { useDimensionalFilterRangeData } from "../../hooks/useDimensionalFilterRangeData";
import {
   FilterSelection,
   useDimensionFilters,
} from "../../hooks/useDimensionFilters";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { DimensionFilter, RetrievalFunction } from "../filter/DimensionFilter";
import {
   extractDimensionSpecs,
   extractSourceFromQuery,
   generateFilterClause,
   getJoinedSources,
   injectWhereClause,
   parseAllSourceInfos,
   parseNotebookFilterAnnotation,
} from "../filter/utils";

import { parseResourceUri } from "../../utils/formatting";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";
import { NotebookCell } from "./NotebookCell";
import { EnhancedNotebookCell } from "./types";

interface NotebookProps {
   resourceUri: string;
   maxResultSize?: number;
   /** Optional retrieval function for semantic search filters */
   retrievalFn?: RetrievalFunction;
}

// Requires PackageProvider
export default function Notebook({
   resourceUri,
   maxResultSize = 0,
   retrievalFn,
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

   // Parse filter configuration from notebook annotations
   const filterConfig = useMemo(() => {
      if (!notebook) return null;
      return parseNotebookFilterAnnotation(notebook.annotations);
   }, [notebook]);

   // Parse all SourceInfos from notebook cells and create a map
   const sourceData = useMemo(() => {
      if (!notebook?.notebookCells) return null;
      return parseAllSourceInfos(notebook.notebookCells);
   }, [notebook]);

   const sourceInfoMap = useMemo(
      () => sourceData?.sourceInfoMap ?? new Map<string, Malloy.SourceInfo>(),
      [sourceData],
   );
   const modelPath = sourceData?.modelPath ?? null;

   // Build dimension specs from filter config and source info map
   // Each spec includes source and model for proper query routing
   const dimensionSpecs = useMemo(() => {
      if (!filterConfig || sourceInfoMap.size === 0 || !modelPath) return [];
      return extractDimensionSpecs(
         sourceInfoMap,
         filterConfig.filters,
         modelPath,
      );
   }, [filterConfig, sourceInfoMap, modelPath]);

   // Initialize dimension filters hook
   const { filterStates, updateFilter, getActiveFilters } = useDimensionFilters(
      {
         dimensionSpecs,
      },
   );

   // Get active filters - include filterStates in deps to ensure updates when individual items change
   const activeFilters = useMemo(
      () => getActiveFilters(),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [filterStates, getActiveFilters],
   );

   // Create a map of dimension name -> source name for quick lookup
   const dimensionToSourceMap = useMemo(() => {
      const map = new Map<string, string>();
      for (const spec of dimensionSpecs) {
         map.set(spec.dimensionName, spec.source);
      }
      return map;
   }, [dimensionSpecs]);

   // Create a map of source name -> set of joined source names
   const sourceJoinsMap = useMemo(() => {
      const map = new Map<string, Set<string>>();
      for (const [sourceName, sourceInfo] of sourceInfoMap) {
         map.set(sourceName, getJoinedSources(sourceInfo));
      }
      return map;
   }, [sourceInfoMap]);

   // Fetch filter range data when we have dimension specs
   // The hook now handles multiple source/model combos internally
   const { data: filterValuesData } = useDimensionalFilterRangeData({
      project: projectName,
      package: packageName,
      dimensionSpecs,
      versionId,
      enabled: dimensionSpecs.length > 0,
      activeFilters,
   });

   // Unified cell execution function
   // Executes all notebook cells, optionally applying filters to query cells
   const executeCells = useCallback(
      async (filtersToApply: FilterSelection[] = []) => {
         if (!isSuccess || !notebook?.notebookCells) return;

         // Initialize or reset cells
         setEnhancedCells((prev) => {
            if (prev.length === 0) {
               return notebook.notebookCells.map((cell) => ({ ...cell }));
            }
            return prev.map((cell) => ({
               ...cell,
               result: undefined,
            }));
         });

         setIsExecuting(true);
         setExecutionError(null);

         try {
            // Execute cells sequentially
            for (let i = 0; i < notebook.notebookCells.length; i++) {
               const rawCell = notebook.notebookCells[i];

               // Markdown cells don't need execution
               if (rawCell.type === "markdown") continue;

               // Execute code cells
               const cellText = rawCell.text || "";
               const hasQuery =
                  cellText.includes("run:") ||
                  cellText.includes("->") ||
                  /^\s*(run|query)\s*:/m.test(cellText);

               try {
                  let result: string | undefined;
                  let newSources: string[] | undefined;

                  if (hasQuery && modelPath && filtersToApply.length > 0) {
                     // Query cell - use models API with optional filters
                     let queryToExecute = cellText;

                     // Apply filters if any match this query's source
                     if (filtersToApply.length > 0) {
                        const querySourceName =
                           extractSourceFromQuery(cellText);

                        // Get the set of joined sources for this query's source
                        const joinedSources =
                           (querySourceName &&
                              sourceJoinsMap.get(querySourceName)) ||
                           new Set<string>();

                        // Filter to only include those matching this query's source or joined sources
                        const filtersForSource = querySourceName
                           ? filtersToApply.filter((filter) => {
                                const filterSourceName =
                                   dimensionToSourceMap.get(
                                      filter.dimensionName,
                                   );
                                if (!filterSourceName) return false;
                                return (
                                   filterSourceName === querySourceName ||
                                   joinedSources.has(filterSourceName)
                                );
                             })
                           : [];

                        if (filtersForSource.length > 0) {
                           const filterClause = generateFilterClause(
                              filtersForSource,
                              dimensionToSourceMap,
                              querySourceName,
                           );
                           if (filterClause) {
                              queryToExecute = injectWhereClause(
                                 cellText,
                                 filterClause,
                              );
                           }
                        }
                     }

                     // Execute using models API
                     const response = await apiClients.models.executeQueryModel(
                        projectName,
                        packageName,
                        modelPath,
                        {
                           query: queryToExecute,
                           versionId,
                        },
                     );
                     result = response.data.result;
                  } else {
                     // Non-query code cell (or no filters applied) - use notebook cell execution API
                     const response =
                        await apiClients.notebooks.executeNotebookCell(
                           projectName,
                           packageName,
                           notebookPath,
                           i,
                           versionId,
                        );

                     const executedCell = response.data;
                     result = executedCell.result;
                     newSources = rawCell.newSources || executedCell.newSources;
                  }

                  // Update state incrementally
                  setEnhancedCells((prev) => {
                     const next = [...prev];
                     // Ensure we have a cell to update (in case state was reset externally, though unlikely)
                     if (!next[i]) {
                        next[i] = { ...rawCell };
                     }
                     next[i] = {
                        ...next[i],
                        result,
                        newSources,
                     };
                     return next;
                  });
               } catch (cellError) {
                  console.error(`Error executing cell ${i}:`, cellError);
                  // Don't update result on error, leave as is (undefined)
               }
            }
         } catch (error) {
            console.error("Error executing notebook cells:", error);
            setExecutionError(error as Error);
         } finally {
            setIsExecuting(false);
         }
      },
      [
         isSuccess,
         notebook,
         modelPath,
         dimensionToSourceMap,
         sourceJoinsMap,
         projectName,
         packageName,
         notebookPath,
         versionId,
         apiClients.models,
         apiClients.notebooks,
      ],
   );

   // Execute cells when notebook is loaded (no filters initially)
   useEffect(() => {
      if (!isSuccess || !notebook?.notebookCells) return;
      executeCells([]);
   }, [isSuccess, notebook, executeCells]);

   // Re-execute when filters change
   // Track previous activeFilters to detect actual changes (not just reference changes)
   const prevActiveFiltersRef = useRef<string>("");

   useEffect(() => {
      // Serialize activeFilters to detect actual value changes
      const serialized = JSON.stringify(
         activeFilters.map((f) => ({
            dim: f.dimensionName,
            type: f.matchType,
            val: f.value,
            val2: f.value2,
         })),
      );

      // Skip if no actual change or if this is the initial empty state
      if (serialized === prevActiveFiltersRef.current) {
         return;
      }

      // Skip the initial render (when prevActiveFiltersRef is empty and filters are also empty)
      if (prevActiveFiltersRef.current === "" && activeFilters.length === 0) {
         prevActiveFiltersRef.current = serialized;
         return;
      }

      prevActiveFiltersRef.current = serialized;

      // Re-execute with current filters (or no filters if cleared)
      if (!isExecuting) {
         executeCells(activeFilters);
      }
   }, [activeFilters, isExecuting, executeCells]);

   // Handle filter change
   const handleFilterChange = useCallback(
      (dimensionName: string) => (selection: FilterSelection | null) => {
         updateFilter(dimensionName, selection);
      },
      [updateFilter],
   );

   // Check if retrieval is supported
   const hasRetrievalFilters = dimensionSpecs.some(
      (spec) => spec.filterType === "Retrieval",
   );
   const _retrievalSupported = !hasRetrievalFilters || !!retrievalFn;

   return (
      <CleanNotebookContainer>
         <CleanNotebookSection>
            <Stack spacing={3} component="section">
               {/* Filter Panel */}
               {dimensionSpecs.length > 0 && filterValuesData && (
                  <Paper
                     elevation={0}
                     sx={{
                        p: 3,
                        backgroundColor: "#ffffff",
                        border: "1px solid #f0f0f0",
                        borderRadius: 2,
                        boxShadow: "0 1px 3px rgba(0, 0, 0, 0.04)",
                        transition: "box-shadow 0.2s ease-in-out",
                        "&:hover": {
                           boxShadow: "0 2px 6px rgba(0, 0, 0, 0.08)",
                        },
                     }}
                  >
                     <Typography
                        variant="subtitle2"
                        sx={{
                           fontWeight: 600,
                           mb: 2,
                           color: "#333",
                        }}
                     >
                        Filters
                     </Typography>
                     <Box
                        sx={{
                           display: "grid",
                           gridTemplateColumns:
                              "repeat(auto-fill, minmax(250px, 1fr))",
                           gap: 3,
                        }}
                     >
                        {dimensionSpecs.map((spec) => {
                           const values =
                              filterValuesData.get(spec.dimensionName) || [];
                           const filterState = filterStates.get(
                              spec.dimensionName,
                           );
                           // Skip Retrieval filters if no retrievalFn provided
                           if (
                              spec.filterType === "Retrieval" &&
                              !retrievalFn
                           ) {
                              return null;
                           }

                           return (
                              <Box key={spec.dimensionName}>
                                 <DimensionFilter
                                    spec={spec}
                                    values={values}
                                    selection={filterState?.selection}
                                    onChange={handleFilterChange(
                                       spec.dimensionName,
                                    )}
                                    retrievalFn={retrievalFn}
                                 />
                              </Box>
                           );
                        })}
                     </Box>
                  </Paper>
               )}

               {/* Loading State */}
               {!isSuccess && !isError && (
                  <Loading text={"Fetching Notebook..."} />
               )}

               {/* Notebook Cells */}
               {isSuccess &&
                  (enhancedCells.length > 0
                     ? enhancedCells
                     : notebook?.notebookCells || []
                  ).map((cell, index) => (
                     <NotebookCell
                        cell={cell as EnhancedNotebookCell}
                        key={index}
                        index={index}
                        resourceUri={resourceUri}
                        maxResultSize={maxResultSize}
                        isExecuting={isExecuting}
                     />
                  ))}

               {/* Error States */}
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
