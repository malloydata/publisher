import "@malloydata/malloy-explorer/styles.css";
import * as Malloy from "@malloydata/malloy-interfaces";
import { Box, Paper, Stack, Typography } from "@mui/material";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { RawNotebook, Source } from "../../client";
import {
   getDimensionKey,
   useDimensionalFilterRangeData,
} from "../../hooks/useDimensionalFilterRangeData";
import {
   FilterSelection,
   useDimensionFilters,
} from "../../hooks/useDimensionFilters";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { DimensionFilter, RetrievalFunction } from "../filter/DimensionFilter";
import {
   extractDimensionSpecs,
   parseAllSourceInfos,
   parseNotebookFilterAnnotation,
} from "../filter/utils";

import { parseResourceUri } from "../../utils/formatting";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";
import { NotebookCell } from "./NotebookCell";
import { EnhancedNotebookCell } from "./types";

// Maximum number of concurrent cell executions to avoid overwhelming the server
const MAX_CONCURRENT = 4;

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

   // Parse filter configuration from notebook annotations (legacy ##(filters) approach)
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

   // Extract server-side filter definitions from notebook sources
   // These come from #(filter) annotations parsed by the server
   const serverFilters = useMemo(() => {
      const result = new Map<string, Source["filters"]>();
      if (!notebook?.sources) return result;
      for (const source of notebook.sources as Source[]) {
         if (source.name && source.filters && source.filters.length > 0) {
            // Exclude implicit filters from the UI
            const visibleFilters = source.filters.filter((f) => !f.implicit);
            if (visibleFilters.length > 0) {
               result.set(source.name, visibleFilters);
            }
         }
      }
      return result;
   }, [notebook]);

   // Determine if we're using server-driven filters (#(filter)) or legacy (##(filters))
   const useServerFilters = serverFilters.size > 0;

   // Build dimension specs from filter config and source info map
   // Each spec includes source and model for proper query routing
   const dimensionSpecs = useMemo(() => {
      if (useServerFilters && modelPath) {
         // Server-driven: build specs from #(filter) metadata
         const specs: import("../../hooks/useDimensionalFilterRangeData").DimensionSpec[] =
            [];
         for (const [sourceName, filters] of serverFilters) {
            for (const filter of filters ?? []) {
               if (!filter.dimension || !filter.type) continue;

               // Choose widget type based on the dimension's data type first,
               // then fall back to the annotation's comparator type
               type FT =
                  import("../../hooks/useDimensionalFilterRangeData").FilterType;
               let filterType: FT;
               const dimType = filter.dimensionType;
               if (dimType === "boolean") {
                  filterType = "Boolean";
               } else if (
                  dimType === "date" ||
                  dimType === "timestamp" ||
                  dimType === "timestamptz"
               ) {
                  filterType = "DateMinMax";
               } else if (dimType === "number") {
                  filterType =
                     filter.type === "equal" || filter.type === "in"
                        ? "Star"
                        : "MinMax";
               } else {
                  const filterTypeMap: Record<string, FT> = {
                     equal: "Star",
                     in: "Star",
                     like: "Star",
                     greater_than: "MinMax",
                     less_than: "MinMax",
                  };
                  filterType = filterTypeMap[filter.type] ?? "Star";
               }

               // Derive the match type from the #(filter) annotation type so
               // the UI never needs to show a match-type dropdown.
               type MT = import("../../hooks/useDimensionFilters").MatchType;
               const matchTypeMap: Record<string, MT> = {
                  equal: "Equals",
                  in: "Equals",
                  like: "Contains",
                  greater_than:
                     filterType === "DateMinMax" ? "After" : "Greater Than",
                  less_than:
                     filterType === "DateMinMax" ? "Before" : "Less Than",
               };
               const defaultMatchType: MT | undefined =
                  matchTypeMap[filter.type!];

               const filterLabel =
                  filter.name !== filter.dimension ? filter.name : undefined;
               specs.push({
                  source: sourceName,
                  model: modelPath,
                  dimensionName: filter.dimension!,
                  filterType,
                  label: filterLabel,
                  filterName: filter.name ?? filter.dimension!,
                  defaultMatchType,
                  required: filter.required ?? false,
               });
            }
         }
         return specs;
      }
      // Legacy: use ##(filters) + #(filter) annotation approach
      if (!filterConfig || sourceInfoMap.size === 0 || !modelPath) return [];
      return extractDimensionSpecs(
         sourceInfoMap,
         filterConfig.filters,
         modelPath,
      );
   }, [
      useServerFilters,
      serverFilters,
      filterConfig,
      sourceInfoMap,
      modelPath,
   ]);

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

   // Create a map of dimension key -> source name for quick lookup (used by filter UI)
   const _dimensionToSourceMap = useMemo(() => {
      const map = new Map<string, string>();
      for (const spec of dimensionSpecs) {
         const key = getDimensionKey(spec);
         map.set(key, spec.source);
      }
      return map;
   }, [dimensionSpecs]);

   // Fetch filter range data when we have dimension specs.
   // Do NOT pass activeFilters here — the index query should return all
   // possible values for each dimension, not just those matching the
   // current selection. Otherwise selecting "FORD" would hide every other
   // manufacturer from the dropdown.
   const { data: filterValuesData } = useDimensionalFilterRangeData({
      project: projectName,
      package: packageName,
      dimensionSpecs,
      versionId,
      enabled: dimensionSpecs.length > 0,
   });

   /**
    * Convert active FilterSelections into a flat { filterName: value } map
    * suitable for the server's filter_params parameter.
    * Uses filterName from the selection (propagated from the spec) as the
    * API param key, falling back to dimensionName.
    */
   const buildFilterParams = useCallback(
      (filtersToApply: FilterSelection[]): string | undefined => {
         if (filtersToApply.length === 0) return undefined;

         const toParamString = (v: unknown): string => {
            if (v instanceof Date) {
               return v.toISOString().slice(0, 10);
            }
            return String(v);
         };

         const params: { [key: string]: string | string[] } = {};
         for (const f of filtersToApply) {
            const paramName = f.filterName ?? f.dimensionName;
            const val = f.value;
            if (Array.isArray(val)) {
               params[paramName] = val.map(toParamString);
            } else if (val !== undefined && val !== null) {
               params[paramName] = toParamString(val);
            }
         }
         return Object.keys(params).length > 0
            ? JSON.stringify(params)
            : undefined;
      },
      [],
   );

   // Unified cell execution function
   // Executes all notebook cells, passing server-side filter params when available
   // Runs up to 4 requests in parallel for better performance
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

         const filterParams = useServerFilters
            ? buildFilterParams(filtersToApply)
            : undefined;

         try {
            // Build execution tasks for code cells
            const executionTasks: Array<() => Promise<void>> = [];

            for (let i = 0; i < notebook.notebookCells.length; i++) {
               const rawCell = notebook.notebookCells[i];

               // Markdown cells don't need execution
               if (rawCell.type === "markdown") continue;

               // Capture cell index for closure
               const cellIndex = i;

               const executeCell = async () => {
                  try {
                     // Use notebook cell execution API with optional filter_params
                     const response =
                        await apiClients.notebooks.executeNotebookCell(
                           projectName,
                           packageName,
                           notebookPath,
                           cellIndex,
                           versionId,
                           filterParams,
                        );

                     const executedCell = response.data;
                     const result = executedCell.result;
                     const newSources =
                        rawCell.newSources || executedCell.newSources;

                     // Update state incrementally
                     setEnhancedCells((prev) => {
                        const next = [...prev];
                        if (!next[cellIndex]) {
                           next[cellIndex] = { ...rawCell };
                        }
                        next[cellIndex] = {
                           ...next[cellIndex],
                           result,
                           newSources,
                        };
                        return next;
                     });
                  } catch (cellError) {
                     console.error(
                        `Error executing cell ${cellIndex}:`,
                        cellError,
                     );
                  }
               };

               executionTasks.push(executeCell);
            }

            // Execute with limited concurrency (up to 4 parallel requests)
            const executing: Promise<void>[] = [];

            for (const task of executionTasks) {
               const promise = task().then(() => {
                  executing.splice(executing.indexOf(promise), 1);
               });
               executing.push(promise);

               if (executing.length >= MAX_CONCURRENT) {
                  await Promise.race(executing);
               }
            }

            // Wait for remaining tasks to complete
            await Promise.all(executing);
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
         useServerFilters,
         buildFilterParams,
         projectName,
         packageName,
         notebookPath,
         versionId,
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

   // Handle filter change using composite key
   const handleFilterChange = useCallback(
      (key: string) => (selection: FilterSelection | null) => {
         updateFilter(key, selection);
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
                           const key = getDimensionKey(spec);
                           const values = filterValuesData.get(key) || [];
                           const filterState = filterStates.get(key);
                           // Skip Retrieval filters if no retrievalFn provided
                           if (
                              spec.filterType === "Retrieval" &&
                              !retrievalFn
                           ) {
                              return null;
                           }

                           return (
                              <Box key={key}>
                                 <DimensionFilter
                                    spec={spec}
                                    values={values}
                                    selection={filterState?.selection}
                                    onChange={handleFilterChange(key)}
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
                  <Typography variant="body2" sx={{ color: "text.secondary" }}>
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
