import { useMemo } from "react";
import {
   DimensionSpec,
   useDimensionalFilterRangeData,
} from "./useDimensionalFilterRangeData";
import { useDimensionFilters } from "./useDimensionFilters";
import { useDimensionFiltersQuery } from "./useDimensionFiltersQuery";

/**
 * Configuration for dimensional filters
 */
export interface DimensionFiltersConfig {
   /** Project name */
   project: string;
   /** Package name */
   package: string;
   /** Version ID (optional) */
   versionId?: string;
   /** Index limit for retrieval queries */
   indexLimit: number;
   /** Dimension specifications for filters (each includes source and model) */
   dimensionSpecs: DimensionSpec[];
   /** Fields/columns to group by in the query (if not specified, uses select *) */
   selectFields?: string[];
}

/**
 * Options passed to useDimensionFiltersFromSpec
 */
export interface UseDimensionFiltersFromSpecOptions {
   config: DimensionFiltersConfig;
   /** Maximum number of results to return from query (default: 100) */
   queryLimit?: number;
}

/**
 * Combined hook that manages dimensional filters, data fetching, and query execution
 * from a single configuration object.
 *
 * @param config - Configuration containing project, package, dimension specs, and index limit
 * @param queryLimit - Maximum number of results to return from query (default: 100)
 * @returns All state and methods needed for dimensional filtering UI
 */
export function useDimensionFiltersFromSpec(
   config: DimensionFiltersConfig,
   queryLimit: number = 5000,
) {
   // Get primary source/model from dimension specs (for query execution)
   const primarySource = config.dimensionSpecs[0]?.source ?? "";
   const primaryModel = config.dimensionSpecs[0]?.model ?? "";

   // Manage filter state and query generation
   const { filterStates, updateFilter, clearAllFilters, getActiveFilters } =
      useDimensionFilters({
         dimensionSpecs: config.dimensionSpecs,
      });

   // Get active filters (memoized to prevent unnecessary re-renders)
   // Note: filterStates must be in the dependency array to ensure activeFilters updates
   // when individual filter values change (e.g., removing a chip from multi-select)
   const activeFilters = useMemo(
      () => getActiveFilters(),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [filterStates, getActiveFilters],
   );

   // Fetch dimensional filter range data
   // When activeFilters change, the hook will automatically re-fetch with filters applied
   // The hook handles multiple source/model combos internally
   const { data, noRowsMatchedFilter, isLoading, isError, error, refetch } =
      useDimensionalFilterRangeData({
         project: config.project,
         package: config.package,
         indexLimit: config.indexLimit,
         dimensionSpecs: config.dimensionSpecs,
         versionId: config.versionId,
         activeFilters,
      });

   // Only show loading on initial load, not during refetches
   const isInitialLoading = isLoading && !data;

   // Generate the embedded query result (uses primary source/model)
   const { embeddedQueryResult, queryString, executeQuery, canExecute } =
      useDimensionFiltersQuery({
         project: config.project,
         package: config.package,
         model: primaryModel,
         source: primarySource,
         filterSelections: activeFilters,
         limit: queryLimit,
         fields: config.selectFields,
      });

   return {
      // Filter state management
      filterStates,
      updateFilter,
      clearAllFilters,
      activeFilters,

      // Data fetching
      data,
      noRowsMatchedFilter,
      isLoading,
      isInitialLoading,
      isError,
      error,
      refetch,

      // Query execution
      embeddedQueryResult,
      queryString,
      executeQuery,
      canExecute,
   };
}
