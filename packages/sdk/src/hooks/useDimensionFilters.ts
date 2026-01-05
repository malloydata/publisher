import { useCallback, useEffect, useState } from "react";
import {
   DimensionSpec,
   getDimensionKey,
} from "./useDimensionalFilterRangeData";

/**
 * Match types for filtering dimensions
 */
export type MatchType =
   | "Equals"
   | "Contains"
   | "After"
   | "Before"
   | "Less Than"
   | "Greater Than"
   | "Between"
   | "Semantic Search";

/**
 * Primitive types that can be used as filter values
 */
export type FilterValuePrimitive = string | number | boolean | Date;

/**
 * Type for filter values - can be a single value or an array for multi-select
 */
export type FilterValue = FilterValuePrimitive | FilterValuePrimitive[];

/**
 * A selected filter value with its match type
 */
export interface FilterSelection {
   dimensionName: string;
   /** Source name - required to uniquely identify filters when same dimension name exists in multiple sources */
   source: string;
   matchType: MatchType;
   value: FilterValue;
   value2?: FilterValuePrimitive; // For "Between" match type
}

/**
 * Complete filter state including the dimension spec and selection
 */
export interface DimensionFilterState {
   spec: DimensionSpec;
   selection: FilterSelection | null;
}

/**
 * Parameters for the useDimensionFilters hook
 */
export interface UseDimensionFiltersParams {
   /** Dimension specifications (each includes source and model) */
   dimensionSpecs: DimensionSpec[];
}

/**
 * Result from the useDimensionFilters hook
 */
export interface UseDimensionFiltersResult {
   /** Current filter states, keyed by composite key (source:dimensionName) */
   filterStates: Map<string, DimensionFilterState>;
   /** Update a filter selection using composite key */
   updateFilter: (key: string, selection: FilterSelection | null) => void;
   /** Clear a specific filter using composite key */
   clearFilter: (key: string) => void;
   /** Clear all filters */
   clearAllFilters: () => void;
   /** Get active filters (with selections) */
   getActiveFilters: () => FilterSelection[];
   /** Generate Malloy query fragment for active filters */
   generateWhereClause: () => string;
}

/**
 * Escapes special characters in strings for Malloy queries
 */
function escapeMalloyString(value: string): string {
   return value.replace(/'/g, "\\'").replace(/\\/g, "\\\\");
}

/**
 * Formats a value for use in Malloy query
 */
function formatMalloyValue(
   value: FilterValuePrimitive | null | undefined,
   isDate: boolean = false,
): string {
   if (value === null || value === undefined) {
      return "null";
   }

   if (isDate) {
      if (value instanceof Date) {
         // Format as YYYY-MM-DD
         return `@${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;
      }
      // If it's a string, assume it's already in ISO format
      return `@${value}`;
   }

   if (typeof value === "string") {
      return `'${escapeMalloyString(value)}'`;
   }

   if (typeof value === "number") {
      return String(value);
   }

   if (typeof value === "boolean") {
      return value ? "true" : "false";
   }

   return String(value);
}

/**
 * Generates a Malloy filter condition for a single filter selection
 */
function generateFilterCondition(selection: FilterSelection): string {
   const { dimensionName, matchType, value, value2 } = selection;
   const fieldName = `\`${dimensionName}\``;

   // Determine if this is a date field based on the value
   const isDate = value instanceof Date;

   switch (matchType) {
      case "Equals":
      case "Semantic Search":
         if (Array.isArray(value) && value.length > 0) {
            // Handle multi-select: (field = val1 or field = val2)
            const conditions = value.map(
               (v) => `${fieldName} = ${formatMalloyValue(v, isDate)}`,
            );
            return `(${conditions.join(" or ")})`;
         }
         // Empty array or single value
         if (Array.isArray(value)) return "";
         return `${fieldName} = ${formatMalloyValue(value, isDate)}`;

      case "Contains":
         if (typeof value === "string") {
            return `${fieldName} ~ f'%${escapeMalloyString(value)}%'`;
         }
         if (Array.isArray(value)) return "";
         return `${fieldName} = ${formatMalloyValue(value)}`;

      case "After":
         if (Array.isArray(value)) return "";
         return `${fieldName} > ${formatMalloyValue(value, isDate)}`;

      case "Before":
         if (Array.isArray(value)) return "";
         return `${fieldName} < ${formatMalloyValue(value, isDate)}`;

      case "Greater Than":
         if (Array.isArray(value)) return "";
         return `${fieldName} > ${formatMalloyValue(value)}`;

      case "Less Than":
         if (Array.isArray(value)) return "";
         return `${fieldName} < ${formatMalloyValue(value)}`;

      case "Between":
         if (Array.isArray(value)) return "";
         if (value2 !== undefined) {
            return `${fieldName} >= ${formatMalloyValue(value, isDate)} and ${fieldName} <= ${formatMalloyValue(value2, isDate)}`;
         }
         return `${fieldName} >= ${formatMalloyValue(value, isDate)}`;

      default:
         return "";
   }
}

/**
 * Custom hook to manage dimension filter state and generate Malloy queries
 *
 * This hook maintains filter state for multiple dimensions and provides utilities
 * to generate Malloy WHERE clauses based on the active filters.
 *
 * @param params - Parameters including dimension specs (each with source and model)
 * @returns Filter state management functions and query generation
 *
 * @example
 * ```tsx
 * const { filterStates, updateFilter, generateWhereClause } = useDimensionFilters({
 *   dimensionSpecs: [
 *     { dimensionName: "category", filterType: "Star", source: "my_source", model: "model.malloy" },
 *     { dimensionName: "price", filterType: "MinMax", source: "my_source", model: "model.malloy" },
 *   ],
 * });
 *
 * // Update a filter
 * updateFilter("category", {
 *   dimensionName: "category",
 *   matchType: "Equals",
 *   value: "Electronics"
 * });
 *
 * // Generate query
 * const whereClause = generateWhereClause();
 * // Returns: "where `category` = 'Electronics'"
 * ```
 */
export function useDimensionFilters(
   params: UseDimensionFiltersParams,
): UseDimensionFiltersResult {
   const { dimensionSpecs } = params;

   // Initialize filter states using composite keys (source:dimensionName)
   const [filterStates, setFilterStates] = useState<
      Map<string, DimensionFilterState>
   >(() => {
      const initialStates = new Map<string, DimensionFilterState>();
      dimensionSpecs.forEach((spec) => {
         const key = getDimensionKey(spec);
         initialStates.set(key, {
            spec,
            selection: null,
         });
      });
      return initialStates;
   });

   // Sync filter states when dimensionSpecs change (e.g., loaded asynchronously)
   useEffect(() => {
      setFilterStates((prevStates) => {
         const newStates = new Map<string, DimensionFilterState>();

         dimensionSpecs.forEach((spec) => {
            const key = getDimensionKey(spec);
            // Preserve existing selection if the dimension already exists
            const existingState = prevStates.get(key);
            newStates.set(key, {
               spec,
               selection: existingState?.selection ?? null,
            });
         });

         return newStates;
      });
   }, [dimensionSpecs]);

   // Update a filter selection using composite key
   const updateFilter = useCallback(
      (key: string, selection: FilterSelection | null) => {
         setFilterStates((prevStates) => {
            const newStates = new Map(prevStates);
            const existingState = newStates.get(key);

            if (existingState) {
               newStates.set(key, {
                  ...existingState,
                  selection,
               });
            }

            return newStates;
         });
      },
      [],
   );

   // Clear a specific filter using composite key
   const clearFilter = useCallback(
      (key: string) => {
         updateFilter(key, null);
      },
      [updateFilter],
   );

   // Clear all filters
   const clearAllFilters = useCallback(() => {
      setFilterStates((prevStates) => {
         const newStates = new Map(prevStates);
         newStates.forEach((state, key) => {
            newStates.set(key, {
               ...state,
               selection: null,
            });
         });
         return newStates;
      });
   }, []);

   // Get active filters (those with selections)
   const getActiveFilters = useCallback((): FilterSelection[] => {
      const activeFilters: FilterSelection[] = [];
      filterStates.forEach((state) => {
         if (state.selection) {
            activeFilters.push(state.selection);
         }
      });
      return activeFilters;
   }, [filterStates]);

   // Generate Malloy WHERE clause from active filters
   const generateWhereClause = useCallback((): string => {
      const activeFilters = getActiveFilters();

      if (activeFilters.length === 0) {
         return "";
      }

      const conditions = activeFilters
         .map((selection) => generateFilterCondition(selection))
         .filter((condition) => condition.length > 0);

      if (conditions.length === 0) {
         return "";
      }

      return `where ${conditions.join(" and ")}`;
   }, [getActiveFilters]);

   return {
      filterStates,
      updateFilter,
      clearFilter,
      clearAllFilters,
      getActiveFilters,
      generateWhereClause,
   };
}
