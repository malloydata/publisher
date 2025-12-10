import { useMemo } from "react";
import { useServer } from "../components/ServerProvider";
import { FilterSelection, FilterValuePrimitive } from "./useDimensionFilters";
import { useQueryWithApiError } from "./useQueryWithApiError";

/**
 * Filter types for dimensions
 */
export type FilterType =
   | "NONE"
   | "Star"
   | "MinMax"
   | "DateMinMax"
   | "Retrieval"
   | "Boolean";

/**
 * Specification for a dimension to filter
 */
export interface DimensionSpec {
   /** Name of the dimension field */
   dimensionName: string;
   /** Type of filter to apply */
   filterType: FilterType;
   /** Source name within the model */
   source: string;
   /** Model path */
   model: string;
   /** Minimum similarity score for Retrieval filter type (default: 0.1) */
   minSimilarityScore?: number;
   /** Optional list of static values to use for the dropdown instead of querying */
   values?: string[];
}

/**
 * Value information for a dimension
 */
export interface DimensionValue {
   value: FilterValuePrimitive;
   count?: number;
}

/**
 * Result type mapping dimension names to their values
 */
export type DimensionValues = Map<string, DimensionValue[]>;

/**
 * Parameters for the useDimensionalFilterRangeData hook
 */
export interface UseDimensionalFilterRangeDataParams {
   /** Project name */
   project: string;
   /** Package name */
   package: string;
   /** List of dimension specifications (each includes source and model) */
   dimensionSpecs: DimensionSpec[];
   /** Version ID (optional) */
   versionId?: string;
   /** Whether the query should be enabled */
   enabled?: boolean;
   /** Maximum number of index results to return (default: 1000) */
   indexLimit?: number;
   /** Active filter selections to apply when fetching dimension values (optional) */
   activeFilters?: FilterSelection[];
}

/**
 * Result from the useDimensionalFilterRangeData hook
 */
export interface DimensionalFilterRangeDataResult {
   /** Map of dimension names to their values */
   data: DimensionValues | undefined;
   /** Whether no rows matched the filter */
   noRowsMatchedFilter: boolean;
   /** Whether the query is loading */
   isLoading: boolean;
   /** Whether the query encountered an error */
   isError: boolean;
   /** Error object if query failed */
   error: Error | null;
   /** Refetch function to manually trigger the query */
   refetch: () => void;
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
      case "Concept Search":
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
            return `${fieldName} ~  f'%${escapeMalloyString(value)}%'`;
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
 * Groups dimension specs by source/model combination
 * Returns a map where key is "source|model" and value is array of specs for that combo
 */
function groupSpecsBySourceModel(
   dimensionSpecs: DimensionSpec[],
): Map<string, DimensionSpec[]> {
   const groups = new Map<string, DimensionSpec[]>();

   for (const spec of dimensionSpecs) {
      const key = `${spec.source}|${spec.model}`;
      const existing = groups.get(key) || [];
      existing.push(spec);
      groups.set(key, existing);
   }

   return groups;
}

/**
 * Builds a Malloy index query to fetch dimension values for a single source
 * Uses Malloy's built-in index operator for dimensional search
 * Reference: https://docs.malloydata.dev/documentation/patterns/dim_index
 */
function buildDimensionalIndexQuery(
   source: string,
   dimensionSpecs: DimensionSpec[],
   indexLimit: number,
   activeFilters?: FilterSelection[],
): string {
   // Filter out specs that don't need data fetching
   const specsToFetch = dimensionSpecs.filter(
      (spec) =>
         (spec.filterType === "Star" ||
            spec.filterType === "MinMax" ||
            spec.filterType === "DateMinMax") &&
         !spec.values,
   );

   if (specsToFetch.length === 0) {
      return "";
   }

   // Build list of dimensions to index
   const dimensionsToIndex = specsToFetch
      .map((spec) => `\`${spec.dimensionName}\``)
      .join(", ");

   // Filter activeFilters to only include those for this source
   const filtersForSource =
      activeFilters?.filter((f) => {
         // Find the spec for this filter's dimension
         const spec = dimensionSpecs.find(
            (s) => s.dimensionName === f.dimensionName,
         );
         return spec?.source === source;
      }) || [];

   // Generate WHERE conditions from active filters (without 'where' keyword)
   const whereConditions =
      filtersForSource.length > 0
         ? filtersForSource
              .map((selection) => generateFilterCondition(selection))
              .filter((condition) => condition.length > 0)
              .join(" and ")
         : "";

   // Use Malloy's index operator to create a dimensional search index
   // The index operator returns: fieldName, fieldPath, fieldType, fieldValue, weight
   // For strings: returns distinct values with weights (counts)
   // For numbers/dates: returns ranges (min to max)
   //
   // Important: where clause and index must be in the SAME query stage
   // Use a two-stage query: first stage creates the filtered index, second stage orders by weight
   return `
        run: ${source} -> {
            ${whereConditions ? `where: ${whereConditions}` : ""}
            index: ${dimensionsToIndex}
        } -> {
            select: *
            order_by: weight desc
            
            limit: ${indexLimit}
        }
    `;
}

/**
 * Cell types from Malloy query results
 */
interface MalloyCell {
   kind: string;
   string_value?: string;
   number_value?: number;
   boolean_value?: boolean;
}

/**
 * Record cell from Malloy query results
 */
interface MalloyRecordCell {
   kind: "record_cell";
   record_value: MalloyCell[];
}

/**
 * Schema field from Malloy query results
 */
interface MalloySchemaField {
   name: string;
}

/**
 * Malloy index query result entry
 */
interface MalloyIndexEntry {
   fieldName?: string;
   fieldPath?: string;
   fieldType?: string;
   fieldValue?: string;
   weight?: number;
}

/**
 * Parsed Malloy query result structure
 */
interface MalloyQueryResult {
   schema: {
      fields: MalloySchemaField[];
   };
   data?: {
      array_value?: MalloyRecordCell[];
   };
}

/**
 * Parses the Malloy index query result and converts it to a DimensionValues map
 * The index operator returns: fieldName, fieldPath, fieldType, fieldValue, weight
 * Reference: https://docs.malloydata.dev/documentation/patterns/dim_index
 */
function parseIndexQueryResult(
   result: string,
   dimensionSpecs: DimensionSpec[],
): { values: DimensionValues; noRowsMatchedFilter: boolean } {
   const dimensionValues = new Map<string, DimensionValue[]>();

   // Initialize empty arrays for all dimensions
   for (const spec of dimensionSpecs) {
      dimensionValues.set(spec.dimensionName, []);
   }

   // Parse the result JSON if it's a string
   const parsedResult = JSON.parse(result) as MalloyQueryResult;

   // Parse schema to understand field positions
   const schema = parsedResult.schema.fields;
   const fieldMap = new Map<string, number>();

   schema.forEach((field: MalloySchemaField, index: number) => {
      fieldMap.set(field.name, index);
   });

   // Helper function to extract value from a cell based on its kind
   const extractCellValue = (
      cell: MalloyCell | undefined,
   ): string | number | boolean | null => {
      if (!cell) return null;

      switch (cell.kind) {
         case "string_cell":
            return cell.string_value ?? null;
         case "number_cell":
            return cell.number_value ?? null;
         case "boolean_cell":
            return cell.boolean_value ?? null;
         case "null_cell":
            return null;
         default:
            console.log("Unknown cell kind: " + cell.kind);
            return null;
      }
   };

   // Convert array_value records to objects using schema
   const rawData = parsedResult.data?.array_value || [];
   const indexData: MalloyIndexEntry[] = rawData.map(
      (record: MalloyRecordCell) => {
         const obj: MalloyIndexEntry = {};

         if (record.kind === "record_cell" && record.record_value) {
            record.record_value.forEach((cell: MalloyCell, index: number) => {
               const fieldName = schema[index]?.name;
               if (fieldName) {
                  const value = extractCellValue(cell);
                  if (fieldName === "fieldName" && typeof value === "string") {
                     obj.fieldName = value;
                  } else if (
                     fieldName === "fieldPath" &&
                     typeof value === "string"
                  ) {
                     obj.fieldPath = value;
                  } else if (
                     fieldName === "fieldType" &&
                     typeof value === "string"
                  ) {
                     obj.fieldType = value;
                  } else if (
                     fieldName === "fieldValue" &&
                     typeof value === "string"
                  ) {
                     obj.fieldValue = value;
                  } else if (
                     fieldName === "weight" &&
                     typeof value === "number"
                  ) {
                     obj.weight = value;
                  }
               }
            });
         }

         return obj;
      },
   );

   const noRowsMatchedFilter =
      indexData.length === 0 ||
      indexData.every((entry: MalloyIndexEntry) => !entry.fieldName);

   // Group index results by fieldName/dimensionName
   for (const spec of dimensionSpecs) {
      if (spec.filterType === "NONE" || spec.filterType === "Retrieval") {
         // These types don't fetch values
         continue;
      }

      // Find all index entries for this dimension
      // Try multiple matching strategies since field naming can vary
      const dimensionEntries = indexData.filter((entry: MalloyIndexEntry) => {
         const fieldName = entry.fieldName || "";
         const fieldPath = entry.fieldPath || "";

         // Try exact match
         if (
            fieldName === spec.dimensionName ||
            fieldPath === spec.dimensionName
         ) {
            return true;
         }

         // Try case-insensitive match
         if (
            fieldName.toLowerCase() === spec.dimensionName.toLowerCase() ||
            fieldPath.toLowerCase() === spec.dimensionName.toLowerCase()
         ) {
            return true;
         }

         // Try matching the last part of a path (e.g., "source.field" matches "field")
         const lastPathPart = fieldPath.split(".").pop() || "";
         if (
            lastPathPart === spec.dimensionName ||
            lastPathPart.toLowerCase() === spec.dimensionName.toLowerCase()
         ) {
            return true;
         }

         return false;
      });

      if (dimensionEntries.length === 0) {
         continue;
      }

      if (spec.filterType === "Star") {
         // For Star filter, we want all distinct values with their weights
         // String fields return individual fieldValue entries
         const values = dimensionEntries
            .filter((entry: MalloyIndexEntry) => entry.fieldType === "string")
            .map((entry: MalloyIndexEntry) => ({
               value: entry.fieldValue ?? "",
               count: entry.weight,
            }))
            .sort(
               (a: DimensionValue, b: DimensionValue) =>
                  (b.count ?? 0) - (a.count ?? 0),
            ); // Sort by count descending

         dimensionValues.set(spec.dimensionName, values);
      } else if (spec.filterType === "MinMax") {
         // For MinMax filter, numeric fields return a range in fieldValue
         // Format is typically "min to max"
         console.log(
            "MinMax dimensionEntries: " +
               JSON.stringify(dimensionEntries, null, 2),
         );
         console.log(
            "All MinMax dimension entries fieldTypes:",
            dimensionEntries.map((e: MalloyIndexEntry) => e.fieldType),
         );

         const numericEntry = dimensionEntries.find(
            (entry: MalloyIndexEntry) => entry.fieldType === "number",
         );

         console.log("numericEntry: " + JSON.stringify(numericEntry, null, 2));
         if (numericEntry?.fieldValue) {
            const rangeString = numericEntry.fieldValue;
            // Parse "min to max" format
            const rangeParts = rangeString.split(" to ");

            console.log("rangeParts: " + rangeParts);

            if (rangeParts.length === 2) {
               const values = [
                  { value: parseFloat(rangeParts[0]) },
                  { value: parseFloat(rangeParts[1]) },
               ];
               dimensionValues.set(spec.dimensionName, values);
            } else {
               console.warn(
                  `Could not parse numeric range for ${spec.dimensionName}: ${rangeString}`,
               );
            }
         } else {
            console.warn(
               `No numeric entry found for ${spec.dimensionName}. Available entries:`,
               dimensionEntries,
            );
         }
      } else if (spec.filterType === "DateMinMax") {
         // For DateMinMax filter, date/timestamp fields return a range in fieldValue
         // Format is typically "YYYY-MM-DD to YYYY-MM-DD"

         // Look for date, timestamp, or string type entries (dates can come back in various formats)
         const dateEntry = dimensionEntries.find(
            (entry: MalloyIndexEntry) =>
               entry.fieldType === "date" ||
               entry.fieldType === "timestamp" ||
               entry.fieldType === "string",
         );

         console.log("dateEntry: " + JSON.stringify(dateEntry, null, 2));
         console.log(
            "All date dimension entries fieldTypes:",
            dimensionEntries.map((e: MalloyIndexEntry) => e.fieldType),
         );

         if (dateEntry?.fieldValue) {
            const rangeString = dateEntry.fieldValue;
            // Parse "YYYY-MM-DD to YYYY-MM-DD" format
            const rangeParts = rangeString.split(" to ");

            console.log("date rangeParts: " + rangeParts);

            if (rangeParts.length === 2) {
               const values = [
                  { value: new Date(rangeParts[0].trim()) },
                  { value: new Date(rangeParts[1].trim()) },
               ];
               dimensionValues.set(spec.dimensionName, values);
            } else {
               console.warn(
                  `Could not parse date range for ${spec.dimensionName}: ${rangeString}`,
               );
            }
         } else {
            console.warn(
               `No date entry found for ${spec.dimensionName}. Available entries:`,
               dimensionEntries,
            );
         }
      }
   }

   return { values: dimensionValues, noRowsMatchedFilter };
}

/**
 * Custom hook to fetch dimensional filter values from a Malloy model
 *
 * This hook uses Malloy's built-in `index` operator to create dimensional search indexes
 * for the specified dimensions. The index operator efficiently returns distinct values
 * and ranges for dimensions in a single query.
 *
 * Filter types:
 * - NONE: Display only, no values fetched
 * - Star: Fetches all distinct values with counts (for string fields)
 * - MinMax: Fetches minimum and maximum values (for numeric fields as ranges)
 * - DateMinMax: Fetches minimum and maximum values (for date fields as Date objects)
 * - Retrieval: Uses retrieval, no values fetched
 *
 * Reference: https://docs.malloydata.dev/documentation/patterns/dim_index
 *
 * The hook groups dimension specs by source/model combination and runs separate
 * index queries for each group, then merges the results.
 *
 * @param params - Parameters including project, package, and dimension specs (each with source/model)
 * @returns Query result with dimension values map, loading state, and error information
 *
 * @example
 * ```tsx
 * const { data, isLoading, error } = useDimensionalFilterRangeData({
 *   project: "my-project",
 *   package: "my-package",
 *   dimensionSpecs: [
 *     { dimensionName: "category", filterType: "Star", source: "my_source", model: "model.malloy" },
 *     { dimensionName: "price", filterType: "MinMax", source: "my_source", model: "model.malloy" },
 *     { dimensionName: "status", filterType: "Star", source: "other_source", model: "other.malloy" },
 *   ],
 *   indexLimit: 1000, // Optional: set higher if needed for many Star dimensions
 * });
 * ```
 */
export function useDimensionalFilterRangeData(
   params: UseDimensionalFilterRangeDataParams,
): DimensionalFilterRangeDataResult {
   const {
      project,
      package: packageName,
      dimensionSpecs,
      versionId,
      enabled = true,
      indexLimit = 10000,
      activeFilters = [],
   } = params;

   // Group dimension specs by source/model combination
   const sourceModelGroups = useMemo(
      () => groupSpecsBySourceModel(dimensionSpecs),
      [dimensionSpecs],
   );

   // Build query strings for each source/model combo
   const queryConfigs = useMemo(() => {
      const configs: Array<{
         key: string;
         source: string;
         model: string;
         query: string;
         specs: DimensionSpec[];
      }> = [];

      for (const [key, specs] of sourceModelGroups) {
         const [source, model] = key.split("|");
         const query = buildDimensionalIndexQuery(
            source,
            specs,
            indexLimit,
            activeFilters,
         );
         if (query) {
            configs.push({ key, source, model, query, specs });
         } else {
            console.log("No query for source: ", source, "model: ", model);
         }
      }

      return configs;
   }, [sourceModelGroups, indexLimit, activeFilters]);

   // Determine if we need to execute queries
   const shouldExecuteQuery = useMemo(() => {
      return (
         enabled &&
         queryConfigs.length > 0 &&
         dimensionSpecs.some(
            (spec) =>
               spec.filterType === "Star" ||
               spec.filterType === "MinMax" ||
               spec.filterType === "DateMinMax",
         )
      );
   }, [enabled, queryConfigs, dimensionSpecs]);

   // Execute the query using the API client
   const { apiClients } = useServer();

   const queryResult = useQueryWithApiError({
      queryKey: [
         "dimensionalFilter",
         project,
         packageName,
         dimensionSpecs,
         versionId,
         activeFilters,
      ],
      queryFn: async () => {
         if (!shouldExecuteQuery) {
            // Return empty map if no query needed
            const emptyMap = new Map<string, DimensionValue[]>();
            for (const spec of dimensionSpecs) {
               emptyMap.set(spec.dimensionName, []);
            }
            return { values: emptyMap, noRowsMatchedFilter: false };
         }

         // Execute queries for each source/model combo in parallel
         const results = await Promise.all(
            queryConfigs.map(async (config) => {
               const response = await apiClients.models.executeQueryModel(
                  project,
                  packageName,
                  config.model,
                  {
                     query: config.query,
                     versionId: versionId,
                  },
               );
               return {
                  config,
                  result: parseIndexQueryResult(
                     response.data.result,
                     config.specs,
                  ),
               };
            }),
         );

         // Merge all results into a single DimensionValues map
         const mergedValues = new Map<string, DimensionValue[]>();
         let anyNoRowsMatched = false;

         for (const { result } of results) {
            for (const [key, val] of result.values) {
               mergedValues.set(key, val);
            }
            if (result.noRowsMatchedFilter) {
               anyNoRowsMatched = true;
            }
         }

         return { values: mergedValues, noRowsMatchedFilter: anyNoRowsMatched };
      },
      enabled: shouldExecuteQuery,
      staleTime: 5 * 60 * 1000, // 5 minutes
      placeholderData: (previousData) => previousData, // Keep showing previous data during refetch
   });
   const fetchedValues = queryResult.data?.values;
   const mergedData = useMemo(() => {
      const result = new Map<string, DimensionValue[]>();

      // Initialize with static values or empty arrays
      for (const spec of dimensionSpecs) {
         if (spec.values) {
            result.set(
               spec.dimensionName,
               spec.values.map((v) => ({ value: v })),
            );
         } else {
            result.set(spec.dimensionName, []);
         }
      }

      // Merge fetched values
      if (fetchedValues) {
         for (const [key, val] of fetchedValues) {
            if (val.length > 0) {
               result.set(key, val);
            }
         }
      }
      return result;
   }, [dimensionSpecs, fetchedValues]);

   return {
      data: mergedData,
      noRowsMatchedFilter: queryResult.data?.noRowsMatchedFilter ?? false,
      isLoading: shouldExecuteQuery ? queryResult.isPending : false,
      isError: shouldExecuteQuery ? queryResult.isError : false,
      error: shouldExecuteQuery ? (queryResult.error as Error | null) : null,
      refetch: queryResult.refetch,
   };
}
