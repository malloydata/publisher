import { useCallback, useMemo, useState } from "react";
import { createEmbeddedQueryResult } from "../components/QueryResult";
import { encodeResourceUri } from "../utils/formatting";
import { FilterSelection, FilterValuePrimitive } from "./useDimensionFilters";

/**
 * Parameters for the useDimensionFiltersQuery hook
 */
export interface UseDimensionFiltersQueryParams {
   /** Project name */
   project: string;
   /** Package name */
   package: string;
   /** Model path */
   model: string;
   /** Source name within the model */
   source: string;
   /** Active filter selections */
   filterSelections: FilterSelection[];
   /** Optional fields to select in the query */
   fields?: string[];
   /** Maximum number of results to return (default: 1000) */
   limit?: number;
}

/**
 * Result from the useDimensionFiltersQuery hook
 */
export interface DimensionFiltersQueryResult {
   /** Embedded query result string suitable for rendering with EmbeddedQueryResult */
   embeddedQueryResult: string | null;
   /** The generated Malloy query string (preview only, not executed) */
   queryString: string;
   /** The resource URI for the model */
   resourceUri: string;
   /** Function to execute the query and generate the embedded result */
   executeQuery: () => void;
   /** Whether a query can be executed (has filter selections) */
   canExecute: boolean;
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
 * Generates a Malloy WHERE clause from active filters
 */
function generateWhereClause(filterSelections: FilterSelection[]): string {
   if (filterSelections.length === 0) {
      return "";
   }

   const conditions = filterSelections
      .map((selection) => generateFilterCondition(selection))
      .filter((condition) => condition.length > 0);

   if (conditions.length === 0) {
      return "";
   }

   return conditions.join(" and ");
}

/**
 * Builds a Malloy query with the specified filters and fields
 */
function buildFilteredQuery(
   source: string,
   filterSelections: FilterSelection[],
   fields?: string[],
   limit: number = 1000,
): string {
   const whereClause = generateWhereClause(filterSelections);

   // Use group_by with specific fields, or select * if none specified
   const hasFields = fields && fields.length > 0;
   const fieldClause = hasFields
      ? `group_by: ${fields.map((f) => `\`${f}\``).join(", ")}`
      : "select: *";

   // Build the query
   if (whereClause) {
      return `
            run: ${source} -> {
                where: ${whereClause}
                ${fieldClause}
                limit: ${limit}
            }
        `;
   } else {
      return `
            run: ${source} -> {
                ${fieldClause}
                limit: ${limit}
            }
        `;
   }
}

/**
 * Custom hook to generate an embedded Malloy query with dimensional filters
 *
 * This hook takes filter selections from useDimensionFilters, builds a complete
 * Malloy query with WHERE clauses, and provides a function to execute the query
 * and create an embedded query result string that can be rendered with the
 * EmbeddedQueryResult component.
 *
 * The query is not executed automatically - you must call executeQuery() to
 * generate the embedded result.
 *
 * @param params - Parameters including model info, source, and filter selections
 * @returns Query metadata, embedded result, and execute function
 *
 * @example
 * ```tsx
 * const { filterStates, getActiveFilters } = useDimensionFilters({
 *   dimensionSpecs: [
 *     { dimensionName: "category", filterType: "Star", source: "my_source", model: "model.malloy" },
 *   ],
 * });
 *
 * const { embeddedQueryResult, executeQuery, canExecute } = useDimensionFiltersQuery({
 *   project: "my-project",
 *   package: "my-package",
 *   model: "model.malloy",
 *   source: "my_source",
 *   filterSelections: getActiveFilters(),
 *   fields: ["category", "price", "quantity"],
 *   limit: 1000, // Optional: defaults to 1000
 * });
 *
 * // Execute button
 * <Button onClick={executeQuery} disabled={!canExecute}>
 *   Execute Query
 * </Button>
 *
 * // Render with EmbeddedQueryResult component
 * {embeddedQueryResult && (
 *   <EmbeddedQueryResult embeddedQueryResult={embeddedQueryResult} />
 * )}
 * ```
 */
export function useDimensionFiltersQuery(
   params: UseDimensionFiltersQueryParams,
): DimensionFiltersQueryResult {
   const {
      project,
      package: packageName,
      model,
      source,
      filterSelections,
      fields,
      limit = 1000,
   } = params;

   // State to store the executed query result
   const [embeddedQueryResult, setEmbeddedQueryResult] = useState<
      string | null
   >(null);

   // Build the query string (for preview)
   const queryString = useMemo(
      () => buildFilteredQuery(source, filterSelections, fields, limit),
      [source, filterSelections, fields, limit],
   );

   // Build the resource URI
   const resourceUri = useMemo(
      () =>
         encodeResourceUri({
            projectName: project,
            packageName,
            modelPath: model,
         }),
      [project, packageName, model],
   );

   // Check if query can be executed
   const canExecute = filterSelections.length > 0;

   // Function to execute the query
   const executeQuery = useCallback(() => {
      if (!canExecute) {
         setEmbeddedQueryResult(null);
         return;
      }

      const result = createEmbeddedQueryResult({
         resourceUri,
         query: queryString,
      });
      setEmbeddedQueryResult(result);
   }, [canExecute, resourceUri, queryString]);

   return {
      embeddedQueryResult,
      queryString,
      resourceUri,
      executeQuery,
      canExecute,
   };
}
