import * as Malloy from "@malloydata/malloy-interfaces";
import {
   DimensionSpec,
   FilterType,
} from "../../hooks/useDimensionalFilterRangeData";
import { FilterSelection } from "../../hooks/useDimensionFilters";

/**
 * A single filter specification with source and dimension
 * Parsed from "source.dimension" string format
 */
export interface FilterSpec {
   source: string;
   dimension: string;
}

/**
 * Configuration parsed from notebook ##(filter) annotation
 */
export interface NotebookFilterConfig {
   filters: FilterSpec[];
}

/**
 * Filter annotation parsed from dimension #(filter) tag
 */
export interface DimensionFilterAnnotation {
   type: FilterType;
}

/**
 * Get the names of all sources that are joined into a given source
 * Looks for fields with kind: "join" in the SourceInfo
 */
export function getJoinedSources(sourceInfo: Malloy.SourceInfo): Set<string> {
   const joinedSources = new Set<string>();

   const fields = sourceInfo.schema?.fields;
   if (!fields) return joinedSources;

   for (const field of fields) {
      // Check if this field is a join (kind === "join")
      if ("kind" in field && field.kind === "join" && field.name) {
         joinedSources.add(field.name);
      }
   }
   return joinedSources;
}

/**
 * Parse ##(filter) annotation from notebook annotations
 * Format: ##(filters) ["source.dimension", "source2.dimension2", ...]
 * Returns the filter config or null if not found
 */
export function parseNotebookFilterAnnotation(
   annotations: string[] | undefined,
): NotebookFilterConfig | null {
   if (!annotations) return null;

   for (const annotation of annotations) {
      if (annotation.startsWith("##(filters)")) {
         const jsonPart = annotation.slice("##(filters)".length).trim();
         try {
            const parsed = JSON.parse(jsonPart);
            // Format: array of "source.dimension" strings
            if (Array.isArray(parsed)) {
               const filters: FilterSpec[] = [];
               for (const item of parsed) {
                  if (typeof item === "string") {
                     const dotIndex = item.indexOf(".");
                     if (dotIndex > 0 && dotIndex < item.length - 1) {
                        filters.push({
                           source: item.substring(0, dotIndex),
                           dimension: item.substring(dotIndex + 1),
                        });
                     }
                  }
               }
               if (filters.length > 0) {
                  return { filters };
               }
            }
         } catch (e) {
            console.error(`Failed to parse "${annotation}" annotation:`, e);
         }
      }
   }
   return null;
}

/**
 * Parse #(filter) annotation from a dimension annotation string
 * Returns the filter annotation or null if not found
 */
export function parseDimensionFilterAnnotation(
   annotation: string,
): DimensionFilterAnnotation | null {
   if (annotation.startsWith("#(filter)")) {
      const jsonPart = annotation.slice("#(filter)".length).trim();
      try {
         const config = JSON.parse(jsonPart);
         if (config.type) {
            return config as DimensionFilterAnnotation;
         }
      } catch (e) {
         console.error(`Failed to parse "${annotation}" annotation:`, e);
      }
   }
   return null;
}

/**
 * Parse all source infos from notebook cells and create a map of source_name -> SourceInfo
 * Also returns the model path from the first import statement found
 */
export function parseAllSourceInfos(
   cells: Array<{ type?: string; newSources?: string[]; text?: string }>,
): { sourceInfoMap: Map<string, Malloy.SourceInfo>; modelPath: string } {
   const sourceInfoMap = new Map<string, Malloy.SourceInfo>();
   let modelPath = "";

   for (const cell of cells) {
      if (cell.type === "code") {
         // Extract model path from the first import statement found
         if (!modelPath && cell.text) {
            const importMatch = cell.text.match(
               /import\s+(?:\{[^}]*\}\s+from\s+)?['"]([^'"]+)['"]/,
            );
            if (importMatch) {
               modelPath = importMatch[1];
            }
         }

         // Parse all newSources in this cell
         if (cell.newSources && cell.newSources.length > 0) {
            for (const sourceJson of cell.newSources) {
               try {
                  const sourceInfo = JSON.parse(
                     sourceJson,
                  ) as Malloy.SourceInfo;
                  if (sourceInfo.name) {
                     sourceInfoMap.set(sourceInfo.name, sourceInfo);
                  }
               } catch (e) {
                  console.error("Failed to parse SourceInfo:", e);
               }
            }
         }
      }
   }

   return { sourceInfoMap, modelPath };
}

/**
 * Extract dimension specs from SourceInfo map based on filter specs from notebook annotation
 * Looks for #(filter) annotations on the dimensions to determine filter types
 * Each spec includes source and model for proper query routing
 */
export function extractDimensionSpecs(
   sourceInfoMap: Map<string, Malloy.SourceInfo>,
   filterSpecs: FilterSpec[],
   modelPath: string,
): DimensionSpec[] {
   const specs: DimensionSpec[] = [];
   for (const { source, dimension } of filterSpecs) {
      const sourceInfo = sourceInfoMap.get(source);

      if (!sourceInfo) {
         console.warn(`Source "${source}" not found in notebook sources`);
         // Still add the spec with default filter type
         specs.push({
            source,
            model: modelPath,
            dimensionName: dimension,
            filterType: "Star",
         });
         continue;
      }

      // Access fields through schema.fields
      const fields = sourceInfo.schema?.fields;
      if (!fields) {
         specs.push({
            source,
            model: modelPath,
            dimensionName: dimension,
            filterType: "Star",
         });
         continue;
      }

      // Find the field in the source info
      const field = fields.find((f) => f.name === dimension);

      if (!field) {
         console.warn(
            `Dimension "${dimension}" not found in source "${source}"`,
         );
         specs.push({
            source,
            model: modelPath,
            dimensionName: dimension,
            filterType: "Star",
         });
         continue;
      }

      // Check for #(filter) annotation
      let filterType: FilterType = "Star"; // Default

      // Check annotations on the field (dimension/measure fields have annotations)
      if ("annotations" in field && field.annotations) {
         for (const annotation of field.annotations) {
            // Annotation type has a 'value' property
            if (annotation.value) {
               const filterAnn = parseDimensionFilterAnnotation(
                  annotation.value,
               );
               if (filterAnn) {
                  filterType = filterAnn.type;
                  break;
               }
            }
         }
      }

      specs.push({
         source,
         model: modelPath,
         dimensionName: dimension,
         filterType,
      });
   }

   return specs;
}

/**
 * Generate a Malloy WHERE clause filter string from filter selections
 * Uses source.dimension format only for joined sources (not the query's main source)
 */
export function generateFilterClause(
   activeFilters: FilterSelection[],
   dimensionToSourceMap: Map<string, string>,
   querySourceName: string | null,
): string {
   if (activeFilters.length === 0) return "";

   const conditions = activeFilters
      .map((selection) => {
         const { dimensionName, matchType, value, value2 } = selection;
         // Get the source name for this dimension
         const filterSourceName = dimensionToSourceMap.get(dimensionName);
         // Only use source.dimension format if the filter's source is different from the query's source
         // (i.e., it's a joined source, not the main source)
         const needsSourcePrefix =
            filterSourceName && filterSourceName !== querySourceName;
         const fieldName = needsSourcePrefix
            ? `\`${filterSourceName}\`.\`${dimensionName}\``
            : `\`${dimensionName}\``;
         const isDate = value instanceof Date;

         const formatValue = (
            v: unknown,
            isDateVal: boolean = false,
         ): string => {
            if (v === null || v === undefined) return "null";
            if (isDateVal && v instanceof Date) {
               return `@${v.getFullYear()}-${String(v.getMonth() + 1).padStart(2, "0")}-${String(v.getDate()).padStart(2, "0")}`;
            }
            if (typeof v === "string") {
               return `'${v.replace(/'/g, "\\'")}'`;
            }
            if (typeof v === "boolean") return v ? "true" : "false";
            return String(v);
         };

         switch (matchType) {
            case "Equals":
            case "Concept Search":
               if (Array.isArray(value) && value.length > 0) {
                  const conditions = value.map(
                     (v) => `${fieldName} = ${formatValue(v, isDate)}`,
                  );
                  return `(${conditions.join(" or ")})`;
               }
               return `${fieldName} = ${formatValue(value, isDate)}`;

            case "Contains":
               if (typeof value === "string") {
                  return `${fieldName} ~ f'%${value.replace(/'/g, "\\'")}%'`;
               }
               return `${fieldName} = ${formatValue(value)}`;

            case "After":
            case "Greater Than":
               return `${fieldName} > ${formatValue(value, isDate)}`;

            case "Before":
            case "Less Than":
               return `${fieldName} < ${formatValue(value, isDate)}`;

            case "Between":
               if (value2 !== undefined) {
                  return `${fieldName} >= ${formatValue(value, isDate)} and ${fieldName} <= ${formatValue(value2, isDate)}`;
               }
               return `${fieldName} >= ${formatValue(value, isDate)}`;

            default:
               return "";
         }
      })
      .filter((c) => c.length > 0);

   return conditions.join(" and ");
}

/**
 * Extract the source name from a Malloy query
 * Handles patterns like:
 * - "run: source_name -> ..."
 * - "run: source_name->view_name"
 * Returns null if no source name can be extracted
 */
export function extractSourceFromQuery(query: string): string | null {
   // Match "run:" followed by optional whitespace, then the source name
   // Source name ends at "->" or whitespace
   const runMatch = query.match(/run\s*:\s*(\w+)\s*->/);
   if (runMatch) {
      return runMatch[1];
   }

   // Also try matching without "run:" for inline queries
   const arrowMatch = query.match(/^\s*(\w+)\s*->/m);
   if (arrowMatch) {
      return arrowMatch[1];
   }

   return null;
}

/**
 * Injects a where clause into a Malloy query by appending "+ {where: ...}"
 * Uses Malloy's refinement syntax which works with any query pattern.
 */
export function injectWhereClause(query: string, filterClause: string): string {
   // Simply append the where clause as a refinement
   // This works for all patterns:
   // - run: source -> { ... } + {where: ...}
   // - run: source -> view_name + {where: ...}
   // - run: source -> view_name + { ... } + {where: ...}
   return `${query.trimEnd()} + {where: ${filterClause}}`;
}
