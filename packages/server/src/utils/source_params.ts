import type * as Malloy from "@malloydata/malloy-interfaces";
import { BadRequestError } from "../errors";

/**
 * Converts a raw value (string, number, boolean, etc.) into a Malloy
 * literal expression based on the declared parameter type.
 */
export function paramValueToMalloyLiteral(
   rawValue: unknown,
   paramInfo: Malloy.ParameterInfo,
): string {
   const kind = paramInfo.type.kind;
   const str = String(rawValue);

   switch (kind) {
      case "number_type": {
         if (typeof rawValue === "number") {
            if (isNaN(rawValue)) {
               throw new BadRequestError(
                  `Parameter "${paramInfo.name}" expects a number, got NaN`,
               );
            }
            return String(rawValue);
         }
         const num = Number(str);
         if (isNaN(num)) {
            throw new BadRequestError(
               `Parameter "${paramInfo.name}" expects a number, got "${str}"`,
            );
         }
         return str;
      }

      case "boolean_type": {
         if (typeof rawValue === "boolean") return String(rawValue);
         const lower = str.toLowerCase();
         if (lower === "true" || lower === "1") return "true";
         if (lower === "false" || lower === "0") return "false";
         throw new BadRequestError(
            `Parameter "${paramInfo.name}" expects a boolean (true/false), got "${str}"`,
         );
      }

      case "date_type":
         return `@${str}`;

      case "timestamp_type":
      case "timestamptz_type":
         return `@${str}`;

      case "filter_expression_type":
         return `f'${str}'`;

      case "string_type":
      default:
         return `"${str.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
   }
}

/**
 * Validates that all required source parameters (those without a default
 * value) have been provided. Throws BadRequestError listing any missing
 * required parameters.
 */
export function validateRequiredParams(
   declaredParams: Malloy.ParameterInfo[],
   providedParams: Record<string, unknown>,
): void {
   const missing: string[] = [];
   for (const param of declaredParams) {
      if (!param.default_value && !(param.name in providedParams)) {
         missing.push(param.name);
      }
   }
   if (missing.length === 1) {
      throw new BadRequestError(
         `Parameter "${missing[0]}" is required`,
      );
   }
   if (missing.length > 1) {
      throw new BadRequestError(
         `Parameters ${missing.map((n) => `"${n}"`).join(", ")} are required`,
      );
   }
}

/**
 * Builds the Malloy parameter-passing syntax for a source invocation.
 * Returns a string like `(param1 is 42, param2 is "hello")` or empty
 * string if no params are provided.
 */
export function buildMalloyParamClause(
   providedParams: Record<string, unknown>,
   declaredParams: Malloy.ParameterInfo[],
): string {
   const paramsByName = new Map(declaredParams.map((p) => [p.name, p]));
   const assignments: string[] = [];

   for (const [name, rawValue] of Object.entries(providedParams)) {
      const paramInfo = paramsByName.get(name);
      if (!paramInfo) {
         throw new BadRequestError(
            `Unknown source parameter "${name}". Available parameters: ${declaredParams.map((p) => p.name).join(", ") || "(none)"}`,
         );
      }
      const literal = paramValueToMalloyLiteral(rawValue, paramInfo);
      assignments.push(`${name} is ${literal}`);
   }

   if (assignments.length === 0) return "";
   return `(${assignments.join(", ")})`;
}

/**
 * Finds the ParameterInfo array for a source by name.  Returns an empty
 * array when the source has no parameters or is not found.
 */
export function getSourceParams(
   sourceInfos: Malloy.SourceInfo[] | undefined,
   sourceName: string | undefined,
): Malloy.ParameterInfo[] {
   if (!sourceInfos || !sourceName) return [];
   const source = sourceInfos.find((s) => s.name === sourceName);
   return source?.parameters ?? [];
}
