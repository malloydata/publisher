import type * as Malloy from "@malloydata/malloy-interfaces";
import { BadRequestError } from "../errors";

/**
 * Parses a raw string value into a Malloy literal expression based on
 * the declared parameter type from the SourceInfo.
 */
export function paramValueToMalloyLiteral(
   rawValue: string,
   paramInfo: Malloy.ParameterInfo,
): string {
   const kind = paramInfo.type.kind;

   switch (kind) {
      case "number_type": {
         const num = Number(rawValue);
         if (isNaN(num)) {
            throw new BadRequestError(
               `Parameter "${paramInfo.name}" expects a number, got "${rawValue}"`,
            );
         }
         return rawValue;
      }

      case "boolean_type": {
         const lower = rawValue.toLowerCase();
         if (lower === "true" || lower === "1") return "true";
         if (lower === "false" || lower === "0") return "false";
         throw new BadRequestError(
            `Parameter "${paramInfo.name}" expects a boolean (true/false), got "${rawValue}"`,
         );
      }

      case "date_type":
         return `@${rawValue}`;

      case "timestamp_type":
      case "timestamptz_type":
         return `@${rawValue}`;

      case "filter_expression_type":
         return `f'${rawValue}'`;

      case "string_type":
      default:
         return `"${rawValue.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
   }
}

/**
 * Validates that all required source parameters (those without a default
 * value) have been provided. Throws BadRequestError listing any missing
 * required parameters.
 */
export function validateRequiredParams(
   declaredParams: Malloy.ParameterInfo[],
   providedParams: Record<string, string>,
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
   providedParams: Record<string, string>,
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
