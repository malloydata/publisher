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
      throw new BadRequestError(`Parameter "${missing[0]}" is required`);
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
      if (!paramInfo) continue;
      const literal = paramValueToMalloyLiteral(rawValue, paramInfo);
      assignments.push(`${name} is ${literal}`);
   }

   if (assignments.length === 0) return "";
   return `(${assignments.join(", ")})`;
}

/**
 * Returns a Malloy literal stub value for a parameter based on its type.
 * Used during notebook loading to fill in required parameters so the
 * notebook compiles (validating structure) without real values.
 */
export function stubValueForParam(paramInfo: Malloy.ParameterInfo): string {
   switch (paramInfo.type.kind) {
      case "number_type":
         return "0";
      case "boolean_type":
         return "true";
      case "date_type":
         return "@2000-01-01";
      case "timestamp_type":
      case "timestamptz_type":
         return "@2000-01-01 00:00:00";
      case "filter_expression_type":
         return "f'true'";
      case "string_type":
      default:
         return '""';
   }
}

/**
 * Builds a Malloy parameter clause containing stub values for every
 * *required* parameter (those without a default value).  Returns an
 * empty string when no stubs are needed.
 */
export function buildStubParamClause(
   declaredParams: Malloy.ParameterInfo[],
): string {
   const assignments: string[] = [];
   for (const param of declaredParams) {
      if (!param.default_value) {
         assignments.push(`${param.name} is ${stubValueForParam(param)}`);
      }
   }
   if (assignments.length === 0) return "";
   return `(${assignments.join(", ")})`;
}

/**
 * Scans cell text for `run: <sourceName>` patterns that reference known
 * parameterized sources and injects the given parameter clause after the
 * source name.  Returns the modified text and whether any injection
 * occurred.
 */
export function injectParamClauseIntoText(
   cellText: string,
   parameterizedSources: Map<string, string>,
): { text: string; modified: boolean } {
   let text = cellText;
   let modified = false;
   for (const [sourceName, clause] of parameterizedSources) {
      if (!clause) continue;
      const pattern = new RegExp(`(run:\\s+${sourceName})\\s*(->|$)`, "gm");
      const replaced = text.replace(pattern, `$1${clause} $2`);
      if (replaced !== text) {
         text = replaced;
         modified = true;
      }
   }
   return { text, modified };
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
