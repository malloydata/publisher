// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stable, whitespace-free JSON-like stringification with object
 * keys sorted at every depth.
 *
 * Used by {@link "./package_sanitization"} when logging an
 * unknown parameter-default-value shape — keeping the log line
 * deterministic so log-grep across runs stays useful.
 */

export function stableStringify(value: unknown): string {
   if (value === null || value === undefined) {
      return "null";
   }
   if (Array.isArray(value)) {
      return `[${value.map((item) => stableStringify(item)).join(",")}]`;
   }
   if (typeof value === "object") {
      const entries = Object.entries(value as Record<string, unknown>)
         .filter(([, v]) => v !== undefined)
         .sort(([a], [b]) => a.localeCompare(b));
      return `{${entries
         .map(
            ([key, nested]) =>
               `${JSON.stringify(key)}:${stableStringify(nested)}`,
         )
         .join(",")}}`;
   }
   return JSON.stringify(value);
}
