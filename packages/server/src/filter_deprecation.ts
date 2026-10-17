// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { recordDeprecatedUsage } from "./deprecated_usage_metrics";

/** Narrower than `express.Response` so tests can pass a stub. */
export interface HeaderSetter {
   setHeader(name: string, value: string): unknown;
}

/** The legacy `#(filter)` request fields, as a query passed them. */
interface FilterFields {
   filterParams?: unknown;
   bypassFilters?: unknown;
}

/**
 * Which legacy fields a query actually used. `filterParams` is the *parsed*
 * value (after JSON.parse for the GET notebook-cell path), so an empty `{}` is
 * a no-op opt-out, not a use.
 */
function usedFilterFields(options: FilterFields): {
   filterParams: boolean;
   bypassFilters: boolean;
} {
   const { filterParams } = options;
   return {
      filterParams:
         filterParams !== undefined &&
         filterParams !== null &&
         !(
            typeof filterParams === "object" &&
            !Array.isArray(filterParams) &&
            Object.keys(filterParams as Record<string, unknown>).length === 0
         ),
      bypassFilters: options.bypassFilters !== undefined,
   };
}

/**
 * Count a query that used `filterParams` / `filter_params` or `bypassFilters`
 * / `bypass_filters`. The REST routes count through
 * {@link setFilterDeprecationHeaders}; MCP, which has no headers to set,
 * calls this directly.
 */
export function recordDeprecatedFilterUsage(options: FilterFields): {
   filterParams: boolean;
   bypassFilters: boolean;
} {
   const used = usedFilterFields(options);
   if (used.filterParams) recordDeprecatedUsage("filter_params");
   if (used.bypassFilters) recordDeprecatedUsage("bypass_filters");
   return used;
}

/**
 * Attach RFC 8594 deprecation headers, and count the use, when the request
 * actually supplied legacy `#(filter)` values (`filterParams` /
 * `bypassFilters` on POST, `filter_params` / `bypass_filters` on the
 * notebook-cell GET).
 */
export const setFilterDeprecationHeaders = (
   res: HeaderSetter,
   options: FilterFields,
): void => {
   const used = recordDeprecatedFilterUsage(options);
   if (used.filterParams || used.bypassFilters) {
      res.setHeader("Deprecation", "true");
      res.setHeader(
         "Link",
         '<https://github.com/malloydata/publisher/blob/main/docs/givens.md>; rel="deprecation"; type="text/markdown"',
      );
   }
};
