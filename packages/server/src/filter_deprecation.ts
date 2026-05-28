/**
 * Minimal structural type the helper needs from an HTTP response. Kept
 * narrower than `express.Response` so tests can pass a tiny stub instead
 * of constructing a full Express response.
 */
export interface HeaderSetter {
   setHeader(name: string, value: string): unknown;
}

/**
 * Attach RFC 8594 deprecation headers when the request carries any of the
 * legacy `#(filter)` API surface (`filterParams` / `bypassFilters` on POST,
 * `filter_params` / `bypass_filters` on the notebook-cell GET). The
 * complementary operator-facing notice for legacy *models* (independent of
 * whether the caller used the deprecated request fields) ships as a
 * one-time warn log in `Model`'s constructor — see service/model.ts.
 *
 * `filterParams` here is the *parsed* value (after JSON.parse for the GET
 * notebook-cell path), so an empty `{}` is treated as a no-op opt-out — the
 * header only fires when the caller actually supplied filter values.
 */
export const setFilterDeprecationHeaders = (
   res: HeaderSetter,
   options: { filterParams?: unknown; bypassFilters?: unknown },
): void => {
   const hasFilterParams =
      options.filterParams !== undefined &&
      options.filterParams !== null &&
      !(
         typeof options.filterParams === "object" &&
         !Array.isArray(options.filterParams) &&
         Object.keys(options.filterParams as Record<string, unknown>).length ===
            0
      );
   if (hasFilterParams || options.bypassFilters !== undefined) {
      res.setHeader("Deprecation", "true");
      res.setHeader(
         "Link",
         '<https://github.com/malloydata/publisher/blob/main/docs/givens.md>; rel="deprecation"; type="text/markdown"',
      );
   }
};
