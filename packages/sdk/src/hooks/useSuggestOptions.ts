import { useQueries } from "@tanstack/react-query";
import { useMemo } from "react";
import type { Given } from "../client";
import { useServer } from "../components/ServerProvider";

/**
 * How many options a generated `suggest { source=… dimension=… }` asks for.
 *
 * Stated here rather than left to the server's `DEFAULT_QUERY_ROW_LIMIT`, which
 * is also 1000 but caps silently and in whatever order the warehouse returns.
 * An explicit limit with an explicit order makes a truncated list the same list
 * every time, and makes the cap something a reader of this file can see.
 * A dimension with more distinct values than this needs a hand-written
 * `suggest { query=… }`: a picker over thousands of options is the wrong
 * control anyway.
 */
export const SUGGEST_OPTION_LIMIT = 1000;

/**
 * The name a `group_by` gives its output column, which is the last segment of a
 * field path.
 *
 * `order_by` takes an output field name, not a path: `order_by:
 * products.category` is refused outright ("order_by takes the name of a field in
 * the query output, not a path"), while `group_by: products.category` names its
 * column `category`. A joined dimension is the ordinary case for a `suggest`, so
 * getting this wrong would have failed the option query for most of them.
 */
function outputFieldName(dimension: string): string {
   const segments = dimension.split(".");
   return segments[segments.length - 1];
}

/**
 * The query a `suggest { source=… dimension=… }` runs.
 *
 * Exported so the spec can assert the text this hook actually sends. A test that
 * rebuilds the same string alongside it proves only that the test agrees with
 * itself, and this text has already been wrong once: `order_by` takes an output
 * field name, not a path, so a joined dimension failed the query outright.
 */
export function buildSuggestQuery(source: string, dimension: string): string {
   return (
      `run: ${source} -> {\n` +
      `  group_by: ${dimension}\n` +
      `  order_by: ${outputFieldName(dimension)} asc\n` +
      `  limit: ${SUGGEST_OPTION_LIMIT}\n` +
      `}`
   );
}

/**
 * Resolve the option lists behind `select` / `multiselect` controls.
 *
 * A `suggest` is an ordinary query: either a named one (`suggest { query=… }`)
 * or a grouping over a dimension (`suggest { source=… dimension=… }`), so this
 * runs it on the same governed query endpoint everything else uses. No new
 * endpoint, and the row caps and authorize gates that apply to a surface's own
 * queries apply to its dropdowns too.
 *
 * Takes plain `Given`s: `suggest` is declared on the given, so a notebook's
 * Parameters panel and a dashboard's control row fill the same dropdown from
 * the same declaration.
 *
 * Requested with `compactJson` because these results are never rendered: the
 * flat row form is what the widget needs, and asking for the full Malloy result
 * would mean parsing rendering metadata to get at a list of strings.
 *
 * KNOWN GAP, latent until something populates `suggest`. The request carries no
 * `givens`, and `QueryRequest` has a field for them. That is fine for row caps
 * but not for `#(authorize)`: a gate is evaluated against the request's givens,
 * so a `suggest` over a gated source is denied and every dropdown on it resolves
 * to "Options unavailable": the failure surfaces honestly, but the control is
 * unusable. Deliberately not guessed at here, because *which* givens a suggest
 * should carry is a real decision: the applied ones make the option list depend
 * on the current filters, which this hook's caching deliberately avoids, while
 * only the gate-relevant ones means knowing which those are. Whoever populates
 * `suggest` server-side should settle it, and this hook's signature will need
 * the values passed in.
 */
export function useSuggestOptions(
   environmentName: string,
   packageName: string,
   modelPath: string | undefined,
   specs: Given[],
): {
   options: Map<string, string[]>;
   isLoading: boolean;
   /**
    * Givens whose suggest query failed.
    *
    * Reported separately from an empty option list because the two look
    * identical in a dropdown and mean opposite things: "this dimension has no
    * values" is data, "the query did not run" is a fault, and a reader offered
    * an empty picker has no way to tell which they are looking at.
    */
   failed: Set<string>;
} {
   const { apiClients } = useServer();

   // Only a picker needs options; a text box or a slider would just be issuing
   // a query whose answer it never shows.
   const suggestable = useMemo(
      () =>
         specs.filter(
            (spec) =>
               spec.name !== undefined &&
               (spec.control === "select" || spec.control === "multiselect") &&
               (spec.suggest?.query !== undefined ||
                  (spec.suggest?.source !== undefined &&
                     spec.suggest?.dimension !== undefined)),
         ),
      [specs],
   );

   const results = useQueries({
      queries: suggestable.map((spec) => ({
         queryKey: [
            "givenSuggest",
            environmentName,
            packageName,
            modelPath,
            spec.name,
            spec.suggest?.query,
            spec.suggest?.source,
            spec.suggest?.dimension,
         ],
         enabled: modelPath !== undefined,
         // Option lists change with the data, not with the filters, so they are
         // cached well past a single control interaction.
         staleTime: 5 * 60 * 1000,
         refetchOnWindowFocus: false,
         queryFn: async () => {
            const suggest = spec.suggest ?? {};
            const response = await apiClients.models.executeQueryModel(
               environmentName,
               packageName,
               modelPath as string,
               suggest.query !== undefined
                  ? { queryName: suggest.query, compactJson: true }
                  : {
                       query: buildSuggestQuery(
                          suggest.source as string,
                          suggest.dimension as string,
                       ),
                       compactJson: true,
                    },
            );
            return readOptionValues(
               response.data.result,
               suggest.dimension === undefined
                  ? undefined
                  : outputFieldName(suggest.dimension),
            );
         },
      })),
   });

   const options = useMemo(() => {
      const byName = new Map<string, string[]>();
      suggestable.forEach((spec, index) => {
         const value = results[index]?.data;
         if (spec.name && value) byName.set(spec.name, value);
      });
      return byName;
      // `results` is a fresh array each render; its data identity is what
      // matters, so depend on that rather than the wrapper objects.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [suggestable, results.map((result) => result.data).join("|")]);

   const failed = useMemo(() => {
      const names = new Set<string>();
      suggestable.forEach((spec, index) => {
         if (spec.name && results[index]?.isError) names.add(spec.name);
      });
      return names;
      // Same reasoning as `options`: `results` is a fresh array each render.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [suggestable, results.map((result) => result.isError).join("|")]);

   return {
      options,
      isLoading: results.some((result) => result.isLoading),
      failed,
   };
}

/**
 * Pull the option strings out of a compact-JSON result.
 *
 * Prefers the named column and otherwise takes the first, which covers a
 * `suggest { query=… }` whose column is named something else. Nulls are
 * dropped: "no value" is expressed by clearing the control, not by picking an
 * empty option.
 *
 * The name to pass is the OUTPUT column, so a joined `products.category` is
 * looked up as `category`. Passing the path found nothing and fell through to
 * the first column, which happened to be right only because the generated query
 * selects exactly one.
 */
export function readOptionValues(
   result: string | undefined,
   dimension: string | undefined,
): string[] {
   if (!result) return [];
   let rows: unknown;
   try {
      rows = JSON.parse(result);
   } catch {
      return [];
   }
   if (!Array.isArray(rows)) return [];

   const values: string[] = [];
   const seen = new Set<string>();
   for (const row of rows) {
      if (row === null || typeof row !== "object") continue;
      const record = row as Record<string, unknown>;
      const keys = Object.keys(record);
      const key =
         dimension !== undefined && dimension in record ? dimension : keys[0];
      const value = key === undefined ? undefined : record[key];
      if (value === null || value === undefined) continue;
      const text = String(value);
      if (seen.has(text)) continue;
      seen.add(text);
      values.push(text);
   }
   return values;
}
