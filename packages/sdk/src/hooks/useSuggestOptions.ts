import { useQueries } from "@tanstack/react-query";
import { useMemo } from "react";
import type { Given } from "../client";
import { useServer } from "../components/ServerProvider";

/**
 * Resolve the option lists behind `select` / `multiselect` controls.
 *
 * A `suggest` is an ordinary query — either a named one (`suggest { query=… }`)
 * or a grouping over a dimension (`suggest { source=… dimension=… }`) — so this
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
 */
export function useSuggestOptions(
   environmentName: string,
   packageName: string,
   modelPath: string | undefined,
   specs: Given[],
): { options: Map<string, string[]>; isLoading: boolean } {
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
                       query: `run: ${suggest.source} -> { group_by: ${suggest.dimension} }`,
                       compactJson: true,
                    },
            );
            return readOptionValues(response.data.result, suggest.dimension);
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

   return {
      options,
      isLoading: results.some((result) => result.isLoading),
   };
}

/**
 * Pull the option strings out of a compact-JSON result.
 *
 * Prefers the named dimension's column and otherwise takes the first, which
 * covers a `suggest { query=… }` whose column is named something else. Nulls
 * are dropped: "no value" is expressed by clearing the control, not by picking
 * an empty option.
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
