import { useMemo } from "react";
import { Given, RawNotebook } from "../client";

/**
 * Extracts the deduplicated list of model-level `given:` declarations from
 * a notebook's sources. The server attaches the same model-level givens to
 * every `Source` for SDK ergonomics (see #761); this hook collapses them
 * back to one entry per name so callers render a single input per given.
 *
 * Returns an empty array when no givens are declared on any source.
 */
export function useModelGivens(notebook: RawNotebook | undefined): Given[] {
   return useMemo(() => {
      if (!notebook?.sources?.length) return [];
      const byName = new Map<string, Given>();
      for (const source of notebook.sources) {
         if (!source.givens?.length) continue;
         for (const given of source.givens) {
            if (given.name && !byName.has(given.name)) {
               byName.set(given.name, given);
            }
         }
      }
      return Array.from(byName.values());
   }, [notebook]);
}
