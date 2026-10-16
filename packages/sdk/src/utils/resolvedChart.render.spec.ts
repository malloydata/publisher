// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, setDefaultTimeout } from "bun:test";
import { resolvedChart } from "./resolvedChart";

/**
 * `resolvedChart` against the renderer itself: a real query run by Malloy on
 * DuckDB, its view tagged one way and the named query over it another, handed
 * to `@malloydata/render` and asked which plugin draws it. The tags merge the
 * way a report tile's do (the view's first, then the query's own).
 */

// DuckDB and the renderer from cold.
setDefaultTimeout(30_000);

/** The renderer's plugin names, in `resolvedChart`'s vocabulary. */
const PLUGIN: Record<string, string> = {
   bar: "bar_chart",
   line: "line_chart",
   combo: "combo_chart",
};

async function drawnBy(viewTag: string, queryTag: string) {
   const { Runtime, API } = await import("@malloydata/malloy");
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const { MalloyRenderer } = await import("@malloydata/render");
   const text = `source: s is duckdb.sql("select 'a' as k, 1 as v union all select 'b', 2") extend {
  ${viewTag}
  view: by_k is { group_by: k; aggregate: n is v.sum() }
}
${queryTag}
query: q is s -> by_k
`;
   const connection = new DuckDBConnection("duckdb", ":memory:");
   try {
      const runtime = new Runtime({
         urlReader: { readURL: async () => text },
         connections: { lookupConnection: async () => connection },
      } as never);
      const result = await runtime
         .loadModel(new URL("file:///m.malloy"))
         .loadQueryByName("q")
         .run();
      const viz = new MalloyRenderer().createViz();
      viz.setResult(API.util.wrapResult(result));
      const root = (
         viz.getMetadata() as unknown as {
            getRootField(): {
               renderAs(): string;
               getPlugins(): Array<{ name: string }>;
            };
         }
      ).getRootField();
      const plugin = root.getPlugins()[0]?.name;
      if (plugin) return PLUGIN[plugin] ?? plugin;
      return root.renderAs() === "table" ? "table" : undefined;
   } finally {
      await connection.close();
   }
}

describe("resolvedChart agrees with the renderer", () => {
   const cases: Array<[string, string]> = [
      ["# line_chart", "# viz=bar"],
      ["# bar_chart { size=spark }", "# viz=line"],
      ["# bar_chart", "# viz=table"],
      ["# big_value", "# viz=table"],
      ["# bar_chart", "# scatter_chart"],
      ["# scatter_chart", "# bar_chart"],
      ["# bar_chart", "# line_chart"],
      ["", "# viz=scatter"],
      ["# bar_chart", "# viz=scatter scatter_chart"],
   ];
   for (const [viewTag, queryTag] of cases)
      it(`${JSON.stringify(viewTag)} under ${JSON.stringify(queryTag)}`, async () => {
         const drawn = await drawnBy(viewTag, queryTag);
         const resolved = resolvedChart([viewTag, queryTag]) ?? "table";
         expect(resolved).toBe(drawn ?? "table");
      });
});
