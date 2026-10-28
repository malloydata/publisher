// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { toExplorerQueries } from "./schemaModel";

describe("toExplorerQueries", () => {
   it("maps a named query's doc and renderer tags from the publisher's raw annotation lines", () => {
      const [q] = toExplorerQueries([
         {
            name: "genre_movie_map",
            sourceName: undefined,
            annotations: [
               "#(doc) Movies by genre, as a map.\n",
               "# bar_chart\n",
            ],
         },
      ]);
      expect(q).toEqual({
         name: "genre_movie_map",
         doc: "Movies by genre, as a map.",
         tags: ["bar_chart"],
      });
   });

   it("drops entries the publisher left unnamed and tolerates a missing list", () => {
      expect(toExplorerQueries([{ name: undefined }])).toEqual([]);
      expect(toExplorerQueries(undefined)).toEqual([]);
   });

   it("yields no doc and no tags for an unannotated query", () => {
      const [q] = toExplorerQueries([{ name: "plain" }]);
      expect(q.name).toBe("plain");
      expect(q.doc).toBeUndefined();
      expect(q.tags ?? []).toEqual([]);
   });
});
