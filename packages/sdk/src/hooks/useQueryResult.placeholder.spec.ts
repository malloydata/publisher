// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { onlyGivensDiffer, queryResultKey } from "./useQueryResult";

const spec = {
   environmentName: "e",
   packageName: "p",
   modelPath: "m.malloy",
   query: "run: a -> by_cat",
};

describe("which earlier answer a re-run may show", () => {
   it("keeps the last answer when only the control values changed", () => {
      expect(
         onlyGivensDiffer(
            [...queryResultKey({ ...spec, givens: { R: "w" } }), "server"],
            [...queryResultKey({ ...spec, givens: { R: "e" } }), "server"],
         ),
      ).toBe(true);
   });

   it("drops it when the query changed: another view is not its answer", () => {
      expect(
         onlyGivensDiffer(
            [...queryResultKey(spec), "server"],
            [
               ...queryResultKey({ ...spec, query: "run: a -> by_brand" }),
               "server",
            ],
         ),
      ).toBe(false);
   });

   it("drops it for another server", () => {
      expect(
         onlyGivensDiffer(
            [...queryResultKey(spec), "one"],
            [...queryResultKey(spec), "two"],
         ),
      ).toBe(false);
   });
});
