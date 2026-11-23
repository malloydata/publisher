// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The path report reads the queried source's chain from what the model
 * recorded at its compile, not by walking the source map per query: a
 * query on a declared source walks nothing.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   clearQueryWalks,
   queryWalks,
} from "./materialization/serve/admission_parity";
import { queriedChain } from "./query_serving";

const MODEL = { contents: {} };

function hostWith(chains: Map<string, readonly string[]>) {
   return {
      modelDef: () => MODEL as never,
      sourceChains: () => chains,
   };
}

describe("the path report's source chain", () => {
   beforeEach(() => clearQueryWalks());
   afterEach(() => clearQueryWalks());

   it("is looked up for a source the model declares, walking nothing", async () => {
      const host = hostWith(new Map([["orders@m", ["orders@m", "base@m"]]]));
      expect(queriedChain(host, { sourceID: "orders@m" }, MODEL)).toEqual([
         "orders@m",
         "base@m",
      ]);
      expect(queryWalks().get("path_chain")).toBeUndefined();
   });

   it("walks, and counts the walk, for a source only the query's compile holds", async () => {
      const host = hostWith(new Map());
      expect(queriedChain(host, { sourceID: "inline@q" }, MODEL)).toEqual([
         "inline@q",
      ]);
      expect(queryWalks().get("path_chain")).toBe(1);
   });
});
