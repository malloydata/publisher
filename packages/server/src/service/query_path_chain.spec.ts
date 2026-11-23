// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The path report reads the queried source's chain from what the model
 * recorded at its compile, not by walking the source map per query: under
 * `lookup` a query on a declared source walks nothing; under `shadow` a
 * recorded chain that disagrees with the walk is drift.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   clearParityDrifts,
   clearQueryWalks,
   queryWalks,
   withAdmissionMode,
} from "../test_helpers/admission_parity";
import { parityDrifts } from "./materialization/serve/admission_parity";
import { queriedChain } from "./query_serving";

const MODEL = { contents: {} };

function hostWith(chains: Map<string, readonly string[]>) {
   return {
      modelDef: () => MODEL as never,
      sourceChains: () => chains,
   };
}

describe("the path report's source chain", () => {
   beforeEach(() => {
      clearParityDrifts();
      clearQueryWalks();
   });
   afterEach(() => {
      clearParityDrifts();
      clearQueryWalks();
   });

   it("is looked up for a source the model declares, walking nothing", async () => {
      const host = hostWith(new Map([["orders@m", ["orders@m", "base@m"]]]));
      await withAdmissionMode("lookup", async () => {
         expect(queriedChain(host, { sourceID: "orders@m" }, MODEL)).toEqual([
            "orders@m",
            "base@m",
         ]);
      });
      expect(queryWalks().get("path_chain")).toBeUndefined();
   });

   it("walks, and counts the walk, for a source only the query's compile holds", async () => {
      const host = hostWith(new Map());
      await withAdmissionMode("lookup", async () => {
         expect(queriedChain(host, { sourceID: "inline@q" }, MODEL)).toEqual([
            "inline@q",
         ]);
      });
      expect(queryWalks().get("path_chain")).toBe(1);
   });

   it("answers the walk in shadow, and records drift when the record differs", async () => {
      const host = hostWith(new Map([["orders@m", ["orders@m", "base@m"]]]));
      await withAdmissionMode("shadow", async () => {
         expect(queriedChain(host, { sourceID: "orders@m" }, MODEL)).toEqual([
            "orders@m",
         ]);
      });
      expect(parityDrifts().map((d) => d.side)).toEqual(["path_chain"]);
   });
});
