// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * 6e: the conformance runner's classification of a
 * query live refuses (a source gate denies the caller), and of two queries
 * that share a name.
 */

import { expect, it } from "bun:test";
import { runV1CutoverConformance } from "../../../src/test_helpers/conformance/v1_cutover_runner";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";

const MODEL = `##! experimental { persistence givens }
import "raw.malloy"
given: ROLE :: string
#(authorize) $ROLE = 'admin'
#@ persist name="authz"
source: authz is orders_raw -> { select: id, region, amount } extend {
  measure: total is amount.sum()
}
`;
const Q = "run: authz -> { group_by: region; aggregate: total }";

it("a refusal both live and after the cutover is not a bug; same-named queries keep their own answers", async () => {
   const rows = await runV1CutoverConformance({
      package: { ...DATA_FILES, "raw.malloy": RAW_MODEL, "m.malloy": MODEL },
      queries: [
         {
            name: "admin",
            model: "m.malloy",
            malloy: Q,
            givens: { ROLE: "admin" },
         },
         {
            name: "viewer",
            model: "m.malloy",
            malloy: Q,
            givens: { ROLE: "viewer" },
         },
         {
            name: "dup",
            model: "m.malloy",
            malloy: Q,
            givens: { ROLE: "admin" },
         },
         {
            name: "dup",
            model: "m.malloy",
            malloy: Q,
            givens: { ROLE: "viewer" },
         },
      ],
   });
   expect(rows[1].verdict).not.toBe("bug");
   // The two `dup` rows were answered for different callers.
   expect(rows[2]).not.toEqual(rows[3]);
}, 300_000);
