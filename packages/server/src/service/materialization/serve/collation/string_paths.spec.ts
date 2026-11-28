// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { stringPaths } from "./string_paths";

const field = (path: string[]) => ({ node: "field", path });

describe("stringPaths over a query-shaped source", () => {
   // `deals is events -> { group_by: deal_id } extend { join_one: close …;
   //  dimension: is_open is close.type is null }`
   const close = {
      name: "close",
      join: "one",
      fields: [{ name: "type", type: "string" }],
   };
   const deals = {
      type: "query_source",
      query: { pipeline: [] },
      fields: [
         { name: "deal_id", type: "string" },
         { name: "stage_n", type: "number" },
         {
            name: "is_open",
            type: "boolean",
            e: { node: "is-null", e: field(["close", "type"]) },
         },
         close,
      ],
   };

   it("follows a field its extend declares through its expression", () => {
      expect(stringPaths(deals, [["is_open"], ["close", "type"]])).toEqual([
         ["close", "type"],
      ]);
   });

   it("still fails closed on a stage output that is no string", () => {
      expect(stringPaths(deals, [["stage_n"]])).toBeUndefined();
   });

   it("fails closed when the extend's field reads a path not listed", () => {
      expect(stringPaths(deals, [["is_open"]])).toBeUndefined();
   });

   it("keeps a string stage output as a compared path", () => {
      expect(stringPaths(deals, [["deal_id"]])).toEqual([["deal_id"]]);
   });
});
