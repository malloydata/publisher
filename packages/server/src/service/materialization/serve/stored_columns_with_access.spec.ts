// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { storedColumnsWithAccess } from "./served_copy_serve";

describe("storedColumnsWithAccess (the v1 cutover's G2)", () => {
   it("keeps every stored column a field names, with its access modifier", () => {
      expect(
         storedColumnsWithAccess(
            [
               { name: "id", type: "BIGINT" },
               { name: "HOME_REGION", type: "VARCHAR" },
               { name: "secret", type: "VARCHAR" },
               { name: "orphan", type: "VARCHAR" },
            ],
            [
               { name: "id", type: "number" },
               {
                  name: "home_region",
                  type: "string",
                  accessModifier: "internal",
               },
               { name: "secret", type: "string", accessModifier: "private" },
               { name: "public_one", type: "string", accessModifier: "public" },
            ],
         ),
      ).toEqual([
         { name: "id", type: "BIGINT" },
         // Matched by its one case-folded field, under the author's spelling.
         { name: "home_region", type: "VARCHAR", access: "internal" },
         { name: "secret", type: "VARCHAR", access: "private" },
      ]);
   });

   it("matches nothing on an ambiguous fold", () => {
      expect(
         storedColumnsWithAccess(
            [{ name: "Ab", type: "VARCHAR" }],
            [
               { name: "ab", type: "string" },
               { name: "AB", type: "string" },
            ],
         ),
      ).toEqual([]);
   });
});
