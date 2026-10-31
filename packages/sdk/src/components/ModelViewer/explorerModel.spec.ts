// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { describe, expect, it } from "bun:test";
import { requiredFiltersBySource, sourceNameSet } from "./explorerModel";

function sourceInfo(
   name: string,
   annotations: string[] = [],
): Malloy.SourceInfo {
   return {
      name,
      schema: { fields: [] },
      annotations: annotations.map((value) => ({ value })),
   } as Malloy.SourceInfo;
}

describe("requiredFiltersBySource", () => {
   it("lists each source's required, visible filters and omits sources with none", () => {
      const out = requiredFiltersBySource([
         sourceInfo("orders", [
            "#(filter) name=region dimension=region type=equal required\n",
            "#(filter) name=tenant dimension=tenant type=equal implicit required\n",
            "#(filter) name=status dimension=status type=equal\n",
         ]),
         sourceInfo("plain"),
      ]);
      expect([...out]).toEqual([["orders", ["region"]]]);
   });
});

describe("sourceNameSet", () => {
   it("names every source once", () => {
      const names = sourceNameSet([
         { name: "a" },
         { name: "b" },
         { name: "a" },
      ]);
      expect([...names]).toEqual(["a", "b"]);
      expect(sourceNameSet([]).size).toBe(0);
   });
});
