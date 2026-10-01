// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The acceptance set of Credible's pycommon test_index_annotation.py, so a
// model tagged for one is read the same by the other, plus the `n=` cap this
// implementation honours.

import { describe, expect, it } from "bun:test";
import { globToRegExp, parseIndexTag } from "./index_annotation";

describe("parseIndexTag: tags the service also accepts", () => {
   it.each([
      "#(index)",
      "#(index_values)",
      "#(index_values) n=-1",
      "#( index_values n=200 )",
      '#(index freshness.window="24h")',
      '#(index freshness.window="7d")',
      '#(index sharing="private" refresh="incremental")',
      "# ( index )",
   ])("recognises %s", (line) => {
      expect(parseIndexTag([line])).not.toBeNull();
   });

   it("finds the tag among other annotations", () => {
      expect(
         parseIndexTag(["#(doc) Status.", '#(index freshness.window="1h")']),
      ).not.toBeNull();
   });

   it("reads annotation objects as well as strings", () => {
      expect(parseIndexTag([{ value: "#(index)" }])).toEqual({});
   });
});

describe("parseIndexTag: things that are not the tag", () => {
   it.each([
      "#(indexed)",
      "#(index_of_things)",
      "#(filter) dimension=region required",
      "#(hidden)",
      "#(doc) index this",
   ])("rejects %s", (line) => {
      expect(parseIndexTag([line])).toBeNull();
   });

   it("has nothing to find with no annotations", () => {
      expect(parseIndexTag(undefined)).toBeNull();
      expect(parseIndexTag([])).toBeNull();
   });
});

describe("parseIndexTag: the n cap", () => {
   it("reads n inside the parens", () => {
      expect(parseIndexTag(["#( index_values n=200 )"])).toEqual({ n: 200 });
      expect(parseIndexTag(["#(index_values n=15)"])).toEqual({ n: 15 });
   });

   it("reads n after the parens, as the service's own tests write it", () => {
      expect(parseIndexTag(["#(index_values) n=40"])).toEqual({ n: 40 });
   });

   it("treats n=-1 and n=0 as no cap of the author's own", () => {
      expect(parseIndexTag(["#(index_values) n=-1"])).toEqual({});
      expect(parseIndexTag(["#(index_values n=0)"])).toEqual({});
   });

   it("does not mistake another key ending in n for the cap", () => {
      expect(parseIndexTag(["#(index refresh=incremental)"])).toEqual({});
      expect(parseIndexTag(["#(index min=5)"])).toEqual({});
   });
});

describe("globToRegExp", () => {
   it("matches source.dimension patterns, case-insensitively", () => {
      const re = globToRegExp("customers.*");
      expect(re.test("customers.tier")).toBe(true);
      expect(re.test("Customers.Tier")).toBe(true);
      expect(re.test("orders.tier")).toBe(false);
   });

   it("matches across the dot with a leading star", () => {
      expect(globToRegExp("*.status").test("orders.status")).toBe(true);
      expect(globToRegExp("*.status").test("orders.status_code")).toBe(false);
   });

   it("treats every other character literally", () => {
      expect(globToRegExp("a.b").test("axb")).toBe(false);
      expect(globToRegExp("a+b").test("a+b")).toBe(true);
   });
});
