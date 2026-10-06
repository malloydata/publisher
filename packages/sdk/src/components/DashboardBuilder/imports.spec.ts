// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { importPathOf, reaches, withSource } from "./imports";

const doc = (from: string) => ({
   imports: [{ kind: "names" as const, names: ["orders"], from }],
   sources: [],
});

describe("importPathOf", () => {
   it("names a model from wherever the document sits", () => {
      expect(importPathOf("storefront.malloy", "dashboards/x.malloy")).toBe(
         "../storefront.malloy",
      );
      expect(importPathOf("storefront.malloy", "notebooks/sub/x.malloy")).toBe(
         "../../storefront.malloy",
      );
      expect(importPathOf("models/m.malloy", "models/x.malloy")).toBe(
         "./m.malloy",
      );
      expect(importPathOf("m.malloy", "x.malloy")).toBe("./m.malloy");
   });
});

describe("withSource", () => {
   it("writes a new import relative to a document one folder deeper", () => {
      const next = withSource(
         { imports: [], sources: [] },
         "regions",
         "storefront.malloy",
         "notebooks/sub/x.malloy",
      );
      expect(next).toEqual([
         { kind: "names", names: ["regions"], from: "../../storefront.malloy" },
      ]);
   });

   it("joins the model's existing import however it is spelled", () => {
      const next = withSource(
         doc("./../storefront.malloy"),
         "regions",
         "storefront.malloy",
         "dashboards/x.malloy",
      );
      expect(next).toEqual([
         {
            kind: "names",
            names: ["orders", "regions"],
            from: "./../storefront.malloy",
         },
      ]);
   });

   it("adds nothing for a source the file already imports by name", () => {
      expect(
         reaches(doc("../storefront.malloy"), "orders", "storefront.malloy"),
      ).toBe(true);
   });
});
