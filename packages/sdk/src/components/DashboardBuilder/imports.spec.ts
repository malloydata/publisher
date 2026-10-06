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

   it("joins an exporter's existing import when a re-exporting model is the one credited", () => {
      const next = withSource(
         doc("../storefront.malloy"),
         "regions",
         "data_app.malloy",
         "dashboards/x.malloy",
         ["data_app.malloy", "storefront.malloy"],
      );
      expect(next).toEqual([
         {
            kind: "names",
            names: ["orders", "regions"],
            from: "../storefront.malloy",
         },
      ]);
   });
});

describe("reaches, through a whole-file import", () => {
   const whole = (from: string) => ({
      imports: [{ kind: "all" as const, from }],
      sources: [],
   });

   it("counts an import of the model that exports the source", () => {
      expect(
         reaches(whole("../storefront.malloy"), "orders", "storefront.malloy"),
      ).toBe(true);
   });

   it("counts an import of any exporter, not only the one the catalog credits", () => {
      const args = [
         "orders",
         "data_app.malloy",
         "dashboards/x.malloy",
      ] as const;
      expect(reaches(whole("../storefront.malloy"), ...args)).toBe(false);
      expect(
         reaches(whole("../storefront.malloy"), ...args, [
            "data_app.malloy",
            "storefront.malloy",
         ]),
      ).toBe(true);
   });

   it("reads './../m.malloy' as '../m.malloy'", () => {
      expect(
         reaches(
            whole("./../storefront.malloy"),
            "orders",
            "storefront.malloy",
            "dashboards/x.malloy",
         ),
      ).toBe(true);
   });
});
