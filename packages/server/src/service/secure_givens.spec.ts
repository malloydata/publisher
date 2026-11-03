// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   type SecureGivenModelInput,
   summarizeSecureGivens,
} from "./secure_givens";

const secure = (name: string, type: string) => ({ name, type, secure: true });

function model(
   modelPath: string,
   givens: SecureGivenModelInput["givens"],
   compiled = true,
): SecureGivenModelInput {
   return { modelPath, givens, compiled };
}

describe("summarizeSecureGivens", () => {
   it("is empty for no models, and for models with no secure given", () => {
      expect(summarizeSecureGivens([])).toEqual([]);
      expect(
         summarizeSecureGivens([
            model("a.malloy", []),
            model("b.malloy", [{ name: "REGION", type: "string" }]),
         ]),
      ).toEqual([]);
   });

   it("leaves out a given that is not #(secure)", () => {
      expect(
         summarizeSecureGivens([
            model("a.malloy", [
               { name: "REGION", type: "string" },
               { name: "OPEN", type: "string", secure: false },
               secure("ROLES", "string[]"),
            ]),
         ]),
      ).toEqual([
         { name: "ROLES", type: "string[]", modelPaths: ["a.malloy"] },
      ]);
   });

   it("names every model that surfaces the given, the importer included, sorted and once each", () => {
      // `orders.malloy` imports `shared.malloy`, so both compiled models
      // report ROLES; a duplicate in one model's list is counted once.
      expect(
         summarizeSecureGivens([
            model("shared.malloy", [secure("ROLES", "string[]")]),
            model("orders.malloy", [
               secure("ROLES", "string[]"),
               secure("ROLES", "string[]"),
            ]),
         ]),
      ).toEqual([
         {
            name: "ROLES",
            type: "string[]",
            modelPaths: ["orders.malloy", "shared.malloy"],
         },
      ]);
   });

   it("leaves out notebooks and keeps hidden and nested model files", () => {
      expect(
         summarizeSecureGivens([
            model("nb.malloynb", [secure("ROLES", "string[]")]),
            model(".hidden.malloy", [secure("ROLES", "string[]")]),
            model("sub/dir/m.malloy", [secure("ROLES", "string[]")]),
         ]),
      ).toEqual([
         {
            name: "ROLES",
            type: "string[]",
            modelPaths: [".hidden.malloy", "sub/dir/m.malloy"],
         },
      ]);
   });

   it("leaves out a model that did not compile", () => {
      expect(
         summarizeSecureGivens([
            model("broken.malloy", [secure("ROLES", "string[]")], false),
         ]),
      ).toEqual([]);
   });

   it("keeps one entry per (name, type): one name with two types is two entries", () => {
      expect(
         summarizeSecureGivens([
            model("a.malloy", [secure("TENANT", "string")]),
            model("b.malloy", [secure("TENANT", "number")]),
            model("c.malloy", [secure("TENANT", "string")]),
         ]),
      ).toEqual([
         { name: "TENANT", type: "number", modelPaths: ["b.malloy"] },
         {
            name: "TENANT",
            type: "string",
            modelPaths: ["a.malloy", "c.malloy"],
         },
      ]);
   });

   it("sorts entries by name then type, by code unit, whatever the input order", () => {
      const models = [
         model("z.malloy", [
            secure("b_given", "filter<string>"),
            secure("A_GIVEN", "number[]"),
            secure("a_given", "string"),
         ]),
         model("a.malloy", [secure("b_given", "boolean")]),
      ];
      const expected = [
         { name: "A_GIVEN", type: "number[]", modelPaths: ["z.malloy"] },
         { name: "a_given", type: "string", modelPaths: ["z.malloy"] },
         { name: "b_given", type: "boolean", modelPaths: ["a.malloy"] },
         { name: "b_given", type: "filter<string>", modelPaths: ["z.malloy"] },
      ];
      expect(summarizeSecureGivens(models)).toEqual(expected);
      expect(summarizeSecureGivens([...models].reverse())).toEqual(expected);
   });

   it("carries the rendered type through as given (array and filter forms)", () => {
      const summary = summarizeSecureGivens([
         model("a.malloy", [
            secure("IDS", "number[]"),
            secure("F", "filter<timestamp>"),
         ]),
      ]);
      expect(summary.map((s) => s.type)).toEqual([
         "filter<timestamp>",
         "number[]",
      ]);
   });
});
