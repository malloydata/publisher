// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { dropExceptItems } from "./declaration_text";

const absent = (names: string[]) => (n: string) => names.includes(n);

describe("dropExceptItems", () => {
   it("deletes an except: statement whose only item names nothing stored", () => {
      expect(
         dropExceptItems("extend {\n  except: region\n}", absent(["region"])),
      ).toEqual({ text: "extend {\n  \n}", dropped: ["region"] });
   });

   it("keeps the items that name a stored column", () => {
      expect(
         dropExceptItems(
            "extend { except: region, secret }",
            absent(["region"]),
         ),
      ).toEqual({ text: "extend { except: secret }", dropped: ["region"] });
   });

   it("handles a backquoted name", () => {
      expect(
         dropExceptItems("extend { except: `my col` }", absent(["my col"])),
      ).toEqual({ text: "extend {  }", dropped: ["my col"] });
   });

   it("never deletes an item the text itself defines, which may hide a field", () => {
      const text = "extend { dimension: x is 1 } extend { except: x }";
      expect(dropExceptItems(text, absent(["x"]))).toEqual({
         text,
         dropped: [],
      });
   });

   it("leaves accept: and other statements alone", () => {
      const text = "extend { accept: a, b; measure: n is count() }";
      expect(dropExceptItems(text, () => true)).toEqual({ text, dropped: [] });
   });

   it("never deletes an except: inside a join's own extend, which hides the joined source's field", () => {
      // The re-rooted table stores none of the joined source's columns, so
      // every such name is "unstored"; deleting it would expose c.name.
      const text =
         "extend {\n  join_one: c is customers extend { except: name } on c.id = customer_id\n  except: region\n}";
      expect(dropExceptItems(text, absent(["name", "region"]))).toEqual({
         text: "extend {\n  join_one: c is customers extend { except: name } on c.id = customer_id\n  \n}",
         dropped: ["region"],
      });
   });

   it("deletes an unstored item of a top-level include's except:", () => {
      expect(
         dropExceptItems(
            "include { public: *; except: region }",
            absent(["region"]),
         ),
      ).toEqual({ text: "include { public: *;  }", dropped: ["region"] });
   });
});
