// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { dropValueUrlTags } from "./dropValueUrlTags";

const resultWith = (...values: string[]) =>
   JSON.stringify({
      schema: {
         fields: [
            {
               kind: "dimension",
               name: "pic",
               annotations: values.map((value) => ({ value })),
            },
         ],
      },
      data: { kind: "array_cell", array_value: [] },
   });

const annotationsOf = (result: string) =>
   (
      JSON.parse(result) as {
         schema: { fields: { annotations: { value: string }[] }[] };
      }
   ).schema.fields[0].annotations.map((a) => a.value);

describe("dropValueUrlTags", () => {
   it("removes an image tag and keeps the other properties on the line", () => {
      const out = annotationsOf(
         dropValueUrlTags(resultWith('# image { height=40px } label="Pic"\n')),
      );
      expect(out).toHaveLength(1);
      expect(out[0]).not.toContain("image");
      expect(out[0]).toContain("Pic");
   });

   it("removes a link tag and its url_template", () => {
      const out = annotationsOf(
         dropValueUrlTags(
            resultWith(
               '# link { url_template="https://attacker.example/$$" }\n',
            ),
         ),
      );
      expect(out.join("")).not.toContain("attacker");
      expect(out.join("")).not.toContain("link");
   });

   it("removes a backtick-quoted tag and one nested in another tag", () => {
      expect(
         annotationsOf(dropValueUrlTags(resultWith("# `image`\n"))).join(""),
      ).not.toContain("image");
      expect(
         annotationsOf(
            dropValueUrlTags(resultWith("# column { image }\n")),
         ).join(""),
      ).not.toContain("image");
   });

   it("removes a label that carries markup, and keeps a plain one", () => {
      const markup = annotationsOf(
         dropValueUrlTags(
            resultWith('# label="<img src=x onerror=alert(1)>"\n'),
         ),
      );
      expect(markup.join("")).not.toContain("<img");
      const plain = annotationsOf(
         dropValueUrlTags(resultWith('# label="a < b"\n')),
      );
      expect(plain[0]).toContain("a < b");
   });

   it("leaves chart tags, doc notes and a field named link as a value alone", () => {
      const source = resultWith(
         "# bar_chart { x = link }\n",
         "#(docs) image of the data\n# pivot { dimensions=[link] }\n",
      );
      expect(dropValueUrlTags(source)).toBe(source);
   });

   it("cleans an annotation nested in a record field, and in the result's own", () => {
      const result = JSON.stringify({
         annotations: [{ value: "# link\n" }],
         schema: {
            fields: [
               {
                  name: "r",
                  type: {
                     kind: "record_type",
                     fields: [
                        { name: "p", annotations: [{ value: "# image\n" }] },
                     ],
                  },
               },
            ],
         },
      });
      const out = dropValueUrlTags(result);
      expect(out).not.toContain("image");
      expect(out).not.toMatch(/"# link/);
   });

   it("returns text that is not JSON untouched", () => {
      expect(dropValueUrlTags("not json")).toBe("not json");
   });
});
