// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { parseAnnotation } from "@malloydata/malloy-tag";
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
   it("removes an image tag and keeps the other properties on the line, as the renderer reads them", () => {
      const out = annotationsOf(
         dropValueUrlTags(resultWith('# image { height=40px } label="Pic"\n')),
      );
      expect(out).toHaveLength(1);
      const tag = parseAnnotation(out[0]).tag;
      expect(tag.has("image")).toBe(false);
      expect(tag.text("label")).toBe("Pic");
   });

   it("leaves a well-formed empty line when an image tag was the only property", () => {
      const out = annotationsOf(dropValueUrlTags(resultWith("# image\n")));
      expect(out).toHaveLength(1);
      expect(out[0]).not.toContain("# #");
      expect(parseAnnotation(out[0]).log).toEqual([]);
   });

   it("drops a line that reads the environment, whatever else it carries", () => {
      for (const line of [
         "# image=@env.HOME\n",
         "# image { alt=@env.HOME }\n",
         "# link { url_template=@env.X }\n",
         '# label="<img src=x>" x=@env.X\n',
         "#|\nimage=@env.X\n|#\n",
      ]) {
         expect(annotationsOf(dropValueUrlTags(resultWith(line)))).toEqual([
            "",
         ]);
      }
   });

   it("drops a line the tag parser rejects, since the renderer may read it differently", () => {
      expect(
         annotationsOf(dropValueUrlTags(resultWith("# image {\n"))),
      ).toEqual([""]);
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

   it("removes each markup shape in a label and keeps a bare angle bracket", () => {
      for (const label of [
         "<b>x</b>",
         "</b>",
         "<!-- x -->",
         "<b onclick=x a> y",
      ]) {
         const out = annotationsOf(
            dropValueUrlTags(resultWith(`# label="${label}"\n`)),
         );
         expect(out.join("")).not.toContain("label");
      }
      for (const label of ["a<b", "Actual<Target", "a < b"]) {
         const source = resultWith(`# label="${label}"\n`);
         expect(dropValueUrlTags(source)).toBe(source);
      }
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
