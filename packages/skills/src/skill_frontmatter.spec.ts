// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readFrontmatter } from "./skill_frontmatter";

describe("readFrontmatter", () => {
   it("reads a plain name and description", () => {
      expect(
         readFrontmatter("name: my-skill\ndescription: Does a thing.", "dir"),
      ).toEqual({ name: "my-skill", description: "Does a thing." });
   });

   it("reads a block scalar description to its true length", () => {
      const text =
         "name: my-skill\ndescription: >-\n  First line\n  second line";
      expect(readFrontmatter(text, "dir").description).toBe(
         "First line second line",
      );
   });

   it("unescapes a YAML single-quoted description the line read would keep doubled", () => {
      expect(
         readFrontmatter("name: x\ndescription: 'the model''s file'", "dir")
            .description,
      ).toBe("the model's file");
   });

   it("falls back to a line read when the YAML does not parse", () => {
      expect(
         readFrontmatter("name: x\ndescription: Use when: a thing", "dir"),
      ).toEqual({ name: "x", description: "Use when: a thing" });
   });

   it("falls back to a line read when the name breaks the spec's character set", () => {
      expect(
         readFrontmatter("name: My_Skill\ndescription: d", "My_Skill"),
      ).toEqual({ name: "My_Skill", description: "d" });
      expect(readFrontmatter("name: a--b\ndescription: d", "dir").name).toBe(
         "a--b",
      );
   });

   it("takes the directory name when none is declared", () => {
      expect(readFrontmatter("description: d", "from-dir")).toEqual({
         name: "from-dir",
         description: "d",
      });
      expect(readFrontmatter("", "from-dir")).toEqual({
         name: "from-dir",
         description: "",
      });
   });
});
