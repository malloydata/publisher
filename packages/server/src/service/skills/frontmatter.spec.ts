// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { parseFrontmatter } from "./frontmatter";

const skill = (front: string) => `---\n${front}\n---\nbody\n`;
const refs = (anchor: string, n: number) =>
   Array.from({ length: n }, (_, i) => `ref${i}: *${anchor}`).join("\n");

describe("parseFrontmatter: what an alias can expand to", () => {
   it("falls back when a wide anchored list is referenced 99 times", () => {
      const list = `[${Array.from({ length: 2000 }, (_, i) => i).join(", ")}]`;
      const out = parseFrontmatter(
         skill(
            `name: wide\ndescription: d\nlist: &l ${list}\n${refs("l", 99)}`,
         ),
         "wide",
      );
      expect(out.frontmatter).toEqual({});
      expect(out.warnings[0]).toContain("not valid YAML");
      expect(out.name).toBe("wide");
      expect(out.description).toBe("d");
   });

   it("falls back when an anchored long string is referenced many times", () => {
      const out = parseFrontmatter(
         skill(
            `name: fat\ndescription: d\ntext: &s "${"x".repeat(5000)}"\n${refs("s", 60)}`,
         ),
         "fat",
      );
      expect(out.frontmatter).toEqual({});
      expect(out.warnings[0]).toContain("not valid YAML");
   });

   it("still parses an alias-free list of 6,000 items", () => {
      const items = Array.from({ length: 6000 }, (_, i) => `- i${i}`).join(
         "\n",
      );
      const out = parseFrontmatter(
         skill(`name: long\ndescription: d\nitems:\n${items}`),
         "long",
      );
      expect(out.warnings).toEqual([]);
      expect((out.frontmatter.items as string[]).length).toBe(6000);
   });

   it("still parses an alias-free list of 6,000 values that print far longer than they are written", () => {
      const out = parseFrontmatter(
         skill(
            `name: big\ndescription: d\nv: [${Array(6000).fill("1e20").join(",")}]`,
         ),
         "big",
      );
      expect(out.warnings).toEqual([]);
      expect((out.frontmatter.v as number[]).length).toBe(6000);
   });

   it("still parses a short mapping referenced twice", () => {
      const out = parseFrontmatter(
         skill("name: ok\ndescription: d\nbase: &b {x: 1}\na: *b\nc: *b"),
         "ok",
      );
      expect(out.warnings).toEqual([]);
      expect(out.frontmatter.a).toEqual({ x: 1 });
      expect(out.frontmatter.c).toEqual({ x: 1 });
   });
});
