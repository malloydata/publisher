// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { parseFrontmatter } from "./frontmatter";

const skill = (front: string) => `---\n${front}\n---\nbody\n`;
const refs = (anchor: string, n: number) =>
   Array.from({ length: n }, (_, i) => `ref${i}: *${anchor}`).join("\n");

describe("parseFrontmatter: what an alias can expand to", () => {
   it("falls back when a wide anchored list is referenced 99 times", () => {
      const list = `[${Array.from({ length: 1000 }, (_, i) => i).join(", ")}]`;
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

   it("still parses an alias-free list of 900 items", () => {
      const items = Array.from({ length: 900 }, (_, i) => `- i${i}`).join("\n");
      const out = parseFrontmatter(
         skill(`name: long\ndescription: d\nitems:\n${items}`),
         "long",
      );
      expect(out.warnings).toEqual([]);
      expect((out.frontmatter.items as string[]).length).toBe(900);
   });

   it("still parses an alias-free list of 1,200 values that print far longer than they are written", () => {
      const out = parseFrontmatter(
         skill(
            `name: big\ndescription: d\nv: [${Array(1200).fill("1e20").join(",")}]`,
         ),
         "big",
      );
      expect(out.warnings).toEqual([]);
      expect((out.frontmatter.v as number[]).length).toBe(1200);
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

const range = (n: number) => Array.from({ length: n }, (_, i) => i);

/** Shapes that make the YAML parser's time grow faster than the text, each past the 8 KiB cap. */
const OVERSIZE: Array<[string, string]> = [
   [
      "21,400 plain keys",
      range(21_400)
         .map((i) => `a${i}: x`)
         .join("\n"),
   ],
   [
      "10,700 anchor pairs",
      range(10_700)
         .map((i) => `a${i}: &k${i} x\nb${i}: *k${i}`)
         .join("\n"),
   ],
   [
      "10,700 anchor pairs with names led by U+00A0",
      range(10_700)
         .map((i) => `a${i}: &\u00a0k${i} x\nb${i}: *\u00a0k${i}`)
         .join("\n"),
   ],
   [
      "10,700 empty-name pairs",
      range(10_700)
         .map((i) => `a${i}: & x\nb${i}: *`)
         .join("\n"),
   ],
   [
      "15,000 lines with a scalar error",
      range(15_000)
         .map(() => `a: "b`)
         .join("\n"),
   ],
];

describe("parseFrontmatter: how much YAML it reads", () => {
   it.each(OVERSIZE)("falls back, quickly, on %s", (_label, filler) => {
      const text = skill(`name: many\ndescription: d\n${filler}`);
      expect(text.length).toBeGreaterThan(64 * 1024);
      const started = performance.now();
      const out = parseFrontmatter(text, "many");
      // Parsing this much took seconds; the bound is loose so a slow runner never flakes.
      expect(performance.now() - started).toBeLessThan(1_000);
      expect(out.frontmatter).toEqual({});
      expect(out.warnings[0]).toContain("over 8 KiB");
      expect(out.name).toBe("many");
      expect(out.description).toBe("d");
   });

   const sized = (bytes: number, char = "d") => {
      const head = "name: x\ndescription: ";
      const fill = Math.floor(
         (bytes - Buffer.byteLength(head)) / Buffer.byteLength(char),
      );
      return head + char.repeat(fill);
   };

   it("reads a frontmatter of exactly 8 KiB as YAML, and falls back one byte over", () => {
      const at = sized(8192);
      expect(Buffer.byteLength(at)).toBe(8192);
      const parsed = parseFrontmatter(skill(at), "dir");
      expect(parsed.warnings).toEqual([]);
      expect(parsed.frontmatter.name).toBe("x");
      const over = parseFrontmatter(skill(`${at}d`), "dir");
      expect(over.warnings[0]).toContain("over 8 KiB");
      expect(over.frontmatter).toEqual({});
      expect(over.name).toBe("x");
   });

   it("counts bytes, not characters", () => {
      // 4,100 two-byte characters are under 8,192 characters and over 8,192 bytes.
      const front = sized(8200, "\u00e9");
      expect(front.length).toBeLessThan(8192);
      expect(Buffer.byteLength(front)).toBeGreaterThan(8192);
      expect(parseFrontmatter(skill(front), "dir").warnings[0]).toContain(
         "over 8 KiB",
      );
   });
});
