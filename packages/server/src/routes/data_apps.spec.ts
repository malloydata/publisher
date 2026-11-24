// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { stripNonTagText } from "./data_apps";

describe("stripNonTagText", () => {
   it("drops a comment, and a comment left unterminated runs to the end", () => {
      expect(stripNonTagText('<a><!-- <meta name="x"> --><b>')).toBe("<a><b>");
      expect(stripNonTagText("<a><!-- <b>")).toBe("<a>");
   });

   it("drops the content of each raw-text element, closing tag included", () => {
      for (const [open, close] of [
         ["<script>", "</script>"],
         ["<SCRIPT type=module>", "</Script >"],
         ["<style>", "</style>"],
         ["<title>", "</title>"],
         ["<textarea>", "</textarea>"],
         ["<noscript>", "</noscript>"],
         ["<xmp>", "</xmp>"],
         ["<iframe>", "</iframe>"],
         ["<noembed>", "</noembed>"],
         ["<noframes>", "</noframes>"],
      ]) {
         expect(stripNonTagText(`<a>${open}<meta name=x>${close}<b>`)).toBe(
            "<a><b>",
         );
      }
   });

   it("treats a plaintext element, or an unclosed raw-text element, as text to the end", () => {
      expect(stripNonTagText("<a><plaintext><meta name=x>")).toBe("<a>");
      expect(stripNonTagText("<a><script>var a='<meta>'")).toBe("<a>");
   });

   it("does not close a raw-text element on a longer tag name", () => {
      expect(stripNonTagText("<a><script>a</scripts><meta>b</script><b>")).toBe(
         "<a><b>",
      );
   });

   it("blanks brackets inside a quoted attribute value, so they read as no tag", () => {
      expect(stripNonTagText('<meta data-x="<script>" name=a>')).toBe(
         '<meta data-x=" script " name=a>',
      );
      expect(stripNonTagText(`<p a='<!--'><b>`)).toBe(`<p a=' !--'><b>`);
   });

   it("follows a tag through any number of quoted values", () => {
      const attrs = Array.from({ length: 17 }, (_, n) => ` a${n}="${n}"`).join(
         "",
      );
      expect(stripNonTagText(`<html${attrs}><!-- x --><b>`)).toBe(
         `<html${attrs}><b>`,
      );
   });

   it("reads a stray quote that opens no value as plain text", () => {
      const page = "<html lang=en data-tip=don't><title>It's</title><b>";
      expect(stripNonTagText(page)).toBe("<html lang=en data-tip=don't><b>");
   });

   it("scans on past a tag that never closes, resuming where it stopped", () => {
      const open = `<p a="${"x".repeat(3000)}`;
      expect(stripNonTagText(`${open}<!-- c --><b>`)).toBe(`${open}<b>`);
   });

   describe("on adversarial input", () => {
      const fill = (unit: string) =>
         unit.repeat(Math.ceil((1024 * 1024) / unit.length));
      const cases: [string, string][] = [
         ["an unclosed comment per tag", fill("<!--")],
         ["repeated unclosed scripts", fill("<script ")],
         ["repeated unclosed quoted attributes", fill('<a x="')],
         ["quotes among unclosed tags", fill('<a"<a<a<a<a<a"')],
         ["repeated empty quoted values", fill('<a x=""')],
      ];
      for (const [name, html] of cases) {
         it(`is fast on ${name}`, () => {
            const started = performance.now();
            stripNonTagText(html);
            expect(performance.now() - started).toBeLessThan(50);
         });
      }
   });
});
