// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { injectAfterHeadStart } from "./html_inject";

const SNIPPET = "<script id=ctx></script>";

/** The document as text after the splice, read as latin1 so bytes map 1:1. */
function inject(html: string | Buffer): string {
   const buffer = typeof html === "string" ? Buffer.from(html, "latin1") : html;
   const out = injectAfterHeadStart(buffer, SNIPPET);
   if (!out) throw new Error("document was left alone");
   return out.toString("latin1");
}

describe("injectAfterHeadStart", () => {
   it("goes right after a charset meta, so the meta stays first in the prescan window", () => {
      expect(
         inject(
            '<!doctype html><html><head><meta charset="utf-8"><title>t</title></head></html>',
         ),
      ).toBe(
         `<!doctype html><html><head><meta charset="utf-8">${SNIPPET}<title>t</title></head></html>`,
      );
   });

   it("recognizes the http-equiv spelling of the charset declaration", () => {
      const meta =
         '<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">';
      expect(inject(`<head>${meta}<title>t</title>`)).toBe(
         `<head>${meta}${SNIPPET}<title>t</title>`,
      );
   });

   it("matches tags case-insensitively", () => {
      expect(inject("<HEAD><META CHARSET=utf-8><TITLE>t</TITLE>")).toBe(
         `<HEAD><META CHARSET=utf-8>${SNIPPET}<TITLE>t</TITLE>`,
      );
      expect(inject("<HTML><HEAD ><TITLE>t</TITLE>")).toBe(
         `<HTML><HEAD >${SNIPPET}<TITLE>t</TITLE>`,
      );
   });

   it("falls back to after <head> when the page declares no charset", () => {
      expect(inject("<!doctype html><html><head><title>t</title></head>")).toBe(
         `<!doctype html><html><head>${SNIPPET}<title>t</title></head>`,
      );
      expect(inject('<head lang="en"><title>t</title>')).toBe(
         `<head lang="en">${SNIPPET}<title>t</title>`,
      );
   });

   it("does not take <header> or <headline> for <head>", () => {
      expect(inject("<!doctype html><header>x</header>")).toBe(
         `<!doctype html>${SNIPPET}<header>x</header>`,
      );
   });

   it("falls back to after the doctype when there is no <head>", () => {
      expect(inject("<!DOCTYPE html>\n<p>hi</p>")).toBe(
         `<!DOCTYPE html>${SNIPPET}\n<p>hi</p>`,
      );
   });

   it("falls back to the start of a fragment", () => {
      expect(inject("<p>hi</p>")).toBe(`${SNIPPET}<p>hi</p>`);
      expect(inject("")).toBe(SNIPPET);
   });

   it("puts the snippet after a UTF-8 byte-order mark, never before it", () => {
      const bom = Buffer.from([0xef, 0xbb, 0xbf]);
      const out = injectAfterHeadStart(
         Buffer.concat([bom, Buffer.from("<p>hi</p>")]),
         SNIPPET,
      )!;
      expect([...out.subarray(0, 3)]).toEqual([0xef, 0xbb, 0xbf]);
      expect(out.subarray(3).toString()).toBe(`${SNIPPET}<p>hi</p>`);
   });

   it("leaves a UTF-16 document alone, whichever way it is ordered", () => {
      for (const bom of [
         [0xff, 0xfe],
         [0xfe, 0xff],
      ]) {
         const html = Buffer.from([...bom, 0x3c, 0x00, 0x70, 0x00]);
         expect(injectAfterHeadStart(html, SNIPPET)).toBeUndefined();
      }
   });

   it("ignores a tag inside an HTML comment", () => {
      expect(
         inject(
            '<!doctype html><!-- <head><meta charset="x"> --><head><title>t</title>',
         ),
      ).toBe(
         `<!doctype html><!-- <head><meta charset="x"> --><head>${SNIPPET}<title>t</title>`,
      );
   });

   it("treats an unterminated comment as running to the end", () => {
      expect(inject("<!doctype html><!-- <head>")).toBe(
         `<!doctype html>${SNIPPET}<!-- <head>`,
      );
   });

   it("ignores a charset meta that is not within the first 1024 bytes", () => {
      const padding = `<!-- ${"x".repeat(1100)} -->`;
      expect(
         inject(`<head>${padding}<meta charset="utf-8"><title>t</title>`),
      ).toBe(
         `<head>${SNIPPET}${padding}<meta charset="utf-8"><title>t</title>`,
      );
   });

   it("does not take a meta that only mentions charset in prose for a declaration", () => {
      expect(inject('<head><meta name="description" content="a b"><x>')).toBe(
         `<head>${SNIPPET}<meta name="description" content="a b"><x>`,
      );
   });

   it("never decodes the document, so bytes outside ASCII pass through exactly", () => {
      const body = Buffer.from([0xe9, 0xff, 0x80, 0x00, 0xc3, 0xa9]);
      const html = Buffer.concat([Buffer.from("<head><x>"), body]);
      const out = injectAfterHeadStart(html, SNIPPET)!;
      expect(out.length).toBe(html.length + SNIPPET.length);
      expect(out.subarray(out.length - body.length).equals(body)).toBe(true);
   });

   describe("on adversarial input at the 2 MiB cap", () => {
      const CAP = 2 * 1024 * 1024;
      const fill = (unit: string) => unit.repeat(Math.ceil(CAP / unit.length));
      const cases: [string, string][] = [
         ["repeated commented-out heads", fill("<!--<head>-->")],
         ["an unclosed comment per tag", fill("<!--")],
         ["an unclosed script", `<script ${"x".repeat(CAP)}`],
         ["repeated unclosed scripts", fill("<script ")],
         ["unclosed closing tags in a script", `<script>${fill("</script ")}`],
         ["repeated unclosed metas", fill("<meta ")],
         ["repeated unclosed heads", fill("<head ")],
         ["repeated unclosed titles", fill("<title>")],
      ];
      for (const [name, html] of cases) {
         it(`is fast on ${name}`, () => {
            const started = performance.now();
            injectAfterHeadStart(Buffer.from(html, "latin1"), SNIPPET);
            expect(performance.now() - started).toBeLessThan(200);
         });
      }
   });

   it("does not take a tag inside a script, style, title, textarea or comment for a live one", () => {
      for (const [open, close] of [
         ["<script>", "</script>"],
         ["<SCRIPT type=module>", "</Script >"],
         ["<style>", "</style>"],
         ["<title>", "</title>"],
         ["<textarea>", "</textarea>"],
         ["<!--", "-->"],
      ]) {
         const decoy = `${open}<head><meta charset="x">${close}`;
         expect(inject(`<!doctype html>${decoy}<head><p>`)).toBe(
            `<!doctype html>${decoy}<head>${SNIPPET}<p>`,
         );
      }
   });

   it("does not insert after an unclosed raw-text element or inside its text", () => {
      expect(inject("<!doctype html><script>var a='<head>'")).toBe(
         `<!doctype html>${SNIPPET}<script>var a='<head>'`,
      );
      expect(inject("<title>t")).toBe(`${SNIPPET}<title>t`);
   });

   it("does not close a raw-text element on a longer tag name", () => {
      expect(inject("<script>a</scripts><head>b</script><head>")).toBe(
         `<script>a</scripts><head>b</script><head>${SNIPPET}`,
      );
   });

   it("leaves a document alone when nothing is found in its first 64 KiB", () => {
      const pad = "x".repeat(64 * 1024);
      expect(
         injectAfterHeadStart(Buffer.from(`${pad}<head>`), SNIPPET),
      ).toBeUndefined();
      expect(
         injectAfterHeadStart(Buffer.from(`<!--${pad}--><head>`), SNIPPET),
      ).toBeUndefined();
   });

   it("scans only the first 64 KiB, and splices at a head found there", () => {
      const tail = "<head>".repeat(1000) + "x".repeat(200 * 1024);
      expect(inject(`<head>${tail}`)).toBe(`<head>${SNIPPET}${tail}`);
   });
});
