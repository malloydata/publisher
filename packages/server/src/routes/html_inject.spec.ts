// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { injectAfterDoctype } from "./html_inject";

const SNIPPET = "<script id=ctx></script>";
const BOM = Buffer.from([0xef, 0xbb, 0xbf]);

/** The document as text after the splice, read as latin1 so bytes map 1:1. */
function inject(html: string | Buffer): string {
   const buffer = typeof html === "string" ? Buffer.from(html, "latin1") : html;
   const out = injectAfterDoctype(buffer, SNIPPET);
   if (!out) throw new Error("document was left alone");
   return out.toString("latin1");
}

describe("injectAfterDoctype", () => {
   it("goes right after the doctype, ahead of <html> and <head>", () => {
      expect(
         inject(
            '<!doctype html><html><head><meta charset="utf-8"></head></html>',
         ),
      ).toBe(
         `<!doctype html>${SNIPPET}<html><head><meta charset="utf-8"></head></html>`,
      );
   });

   it("matches the doctype case-insensitively, with attributes", () => {
      expect(inject("<!DOCTYPE html>\n<p>hi</p>")).toBe(
         `<!DOCTYPE html>${SNIPPET}\n<p>hi</p>`,
      );
      expect(
         inject('<!DocType HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"><p>'),
      ).toBe(`<!DocType HTML PUBLIC "-//W3C//DTD HTML 4.01//EN">${SNIPPET}<p>`);
   });

   it("finds a doctype after a byte-order mark, whitespace and comments", () => {
      const lead = " \t\r\n\f<!-- a --><!-- <!doctype x> -->\n";
      expect(inject(`${lead}<!doctype html><p>`)).toBe(
         `${lead}<!doctype html>${SNIPPET}<p>`,
      );
      const out = injectAfterDoctype(
         Buffer.concat([BOM, Buffer.from(`<!-- c -->\n<!doctype html><p>`)]),
         SNIPPET,
      )!;
      expect([...out.subarray(0, 3)]).toEqual([...BOM]);
      expect(out.subarray(3).toString()).toBe(
         `<!-- c -->\n<!doctype html>${SNIPPET}<p>`,
      );
   });

   it("goes at the start of a document with no doctype, whatever its size", () => {
      expect(inject("<p>hi</p>")).toBe(`${SNIPPET}<p>hi</p>`);
      expect(inject("<head><meta charset=x><title>t")).toBe(
         `${SNIPPET}<head><meta charset=x><title>t`,
      );
      expect(inject("")).toBe(SNIPPET);
      const big = `<p>${"x".repeat(200 * 1024)}`;
      expect(inject(big)).toBe(`${SNIPPET}${big}`);
   });

   it("puts the snippet after a UTF-8 byte-order mark, never before it", () => {
      const out = injectAfterDoctype(
         Buffer.concat([BOM, Buffer.from("<p>hi</p>")]),
         SNIPPET,
      )!;
      expect([...out.subarray(0, 3)]).toEqual([...BOM]);
      expect(out.subarray(3).toString()).toBe(`${SNIPPET}<p>hi</p>`);
   });

   it("leaves a UTF-16 document alone, whichever way it is ordered", () => {
      for (const bom of [
         [0xff, 0xfe],
         [0xfe, 0xff],
      ]) {
         const html = Buffer.from([...bom, 0x3c, 0x00, 0x70, 0x00]);
         expect(injectAfterDoctype(html, SNIPPET)).toBeUndefined();
      }
   });

   it("never takes a doctype inside a later comment or script for the document's", () => {
      for (const page of [
         "<!-- x --><html><!-- <!doctype html> -->",
         "<html><script>var a='<!doctype html>'</script>",
         "<p>text</p><!doctype html>",
         "<!-- <!doctype html>",
      ]) {
         expect(inject(page)).toBe(`${SNIPPET}${page}`);
      }
   });

   it("does not look for a doctype past the first 64 KiB", () => {
      const page = `<!--${"x".repeat(64 * 1024)}--><!doctype html><p>`;
      expect(inject(page)).toBe(`${SNIPPET}${page}`);
   });

   it("never decodes the document, so bytes outside ASCII pass through exactly", () => {
      const body = Buffer.from([0xe9, 0xff, 0x80, 0x00, 0xc3, 0xa9]);
      for (const lead of ["<!doctype html>", "<head>"]) {
         const html = Buffer.concat([Buffer.from(lead), body]);
         const out = injectAfterDoctype(html, SNIPPET)!;
         expect(out.length).toBe(html.length + SNIPPET.length);
         expect(out.subarray(out.length - body.length).equals(body)).toBe(true);
      }
   });

   describe("on adversarial input at the 2 MiB cap", () => {
      const CAP = 2 * 1024 * 1024;
      const fill = (unit: string) => unit.repeat(Math.ceil(CAP / unit.length));
      const cases: [string, string][] = [
         ["an unclosed comment", fill("<!--")],
         ["whitespace", fill(" \n")],
         ["closed comments", fill("<!---->")],
         ["unclosed doctypes", fill("<!doctype ")],
      ];
      for (const [name, html] of cases) {
         it(`is fast on ${name}`, () => {
            const started = performance.now();
            injectAfterDoctype(Buffer.from(html, "latin1"), SNIPPET);
            expect(performance.now() - started).toBeLessThan(50);
         });
      }
   });
});
