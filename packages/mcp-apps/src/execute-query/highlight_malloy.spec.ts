import { describe, expect, it } from "bun:test";
import { highlightMalloy } from "./highlight_malloy";

// highlightMalloy output is assigned via `code.innerHTML` (renderer.ts), so the
// escaping is the load-bearing safety property: any `<`, `>`, `&` in the query
// — whether in plain text, inside a recognized token, or across the whole
// string — must come out escaped so the query can never inject markup. These
// tests are the regression guard the bundle cannot get from a real grammar here.

describe("highlightMalloy", () => {
   describe("HTML-escaping (XSS safety)", () => {
      it("escapes an adversarial tag so no live markup survives", () => {
         const html = highlightMalloy(`<img src=x onerror="alert(1)">`);
         // The angle brackets are neutralized...
         expect(html).toContain("&lt;img");
         expect(html).toContain("&gt;");
         // ...so no real element/attribute is ever emitted.
         expect(html).not.toContain("<img");
         expect(html).not.toContain('onerror="alert');
      });

      it("escapes ampersands", () => {
         expect(highlightMalloy("a & b")).toContain("a &amp; b");
      });

      it("escapes angle brackets inside a recognized string token", () => {
         // The whole `'<b>'` is tokenized as a string, but its contents are still
         // escaped before being wrapped in the span — the class attr is a literal,
         // so there's no attribute-injection surface either.
         const html = highlightMalloy(`where: x = '<b>hi</b>'`);
         expect(html).toContain('<span class="mh-str">');
         expect(html).toContain("&lt;b&gt;hi&lt;/b&gt;");
         expect(html).not.toContain("<b>hi</b>");
      });

      it("keeps an escaped-quote string literal as one token and escapes it", () => {
         // `r'…\'…'` — the backslash-escaped quote must not end the string early,
         // and any markup inside is still escaped.
         const html = highlightMalloy(`where: name ~ r'it\\'s <x>'`);
         const spanCount = (html.match(/<span class="mh-str">/g) || []).length;
         expect(spanCount).toBe(1);
         expect(html).toContain("&lt;x&gt;");
      });

      it("round-trips every character through escaping (no unescaped < or & leaks)", () => {
         const html = highlightMalloy(`a<b && c>d // <not a tag>`);
         // The only `<` / unescaped-looking chars in the output belong to our own
         // <span> wrappers; strip those and nothing from the input remains raw.
         const stripped = html.replace(/<\/?span[^>]*>/g, "");
         expect(stripped).not.toMatch(/<(?!span)/);
         expect(stripped).not.toMatch(/&(?!(amp|lt|gt);)/);
      });
   });

   describe("token classes", () => {
      it("colors keywords, functions, numbers, strings, and comments", () => {
         const html = highlightMalloy(
            `source: flights is duckdb.table('f.parquet') extend {\n` +
               `  // a comment\n` +
               `  measure: c is count()\n` +
               `  dimension: big is distance > 1000\n` +
               `}`,
         );
         expect(html).toContain('<span class="mh-kw">source</span>');
         expect(html).toContain('<span class="mh-cmt">// a comment</span>');
         // escapeHtml only touches & < >, so the single quotes stay literal.
         expect(html).toContain(`<span class="mh-str">'f.parquet'</span>`);
         expect(html).toContain('<span class="mh-fn">count</span>');
         expect(html).toContain('<span class="mh-num">1000</span>');
      });

      it("treats an identifier followed by `(` as a function", () => {
         expect(highlightMalloy("sum(x)")).toContain(
            '<span class="mh-fn">sum</span>',
         );
      });

      it("colors constants (true/false/null) as numeric", () => {
         const html = highlightMalloy("where: a is true and b is null");
         expect(html).toContain('<span class="mh-num">true</span>');
         expect(html).toContain('<span class="mh-num">null</span>');
      });

      it("leaves a bare identifier unwrapped", () => {
         // A plain identifier that isn't a keyword/constant/function stays plain.
         expect(highlightMalloy("carrier")).toBe("carrier");
      });
   });

   it("returns empty string for empty input", () => {
      expect(highlightMalloy("")).toBe("");
   });
});
