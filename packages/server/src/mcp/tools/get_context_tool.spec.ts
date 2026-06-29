import { describe, expect, it } from "bun:test";
import { docText, sanitize } from "./get_context_tool";

describe("get_context docText", () => {
   it("extracts #(doc) text from annotation lines", () => {
      expect(docText(["#(doc) Total revenue in USD."])).toBe(
         "Total revenue in USD.",
      );
   });

   it("accepts Annotation objects ({ value }) as well as raw strings", () => {
      expect(docText([{ value: "#(doc) Customer state." }])).toBe(
         "Customer state.",
      );
   });

   it("joins multiple #(doc) lines and collapses whitespace", () => {
      expect(docText(["#(doc) line one", "#(doc)   line   two"])).toBe(
         "line one line two",
      );
   });

   it("falls back to the raw lines when no #(doc) annotation is present", () => {
      expect(docText(["# bar_chart"])).toBe("# bar_chart");
   });

   it("returns an empty string for missing or empty annotations", () => {
      expect(docText()).toBe("");
      expect(docText([])).toBe("");
   });
});

describe("get_context sanitize", () => {
   it("strips lunr operator characters so a plain-English query never throws", () => {
      const cleaned = sanitize('revenue: +top ~10 "exact" -minus ^boost *wild');
      expect(cleaned).not.toMatch(/[~^:*+\-"]/);
   });

   it("keeps an ordinary query intact", () => {
      expect(sanitize("revenue by product category")).toBe(
         "revenue by product category",
      );
   });
});
