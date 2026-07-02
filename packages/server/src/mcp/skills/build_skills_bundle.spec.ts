import { describe, expect, it } from "bun:test";
import { parseSkill, unquote } from "./build_skills_bundle";

describe("build_skills_bundle parseSkill", () => {
   it("parses name, description, and body from frontmatter", () => {
      const md =
         "---\nname: malloy-queries\ndescription: Query patterns.\n---\n# Body\n\nText.";
      const s = parseSkill(md, "dir");
      expect(s.name).toBe("malloy-queries");
      expect(s.description).toBe("Query patterns.");
      expect(s.body).toBe("# Body\n\nText.");
   });

   it("keeps colons in the description (line-based, not split on colon)", () => {
      const md =
         "---\nname: x\ndescription: Use when: a thing happens.\n---\nbody";
      expect(parseSkill(md, "dir").description).toBe(
         "Use when: a thing happens.",
      );
   });

   it("tolerates CRLF line endings", () => {
      const md = "---\r\nname: x\r\ndescription: d\r\n---\r\nbody line";
      const s = parseSkill(md, "dir");
      expect(s.name).toBe("x");
      expect(s.description).toBe("d");
      expect(s.body).toBe("body line");
   });

   it("falls back to the dir name and empty description when frontmatter is missing", () => {
      const s = parseSkill("# Just a body, no frontmatter", "fallback-name");
      expect(s.name).toBe("fallback-name");
      expect(s.description).toBe("");
      expect(s.body).toBe("# Just a body, no frontmatter");
   });

   it("strips surrounding quotes from values", () => {
      const md = "---\nname: \"quoted-name\"\ndescription: 'q'\n---\nb";
      const s = parseSkill(md, "dir");
      expect(s.name).toBe("quoted-name");
      expect(s.description).toBe("q");
   });
});

describe("build_skills_bundle unquote", () => {
   it("removes a single layer of surrounding quotes", () => {
      expect(unquote('"hi"')).toBe("hi");
      expect(unquote("'hi'")).toBe("hi");
      expect(unquote("plain")).toBe("plain");
   });
});
