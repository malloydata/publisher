import { describe, expect, it } from "bun:test";
import { locateFrontmatterClose } from "../scripts/frontmatter";

describe("locateFrontmatterClose", () => {
   it("finds the close in an LF file", () => {
      const text = "---\nname: foo\n---\nbody";
      const location = locateFrontmatterClose(text);
      expect(location).not.toBeNull();
      expect(text.slice(0, location!.index)).toBe("---\nname: foo");
      expect(location!.newline).toBe("\n");
   });

   /**
    * A Windows checkout (or an upstream sync) can leave SKILL.md as CRLF. The
    * original LF-only parser (`text.startsWith("---\n")`) read a CRLF file's
    * opening line as `---\r\n`, never matched, and classified every skill as
    * having no frontmatter block at all — turning a Windows `copy-skills` run
    * into a hard failure, not a degraded one.
    */
   it("finds the close in a CRLF file", () => {
      const text = "---\r\nname: foo\r\n---\r\nbody";
      const location = locateFrontmatterClose(text);
      expect(location).not.toBeNull();
      expect(text.slice(0, location!.index)).toBe("---\r\nname: foo");
      expect(location!.newline).toBe("\r\n");
   });

   it("returns null when there is no leading frontmatter block", () => {
      expect(locateFrontmatterClose("just a body, no frontmatter")).toBeNull();
   });

   it("returns null when the frontmatter is never closed", () => {
      expect(
         locateFrontmatterClose("---\nname: foo\nbody, no close"),
      ).toBeNull();
   });
});
