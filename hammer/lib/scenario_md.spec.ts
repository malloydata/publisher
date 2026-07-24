// Negative tests for the markdown grammar's strict parse.
//
// These exist because the bug they guard against is invisible by construction: a
// misspelled attribute or body key is simply not read, so the assertion it was
// meant to carry never runs and the scenario passes. Every case below MUST throw;
// if one silently parses, a whole class of scenario typos goes back to reporting
// success for checks that do not exist.

import { describe, expect, it } from "bun:test";
import { parseMarkdownForTest, stepMustAssert } from "./scenario_md";

const FRONT = `---
id: t
package: p
---

# Title
`;

/** A minimal well-formed scenario, as the control. */
const WELL_FORMED = `${FRONT}
## Publisher

- PERSIST_STORAGE_MODE: on

## Query rollup

\`\`\`malloy
run: daily -> { select: x }
\`\`\`

Expect:

| x |
| - |
| 1 |
`;

describe("scenario grammar: strict parse", () => {
   it("accepts a well-formed scenario", () => {
      const parsed = parseMarkdownForTest(WELL_FORMED, "t");
      expect(parsed.steps.map((s) => s.kind)).toEqual(["publisher", "query"]);
   });

   it("rejects an unknown section kind", () => {
      expect(() => parseMarkdownForTest(`${FRONT}\n## Bulid targets\n`, "t")).toThrow(
         /Unknown section kind "bulid"/,
      );
   });

   it("rejects a misspelled body key, naming the valid ones", () => {
      expect(() =>
         parseMarkdownForTest(`${FRONT}\n## Build refused\n\nexcldues: secret\n`, "t"),
      ).toThrow(/unknown body key "excldues:".*cites, excludes, reference/s);
   });

   it("rejects a misspelled header attribute, naming the valid ones", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Connection lake (row=1)\n\n\`\`\`sql\nSELECT 1\n\`\`\`\n`,
            "t",
         ),
      ).toThrow(/unknown attribute "row".*refused, rows, type/s);
   });

   // The case that actually reached the suite: a REAL key, on a kind whose branch
   // never consumed it. Syntactically plausible, silently ignored before this.
   it("rejects a valid key used on a section kind that does not read it", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Query q\n\n\`\`\`malloy\nrun: x\n\`\`\`\n\nexcludes: nope\n\nExpect:\n\n| x |\n| - |\n| 1 |\n`,
            "t",
         ),
      ).toThrow(/unknown body key "excludes:".*cites, columns, givens/s);
   });

   it("does not mistake Malloy inside a fenced block for a body key", () => {
      // `group_by:` would otherwise read as an unknown body key and reject every
      // scenario in the suite.
      const parsed = parseMarkdownForTest(
         `${FRONT}\n## Query q\n\n\`\`\`malloy\nrun: daily -> { group_by: order_date }\n\`\`\`\n\nExpect:\n\n| order_date |\n| ---------- |\n| 2026-01-01 |\n`,
         "t",
      );
      expect(parsed.steps).toHaveLength(1);
   });

   it("does not mistake a publisher env bullet for a body key", () => {
      const parsed = parseMarkdownForTest(
         `${FRONT}\n## Publisher\n\n- PERSIST_COLLISION_ENFORCE: true\n`,
         "t",
      );
      expect(parsed.steps).toHaveLength(1);
   });

   it("resolves a bare mode flag on ## Publisher", () => {
      // `## Publisher (off)` was accepted-but-unread before the strict parse
      // surfaced it; a scenario relying on the header form alone would have booted
      // in the wrong mode and asserted against it.
      const parsed = parseMarkdownForTest(`${FRONT}\n## Publisher (off)\n`, "t");
      expect(parsed.steps[0]).toMatchObject({ kind: "publisher", mode: "off" });
   });
});

describe("scenario grammar: every step must verify something", () => {
   // A step that contributes no check looks like coverage in the report and is not.
   // The kinds below assert; the exemptions are the ones that exist purely for a
   // side effect. Pinned so an exemption is a deliberate edit, not a drive-by.
   it("requires a check from every assertion-bearing kind", () => {
      for (const kind of [
         "query",
         "connection",
         "sql",
         "buildTargets",
         "buildRefused",
         "orchestratedBuild",
         "compile",
         "warns",
         "rejected",
         "republish",
         "await",
         "delete",
         "reclaim",
      ]) {
         expect(stepMustAssert(kind)).toBe(true);
      }
   });

   it("exempts the side-effect-only kinds", () => {
      for (const kind of [
         "model",
         "mutate",
         "operator",
         "publisher",
         "restart",
         "bind",
         // Pure setup is a legitimate hook; requiring a check would only teach
         // authors to write a tautological assert to satisfy the rule.
         "hook",
         // Optional `expect binding:` lines; most scenarios publish to build.
         "publish",
      ]) {
         expect(stepMustAssert(kind)).toBe(false);
      }
   });
});
