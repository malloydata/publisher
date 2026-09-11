// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Negative tests for the markdown grammar's strict parse.
//
// These exist because the bug they guard against is invisible by construction: a
// misspelled attribute or body key is simply not read, so the assertion it was
// meant to carry never runs and the scenario passes. Every case below MUST throw;
// if one silently parses, a whole class of scenario typos goes back to reporting
// success for checks that do not exist.

import { describe, expect, it } from "bun:test";
import path from "path";
import {
   parseMarkdownForTest,
   parseScenarioFile,
   stepMustAssert,
} from "./scenario_md";

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
      expect(() =>
         parseMarkdownForTest(`${FRONT}\n## Bulid targets\n`, "t"),
      ).toThrow(/Unknown section kind "bulid"/);
   });

   it("rejects a misspelled body key, naming the valid ones", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Build refused\n\nexcldues: secret\n`,
            "t",
         ),
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
      const parsed = parseMarkdownForTest(
         `${FRONT}\n## Publisher (off)\n`,
         "t",
      );
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
         "buildRefusals",
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

describe("scenario grammar: ${…} substitutions", () => {
   // An unsubstituted token can never match, so an `excludes:` carrying one would
   // pass unconditionally — a redaction check that always reports "no leak".
   it("rejects an unknown token", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Build refused\n\nexcludes: password=\${pg.passwrod}\n`,
            "t",
         ),
      ).toThrow(/unknown substitution "\$\{pg\.passwrod\}"/);
   });

   it("rejects a token in a key that is never substituted", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Build refused\n\ncites: host=\${pg.host}\n`,
            "t",
         ),
      ).toThrow(/"cites:" is not substituted/);
   });

   it("accepts a known token in excludes", () => {
      const parsed = parseMarkdownForTest(
         `${FRONT}\n## Build refused\n\nexcludes: password=\${pg.password}\n`,
         "t",
      );
      expect(parsed.steps[0]).toMatchObject({
         kind: "buildRefused",
         excludes: "password=${pg.password}",
      });
   });
});

describe("scenario loading: hooks.ts hygiene", () => {
   // A hook no step references is dead code — usually a renamed or deleted step
   // leaving its assertions behind, never running them.
   it("rejects an exported hook that no ## Hook step references", async () => {
      const dir = path.join(import.meta.dir, "__fixtures__", "orphan-hook");
      await expect(parseScenarioFile(dir)).rejects.toThrow(
         /exports "neverReferenced" that no "## Hook" step references/,
      );
   });
});

describe("scenario grammar: prose tables vs assertion tables", () => {
   const QUERY = (body: string) => `${FRONT}\n## Query q\n${body}`;

   it("compares the table after Expect:, not one in the prose before it", () => {
      // The illustrative table must be ignored entirely — not compared, and not
      // merged into the real one (the old whole-body scan concatenated every table
      // row in a section, so a second table arrived as data rows of the first).
      const parsed = parseMarkdownForTest(
         QUERY(`
Background — the source data looks roughly like this:

| whatever | junk |
| -------- | ---- |
| a        | b    |
| c        | d    |

\`\`\`malloy
run: daily -> { select: total }
\`\`\`

Expect:

| total |
| ----- |
| 150   |
`),
         "t",
      );
      const step = parsed.steps[0] as {
         expect?: { cols: { name: string }[]; rows: string[][] };
      };
      expect(step.expect?.cols.map((c) => c.name)).toEqual(["total"]);
      expect(step.expect?.rows).toEqual([["150"]]);
   });

   it("ignores a prose table AFTER the expectation", () => {
      const parsed = parseMarkdownForTest(
         QUERY(`
\`\`\`malloy
run: daily -> { select: total }
\`\`\`

Expect:

| total |
| ----- |
| 150   |

For contrast, serving live would give:

| total |
| ----- |
| 1150  |
`),
         "t",
      );
      const step = parsed.steps[0] as { expect?: { rows: string[][] } };
      expect(step.expect?.rows).toEqual([["150"]]);
   });

   it("rejects an assertion table with no Expect: label", () => {
      expect(() =>
         parseMarkdownForTest(
            QUERY(`
\`\`\`malloy
run: daily -> { select: total }
\`\`\`

| total |
| ----- |
| 150   |
`),
            "t",
         ),
      ).toThrow(/requires an "Expect:" line/);
   });

   it("still takes an input table unlabelled (## Data is a payload, not an assertion)", () => {
      const parsed = parseMarkdownForTest(
         `${FRONT}\n## Data orders_pg.t\n\n| id:int |\n| ------ |\n| 1      |\n`,
         "t",
      );
      expect(parsed.dataSeeds[0].data.rows).toEqual([["1"]]);
   });

   it("reads per-entry ## Manifest attributes", () => {
      const parsed = parseMarkdownForTest(
         `${FRONT}\n## Manifest\n\n- a -> t_a @ lake (unplanned)\n- b -> t_b @ lake (fallback=live)\n- c -> t_c @ lake\n`,
         "t",
      );
      const step = parsed.steps[0] as {
         entries: { src: string; unplanned: boolean; fallback?: string }[];
      };
      expect(step.entries).toEqual([
         { src: "a", table: "t_a", dest: "lake", unplanned: true },
         {
            src: "b",
            table: "t_b",
            dest: "lake",
            unplanned: false,
            fallback: "live",
         },
         { src: "c", table: "t_c", dest: "lake", unplanned: false },
      ]);
   });

   it("rejects a misspelled ## Manifest attribute", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Manifest\n\n- a -> t_a @ lake (unplaned)\n`,
            "t",
         ),
      ).toThrow(/unknown attribute "unplaned"/);
   });

   it("reads per-source (failed) on an orchestrated build", () => {
      const parsed = parseMarkdownForTest(
         `${FRONT}\n## Build (orchestrated)\n\n- a -> t_a @ lake\n- b -> nosuch.t_b @ lake (failed)\n`,
         "t",
      );
      const step = parsed.steps[0] as {
         sources: { src: string; name: string; dest: string; failed: boolean }[];
      };
      expect(step.sources).toEqual([
         { src: "a", name: "t_a", dest: "lake", failed: false },
         { src: "b", name: "nosuch.t_b", dest: "lake", failed: true },
      ]);
   });

   it("rejects a misspelled orchestrated source attribute", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Build (orchestrated)\n\n- a -> t_a @ lake (faild)\n`,
            "t",
         ),
      ).toThrow(/unknown attribute "faild"/);
   });

   // `refused` asserts the RUN failed, so it records no per-source outcome for
   // `(failed)` to check. Accepting both would silently drop the per-source
   // assertion.
   it("rejects (failed) on a refused build", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Build refused (orchestrated)\n\n- a -> t_a @ lake (failed)\n`,
            "t",
         ),
      ).toThrow(/`\(failed\)` is meaningless on a refused build/);
   });

   // The two headers almost share a prefix, and they do opposite things: one
   // asserts a collection in the plan and runs nothing, the other runs a build and
   // demands it fail. Pinned because the parse order is what keeps them apart.
   it("tells ## Build refusals apart from ## Build refused", () => {
      const refusals = parseMarkdownForTest(
         `${FRONT}\n## Build refusals\n\nExpect:\n\n| source | tier    | reason |\n| ------ | ------- | ------ |\n| s      | storage | given  |\n`,
         "t",
      );
      expect(refusals.steps[0]).toMatchObject({ kind: "buildRefusals" });

      const refused = parseMarkdownForTest(
         `${FRONT}\n## Build refused\n\ncites: nope\n`,
         "t",
      );
      expect(refused.steps[0]).toMatchObject({ kind: "buildRefused" });
   });

   it("accepts an EMPTY ## Build refusals table (nothing was refused)", () => {
      const parsed = parseMarkdownForTest(
         `${FRONT}\n## Build refusals\n\nExpect:\n\n| source |\n| ------ |\n`,
         "t",
      );
      const step = parsed.steps[0] as { kind: string; expect: { rows: unknown[] } };
      expect(step.kind).toBe("buildRefusals");
      expect(step.expect.rows).toEqual([]);
   });

   it("rejects a ## Build refusals table with no source column", () => {
      expect(() =>
         parseMarkdownForTest(
            `${FRONT}\n## Build refusals\n\nExpect:\n\n| tier    | reason |\n| ------- | ------ |\n| storage | given  |\n`,
            "t",
         ),
      ).toThrow(/requires a "source" column/);
   });
});
