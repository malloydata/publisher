// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The report rule (`report.ts`, `docs/reports-design.md` §1, §2) as it stands,
 * pinned function by function so a refactor of the rule's readers has to keep
 * every answer. Where an answer looks odd it is still pinned, with a note: this
 * file locks behavior, it does not judge it.
 */
import { describe, expect, it } from "bun:test";
import {
   artifactPropertyBody,
   isReportEligiblePath,
   isUnderPublicDir,
   layoutFactsOfText,
   queryArtifactRewrite,
   reportName,
   resolveReportLayout,
   textClaimsToBeAReport,
   type ReportLayoutFacts,
} from "./report";

const facts = (over: Partial<ReportLayoutFacts>): ReportLayoutFacts => ({
   modelTag: true,
   queryTag: false,
   tiles: false,
   gridTag: false,
   ...over,
});

const layoutOf = (over: Partial<ReportLayoutFacts>) => {
   const { layout, decidedBy } = resolveReportLayout(facts(over));
   return [layout, decidedBy];
};

describe("resolveReportLayout: §2, first match wins", () => {
   it("1. `layout=` decides first", () => {
      expect(layoutOf({ layout: "grid" })).toEqual(["grid", "layout"]);
      expect(layoutOf({ layout: "linear" })).toEqual(["linear", "layout"]);
      // Over everything below it.
      expect(
         layoutOf({
            layout: "linear",
            queryTag: true,
            tiles: true,
            gridTag: true,
         }),
      ).toEqual(["linear", "layout"]);
   });

   it("1. the legacy `kind=` decides when `layout=` names nothing", () => {
      expect(layoutOf({ kind: "dashboard" })).toEqual(["grid", "kind"]);
      expect(layoutOf({ kind: "notebook" })).toEqual(["linear", "kind"]);
      expect(
         layoutOf({ layout: "wide", kind: "notebook", tiles: true }),
      ).toEqual(["linear", "kind"]);
      expect(layoutOf({ kind: "notebook", queryTag: true })).toEqual([
         "linear",
         "kind",
      ]);
   });

   it("1. a conflict between `layout=` and `kind=` is read by `layout=`", () => {
      expect(layoutOf({ layout: "linear", kind: "dashboard" })).toEqual([
         "linear",
         "layout",
      ]);
      expect(layoutOf({ layout: "grid", kind: "notebook" })).toEqual([
         "grid",
         "layout",
      ]);
      // Agreement is not a conflict either way.
      expect(layoutOf({ layout: "grid", kind: "dashboard" })).toEqual([
         "grid",
         "layout",
      ]);
   });

   it("1. values are exact: case and unknown words name no layout", () => {
      expect(layoutOf({ layout: "Grid" })).toEqual(["linear", "default"]);
      expect(layoutOf({ kind: "Dashboard" })).toEqual(["linear", "default"]);
      expect(layoutOf({ kind: "text" })).toEqual(["linear", "default"]);
   });

   it("1. `layout=` and `kind=` count only on a model-level tag", () => {
      expect(
         layoutOf({ modelTag: false, layout: "linear", queryTag: true }),
      ).toEqual(["grid", "query"]);
      expect(layoutOf({ modelTag: false, kind: "dashboard" })).toEqual([
         "linear",
         "default",
      ]);
   });

   it("2. a query-level `# artifact` is grid", () => {
      expect(layoutOf({ modelTag: false, queryTag: true })).toEqual([
         "grid",
         "query",
      ]);
      // Ahead of the tiles rule.
      expect(layoutOf({ queryTag: true, tiles: true })).toEqual([
         "grid",
         "query",
      ]);
   });

   it("3. a model-level tag with tiles, or beside a grid tag, is grid", () => {
      expect(layoutOf({ tiles: true })).toEqual(["grid", "tiles"]);
      expect(layoutOf({ gridTag: true })).toEqual(["grid", "tiles"]);
      // Only on a model-level tag.
      expect(layoutOf({ modelTag: false, tiles: true, gridTag: true })).toEqual(
         ["linear", "default"],
      );
   });

   it("4. anything else is linear", () => {
      expect(layoutOf({})).toEqual(["linear", "default"]);
      expect(layoutOf({ modelTag: false })).toEqual(["linear", "default"]);
   });
});

describe("layoutFactsOfText", () => {
   it("reads a parsed model-level tag", () => {
      expect(
         layoutFactsOfText(
            '## artifact { layout=grid kind=dashboard tiles=["a -> b"] }\n',
         ),
      ).toEqual({
         layout: "grid",
         kind: "dashboard",
         modelTag: true,
         queryTag: false,
         tiles: true,
         gridTag: false,
      });
   });

   it("reads a `dashboard { … }` grid tag on the artifact line only", () => {
      expect(
         layoutFactsOfText("## artifact { } dashboard { columns=6 }\n").gridTag,
      ).toBe(true);
      // A separate `## dashboard` line is not read off text. The compiled
      // reader sees it (see reportLayoutParity.json's knownDifference).
      expect(
         layoutFactsOfText(
            '## artifact { title="x" }\n## dashboard { columns=6 }\n',
         ).gridTag,
      ).toBe(false);
   });

   it("reads a `##|` block tag", () => {
      expect(layoutFactsOfText("##| artifact { layout=grid }\n|##\n")).toEqual({
         layout: "grid",
         modelTag: true,
         queryTag: false,
         tiles: false,
         gridTag: false,
      });
   });

   it("reads a query-level `# artifact` as the query tag, not a model tag", () => {
      expect(layoutFactsOfText("# artifact\nquery: q is s -> v\n")).toEqual({
         modelTag: false,
         queryTag: true,
         tiles: false,
         gridTag: false,
      });
      // `#artifact` with no space is not a tag line.
      expect(
         layoutFactsOfText("#artifact\nquery: q is s -> v\n").queryTag,
      ).toBe(false);
   });

   it("still reads an unparsable model-level tag by its spelling", () => {
      // An unclosed brace does not parse; the tag still claims a report.
      expect(layoutFactsOfText("## artifact {\nrun: x\n")).toEqual({
         modelTag: true,
         queryTag: false,
         tiles: false,
         gridTag: false,
      });
      expect(
         layoutFactsOfText("## artifact { kind=notebook tiles=[ \nrun: x\n"),
      ).toEqual({
         kind: "notebook",
         modelTag: true,
         queryTag: false,
         tiles: true,
         gridTag: false,
      });
      expect(
         layoutFactsOfText("## artifact { layout=grid tiles=[a b,, }"),
      ).toMatchObject({ layout: "grid", modelTag: true, tiles: true });
   });

   it("reads nothing off a file with no tag, a commented tag, or a quoted one", () => {
      const none = {
         modelTag: false,
         queryTag: false,
         tiles: false,
         gridTag: false,
      };
      expect(layoutFactsOfText("source: s is x\n")).toEqual(none);
      expect(layoutFactsOfText("source: s is x\n// ## artifact\n")).toEqual(
         none,
      );
      expect(layoutFactsOfText('"## artifact"')).toEqual(none);
   });
});

describe("artifactPropertyBody", () => {
   it("is empty for a bare tag, an empty body, or no artifact property", () => {
      expect(artifactPropertyBody("# artifact")).toBe("");
      expect(artifactPropertyBody("# artifact { }")).toBe("");
      expect(artifactPropertyBody("# artifacts { a=1 }")).toBe("");
      expect(artifactPropertyBody("# title=x")).toBe("");
   });

   it("keeps nested braces whole", () => {
      expect(artifactPropertyBody("# x artifact { a=1 { b=2 } }")).toBe(
         "a=1 { b=2 }",
      );
   });

   it("skips braces and escaped quotes inside quoted strings", () => {
      expect(
         artifactPropertyBody(
            "## artifact { kind=dashboard givens { a { x=\"}\" } } title='it\\'s' }",
         ),
      ).toBe("kind=dashboard givens { a { x=\"}\" } } title='it\\'s'");
   });

   it("reads the artifact property wherever it sits among others", () => {
      expect(
         artifactPropertyBody("# dashboard { columns=4 } artifact { title=x }"),
      ).toBe("title=x");
   });

   it("takes the rest of the line when the body never closes", () => {
      expect(artifactPropertyBody('#   artifact   {  title="t"  ')).toBe(
         'title="t"',
      );
   });
});

describe("queryArtifactRewrite", () => {
   it("carries the tag's properties and strips `kind=` and `layout=`", () => {
      expect(
         queryArtifactRewrite("by_month", [
            '# artifact { title="T" kind=dashboard layout=linear }',
         ]),
      ).toBe('## artifact { layout=grid title="T" tiles=["by_month"] }');
      expect(
         queryArtifactRewrite("q", [
            "# title=x",
            '# artifact { kind = notebook  title="Y" }',
         ]),
      ).toBe('## artifact { layout=grid title="Y" tiles=["q"] }');
   });

   it("backtick-quotes a query name that is not an identifier", () => {
      expect(queryArtifactRewrite("by month", ["# artifact"])).toBe(
         '## artifact { layout=grid tiles=["`by month`"] }',
      );
      expect(queryArtifactRewrite("1q", ["# artifact"])).toBe(
         '## artifact { layout=grid tiles=["`1q`"] }',
      );
   });

   it("carries a `dashboard { columns }` grid width over", () => {
      expect(
         queryArtifactRewrite("q", [
            "# artifact { }",
            "# dashboard { columns=6 }",
         ]),
      ).toBe('## artifact { layout=grid tiles=["q"] } dashboard { columns=6 }');
   });

   it("writes a bare tile when the query has no notes", () => {
      expect(queryArtifactRewrite("q", [])).toBe(
         '## artifact { layout=grid tiles=["q"] }',
      );
   });
});

describe("isUnderPublicDir and isReportEligiblePath", () => {
   const cases: [string, boolean, boolean][] = [
      // path, under public/, can be a report
      ["x.malloy", false, true],
      ["x.malloynb", false, true],
      ["a/b/c/x.malloy", false, true],
      ["x.txt", false, false],
      // The suffix is exact.
      ["x.MALLOY", false, false],
      ["public/x.malloy", true, false],
      ["Public/x.malloy", true, false],
      ["PUBLIC/a/b.malloy", true, false],
      ["./public/x.malloy", true, false],
      ["public/x.malloynb", true, false],
      ["public", true, false],
      ["publicity/x.malloy", false, true],
      ["a/public/x.malloy", false, true],
      ["index.malloy", false, false],
      ["a/index.malloy", false, true],
      // The root `index.malloy` is matched exactly, with case and without a
      // leading `./`. Pinned as it stands, not as it should be.
      ["Index.malloy", false, true],
      ["./index.malloy", false, true],
   ];
   for (const [path, underPublic, eligible] of cases) {
      it(`${path}: public ${underPublic}, eligible ${eligible}`, () => {
         expect(isUnderPublicDir(path)).toBe(underPublic);
         expect(isReportEligiblePath(path)).toBe(eligible);
      });
   }
});

describe("reportName", () => {
   it("is the basename without `.malloy` or `.malloynb`", () => {
      expect(reportName("a/b/q3.malloy")).toBe("q3");
      expect(reportName("x.malloynb")).toBe("x");
      expect(reportName("index.malloy")).toBe("index");
   });

   it("keeps any other suffix, and is empty for a trailing slash", () => {
      expect(reportName("x.txt")).toBe("x.txt");
      expect(reportName("a/x.MALLOY")).toBe("x.MALLOY");
      expect(reportName("dir/")).toBe("");
   });
});

describe("textClaimsToBeAReport", () => {
   const cases: [string, string, boolean][] = [
      ["a model-level tag", "## artifact { layout=grid }\n", true],
      ["a `##|` block tag", "##| artifact { }\n|##\n", true],
      ["an unparsable model-level tag", "## artifact {\n", true],
      ["a query-level tag", "# artifact\nquery: q is s -> v\n", true],
      [
         "a tag after other properties",
         "## title=x artifact { layout=linear }\n",
         true,
      ],
      ["no tag", "source: s is x\n", false],
      ["a commented tag", "// ## artifact\n", false],
      ["a quoted tag", '"## artifact"', false],
      ["`#artifact` with no space", "#artifact\nquery: q is s -> v\n", false],
      [
         "a tag inside a markdown block",
         "##|(markdown) intro\n## artifact { }\n|##\n",
         false,
      ],
   ];
   for (const [name, source, claims] of cases) {
      it(`${name}: ${claims}`, () => {
         expect(textClaimsToBeAReport(source)).toBe(claims);
      });
   }
});
