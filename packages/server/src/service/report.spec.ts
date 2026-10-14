// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The report rule (`report.ts`, `docs/reports-design.md` §1, §2), pinned
 * function by function. The compiled and text readers are one reader over a
 * file's model-level notes; `report_layout_parity.spec.ts` holds the notes read
 * off text to the ones a compile collects.
 */
import { describe, expect, it } from "bun:test";
import {
   carriesArtifactTag,
   isReportEligiblePath,
   isUnderPublicDir,
   readReportTag,
   readReportTagOfText,
   reportName,
} from "./report";

describe("readReportTag: what model-level notes make a file", () => {
   it("is no report with no artifact note", () => {
      expect(readReportTag([])).toEqual({ status: "none" });
      expect(readReportTag(['## title="x"'])).toEqual({ status: "none" });
   });

   it("reads `layout=` first", () => {
      expect(readReportTag(["## artifact { layout=grid }"])).toEqual({
         status: "report",
         layout: "grid",
      });
      // Over a tiles list and a grid tag.
      expect(
         readReportTag([
            '## artifact { layout=linear tiles=["a -> v"] } dashboard { columns=6 }',
         ]),
      ).toEqual({ status: "report", layout: "linear" });
   });

   it("is grid by a tiles list, empty or not, or a model-level grid tag", () => {
      for (const notes of [
         ['## artifact { tiles=["a -> v"] }'],
         ["## artifact { tiles=[] }"],
         ["## artifact { } dashboard { columns=6 }"],
         ['## artifact { title="x" }', "## dashboard { columns=6 }"],
      ]) {
         expect(readReportTag(notes)).toEqual({
            status: "report",
            layout: "grid",
         });
      }
   });

   it("makes no report of a linear tag with no `tiles`: the retired cell format", () => {
      for (const notes of [
         ["## artifact { layout=linear }"],
         ['## artifact { layout=linear title="x" }'],
         // Linear by default: neither a layout nor tiles.
         ["## artifact { }"],
         ['## artifact { title="x" }'],
         // `layout=linear` wins over a grid tag, and still lists no tiles.
         ["## artifact { layout=linear } dashboard { columns=6 }"],
      ]) {
         expect(readReportTag(notes)).toEqual({ status: "cells-removed" });
      }
   });

   it("serves a linear tag with an empty tile list", () => {
      expect(readReportTag(["## artifact { layout=linear tiles=[] }"])).toEqual(
         { status: "report", layout: "linear" },
      );
   });

   it("reads layout values exactly: case and unknown words name no layout", () => {
      // No tiles and an unknown layout: no report, and the lint names the layout.
      expect(readReportTag(["## artifact { layout=Grid }"])).toEqual({
         status: "cells-removed",
         unknownLayout: "Grid",
      });
      expect(readReportTag(["## artifact { layout=linear }"])).toEqual({
         status: "cells-removed",
      });
      expect(
         readReportTag(['## artifact { layout=wide tiles=["a -> v"] }']),
      ).toMatchObject({ layout: "grid" });
   });

   it("reads a `tiles` that is not a list as a tag it cannot use", () => {
      expect(
         readReportTag(["## artifact { layout=linear tiles=foo }"]),
      ).toEqual({ status: "unparsed" });
      expect(
         readReportTag(["## artifact { layout=grid tiles { a } }"]),
      ).toEqual({ status: "unparsed" });
      expect(readReportTag(["## artifact { layout=linear tiles=[] }"])).toEqual(
         { status: "report", layout: "linear" },
      );
   });

   it("makes no report of a tag that sets `kind=`, whatever else it says", () => {
      expect(readReportTag(["## artifact { kind=notebook }"])).toEqual({
         status: "kind-removed",
         kind: "notebook",
      });
      expect(
         readReportTag([
            '## artifact { layout=grid kind=dashboard tiles=["a -> v"] }',
         ]),
      ).toEqual({ status: "kind-removed", kind: "dashboard" });
      // A tile entry's own `kind=text` is not the tag's.
      expect(
         readReportTag([
            "## artifact { layout=grid tiles=[intro { kind=text }] }",
         ]),
      ).toMatchObject({ status: "report" });
   });

   it("makes no report of an artifact note that does not parse", () => {
      expect(readReportTag(["## artifact {"])).toEqual({ status: "unparsed" });
   });
});

describe("readReportTagOfText", () => {
   it("reads the model-level notes, a `##|` block included", () => {
      expect(
         readReportTagOfText("##| artifact { layout=grid }\n|##\n"),
      ).toEqual({ status: "report", layout: "grid" });
      expect(
         readReportTagOfText(
            '## artifact { title="x" }\n## dashboard { columns=6 }\n',
         ),
      ).toMatchObject({ layout: "grid" });
   });

   it("reads no query-level `# artifact`: only a model-level tag makes a report", () => {
      expect(readReportTagOfText("# artifact\nquery: q is s -> v\n")).toEqual({
         status: "none",
      });
   });

   it("reads nothing off a file with no tag, a commented tag, or a quoted one", () => {
      for (const text of [
         "source: s is x\n",
         "source: s is x\n// ## artifact\n",
         '"## artifact"',
         "##|(markdown) intro\n## artifact { }\n|##\n",
      ]) {
         expect(readReportTagOfText(text)).toEqual({ status: "none" });
      }
   });
});

describe("carriesArtifactTag", () => {
   const cases: [string, string, boolean][] = [
      ["a model-level tag", "## artifact { layout=grid }\n", true],
      ["a `##|` block tag", "##| artifact { }\n|##\n", true],
      ["an unparsable model-level tag", "## artifact {\n", true],
      [
         "a tag setting the removed `kind=`",
         "## artifact { kind=notebook }\n",
         true,
      ],
      [
         "a tag after other properties",
         "## title=x artifact { layout=linear }\n",
         true,
      ],
      ["a query-level tag", "# artifact\nquery: q is s -> v\n", false],
      ["no tag", "source: s is x\n", false],
      ["a commented tag", "// ## artifact\n", false],
      ["a tag inside a block comment", "/*\n## artifact { }\n*/\n", false],
      ["a quoted tag", '"## artifact"', false],
      [
         "a tag inside a markdown block",
         "##|(markdown) intro\n## artifact { }\n|##\n",
         false,
      ],
   ];
   for (const [name, source, claims] of cases) {
      it(`${name}: ${claims}`, () => {
         expect(carriesArtifactTag(source)).toBe(claims);
      });
   }
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
      // The root `index.malloy` in any case and spelling, as the write path
      // refuses it: on a case-insensitive file system `Index.malloy` IS it.
      ["Index.malloy", false, false],
      ["INDEX.malloy", false, false],
      ["./index.malloy", false, false],
      ["a/Index.malloy", false, true],
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
