// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { buildReport, renderMarkdown, type ResultLike } from "./report";

const result = (
   id: string,
   checks: [string, boolean, string?][],
   extra: Partial<ResultLike> & { tags?: string[] } = {},
): ResultLike => ({
   scenario: { id, title: `title of ${id}`, tags: extra.tags ?? [] },
   assert: {
      checks: checks.map(([name, ok, detail]) => ({ name, ok, detail })),
   },
   ...extra,
});

const at = (s: number) => new Date(Date.UTC(2026, 0, 1, 0, 0, s));

describe("buildReport", () => {
   it("classifies every outcome the run's exit code reads", () => {
      const report = buildReport({
         results: [
            result("green", [["a", true]]),
            result("red", [
               ["a", true],
               ["b", false, "expected 1, got 2"],
            ]),
            result("debt", [["a", false]], { tags: ["known-red"] }),
            result("now-holds", [["a", true]], { tags: ["known-red"] }),
            result("skipped", [], { skipped: "needs dialect:snowflake" }),
         ],
         knownRedTag: "known-red",
         startedAt: at(0),
         finishedAt: at(30),
         materialize: "true",
      });
      expect(report.counts).toEqual({
         pass: 1,
         fail: 1,
         "known-red": 1,
         fixed: 1,
         skip: 1,
      });
      expect(report.ok).toBe(false);
      expect(report.durationMs).toBe(30_000);
      expect(report.materialize).toBe("true");
      const red = report.scenarios.find((s) => s.id === "red")!;
      expect(red.failedChecks).toEqual([
         { name: "b", detail: "expected 1, got 2" },
      ]);
      expect(red.passedChecks).toBe(1);
      expect(report.scenarios.find((s) => s.id === "skipped")!.reason).toBe(
         "needs dialect:snowflake",
      );
   });

   it("is ok with only passes, known-reds and skips", () => {
      const report = buildReport({
         results: [
            result("green", [["a", true]]),
            result("debt", [["a", false]], { tags: ["known-red"] }),
         ],
         knownRedTag: "known-red",
         startedAt: at(0),
         finishedAt: at(1),
      });
      expect(report.ok).toBe(true);
      expect("materialize" in report).toBe(false);
   });

   it("is not ok on a harness error even when no scenario failed", () => {
      const report = buildReport({
         results: [],
         knownRedTag: "known-red",
         startedAt: at(0),
         finishedAt: at(1),
         harnessError: "docker: not found",
      });
      expect(report.ok).toBe(false);
      expect(renderMarkdown(report, "Hammer")).toContain("docker: not found");
   });
});

describe("renderMarkdown", () => {
   it("lists each failed check, and keeps a table cell on one line", () => {
      const report = buildReport({
         results: [
            result("green", [["a", true]]),
            result("red", [
               ["first | check", false, "line one\nline two"],
               ["second", false],
            ]),
            result("threw", [["scenario threw", false, "boom"]], {
               error: "Error: boom\n    at x",
            }),
         ],
         knownRedTag: "known-red",
         startedAt: at(0),
         finishedAt: at(90),
      });
      const md = renderMarkdown(report, "Hammer (materialize=true)");
      expect(md).toContain("### Hammer (materialize=true): FAILED");
      expect(md).toContain("1 passed, 2 failed");
      expect(md).toContain(
         "| `red` | 0/2 | first \\| check | line one line two |",
      );
      expect(md).toContain("|  |  | second |  |");
      expect(md).not.toContain("`green`");
   });

   it("escapes a backslash before a pipe, so the pipe cannot end the cell", () => {
      const report = buildReport({
         results: [result("red", [["a", false, "C:\\|x"]])],
         knownRedTag: "known-red",
         startedAt: at(0),
         finishedAt: at(1),
      });
      expect(renderMarkdown(report, "Hammer")).toContain("| C:\\\\\\|x |");
   });

   it("says so when a known-red passes", () => {
      const report = buildReport({
         results: [result("now-holds", [["a", true]], { tags: ["known-red"] })],
         knownRedTag: "known-red",
         startedAt: at(0),
         finishedAt: at(1),
      });
      expect(renderMarkdown(report, "Hammer")).toContain(
         "retire its `known-red` tag",
      );
   });
});
