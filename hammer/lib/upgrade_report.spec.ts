// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   isReleaseVersion,
   mdText,
   renderUpgradeMarkdown,
   type UpgradeReport,
} from "./upgrade_report";

const report = (over: Partial<UpgradeReport> = {}): UpgradeReport => ({
   version: 1,
   ok: true,
   fromVersion: "0.15.0",
   toVersion: "0.15.1",
   startedAt: "2026-01-01T00:00:00.000Z",
   durationMs: 90_000,
   checks: [{ phase: "after", name: "a", ok: true }],
   probes: {
      "persist-storage": {
         route: "#@ persist storage=lake",
         before: { rows: [], servedFrom: "storage" },
         after: { rows: [], servedFrom: "storage" },
      },
      "persist-database": {
         route: "#@ persist",
         before: { rows: [], servedFrom: null },
         after: { rows: [], servedFrom: null, error: "boom" },
      },
   },
   notes: [],
   ...over,
});

describe("isReleaseVersion", () => {
   it("takes a release or a prerelease, and nothing else", () => {
      for (const ok of ["0.15.0", "1.2.3", "0.0.198-dev6", "1.0.0-rc.1"])
         expect(isReleaseVersion(ok)).toBe(true);
      for (const bad of [
         "",
         "latest",
         "0.15",
         "v0.15.0",
         "0.15.0 --registry=http://x",
         "0.15.0\n",
         "0.15.0;rm",
         "file:../x",
         "0.15.0-",
      ])
         expect(isReleaseVersion(bad)).toBe(false);
   });
});

describe("mdText", () => {
   it("keeps a value to one line of plain text", () => {
      expect(mdText("a|b\n`c` <d> **e**")).toBe(
         "a\\|b \\`c\\` \\<d\\> \\*\\*e\\*\\*",
      );
      expect(mdText("x".repeat(10), 5)).toBe("xxxx…");
   });
});

describe("renderUpgradeMarkdown", () => {
   it("gives the verdict, the versions and every probe's tier", () => {
      const md = renderUpgradeMarkdown(report());
      expect(md).toContain("### Upgrade 0.15.0 → main (0.15.1): passed");
      expect(md).toContain("1/1 checks passed; 1.5 min.");
      expect(md).toContain(
         "| persist-storage | \\#@ persist storage=lake | storage | storage | – |",
      );
      expect(md).toContain("| not reported | error | – |");
      expect(md).not.toContain("| Phase |");
   });

   it("lists the failed checks and the harness error", () => {
      const md = renderUpgradeMarkdown(
         report({
            ok: false,
            harnessError: "docker: not found",
            checks: [
               {
                  phase: "after",
                  name: "x: same answer",
                  ok: false,
                  detail: "a|b",
               },
               { phase: "after", name: "fine", ok: true },
            ],
         }),
      );
      expect(md).toContain("FAILED");
      expect(md).toContain("**Harness error:** docker: not found");
      expect(md).toContain("| after | x: same answer | a\\|b |");
      expect(md).not.toContain("| fine |");
   });

   it("escapes a version that would be markup", () => {
      const md = renderUpgradeMarkdown(report({ fromVersion: "`x`<img>" }));
      expect(md).toContain("\\`x\\`\\<img\\>");
      expect(md).not.toContain("<img>");
   });
});
