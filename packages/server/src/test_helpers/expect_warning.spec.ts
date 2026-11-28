// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import { expectWarning } from "./expect_warning";

type PackageWarning = components["schemas"]["PackageWarning"];

const warning = (
   code: string,
   subject: string,
   message: string,
): PackageWarning => ({ code, subject, message, severity: "warn" });

describe("expectWarning", () => {
   it("finds a set finding by its token among several on one subject", () => {
      const warnings = [
         warning("skill-finding", "house", "a skill 'a' has no body"),
         warning("skill-finding", "house", "a skill 'b' names no description"),
         warning("skill-file-skipped", "house", "a file 'c.md' is too large"),
         warning("skill-file-skipped", "house", "a file 'd.md' is too large"),
      ];
      expect(expectWarning(warnings, "skill-finding", "house", "'b'")).toBe(
         warnings[1]!,
      );
      expect(
         expectWarning(warnings, "skill-file-skipped", "house", "'d.md'"),
      ).toBe(warnings[3]!);
   });

   it("fails when no warning of the code and subject holds the token", () => {
      const warnings = [warning("skill-finding", "house", "a skill 'a'")];
      expect(() =>
         expectWarning(warnings, "skill-finding", "house", "'z'"),
      ).toThrow(/found 1 \(0 holding it\)/);
      expect(() =>
         expectWarning(warnings, "skill-finding", "other", "'a'"),
      ).toThrow(/found 0/);
   });

   it("fails when two set findings both hold the token", () => {
      const warnings = [
         warning("skill-finding", "house", "a skill 'a' is bad"),
         warning("skill-finding", "house", "a skill 'a' is also odd"),
      ];
      expect(() =>
         expectWarning(warnings, "skill-finding", "house", "'a'"),
      ).toThrow(/2 holding it/);
   });

   it.each(["workflow-not-served", "skill-set-not-served"])(
      "fails when %s is said twice for one subject, whatever each says",
      (code) => {
         const warnings = [
            warning(code, "house", "refused: a bad key"),
            warning(code, "house", "refused: a bad path"),
         ];
         expect(() => expectWarning(warnings, code, "house", "key")).toThrow(
            /found 2 \(1 holding it\)/,
         );
      },
   );
});
