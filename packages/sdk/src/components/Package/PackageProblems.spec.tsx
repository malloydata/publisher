// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { fireEvent, render, screen } from "@testing-library/react";
import { PackageProblems, withoutRetainedAliases } from "./PackageProblems";

const KIND_REMOVED = {
   model: "notebooks/overview.malloy",
   code: "report-kind-removed",
   severity: "error" as const,
   message:
      "Line 11: `kind=notebook` is no longer read, so this file is not a report.",
};
const NOTE = {
   model: "old.malloynb",
   code: "deprecated-malloynb-notebook",
   severity: "warn" as const,
   message: "A .malloynb notebook is deprecated.",
};

describe("PackageProblems", () => {
   it("draws nothing for a clean package", () => {
      const { container } = render(<PackageProblems warnings={undefined} />);
      expect(container.textContent).toBe("");
   });

   it("says why a file is not a report, naming the file", () => {
      render(<PackageProblems warnings={[KIND_REMOVED]} />);
      expect(screen.getByText("1 problem in this package")).toBeDefined();
      expect(screen.getByText("notebooks/overview.malloy")).toBeDefined();
      expect(screen.getByText(/is no longer read/)).toBeDefined();
   });

   it("folds warn-level findings away until asked", () => {
      render(<PackageProblems warnings={[KIND_REMOVED, NOTE]} />);
      expect(screen.queryByText(/is deprecated/)).toBeNull();
      fireEvent.click(screen.getByRole("button", { name: "Show 1 note" }));
      expect(screen.getByText(/is deprecated/)).toBeDefined();
   });
});

describe("withoutRetainedAliases", () => {
   const notBuilt = {
      model: "m.malloy",
      subject: "orders",
      code: "rollup-not-built",
      severity: "warn" as const,
      message: "not built",
   };

   it("shows a renamed code's one-release repeat once", () => {
      expect(
         withoutRetainedAliases([
            notBuilt,
            { ...notBuilt, code: "preaggregate-not-built" },
         ]),
      ).toEqual([notBuilt]);
   });

   it("keeps an older server's former code when it is the only copy", () => {
      const old = { ...notBuilt, code: "preaggregate-not-built" };
      expect(withoutRetainedAliases([old])).toEqual([old]);
   });
});
