// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { pathToFileURL } from "url";
import {
   type CheckFinding,
   type CheckReport,
   checkConfigPath,
   checkExitCode,
   checkSettings,
   checkSummaryLine,
   couldNotCheck,
   displayPath,
   formatFinding,
   githubAnnotation,
   problemFinding,
} from "./check";
import { checkErrorLine, checkRequested } from "./check_signals";
import { Settings } from "./settings";

const report = (over: Partial<CheckReport> = {}): CheckReport => ({
   findings: [],
   environments: 1,
   packages: 2,
   loadErrors: 0,
   ...over,
});
const error: CheckFinding = {
   severity: "error",
   environment: "e",
   package: "p",
   message: "boom",
};
const warning: CheckFinding = { ...error, severity: "warning" };
const lenient = { failOnWarnings: false };
const strict = { failOnWarnings: true };

describe("check mode settings", () => {
   const read = (argv: string[], env: Record<string, string> = {}) => {
      const loaded = Settings.load({ argv, env });
      expect(loaded.errors()).toEqual([]);
      return {
         check: loaded.get("check"),
         failOnWarnings: loaded.get("checkFailOnWarnings"),
      };
   };

   it("is off unless asked for", () => {
      expect(read([])).toEqual({ check: false, failOnWarnings: false });
   });

   it("--check turns it on, and leaves warnings non-fatal", () => {
      expect(read(["--check"])).toEqual({ check: true, failOnWarnings: false });
   });

   it("PUBLISHER_CHECK turns it on", () => {
      expect(read([], { PUBLISHER_CHECK: "true" }).check).toBe(true);
   });

   it("--check-fail-on-warnings and its variable turn warnings fatal", () => {
      expect(read(["--check", "--check-fail-on-warnings"])).toEqual({
         check: true,
         failOnWarnings: true,
      });
      expect(
         read([], { PUBLISHER_CHECK_FAIL_ON_WARNINGS: "true" }).failOnWarnings,
      ).toBe(true);
   });

   it("a value that is not a boolean fails startup", () => {
      const loaded = Settings.load({
         argv: [],
         env: { PUBLISHER_CHECK: "maybe" },
      });
      expect(
         loaded
            .errors()
            .map((e) => e.message)
            .join("\n"),
      ).toContain("PUBLISHER_CHECK");
   });
});

describe("check exit code", () => {
   it("is 0 when everything loaded and nothing is an error", () => {
      expect(checkExitCode(report(), lenient)).toBe(0);
   });

   it("is 1 when a package failed to load", () => {
      expect(
         checkExitCode(report({ loadErrors: 1, findings: [error] }), lenient),
      ).toBe(1);
   });

   it("is 1 when a load error came with no finding of its own", () => {
      expect(checkExitCode(report({ loadErrors: 1 }), lenient)).toBe(1);
   });

   it("is 1 on an error finding of a package that loaded", () => {
      expect(checkExitCode(report({ findings: [error] }), lenient)).toBe(1);
   });

   it("is 2 when the check could not look (storage failed, no config)", () => {
      expect(checkExitCode(couldNotCheck("no config"), lenient)).toBe(2);
      expect(checkExitCode(couldNotCheck("no config"), strict)).toBe(2);
   });

   it("is 0 on warnings, unless warnings are fatal", () => {
      expect(checkExitCode(report({ findings: [warning] }), lenient)).toBe(0);
      expect(checkExitCode(report({ findings: [warning] }), strict)).toBe(1);
   });
});

describe("check summary line", () => {
   it("reads OK with the counts", () => {
      expect(checkSummaryLine(report({ findings: [warning] }), lenient)).toBe(
         "PUBLISHER_CHECK_OK environments=1 packages=2 load_errors=0 errors=0 warnings=1",
      );
   });

   it("reads FAILED with the counts", () => {
      expect(
         checkSummaryLine(
            report({ packages: 1, loadErrors: 1, findings: [error, error] }),
            lenient,
         ),
      ).toBe(
         "PUBLISHER_CHECK_FAILED environments=1 packages=1 load_errors=1 errors=2 warnings=0",
      );
   });
});

describe("could not check", () => {
   it("prints PUBLISHER_CHECK_ERROR with the reason on one line", () => {
      expect(checkSummaryLine(couldNotCheck("no\nconfig"), lenient)).toBe(
         'PUBLISHER_CHECK_ERROR error="no / config"',
      );
      expect(checkErrorLine('bad "x"')).toBe(
         'PUBLISHER_CHECK_ERROR error="bad \\"x\\""\n',
      );
   });

   it("is asked for by --check even when the settings did not parse", () => {
      const fails = () => {
         throw new Error("PUBLISHER_CHECK: not a boolean");
      };
      expect(checkRequested([], fails)).toBe(true);
      expect(checkRequested(["--check"], () => false)).toBe(true);
      expect(checkRequested([], () => true)).toBe(true);
      expect(checkRequested([], () => false)).toBe(false);
   });
});

describe("the config check mode reads", () => {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "check-config-"));
   const load = (argv: string[]) => Settings.load({ argv, env: {}, cwd: dir });

   it("is never the bundled default: with no config there is nothing to check", () => {
      const empty = fs.mkdtempSync(path.join(os.tmpdir(), "check-empty-"));
      const found = checkConfigPath(
         Settings.load({ argv: ["--check"], env: {}, cwd: empty }),
         empty,
      );
      expect(found).toEqual({
         missing: expect.stringContaining(
            `No publisher.config.json at ${path.join(empty, "publisher.config.json")}`,
         ),
      });
   });

   it("names a --config path that does not exist", () => {
      const found = checkConfigPath(load(["--config", "nope.json"]), dir);
      expect(found).toEqual({
         missing: expect.stringContaining(path.join(dir, "nope.json")),
      });
   });

   it("is the --config file, or the root's", () => {
      const file = path.join(dir, "publisher.config.json");
      fs.writeFileSync(file, JSON.stringify({ environments: [] }));
      expect(checkConfigPath(load([]), dir)).toEqual({ path: file });
      expect(checkConfigPath(load(["--config", file]), dir)).toEqual({
         path: file,
      });
   });

   it("loads under a temporary root, pinned to that config, with no stored state or symlinks", () => {
      const file = path.join(dir, "publisher.config.json");
      fs.writeFileSync(file, JSON.stringify({ environments: [] }));
      const pinned = checkSettings(
         load(["--check", "--init", "--watch-env", "e", "--root", dir]),
         "/tmp/check-root",
         file,
      );
      expect(pinned.get("root")).toBe("/tmp/check-root");
      expect(pinned.get("config")).toBe(file);
      expect(pinned.get("useBundledDefault")).toBe(false);
      expect(pinned.get("initializeStorage")).toBe(false);
      expect(pinned.get("watchEnvironments")).toEqual([]);
      expect(pinned.get("check")).toBe(true);
   });
});

describe("GitHub Actions annotations", () => {
   it("land on the file, line and column", () => {
      expect(
         githubAnnotation({
            ...error,
            file: "models/p/a.malloy",
            line: 3,
            column: 7,
            message: "unexpected '}'",
            code: "syntax-error",
         }),
      ).toBe(
         "::error file=models/p/a.malloy,line=3,col=7,title=Malloy check%3A e/p::unexpected '}' (syntax-error)",
      );
   });

   it("escape what GitHub reads as syntax", () => {
      expect(
         githubAnnotation({
            severity: "warning",
            file: "a,b:c.malloy",
            message: "100% wrong",
         }),
      ).toBe("::warning file=a%2Cb%3Ac.malloy::100%25 wrong");
   });
});

describe("check finding lines", () => {
   it("leads with file:line:col and ends with the package", () => {
      expect(
         formatFinding({
            ...error,
            file: "pkgs/p/a.malloy",
            line: 3,
            column: 7,
            code: "syntax-error",
         }),
      ).toBe("pkgs/p/a.malloy:3:7: error: boom (syntax-error) [e/p]");
   });

   it("names the package where there is no file", () => {
      expect(formatFinding(error)).toBe("e/p: error: boom");
      expect(formatFinding({ severity: "error", message: "no storage" })).toBe(
         "publisher: error: no storage",
      );
   });

   it("keeps a multi-line load error on one line", () => {
      expect(
         formatFinding({
            ...error,
            message: "Error(s) compiling model:\nFILE: a.malloy\nline 3: x\n",
         }),
      ).toBe(
         "e/p: error: Error(s) compiling model: / FILE: a.malloy / line 3: x",
      );
   });
});

describe("a Malloy problem as a finding", () => {
   const cwd = path.join(path.sep, "repo");
   const packageDir = path.join(cwd, "models", "p");
   const where = {
      environment: "e",
      package: "p",
      packageDir,
      modelPath: "a.malloy",
      cwd,
   };

   it("is one-based, at the file its url names, relative to the cwd", () => {
      const finding = problemFinding(
         {
            severity: "error",
            message: "'nope' is not defined",
            code: "field-not-found",
            at: {
               url: pathToFileURL(path.join(packageDir, "b.malloy")).toString(),
               range: { start: { line: 0, character: 66 } },
            },
         },
         where,
      );
      expect(finding).toEqual({
         severity: "error",
         environment: "e",
         package: "p",
         file: path.join("models", "p", "b.malloy"),
         line: 1,
         column: 67,
         message: "'nope' is not defined",
         code: "field-not-found",
      });
   });

   it("falls back to the model it was reported on, with no position", () => {
      const finding = problemFinding({ message: "odd" }, where);
      expect(finding.file).toBe(path.join("models", "p", "a.malloy"));
      expect(finding.line).toBeUndefined();
      expect(finding.severity).toBe("warning");
   });

   it("names a file outside the cwd absolutely", () => {
      const outside = path.join(path.sep, "elsewhere", "x.malloy");
      expect(displayPath(cwd, outside)).toBe(outside);
   });
});
