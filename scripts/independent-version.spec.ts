// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Tests for scripts/independent-version.mjs, the decision logic behind
// release-time versioning of skills and create-malloy-package.
//
// The pure functions are imported directly: unlike set-version.mjs, there is
// no argv/exit-code contract worth re-testing per case, because
// publish-packages.sh consumes these through the CLI's single stdout line and
// exit code, which the last describe block below exercises end to end.
// Everything else here is the decision itself — abort vs skip vs publish —
// which is the part a shell test could not check without a real registry and
// a real checkout.

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  checkFree,
  decideScaffolder,
  decideSkills,
  nextPatch,
} from "./independent-version.mjs";

describe("nextPatch", () => {
  it("bumps the patch component", () => {
    expect(nextPatch("0.1.28")).toBe("0.1.29");
  });

  it("bumps from zero", () => {
    expect(nextPatch("1.2.0")).toBe("1.2.1");
  });

  it("does not touch major or minor", () => {
    expect(nextPatch("2.10.99")).toBe("2.10.100");
  });

  it.each([
    ["a prerelease, which has no obvious next patch", "0.1.28-rc.1"],
    ["a build suffix", "0.1.28+build.5"],
    ["two components", "0.1"],
    ["four components", "0.1.28.1"],
    ["a v prefix", "v0.1.28"],
    ["empty", ""],
    ["not a version at all", "latest"],
    ["an embedded newline", "0.1.28\n0.1.29"],
  ])("refuses %s", (_label, input) => {
    expect(() => nextPatch(input)).toThrow();
  });
});

describe("decideSkills", () => {
  const base = {
    npmOk: true,
    latest: "0.1.28",
    gitHead: "c3e52cc157205f0c3bd21719864a2e494ac5427e",
    objectPresent: true,
  };

  it("publishes the next patch when content changed since npm latest's gitHead", () => {
    const decision = decideSkills({ ...base, changed: "changed" });
    expect(decision).toMatchObject({ action: "publish", version: "0.1.29" });
  });

  it("skips when nothing changed since npm latest's gitHead", () => {
    const decision = decideSkills({ ...base, changed: "unchanged" });
    expect(decision).toMatchObject({ action: "skip" });
  });

  it("aborts when npm did not answer", () => {
    const decision = decideSkills({ ...base, npmOk: false, changed: "changed" });
    expect(decision.action).toBe("abort");
    expect(decision.reason).toContain("npm did not answer");
  });

  it("aborts on an empty gitHead", () => {
    const decision = decideSkills({ ...base, gitHead: "", changed: "changed" });
    expect(decision.action).toBe("abort");
    expect(decision.reason).toContain("40 hex characters");
  });

  it("aborts on a non-hex gitHead", () => {
    const decision = decideSkills({
      ...base,
      gitHead: "not-a-real-commit-sha-at-all-00000000000",
      changed: "changed",
    });
    expect(decision.action).toBe("abort");
    expect(decision.reason).toContain("40 hex characters");
  });

  it("aborts on a short gitHead", () => {
    const decision = decideSkills({ ...base, gitHead: "c3e52cc1", changed: "changed" });
    expect(decision.action).toBe("abort");
  });

  it("aborts when the gitHead commit is missing from the checkout", () => {
    const decision = decideSkills({ ...base, objectPresent: false, changed: "changed" });
    expect(decision.action).toBe("abort");
    expect(decision.reason).toContain("not present in this checkout");
  });

  it("aborts when the diff errored", () => {
    const decision = decideSkills({ ...base, changed: "error" });
    expect(decision.action).toBe("abort");
    expect(decision.reason).toContain("errored");
  });

  it("aborts on a latest that is not a plain version", () => {
    const decision = decideSkills({ ...base, latest: "0.1.28-rc.1", changed: "changed" });
    expect(decision.action).toBe("abort");
  });

  it("never lets an untrusted value start a new line in the reason", () => {
    // A crafted gitHead containing a raw newline could otherwise make the
    // reason text, once echoed by the shell caller, begin a fresh line with
    // "::" and be read by the Actions runner as a workflow command.
    const decision = decideSkills({
      ...base,
      gitHead: "not-hex\n::error title=pwned::gotcha",
      changed: "changed",
    });
    expect(decision.action).toBe("abort");
    expect(decision.reason).not.toContain("\n");
  });
});

describe("decideScaffolder", () => {
  const base = { npmOk: true, latest: "0.0.22", release: "0.8.3" };

  it("publishes the next patch when publisherServer has never been set", () => {
    const decision = decideScaffolder({ ...base, publisherServer: "" });
    expect(decision).toMatchObject({ action: "publish", version: "0.0.23" });
  });

  it("publishes the next patch when publisherServer names a different release", () => {
    const decision = decideScaffolder({ ...base, publisherServer: "0.8.2" });
    expect(decision).toMatchObject({ action: "publish", version: "0.0.23" });
  });

  it("skips when publisherServer already equals this release (a re-run)", () => {
    const decision = decideScaffolder({ ...base, publisherServer: "0.8.3" });
    expect(decision).toMatchObject({ action: "skip" });
  });

  it("aborts when npm did not answer", () => {
    const decision = decideScaffolder({ ...base, npmOk: false, publisherServer: "" });
    expect(decision.action).toBe("abort");
  });

  it("aborts on a latest that is not a plain version", () => {
    const decision = decideScaffolder({ ...base, latest: "not-a-version", publisherServer: "" });
    expect(decision.action).toBe("abort");
  });
});

describe("checkFree", () => {
  it("is free (returns null) when the registry has no record of it", () => {
    expect(checkFree({ status: "free", name: "@malloy-publisher/skills", version: "0.1.29" })).toBeNull();
  });

  it("is an ERROR, not a skip, when the computed version already exists on npm", () => {
    const result = checkFree({
      status: "published",
      name: "@malloy-publisher/skills",
      version: "0.1.29",
    });
    expect(result?.action).toBe("abort");
    expect(result?.reason).toContain("already on npm");
  });

  it("is an error when the registry gave no usable answer", () => {
    const result = checkFree({
      status: "unknown",
      name: "@malloy-publisher/skills",
      version: "0.1.29",
    });
    expect(result?.action).toBe("abort");
  });
});

describe("the CLI, end to end", () => {
  function run(command: string, env: Record<string, string>) {
    const proc = Bun.spawnSync([
      "node",
      new URL("./independent-version.mjs", import.meta.url).pathname,
      command,
    ], {
      env: { ...process.env, ...env },
    });
    return {
      code: proc.exitCode,
      stdout: proc.stdout.toString().trim(),
      stderr: proc.stderr.toString(),
    };
  }

  it("next prints the bumped version", () => {
    const proc = Bun.spawnSync([
      "node",
      new URL("./independent-version.mjs", import.meta.url).pathname,
      "next",
      "0.1.28",
    ]);
    expect(proc.exitCode).toBe(0);
    expect(proc.stdout.toString().trim()).toBe("0.1.29");
  });

  it("decide-skills prints 'publish <version>' and exits 0 when changed", () => {
    const { code, stdout } = run("decide-skills", {
      NPM_OK: "1",
      LATEST: "0.1.28",
      GIT_HEAD: "c3e52cc157205f0c3bd21719864a2e494ac5427e",
      OBJECT_PRESENT: "1",
      CHANGED: "changed",
    });
    expect(code).toBe(0);
    expect(stdout).toBe("publish 0.1.29");
  });

  it("decide-skills prints 'skip' and exits 0 when unchanged", () => {
    const { code, stdout } = run("decide-skills", {
      NPM_OK: "1",
      LATEST: "0.1.28",
      GIT_HEAD: "c3e52cc157205f0c3bd21719864a2e494ac5427e",
      OBJECT_PRESENT: "1",
      CHANGED: "unchanged",
    });
    expect(code).toBe(0);
    expect(stdout).toBe("skip");
  });

  it("decide-skills prints 'abort <reason>' and exits with the distinct abort code", () => {
    const { code, stdout, stderr } = run("decide-skills", {
      NPM_OK: "0",
      LATEST: "",
      GIT_HEAD: "",
      OBJECT_PRESENT: "0",
      CHANGED: "error",
    });
    expect(code).not.toBe(0);
    expect(code).not.toBe(1); // distinct from the usage-error exit code
    expect(stdout).toStartWith("abort ");
    expect(stderr).toBe("");
  });

  it("decide-scaffolder prints 'publish <version>' when the pin does not match this release", () => {
    const { code, stdout } = run("decide-scaffolder", {
      NPM_OK: "1",
      LATEST: "0.0.22",
      PUBLISHER_SERVER: "",
      RELEASE: "0.8.3",
    });
    expect(code).toBe(0);
    expect(stdout).toBe("publish 0.0.23");
  });

  it("decide-scaffolder prints 'skip' when the pin already equals this release", () => {
    const { code, stdout } = run("decide-scaffolder", {
      NPM_OK: "1",
      LATEST: "0.0.22",
      PUBLISHER_SERVER: "0.8.3",
      RELEASE: "0.8.3",
    });
    expect(code).toBe(0);
    expect(stdout).toBe("skip");
  });

  it("check-free aborts on a computed version already published", () => {
    const { code, stdout } = run("check-free", {
      STATUS: "published",
      NAME: "@malloy-publisher/skills",
      VERSION: "0.1.29",
    });
    expect(code).not.toBe(0);
    expect(stdout).toStartWith("abort ");
  });

  it("fails with a usage error, distinct from abort, on an unknown command", () => {
    const proc = Bun.spawnSync([
      "node",
      new URL("./independent-version.mjs", import.meta.url).pathname,
      "not-a-real-command",
    ]);
    expect(proc.exitCode).toBe(1);
  });
});

describe("the is-main guard, through a symlink", () => {
  // `node <symlink>` resolves process.argv[1] to the symlink path, not the
  // real file import.meta.url points at. A guard comparing the two verbatim
  // never matches through a symlink, so the CLI silently does nothing —
  // exits 0 printing nothing, instead of running the command — which is
  // exactly the failure mode this checks for.
  const dirs: string[] = [];
  afterEach(() => {
    while (dirs.length) rmSync(dirs.pop()!, { recursive: true, force: true });
  });

  it("still runs the CLI when invoked through a symlink", () => {
    const real = new URL("./independent-version.mjs", import.meta.url).pathname;
    const dir = mkdtempSync(path.join(tmpdir(), "independent-version-symlink-"));
    dirs.push(dir);
    const link = path.join(dir, "independent-version-link.mjs");
    symlinkSync(real, link);

    const proc = Bun.spawnSync(["node", link, "next", "0.1.28"]);
    expect(proc.exitCode).toBe(0);
    expect(proc.stdout.toString().trim()).toBe("0.1.29");
  });
});
