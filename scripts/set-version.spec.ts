// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Tests for scripts/set-version.mjs, the release workflow's version-stamping
// step.
//
// Like release-notes.mjs, this runs on a release path that cannot be exercised
// in CI: `prepare` calls it on the release branch before npm publishes, and
// `gh-release` calls it again on the post-release stamp branch. A bug in the
// first is discovered by finding the OLD version published under the new tag,
// which npm's immutability makes unfixable; a bug in the second leaves `main`
// wrong, which is the thing this whole change set exists to fix.
//
// The two cases that are not obvious are the two silent failures the shell
// function it replaced actually had: a file whose "version" field the regex did
// not match was rewritten byte-identical and reported success, and an
// unvalidated dispatch input could put a quote inside the JSON string.
//
// Run as a subprocess rather than imported: its argv parsing, exit codes and
// stdout contract are the interface the workflow depends on, and a test calling
// an exported function would cover none of them.

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const SCRIPT = path.join(import.meta.dir, "set-version.mjs");

const workspaces: string[] = [];

/** A throwaway directory holding named files with the given contents. */
function workspace(files: Record<string, string>) {
  const dir = mkdtempSync(path.join(tmpdir(), "set-version-"));
  workspaces.push(dir);
  const paths: Record<string, string> = {};
  for (const [name, content] of Object.entries(files)) {
    paths[name] = path.join(dir, name);
    writeFileSync(paths[name], content);
  }
  return paths;
}

afterEach(() => {
  while (workspaces.length) {
    rmSync(workspaces.pop()!, { recursive: true, force: true });
  }
});

function run(...args: string[]) {
  const proc = Bun.spawnSync(["node", SCRIPT, ...args]);
  return {
    code: proc.exitCode,
    stdout: proc.stdout.toString().trim(),
    stderr: proc.stderr.toString(),
  };
}

// The real shape, including the `workspace:*` range that rules out
// `npm version --workspaces` and the two-space indent and trailing newline the
// replace has to leave alone.
const MANIFEST = `{
  "name": "@malloy-publisher/sdk",
  "version": "0.0.209",
  "type": "module",
  "dependencies": {
    "@malloy-publisher/skills": "workspace:*"
  },
  "scripts": {
    "build": "tsc"
  }
}
`;

describe("setting the version", () => {
  it("rewrites the field and nothing else", () => {
    const { "package.json": file } = workspace({ "package.json": MANIFEST });

    const { code, stdout } = run("0.2.0", file);

    expect(code).toBe(0);
    expect(stdout).toBe("1");
    // Byte-for-byte identical apart from the one field: the indent, the
    // key order, the workspace:* range and the trailing newline all survive.
    // A JSON.parse/stringify round trip would fail this.
    expect(readFileSync(file, "utf8")).toBe(
      MANIFEST.replace('"version": "0.0.209"', '"version": "0.2.0"'),
    );
  });

  it("sets every file it is given, and counts them", () => {
    const files = workspace({
      "sdk.json": MANIFEST,
      "app.json": MANIFEST,
      "server.json": MANIFEST,
    });

    const { code, stdout } = run("0.2.0", ...Object.values(files));

    expect(code).toBe(0);
    expect(stdout).toBe("3");
    for (const file of Object.values(files)) {
      expect(JSON.parse(readFileSync(file, "utf8")).version).toBe("0.2.0");
    }
  });

  it("reports 0 when the version is already set, and still succeeds", () => {
    // This is the case that cannot be an error. `gh-release` runs this over
    // `main`, where the version is usually ALREADY the one being released —
    // the previous release's stamp PR merged — and a non-zero exit there
    // would redden a finished release over nothing.
    const { "package.json": file } = workspace({ "package.json": MANIFEST });

    expect(run("0.0.209", file)).toMatchObject({ code: 0, stdout: "0" });
    expect(readFileSync(file, "utf8")).toBe(MANIFEST);
  });

  it("counts only the files it actually changed", () => {
    const files = workspace({
      "stale.json": MANIFEST,
      "current.json": MANIFEST.replace("0.0.209", "0.2.0"),
    });

    expect(run("0.2.0", ...Object.values(files))).toMatchObject({
      code: 0,
      stdout: "1",
    });
  });

  it("accepts a prerelease, because prereleases are a release shape here", () => {
    // npm-sdk.yml routes a hyphenated version to the `next` dist-tag, so
    // `-f version=0.2.0-rc.1` is a supported dispatch and must not be
    // refused. release.yml's own guard is stricter on purpose: it guards
    // arithmetic, and nothing is computed from the version here.
    const { "package.json": file } = workspace({ "package.json": MANIFEST });

    expect(run("0.2.0-rc.1", file)).toMatchObject({ code: 0 });
    expect(JSON.parse(readFileSync(file, "utf8")).version).toBe("0.2.0-rc.1");
  });
});

describe("refusing to write", () => {
  it("fails on a file with no version field, instead of rewriting it unchanged", () => {
    // The shell function this replaced applied its regex and wrote the result
    // unconditionally, so this file came back byte-identical with exit 0 and
    // the release published the OLD version under the new tag.
    const { "package.json": file } = workspace({
      "package.json": '{\n  "name": "@malloy-publisher/sdk"\n}\n',
    });

    const { code, stderr } = run("0.2.0", file);

    expect(code).toBe(1);
    expect(stderr).toContain('has no "version" field');
  });

  it("fails on a version that would corrupt the JSON", () => {
    const { "package.json": file } = workspace({ "package.json": MANIFEST });

    const { code, stderr } = run('0.2.0", "evil": "1', file);

    expect(code).toBe(1);
    expect(stderr).toContain("refusing to write");
    // Untouched, not half-written.
    expect(readFileSync(file, "utf8")).toBe(MANIFEST);
  });

  it.each([
    ["empty", ""],
    ["not a version at all", "latest"],
    ["a v prefix, which prepare strips before calling this", "v0.2.0"],
    ["two components", "0.2"],
    ["an embedded newline", "0.2.0\n0.3.0"],
  ])("fails on %s", (_label, version) => {
    const { "package.json": file } = workspace({ "package.json": MANIFEST });

    expect(run(version, file).code).toBe(1);
    expect(readFileSync(file, "utf8")).toBe(MANIFEST);
  });

  it("fails on an unreadable file, naming it", () => {
    const { code, stderr } = run("0.2.0", "/nonexistent/package.json");

    expect(code).toBe(1);
    expect(stderr).toContain("/nonexistent/package.json");
  });

  it("fails when the matched field was not the top-level one", () => {
    // The regex takes the FIRST "version" in the file. A manifest that puts
    // one somewhere else first still parses after the replace, so the JSON
    // check alone would not catch it — the assertion that `.version` is what
    // we asked for is what does.
    const { "package.json": file } = workspace({
      "package.json": `{
  "engines": { "version": "1.0.0" },
  "name": "@malloy-publisher/sdk",
  "version": "0.0.209"
}
`,
    });

    const { code, stderr } = run("0.2.0", file);

    expect(code).toBe(1);
    expect(stderr).toContain("not the top-level one");
  });

  it("stops at the first bad file rather than half-applying the set", () => {
    const files = workspace({
      "a.json": MANIFEST,
      "b.json": '{\n  "name": "no-version-here"\n}\n',
      "c.json": MANIFEST,
    });

    expect(
      run("0.2.0", files["a.json"], files["b.json"], files["c.json"]).code,
    ).toBe(1);
    // a.json was already written when b.json failed. That asymmetry is worth
    // asserting rather than pretending: the callers both run this on a
    // throwaway branch and commit only on success, so a partial write is
    // discarded, and making it atomic would buy nothing.
    expect(JSON.parse(readFileSync(files["a.json"], "utf8")).version).toBe(
      "0.2.0",
    );
    expect(JSON.parse(readFileSync(files["c.json"], "utf8")).version).toBe(
      "0.0.209",
    );
  });

  it("fails with usage when given no files", () => {
    expect(run("0.2.0").code).toBe(1);
    expect(run().code).toBe(1);
  });
});

describe("the real manifests it is pointed at", () => {
  it("sets the version in copies of all three real manifests", () => {
    // The workflow passes these three paths literally, so a rename or a reformat
    // that moved the "version" field out of the script's reach would otherwise
    // be discovered by a release.
    //
    // Runs the SCRIPT against copies rather than re-testing its regex here. An
    // earlier version of this test inlined a duplicate of `FIELD`, which meant
    // tightening the regex in set-version.mjs left this passing against the old
    // one — the exact drift it claims to catch. Copies, because the originals
    // must not be touched by a test.
    const repoRoot = path.join(import.meta.dir, "..");
    const originals = ["sdk", "app", "server"].map((pkg) =>
      path.join(repoRoot, "packages", pkg, "package.json"),
    );

    const copies: Record<string, string> = {};
    for (const [i, file] of originals.entries()) {
      copies[`${i}-package.json`] = readFileSync(file, "utf8");
    }
    const paths = workspace(copies);

    const { code, stdout } = run("9.9.9", ...Object.values(paths));

    expect(code).toBe(0);
    expect(stdout).toBe(String(originals.length));
    for (const file of Object.values(paths)) {
      expect(JSON.parse(readFileSync(file, "utf8")).version).toBe("9.9.9");
    }
  });
});
