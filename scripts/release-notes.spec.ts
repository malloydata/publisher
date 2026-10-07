// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Tests for scripts/release-notes.mjs, the release workflow's narrative step.
//
// This script is exercised once per release, on a dispatch-only path that CI
// cannot run: `prepare` stamps the release branch and `gh-release` extracts from
// it after npm and Docker have already published. A bug in it is discovered by
// reading a wrong release page. One of the cases below is a bug that reached
// review: a bare `## [Unreleased]` heading put `## ## [Unreleased]` onto the
// public page.
//
// The script is run as a subprocess rather than imported: its argv parsing,
// exit codes and stdout contract are the interface the workflow depends on, and
// a test that called an exported function would not cover any of them.

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const SCRIPT = path.join(import.meta.dir, "release-notes.mjs");

const HEADER = `# Release Notes

Preamble that mentions the word Unreleased in prose, because these sections
discuss prior releases constantly and a blanket replace would rewrite this line.

---
`;

const workspaces: string[] = [];

/** A throwaway RELEASE_NOTES.md. */
function workspace(notes: string) {
  const dir = mkdtempSync(path.join(tmpdir(), "release-notes-"));
  workspaces.push(dir);
  const file = path.join(dir, "RELEASE_NOTES.md");
  writeFileSync(file, notes);
  return { file };
}

afterEach(() => {
  while (workspaces.length) {
    rmSync(workspaces.pop()!, { recursive: true, force: true });
  }
});

function run(file: string, ...args: string[]) {
  const proc = Bun.spawnSync(["node", SCRIPT, ...args], {
    env: { ...process.env, RELEASE_NOTES_FILE: file },
  });
  return {
    code: proc.exitCode,
    stdout: proc.stdout.toString(),
    stderr: proc.stderr.toString(),
  };
}

describe("extract", () => {
  it("strips every separator this file has actually used", () => {
    // Em dash, colon and hyphen all appear in RELEASE_NOTES.md's history.
    const { file } = workspace(
      `${HEADER}
## [Unreleased] — em dash

dash body

---

## [Unreleased]: colon

colon body

---

## [Unreleased] - hyphen

hyphen body
`,
    );

    const { code, stdout } = run(file, "extract");
    expect(code).toBe(0);
    expect(stdout).toContain("## em dash");
    expect(stdout).toContain("## colon");
    expect(stdout).toContain("## hyphen");
    // The marker itself never survives onto the page, in any form.
    expect(stdout).not.toContain("[Unreleased]");
    // Three sections, joined by the horizontal rule the page separates them by.
    expect(stdout.match(/^## /gm)).toHaveLength(3);
    expect(stdout).toContain("\n\n---\n\n");
  });

  it("carries ### subheadings and fenced code through untouched", () => {
    const { file } = workspace(
      `${HEADER}
## [Unreleased] — has structure

### Breaking changes

\`\`\`bash
## not a heading, it is a shell comment
\`\`\`
`,
    );

    const { code, stdout } = run(file, "extract");
    expect(code).toBe(0);
    expect(stdout).toContain("### Breaking changes");
    expect(stdout).toContain("## not a heading, it is a shell comment");
  });

  it("rejects a bare heading rather than putting `## ` on the page", () => {
    // The bug: an earlier regex required a separator, so this heading matched
    // as a section but had nothing stripped, and `## ## [Unreleased]` went out
    // on the release body.
    const { file } = workspace(`${HEADER}
## [Unreleased]

body with no title above it
`);

    const { code, stdout, stderr } = run(file, "extract");
    expect(code).toBe(1);
    expect(stdout).toBe("");
    expect(stderr).toContain("has no title");
    expect(stdout).not.toContain("## ## [Unreleased]");
  });

  it("rejects a heading whose separator is followed by nothing", () => {
    const { file } = workspace(`${HEADER}
## [Unreleased] —

body
`);

    const { code, stderr } = run(file, "extract");
    expect(code).toBe(1);
    expect(stderr).toContain("has no title");
  });

  it("prints nothing when there is no narrative", () => {
    const { file } = workspace(`${HEADER}
## [0.0.248] — already shipped

body
`);

    const { code, stdout } = run(file, "extract");
    expect(code).toBe(0);
    expect(stdout).toBe("");
  });
});

describe("extract <version>", () => {
  // `gh-release` reads the release branch after `prepare` stamped it, so the
  // narrative it puts on the page is whatever is headed with this version.
  const NOTES = `${HEADER}
## [Unreleased] — merged after the release was cut

later body

---

## [0.0.249] — first

a

---

## [0.0.249]: second

b

---

## [0.0.248] — previous release

old body
`;

  it("prints only the sections headed with that version", () => {
    const { file } = workspace(NOTES);

    const { code, stdout } = run(file, "extract", "0.0.249");
    expect(code).toBe(0);
    expect(stdout).toBe("## first\n\na\n\n---\n\n## second\n\nb\n");
  });

  it("does not match a version that only shares a prefix", () => {
    // `.` is escaped, and the closing bracket is part of the match, so 0.0.24
    // is not read as a prefix of 0.0.249 or 0.0.248.
    const { file } = workspace(NOTES);

    const { code, stdout } = run(file, "extract", "0.0.24");
    expect(code).toBe(0);
    expect(stdout).toBe("");
  });

  it("reads back exactly what [Unreleased] would have printed after a stamp", () => {
    // The workflow's contract: `prepare` stamps, `gh-release` extracts by
    // version, and the release page is the same text the authors wrote.
    const { file } = workspace(`${HEADER}
## [Unreleased] — em dash

dash body

---

## [Unreleased]: colon

colon body

---

## [0.0.248] — already shipped

old body
`);
    const before = run(file, "extract");
    expect(before.code).toBe(0);
    expect(before.stdout).not.toBe("");

    expect(run(file, "stamp", "0.0.249").stdout).toBe("2\n");

    const after = run(file, "extract", "0.0.249");
    expect(after.code).toBe(0);
    expect(after.stdout).toBe(before.stdout);
    expect(run(file, "extract").stdout).toBe("");
  });
});

describe("stamp", () => {
  it("rewrites the [Unreleased] headings and nothing else", () => {
    const { file } = workspace(`${HEADER}
## [Unreleased] — shipping now

body that says Unreleased in prose

---

## [0.0.248] — already shipped

body
`);

    const { code, stdout } = run(file, "stamp", "0.0.249");
    expect(code).toBe(0);
    expect(stdout).toBe("1\n");

    const after = readFileSync(file, "utf8");
    expect(after).toContain("## [0.0.249] — shipping now");
    expect(after).toContain("body that says Unreleased in prose");
    expect(after).toContain("## [0.0.248] — already shipped");
    expect(after).not.toContain("[Unreleased]");
    // The preamble's prose mention is untouched.
    expect(after).toContain("mentions the word Unreleased in prose");
  });

  it("is a no-op, byte for byte, when there is nothing to stamp", () => {
    const notes = `${HEADER}
## [0.0.248] — already shipped

body
`;
    const { file } = workspace(notes);

    const { code, stdout } = run(file, "stamp", "0.0.249");
    expect(code).toBe(0);
    expect(stdout).toBe("0\n");
    expect(readFileSync(file, "utf8")).toBe(notes);
  });

  it("is idempotent: a second run stamps nothing and changes nothing", () => {
    const { file } = workspace(`${HEADER}
## [Unreleased] — shipping now

body
`);

    expect(run(file, "stamp", "0.0.249").stdout).toBe("1\n");
    const once = readFileSync(file, "utf8");

    const second = run(file, "stamp", "0.0.250");
    expect(second.code).toBe(0);
    expect(second.stdout).toBe("0\n");
    expect(readFileSync(file, "utf8")).toBe(once);
  });

  it("stamps a bare heading, which extract refuses", () => {
    // Deliberately asymmetric. `extract` fails on an untitled section because
    // it has nothing to put on the page; `stamp` still records what shipped, so
    // a heading that somehow got past authoring is not left [[Unreleased]]
    // forever.
    const { file } = workspace(`${HEADER}
## [Unreleased]

body
`);

    expect(run(file, "stamp", "0.0.249").stdout).toBe("1\n");
    expect(readFileSync(file, "utf8")).toContain("## [0.0.249]");
  });
});

describe("bad invocations leave the file alone", () => {
  const NOTES = `${HEADER}
## [Unreleased] — shipping now

body
`;

  const cases: Array<[string, string[]]> = [
    ["a version that is not a version", ["stamp", "nope"]],
    ["a partial version", ["stamp", "0.0"]],
    ["no version at all", ["stamp"]],
    ["an empty version", ["stamp", ""]],
    ["an unknown command", ["publish", "0.0.249"]],
    ["no command", []],
    ["extract with a version that is not a version", ["extract", "nope"]],
    ["a stray extra argument", ["stamp", "0.0.249", "0.0.250"]],
  ];

  for (const [name, args] of cases) {
    it(`exits 1 on ${name}`, () => {
      const { file } = workspace(NOTES);
      const result = run(file, ...args);
      expect(result.code).toBe(1);
      expect(result.stderr).not.toBe("");
      expect(readFileSync(file, "utf8")).toBe(NOTES);
    });
  }
});
