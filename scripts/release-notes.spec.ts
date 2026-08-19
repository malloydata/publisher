// Tests for scripts/release-notes.mjs, the release workflow's narrative step.
//
// This script is exercised exactly once per release, on `main`, after npm and
// Docker have already published — so a bug in it is discovered by reading a
// wrong release page, and the cheapest fix is a commit that lands after the
// damage. Two of the cases below are bugs that reached review: a bare
// `## [Unreleased]` heading put `## ## [Unreleased]` onto the public page, and
// `extract`/`stamp` read different trees, so a section merged during the release
// window was stamped with a version it never shipped in and could never appear
// on a later page.
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

/** A throwaway RELEASE_NOTES.md, plus a path for the titles artifact. */
function workspace(notes: string) {
  const dir = mkdtempSync(path.join(tmpdir(), "release-notes-"));
  workspaces.push(dir);
  const file = path.join(dir, "RELEASE_NOTES.md");
  writeFileSync(file, notes);
  return { file, titles: path.join(dir, "titles.txt") };
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

  it("records the exact heading lines it consumed", () => {
    const { file, titles } = workspace(`${HEADER}
## [Unreleased] — first

a

---

## [Unreleased]: second

b
`);

    const { code } = run(file, "extract", "--titles", titles);
    expect(code).toBe(0);
    expect(readFileSync(titles, "utf8")).toBe(
      "## [Unreleased] — first\n## [Unreleased]: second\n",
    );
  });

  it("writes an empty titles file when there is no narrative", () => {
    // Not skipping the write: an empty list is a real answer, and a stamp that
    // then finds sections must leave them alone.
    const { file, titles } = workspace(`${HEADER}
## [0.0.248] — already shipped

body
`);

    expect(run(file, "extract", "--titles", titles).code).toBe(0);
    expect(readFileSync(titles, "utf8")).toBe("");
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

describe("stamp --titles", () => {
  // The window bug, in the shape that exposed it. `extract` runs against the
  // release branch's snapshot; `stamp` runs against whatever `main` became
  // minutes later. Recent releases take 4-10 minutes, so a PR merging inside
  // that window is ordinary.
  const SNAPSHOT = `${HEADER}
## [Unreleased] — feature A in this release

A body
`;

  const MAIN_LATER = `${HEADER}
## [Unreleased] — feature B merged mid-release

B body

---

## [Unreleased] — feature A in this release

A body
`;

  it("stamps only the sections that reached the release page", () => {
    const { file: snapshot, titles } = workspace(SNAPSHOT);
    const { file: main } = workspace(MAIN_LATER);

    const extracted = run(snapshot, "extract", "--titles", titles);
    expect(extracted.code).toBe(0);
    expect(extracted.stdout).toContain("## feature A in this release");
    expect(extracted.stdout).not.toContain("feature B");

    const stamped = run(main, "stamp", "0.0.249", "--titles", titles);
    expect(stamped.code).toBe(0);
    expect(stamped.stdout).toBe("1\n");

    const after = readFileSync(main, "utf8");
    // A shipped, so it is history now.
    expect(after).toContain("## [0.0.249] — feature A in this release");
    // B did not, so it must still be claimable by the next release.
    expect(after).toContain("## [Unreleased] — feature B merged mid-release");
  });

  it("without --titles, stamps the mid-release section too (the bug)", () => {
    // The control for the case above: this is what the workflow used to do, and
    // it is why the titles artifact exists. B is stamped 0.0.249 without ever
    // appearing on that release's page, and no later release can find it.
    const { file: main } = workspace(MAIN_LATER);

    expect(run(main, "stamp", "0.0.249").stdout).toBe("2\n");
    const after = readFileSync(main, "utf8");
    expect(after).toContain("## [0.0.249] — feature B merged mid-release");
    expect(after).not.toContain("[Unreleased]");
  });

  it("stamps nothing when the titles file is empty", () => {
    const { file: snapshot, titles } = workspace(`${HEADER}
## [0.0.248] — already shipped

body
`);
    const { file: main } = workspace(MAIN_LATER);
    const before = readFileSync(main, "utf8");

    expect(run(snapshot, "extract", "--titles", titles).code).toBe(0);

    const stamped = run(main, "stamp", "0.0.249", "--titles", titles);
    expect(stamped.code).toBe(0);
    expect(stamped.stdout).toBe("0\n");
    expect(readFileSync(main, "utf8")).toBe(before);
  });

  it("reports a recorded section that is no longer in the file", () => {
    const { file: snapshot, titles } = workspace(`${HEADER}
## [Unreleased] — edited during the release

body
`);
    // Someone reworded the heading after the release page was built.
    const { file: main } = workspace(`${HEADER}
## [Unreleased] — edited during the release, then reworded

body
`);

    expect(run(snapshot, "extract", "--titles", titles).code).toBe(0);

    const stamped = run(main, "stamp", "0.0.249", "--titles", titles);
    expect(stamped.code).toBe(0);
    expect(stamped.stdout).toBe("0\n");
    expect(stamped.stderr).toContain("no longer in");
    expect(stamped.stderr).toContain("edited during the release");
    // Reported, not corrected: the file is left as the author left it.
    expect(readFileSync(main, "utf8")).toContain("[Unreleased]");
  });

  it("fails loudly when the titles file is missing", () => {
    const { file, titles } = workspace(`${HEADER}
## [Unreleased] — shipping now

body
`);
    const before = readFileSync(file, "utf8");

    const stamped = run(file, "stamp", "0.0.249", "--titles", titles);
    expect(stamped.code).toBe(1);
    expect(stamped.stderr).toContain("could not read the titles file");
    expect(readFileSync(file, "utf8")).toBe(before);
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
    ["--titles with no path", ["extract", "--titles"]],
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
